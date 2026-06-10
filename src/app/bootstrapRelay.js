import { RelayStore } from "../network/RelayStore.js";
import path from "node:path";
import { RMailbox, FileSystemDataStore, createDefaultRegistry, validateRelayDescriptorV1 } from "@rezprotocol/core";
import { bootstrapNodeRelay } from "../relay/NodeRelayBootstrap.js";
import { bootstrapMesh } from "../gateway/MeshBootstrap.js";
import { DescriptorExchange } from "../relay/DescriptorExchange.js";
import { HostedInboxRegistry } from "./HostedInboxRegistry.js";
import { ReceiptSigner } from "../settlement/ReceiptSigner.js";
import { LocalSettlementProvider } from "../settlement/LocalSettlementProvider.js";
import { ConfigPricingResolver } from "../settlement/ConfigPricingResolver.js";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { PeerAttestationService } from "../settlement/PeerAttestationService.js";
import { ReputationScorer } from "../settlement/ReputationScorer.js";
import { MAX_BUFFERED_ITEMS_PER_INBOX, MAX_BUFFERED_BYTES_PER_INBOX } from "../relay/InboxRouter.js";
import { AttestationExchange } from "../settlement/AttestationExchange.js";
import { ChallengeResponseVerifier } from "../settlement/ChallengeResponseVerifier.js";
import { StorageVerificationExchange } from "../settlement/StorageVerificationExchange.js";
import { RouteTable } from "../routing/RouteTable.js";
import { GossipRouteResolver } from "../routing/GossipRouteResolver.js";
import { DhtNode } from "../routing/dht/DhtNode.js";
import { DurableRecordPersistence } from "../routing/dht/DurableRecordPersistence.js";
import { encodeControlMessage, sendControlMessage } from "../network/tcp/TcpFraming.js";
import { HandleRegistry } from "../handle/HandleRegistry.js";
import { HandleExchange } from "../handle/HandleExchange.js";

/**
 * Bootstrap relay infrastructure: relay store, inbox store,
 * transport, InboxRouter, RelayRuntime, connection pool, descriptor exchange,
 * directory server. Called when relay.enabled = true.
 *
 * Does NOT start anything — caller is responsible for start order.
 *
 * @param {object} opts
 * @param {object} opts.resolved - Validated config from NodeConfigValidator
 * @param {object} opts.stableIdentity - Node identity from ensureNodeIdentity
 * @param {object} opts.storageProvider - FsStorageProvider instance
 * @param {object} opts.metrics - NodeMetrics instance
 * @param {boolean} opts.nodeEnabled - Whether node-level services are enabled
 * @returns {object} relay infrastructure components
 */
export async function bootstrapRelayInfrastructure({
  resolved,
  stableIdentity,
  storageProvider,
  metrics,
  nodeEnabled,
}) {
  const publishPublicRelayIdentity =
    typeof resolved.relay === "object"
    && resolved.relay !== null
    && typeof resolved.relay.advertisedHost === "string"
    && resolved.relay.advertisedHost.trim().length > 0;

  const relayStore = new RelayStore({ metrics, storageProvider });
  relayStore.load(resolved.network.knownRelays);
  await relayStore.hydratePersistentDescriptors();

  // Persistent on-disk inbox store so deposits for offline owners survive
  // relay restart. Rooted under the node's data dir alongside the rest of
  // the relay's persistent state. Without persistence, a relay process
  // restart silently drops every queued offline deposit.
  const inboxStoreBasePath = storageProvider && storageProvider.rootDir
    ? path.join(storageProvider.rootDir, "relay-inbox")
    : null;
  if (!inboxStoreBasePath) {
    throw new Error("bootstrapRelayInfrastructure requires storageProvider with rootDir for persistent inbox storage");
  }
  const inboxStore = new RMailbox({
    store: new FileSystemDataStore({ basePath: inboxStoreBasePath }),
    registry: createDefaultRegistry(),
    // Per-inbox DoS guard: cap how many deposits one mailbox can buffer. The
    // InboxRouter offline-buffered path already checked this, but the other
    // ingress paths (RelayConnectionPool, RelayRuntime, NodeDeliveryAdapter,
    // RoutingEngine, RelayIngressClient, SocketFrameRouter) call depositFromWire
    // directly and bypassed it — that let one account accumulate ~33K deposits
    // (16 GB). Enforcing in the store covers every path. Same constant the
    // InboxRouter pre-check uses, so the two cannot drift.
    maxItems: MAX_BUFFERED_ITEMS_PER_INBOX,
    // Companion byte cap: the item cap bounds file count, not size. Without it,
    // 10K items at the max frame size could still fill the disk.
    maxBytes: MAX_BUFFERED_BYTES_PER_INBOX,
  });

  const hostedInboxRegistry = new HostedInboxRegistry({ storageProvider });
  await hostedInboxRegistry.hydrate();

  const selfDescriptorState =
    resolved.relay && publishPublicRelayIdentity
      ? {
          relayStore,
          relayKeyId: stableIdentity.relayKeyId,
          nodeKeyId: stableIdentity.nodeKeyId,
          nodePublicKeyB64: stableIdentity.nodePublicKeyB64,
          nodePrivateKeyB64: stableIdentity.nodePrivateKeyB64,
          advertisedHost: null,
          advertisedPort: resolved.relay.advertisedPort || null,
          endpoints: null,
          tlsEnabled: (resolved.relay && resolved.relay.tls && resolved.relay.tls.enabled === true)
            || resolved.relay.advertisedTls === true,
        }
      : null;

  const relayBootstrapResult = bootstrapNodeRelay({
    relayConfig: resolved.relay,
    identity: stableIdentity,
    inboxStore,
    relayStore,
    getInboxIds: () => hostedInboxRegistry.getInboxIds(),
    getRegistrations: () => hostedInboxRegistry.getRegistrations(),
    maxPeers: resolved.mesh.maxPeers || 32,
    selfDescriptorState,
  });

  const inboxRouter = relayBootstrapResult ? relayBootstrapResult.inboxRouter : null;
  const routeTable = inboxRouter ? inboxRouter.routeTable : new RouteTable();
  const relayRuntime = relayBootstrapResult ? relayBootstrapResult.relayRuntime : null;
  const relayTransport = relayBootstrapResult ? relayBootstrapResult.transport : null;
  const relayConnectionPool = relayBootstrapResult ? relayBootstrapResult.connectionPool : null;
  const onionKeyRotator = relayBootstrapResult ? relayBootstrapResult.onionKeyRotator : null;
  const bridge = relayBootstrapResult ? relayBootstrapResult.bridge : null;
  const frameRouter = relayBootstrapResult ? relayBootstrapResult.frameRouter : null;
  const controlMessageRegistry = relayBootstrapResult ? relayBootstrapResult.controlMessageRegistry : null;
  const relayPeerDirectory = relayBootstrapResult ? relayBootstrapResult.relayPeerDirectory : null;
  const getSelfDescriptorKeyRecords = relayBootstrapResult
    ? relayBootstrapResult.getSelfDescriptorKeyRecords
    : () => [];

  // --- DHT routing (default strategy) ---
  const routingStrategy = (resolved.mesh && resolved.mesh.routingStrategy) || "dht";
  let dhtNode = null;
  let routeResolver = null;

  if (routingStrategy === "dht" && relayBootstrapResult && controlMessageRegistry) {
    const relayKeyId = stableIdentity.relayKeyId;

    dhtNode = new DhtNode({
      selfRelayKeyId: relayKeyId,
      controlMessageRegistry,
      encodeCtl: function (obj) { return encodeControlMessage(obj); },
      trySendFrame: function (socket, bytes) {
        if (!socket || socket.destroyed === true) return;
        try {
          socket.write(bytes);
        } catch (err) {
          console.error("[DHT] trySendFrame failed:", err && err.message ? err.message : err);
        }
      },
      fallbackResolver: new GossipRouteResolver(),
      // LOW-6: key the per-peer dht.store rate limiter on the peer's
      // authenticated relayKeyId (stable across socket reconnects). Both
      // relay-verified and relay-provisional sockets reach `#handleStore`,
      // so `getAuth(socket).relayKeyId` covers both. Falls back to null
      // → DhtProtocol uses socket.id, which is harmless in tests but
      // shouldn't occur in production (SocketFrameRouter gates
      // isAuthenticatedRoutingSocket before dispatch).
      getPeerKey: relayPeerDirectory
        ? function (socket) {
            const auth = relayPeerDirectory.getAuth(socket);
            if (!auth || typeof auth.relayKeyId !== "string" || auth.relayKeyId.length === 0) return null;
            return auth.relayKeyId;
          }
        : null,
      // SECURITY_AUDIT MED-13/14: outer per-IP cap (with /64 aggregation
      // for IPv6) above the per-relayKeyId limiter. peerIpKey covers the
      // truncation; the limiter is constructed by DhtProtocol with the
      // 5000-store/min default.
      getPeerIp: function (socket) {
        if (!socket || typeof socket.remoteAddress !== "string") return null;
        return socket.remoteAddress;
      },
      config: {
        k: 20,
        alpha: 3,
        queryTimeoutMs: 3000,
        valueTtlMs: 86_400_000,
        republishIntervalMs: 3_600_000,
      },
    });
    dhtNode.install();
    routeResolver = dhtNode.routeResolver;

    // Durable signed-record persistence so held records survive relay
    // restart (mirrors the relay-inbox store). Loaded before the node serves.
    const durableRecordsBasePath = path.join(storageProvider.rootDir, "relay-durable-records");
    dhtNode.setRecordPersistence(new DurableRecordPersistence({
      store: new FileSystemDataStore({ basePath: durableRecordsBasePath }),
    }));
    await dhtNode.loadPersistedRecords();

    // Wire InboxRouter to use DHT announcer
    if (inboxRouter) {
      inboxRouter.setRouteAnnouncer(dhtNode.routeAnnouncer);
    }

    // Wire peer lifecycle → DHT k-bucket maintenance
    if (inboxRouter) {
      inboxRouter._onPeerAdded = function (peerRelayKeyId, socket) {
        dhtNode.addPeer(peerRelayKeyId, socket);
      };
      inboxRouter._onPeerRemoved = function (socket) {
        dhtNode.removePeerBySocket(socket);
      };
    }

    console.log("[NODE] DHT routing enabled (relayKeyId=" + relayKeyId + ")");
  }

  // Pull hosted-inbox claimant delegations through the routing layer.
  // HostedInboxRegistry only persists the "this node hosts inbox X for
  // claimant Y" mapping; for that hosted inbox to be findable by the rest
  // of the mesh we also have to put it into the InboxRouter's route table
  // (so the DHT route announcer STOREs it on k-closest peers, and so the
  // anti-entropy republish keeps it alive). Without this step the hosted
  // inbox is invisible to DHT FIND_VALUE — it only reaches the relays we
  // happen to be directly TCP-peered with at this instant, via inbox.register.
  const syncHostedInboxesToRouter = () => {
    if (!inboxRouter) return;
    const registrations = hostedInboxRegistry.getRegistrations();
    for (const reg of registrations) {
      inboxRouter.registerLocal([reg.inboxId], null, {
        announce: true,
        registrations: [reg],
      });
    }
  };

  if (relayConnectionPool) {
    hostedInboxRegistry.setOnChange(() => {
      syncHostedInboxesToRouter();
      relayConnectionPool.updateInboxIds().catch((err) => {
        const msg = err && err.message ? err.message : "";
        console.warn("[NODE] hosted inbox re-register failed", msg);
      });
    });
  }

  const meshBootstrapResult = bootstrapMesh({
    meshConfig: resolved.mesh,
    relayStore,
    metrics,
    identity: stableIdentity,
    relayConnectionPool,
    routeTable,
    inboxRouter,
    inboxStore,
    keyValueStore: storageProvider ? storageProvider.keyValueStore : null,
    routeResolver,
  });

  const meshCoordinator = meshBootstrapResult ? meshBootstrapResult.meshCoordinator : null;
  const gatewayLoop = meshBootstrapResult ? meshBootstrapResult.gatewayLoop : null;

  // Drive durable-record re-replication + eviction off the existing mesh
  // sync tick (the same ~30s churn cadence that republishes routes) rather
  // than spinning up a separate timer.
  if (meshCoordinator && dhtNode && typeof meshCoordinator.setOnSyncTick === "function") {
    meshCoordinator.setOnSyncTick((nowMs) => {
      dhtNode.republishHeldRecords(nowMs);
      dhtNode.evictExpiredRecords(nowMs);
    });
  }
  const outboundQueue = meshBootstrapResult ? meshBootstrapResult.outboundQueue : null;
  const retryScheduler = meshBootstrapResult ? meshBootstrapResult.retryScheduler : null;

  // --- TCP descriptor exchange ---
  const descriptorExchange = new DescriptorExchange({
    relayStore,
    validateDescriptor: validateRelayDescriptorV1,
    maxPeers: resolved.mesh.maxPeers || 32,
    onDescriptorsAccepted: () => {
      if (meshCoordinator && typeof meshCoordinator.connectNewPeers === "function") {
        meshCoordinator.connectNewPeers();
      }
      if (meshCoordinator && typeof meshCoordinator.refreshSeedReachabilityFromStore === "function") {
        meshCoordinator.refreshSeedReachabilityFromStore();
      }
    },
  });
  if (frameRouter) {
    frameRouter.setDescriptorExchange(descriptorExchange);
  }
  if (relayConnectionPool) {
    relayConnectionPool.setDescriptorExchange(descriptorExchange);
  }
  if (meshCoordinator && typeof meshCoordinator.setDescriptorExchange === "function") {
    meshCoordinator.setDescriptorExchange(descriptorExchange);
  }

  // --- Bridge wiring: relay ↔ gateway outbound routing ---
  if (bridge && gatewayLoop && typeof gatewayLoop.sendToInbox === "function") {
    const meshPolicy = resolved.mesh.policy || {};
    const defaultHops = typeof meshPolicy.defaultHops === "number" ? meshPolicy.defaultHops : 1;
    const minHops = typeof meshPolicy.minHops === "number" ? meshPolicy.minHops : defaultHops;
    const maxHops = typeof meshPolicy.maxHops === "number" ? meshPolicy.maxHops : Math.max(minHops, 6);
    bridge.setReceiptSender({
      sendToInbox(opts) {
        return gatewayLoop.sendToInbox({
          ...opts,
          minHops,
          maxHops,
        });
      },
    });
    bridge.setRouteFailedCallback(({ packetId, relayKeyId, reason }) => {
      gatewayLoop.recordRouteFailure(packetId, relayKeyId, reason);
    });
  }

  // --- Settlement / pricing (when enabled) ---
  let settlement = null;
  if (resolved.relay.pricing && resolved.relay.pricing.enabled) {
    const relayKeyId = stableIdentity.relayKeyId;
    const privateKeyBytes = new Uint8Array(Buffer.from(stableIdentity.nodePrivateKeyB64, "base64"));
    const settlementCrypto = new NodeCryptoProvider();
    const receiptSigner = new ReceiptSigner({
      relayKeyId,
      signFn: async (msg) => settlementCrypto.sign({ privateKey: privateKeyBytes, msg }),
    });
    const kvStore = storageProvider ? storageProvider.keyValueStore : null;
    if (!kvStore) {
      console.warn("[NODE] pricing.enabled=true but no storage provider — settlement disabled");
    } else {
      settlement = {
        provider: new LocalSettlementProvider({ kvStore, receiptSigner }),
        pricing: new ConfigPricingResolver({ services: resolved.relay.pricing.services }),
        signer: receiptSigner,
      };
    }
  }

  // --- Relay identity signer (shared by attestation + storage verification) ---
  const relayKeyId = stableIdentity.relayKeyId;
  let relaySigner;
  if (settlement) {
    relaySigner = settlement.signer;
  } else {
    const signerPrivBytes = new Uint8Array(Buffer.from(stableIdentity.nodePrivateKeyB64, "base64"));
    const signerCrypto = new NodeCryptoProvider();
    relaySigner = new ReceiptSigner({
      relayKeyId,
      signFn: async (msg) => signerCrypto.sign({ privateKey: signerPrivBytes, msg }),
    });
  }

  // --- Attestation / reputation ---
  let attestationService = null;
  let attestationExchange = null;
  let reputationScorer = null;

  {
    attestationService = new PeerAttestationService({
      receiptSigner: relaySigner,
      selfRelayKeyId: relayKeyId,
      metrics,
    });
    attestationExchange = new AttestationExchange({ attestationService });
    reputationScorer = new ReputationScorer({ attestationService, relayStore });

    // Broadcast new attestations to peers after each cycle
    attestationService.onAttestationsProduced((produced) => {
      attestationExchange.announceToAllPeers(produced);
    });

    // Register gossip control message
    if (controlMessageRegistry) {
      attestationExchange.install(controlMessageRegistry);
    }

    // Wire peer lifecycle → attestation service (with error isolation)
    if (inboxRouter) {
      const existingOnPeerAdded = inboxRouter._onPeerAdded;
      inboxRouter._onPeerAdded = function (peerRelayKeyId, socket) {
        if (existingOnPeerAdded) {
          try { existingOnPeerAdded(peerRelayKeyId, socket); } catch (err) {
            console.error("[RELAY] onPeerAdded callback failed:", err && err.message ? err.message : err);
          }
        }
        attestationService.addPeer(peerRelayKeyId, socket);
        attestationExchange.addPeer(socket);
      };
      const existingOnPeerRemoved = inboxRouter._onPeerRemoved;
      inboxRouter._onPeerRemoved = function (socket) {
        if (existingOnPeerRemoved) {
          try { existingOnPeerRemoved(socket); } catch (err) {
            console.error("[RELAY] onPeerRemoved callback failed:", err && err.message ? err.message : err);
          }
        }
        attestationService.removePeerBySocket(socket);
        attestationExchange.removePeer(socket);
      };
    }
  }

  // --- Storage verification ---
  let storageVerifier = null;
  let storageVerificationExchange = null;

  if (storageProvider) {
    const objectStore = storageProvider.objectStore;
    if (objectStore) {
      storageVerifier = new ChallengeResponseVerifier({
        receiptSigner: relaySigner,
        selfRelayKeyId: relayKeyId,
        objectStore,
      });
      storageVerificationExchange = new StorageVerificationExchange({
        verifier: storageVerifier,
        metrics,
        onVerificationFailed: ({ targetRelayKeyId, objectId, reason }) => {
          console.warn("[NODE] Storage verification failed: relay=" + targetRelayKeyId + " object=" + objectId + " reason=" + reason);
        },
      });

      if (controlMessageRegistry) {
        storageVerificationExchange.install(controlMessageRegistry);
      }

      // Wire peer lifecycle → storage verification
      if (inboxRouter) {
        const existingOnPeerAdded2 = inboxRouter._onPeerAdded;
        inboxRouter._onPeerAdded = function (peerRelayKeyId, socket) {
          if (existingOnPeerAdded2) {
            try { existingOnPeerAdded2(peerRelayKeyId, socket); } catch (err) {
              console.error("[RELAY] onPeerAdded callback failed:", err && err.message ? err.message : err);
            }
          }
          storageVerificationExchange.addPeer(peerRelayKeyId, socket);
        };
        const existingOnPeerRemoved2 = inboxRouter._onPeerRemoved;
        inboxRouter._onPeerRemoved = function (socket) {
          if (existingOnPeerRemoved2) {
            try { existingOnPeerRemoved2(socket); } catch (err) {
              console.error("[RELAY] onPeerRemoved callback failed:", err && err.message ? err.message : err);
            }
          }
          storageVerificationExchange.removePeerBySocket(socket);
        };
      }
    }
  }

  // --- Handle registry ---
  let handleRegistry = null;
  let handleExchange = null;

  if (storageProvider) {
    const kvStore = storageProvider.keyValueStore;
    if (kvStore) {
      handleRegistry = new HandleRegistry({
        kvStore,
        receiptSigner: relaySigner,
        selfRelayKeyId: relayKeyId,
        // TRUST-5: resolve a registrar's pinned node key + verify gossiped claim
        // signatures. relaySigner signs claims with the node identity key, so the
        // pinned nodePublicKeyB64 (TRUST-7) is the correct verification key.
        relayStore,
        crypto: new NodeCryptoProvider(),
      });
      handleExchange = new HandleExchange({ handleRegistry });

      if (controlMessageRegistry) {
        handleExchange.install(controlMessageRegistry);
      }

      // Wire peer lifecycle — send all known handles to new peers
      if (inboxRouter) {
        const existingOnPeerAdded3 = inboxRouter._onPeerAdded;
        inboxRouter._onPeerAdded = function (peerRelayKeyId, socket) {
          if (existingOnPeerAdded3) {
            try { existingOnPeerAdded3(peerRelayKeyId, socket); } catch (err) {
              console.error("[RELAY] onPeerAdded callback failed:", err && err.message ? err.message : err);
            }
          }
          handleExchange.addPeer(socket);
          handleExchange.announceAllToPeer(socket).catch((err) => {
            console.error("[HANDLE] announceAllToPeer failed:", err && err.message ? err.message : err);
          });
        };
        const existingOnPeerRemoved3 = inboxRouter._onPeerRemoved;
        inboxRouter._onPeerRemoved = function (socket) {
          if (existingOnPeerRemoved3) {
            try { existingOnPeerRemoved3(socket); } catch (err) {
              console.error("[RELAY] onPeerRemoved callback failed:", err && err.message ? err.message : err);
            }
          }
          handleExchange.removePeer(socket);
        };
      }
    }
  }

  return {
    relayStore,
    inboxStore,
    hostedInboxRegistry,
    relayBootstrap: relayBootstrapResult,
    meshCoordinator,
    gatewayLoop,
    descriptorExchange,
    selfDescriptorState,
    publishPublicRelayIdentity,
    relayRuntime,
    relayTransport,
    relayConnectionPool,
    onionKeyRotator,
    bridge,
    getSelfDescriptorKeyRecords,
    inboxRouter,
    routeTable,
    syncHostedInboxesToRouter,
    outboundQueue,
    retryScheduler,
    dhtNode,
    settlement,
    attestationService,
    attestationExchange,
    reputationScorer,
    storageVerifier,
    storageVerificationExchange,
    handleRegistry,
    handleExchange,
  };
}
