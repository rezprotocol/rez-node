import { RelayStore } from "../network/RelayStore.js";
import path from "node:path";
import { RMailbox, FileSystemDataStore, createDefaultRegistry, validateRelayDescriptorV1 } from "@rezprotocol/core";
import { bootstrapNodeRelay } from "../relay/NodeRelayBootstrap.js";
import { bootstrapMesh } from "../gateway/MeshBootstrap.js";
import { DescriptorExchange } from "../relay/DescriptorExchange.js";
import { HostedInboxRegistry } from "./HostedInboxRegistry.js";
import { ReceiptSigner } from "../settlement/ReceiptSigner.js";
import { LocalSettlementProvider } from "../settlement/LocalSettlementProvider.js";
import { PgSettlementProvider } from "../settlement/PgSettlementProvider.js";
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
import { PgDurableInbox } from "../storage/pg/PgDurableInbox.js";
import { PgAccountDeviceRegistry } from "../storage/pg/PgAccountDeviceRegistry.js";
import { PgAccountMutationSerializer } from "../storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceBundleStore } from "../storage/pg/PgAccountDeviceBundleStore.js";
import { AccountAuthorityRevocationCache } from "../protocol/AccountAuthorityRevocationCache.js";
import { DurableHomeInboxStore } from "../storage/DurableHomeInboxStore.js";
import { assertMultiDeviceFanoutReady } from "./deviceFanoutReadiness.js";

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
  inboxClaimRegistry = null,
}) {
  // The release-blocker interlock, enforced at THIS public construction boundary
  // (audit R4 L2c review P1): bootstrapRelayInfrastructure is a package-root export,
  // so a hand-built `resolved` with maxDevices>1 would otherwise open fan-out without
  // going through validateConfig. Fail loud FIRST, before any I/O or construction.
  const resolvedMaxDevices = resolved.device && Number.isFinite(resolved.device.maxDevices)
    ? resolved.device.maxDevices
    : 1;
  assertMultiDeviceFanoutReady(resolvedMaxDevices > 1);

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
  // The transient WAN relay buffer is deliberately NODE-LOCAL filesystem state
  // (WAN-egress hand-off, not the home of record), so it is rooted at the node's
  // data dir directly — independent of the shared storage backend (fs|pg). A pg
  // cluster still keeps this transient buffer on local disk per node.
  const relayLocalDataDir = resolved.storage && resolved.storage.dataDir ? resolved.storage.dataDir : null;
  if (!relayLocalDataDir) {
    throw new Error("bootstrapRelayInfrastructure requires resolved.storage.dataDir for node-local relay storage");
  }
  const inboxStoreBasePath = path.join(relayLocalDataDir, "relay-inbox");
  const transientInboxStore = new RMailbox({
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

  // S2 — on a pg cluster node, owner-hosted inboxes are durable (system of
  // record, drainable from ANY node on reconnect). Wrap the transient RMailbox
  // in a DurableHomeInboxStore that routes per-inbox: claimed-here -> the Pg
  // durable append-log (seq + per-device cursors); everything else -> the
  // transient WAN buffer verbatim (D1). The decorator is the inboxStore EVERY
  // ingress path and the WS notify hook use, so the unified persist-then-notify
  // model needs no hot-path edits. On fs/desktop, durableInbox stays null ⇒ the
  // session capability is off and the delivery path is byte-for-byte unchanged.
  let durableInbox = null;
  let isHostedHere = null;
  let inboxStore = transientInboxStore;
  // S2.5 S11: the account→device registry (canonical device set) + the serialized
  // mutation authority. Only on a pg cluster node (they need the shared Pg); null
  // on fs/desktop so the account.deviceMutation / account.authorityState handlers
  // answer SERVICE_UNAVAILABLE and every verifier's revocationState stays null.
  let accountDeviceRegistry = null;
  let accountMutationSerializer = null;
  // Bounded-staleness cache over the home authority-state, feeding the verify hot
  // paths (session-auth revocationState). Null on fs/desktop ⇒ revocationState
  // stays null (byte-identical primary path).
  let accountAuthorityRevocationCache = null;
  // S2.5 S12: home-aggregated per-device prekey bundle store (multi-device fan-out).
  // Null on fs/desktop ⇒ the bundle publish / device-set handlers answer
  // SERVICE_UNAVAILABLE and the published device set stays single-device.
  let accountDeviceBundleStore = null;
  // E6 multi-device gate: open iff the per-inbox device cap is > 1. Advertised to
  // the client (session.ready) so it knows a proven device.bind is REQUIRED for a
  // cursor (the claim path no-ops the cursor when open). Defaults closed.
  let multiDeviceFanout = false;
  if (
    resolved.storage.backend === "pg"
    && storageProvider
    && storageProvider.connection
    && inboxClaimRegistry
  ) {
    multiDeviceFanout = resolvedMaxDevices > 1;
    durableInbox = new PgDurableInbox({
      connection: storageProvider.connection,
      // S2.5 E6 fan-out gate: the per-inbox device cap is config-driven and defaults
      // to 1 (single active device — the shipped behaviour). It only rises when
      // node.device.multiDeviceFanout is enabled AND every fan-out release blocker is
      // met (S12 suite + audit-R4 F2 legacy-cursor migration + F3 admission control);
      // deviceFanoutReadiness.js is the SSOT and the assert above fails loud otherwise.
      // A 2nd distinct device is refused at registration while the gate is closed.
      maxDevices: resolvedMaxDevices,
      // Preserve the same per-inbox DoS caps the transient buffer enforces —
      // removing delete-after-delivery re-opens unbounded growth without them.
      maxEvents: MAX_BUFFERED_ITEMS_PER_INBOX,
      maxBytes: MAX_BUFFERED_BYTES_PER_INBOX,
    });
    // An inbox's durable home is THIS cluster iff it is claimed in the shared Pg
    // claim registry (authoritative + fresh across nodes — the non-sticky-LB
    // property). A transiently-buffered WAN inbox is not claimed here.
    isHostedHere = (id) => inboxClaimRegistry.hasInbox(id);
    inboxStore = new DurableHomeInboxStore({ rmailbox: transientInboxStore, durableInbox, isHostedHere });
    accountDeviceRegistry = new PgAccountDeviceRegistry({ connection: storageProvider.connection, durableInbox });
    // Audit R4 F5a: the serializer composes the registry's canonical fold InTx
    // methods (no hand-mirrored SQL). Inject the SAME registry instance so there is
    // one device-invariant owner in the process.
    // The serializer self-constructs its PgPropagationOutbox from this same connection and
    // exposes it via .propagationOutbox; the runtime derives the wire-lease outbox from there
    // (audit leaf-3b F5), so the fold and the lease surface are guaranteed the same instance.
    accountMutationSerializer = new PgAccountMutationSerializer({ connection: storageProvider.connection, durableInbox, registry: accountDeviceRegistry });
    accountAuthorityRevocationCache = new AccountAuthorityRevocationCache({ serializer: accountMutationSerializer });
    accountDeviceBundleStore = new PgAccountDeviceBundleStore({ connection: storageProvider.connection });
  }

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
    const durableRecordsBasePath = path.join(relayLocalDataDir, "relay-durable-records");
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
      // pg: the atomic Pg settlement provider (FOR-UPDATE-free guarded debit +
      // idempotency), safe for one wallet shared across the cluster. fs: the
      // KV-backed single-process provider.
      const settlementProvider = resolved.storage.backend === "pg"
        ? new PgSettlementProvider({
            connection: storageProvider.connection,
            receiptSigner,
            networkId: resolved.relay.pricing.networkId,
          })
        : new LocalSettlementProvider({ kvStore, receiptSigner, networkId: resolved.relay.pricing.networkId });
      settlement = {
        provider: settlementProvider,
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
    durableInbox,
    accountDeviceRegistry,
    accountMutationSerializer,
    accountAuthorityRevocationCache,
    accountDeviceBundleStore,
    multiDeviceFanout,
    isHostedHere,
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
