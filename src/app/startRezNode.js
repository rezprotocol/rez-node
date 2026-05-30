import { WsGatewayServer } from "../ws/WsGatewayServer.js";
import { NodeMetrics } from "../metrics/NodeMetrics.js";
import { FsStorageProvider } from "../storage/fs/FsStorageProvider.js";
import { validateConfig } from "./NodeConfigValidator.js";
import { ensureNodeIdentity } from "../identity/NodeIdentity.js";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { createProtocolFactory } from "../protocol/createProtocolFactory.js";
import { createDepositHandler, createRelayOnlyDepositHandler } from "../protocol/DepositHandler.js";
import { createNodeRuntime } from "./createNodeRuntime.js";
import { createRelayRuntime } from "./createRelayRuntime.js";
import { buildSignedRelayDescriptorJson } from "../relay/PeerAuthShared.js";
import { bootstrapRelayInfrastructure } from "./bootstrapRelay.js";
import { bootstrapNodeInfrastructure } from "./bootstrapNode.js";
import { InboxClaimRegistry } from "../inbox/InboxClaimRegistry.js";
import { DepositPolicyStore } from "../inbox/DepositPolicyStore.js";
import { DepositRateLimitStore } from "../inbox/DepositRateLimitStore.js";
import { REZ_CONTRACT_TYPES, CONTRACT_VERSION } from "@rezprotocol/core";
import { randomUUID } from "node:crypto";

export async function startRezNode(config) {
  const resolved = validateConfig(config);
  const nodeEnabled = resolved.node.enabled !== false;
  const relayEnabled = resolved.relay.enabled !== false;
  const metrics = new NodeMetrics();
  const storageProvider = new FsStorageProvider({ rootDir: resolved.storage.dataDir });
  const configuredIdentity = resolved.node && resolved.node.identity ? resolved.node.identity : undefined;
  const stableIdentity = await ensureNodeIdentity({
    storageProvider,
    configuredIdentity,
  });
  // The relayKeyId is the routing-layer address for this node in the
  // mesh. Production callers (SDK delegation signing) need it via
  // getIdentity(); putting it on stableIdentity keeps the single source
  // of truth — the same expression bootstrapRelay uses below.
  stableIdentity.relayKeyId = (resolved.relay && resolved.relay.relayKeyId)
    ? resolved.relay.relayKeyId
    : ("node-" + stableIdentity.deviceId);

  // Derive storage encryption key from node identity and recreate provider
  const crypto = new NodeCryptoProvider();
  const privBytes = new Uint8Array(Buffer.from(stableIdentity.nodePrivateKeyB64, "base64"));
  const hkdfSalt = new TextEncoder().encode("rez:storage:encryption:v1");
  const hkdfInfo = new TextEncoder().encode("rez:kv:aes256gcm");
  const storageEncKey = crypto.hkdfSha256(privBytes, { salt: hkdfSalt, info: hkdfInfo, length: 32 });
  const encryptedStorageProvider = new FsStorageProvider({ rootDir: resolved.storage.dataDir, encryptionKey: storageEncKey });

  // --- Relay layer ---
  let relay = null;
  if (relayEnabled) {
    relay = await bootstrapRelayInfrastructure({
      resolved,
      stableIdentity,
      storageProvider: encryptedStorageProvider,
      metrics,
      nodeEnabled,
    });
  }

  // --- Node layer ---
  let node = null;
  if (nodeEnabled) {
    node = await bootstrapNodeInfrastructure({
      resolved,
      stableIdentity,
      storageProvider: encryptedStorageProvider,
    });
  }

  // --- Inbox claim registry (open registration; trust root for owner-scoped ops) ---
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider: encryptedStorageProvider });
  await inboxClaimRegistry.hydrate();

  // --- Deposit policy store (claimant-signed per-inbox blocklist/allowlist;
  // see docs/SECURITY_AUDIT.md HIGH-1). Default-allow when an inbox has no
  // stored policy. ---
  const depositPolicyStore = new DepositPolicyStore({ storageProvider: encryptedStorageProvider });
  await depositPolicyStore.hydrate();

  // --- Deposit rate-limit store (per-(depositor, inbox) sliding window;
  // persisted so restarts don't reset attacker counters). ---
  const depositRateLimitStore = new DepositRateLimitStore({ storageProvider: encryptedStorageProvider });
  await depositRateLimitStore.hydrate();

  // --- Runtime ---
  const relayStore = relay ? relay.relayStore : null;
  const inboxStore = relay ? relay.inboxStore : null;
  const inboxRouter = relay ? relay.inboxRouter : null;
  const routeTable = relay ? relay.routeTable : null;
  const hostedInboxRegistry = relay ? relay.hostedInboxRegistry : null;
  const meshCoordinator = relay ? relay.meshCoordinator : null;
  const gatewayLoop = relay ? relay.gatewayLoop : null;
  const outboundQueue = relay ? relay.outboundQueue : null;
  const retryScheduler = relay ? relay.retryScheduler : null;

  const runtime = nodeEnabled
    ? createNodeRuntime({
        relayStore,
        inboxStore,
        identity: stableIdentity,
        serverServices: node ? node.serverServices : null,
        serviceCache: node ? node.serviceCache : null,
        storageProvider: encryptedStorageProvider,
        metrics,
        meshCoordinator,
        meshConfig: resolved.mesh,
        gatewayLoop,

        groupLookupClass: resolved.node.groupLookupClass,
        inboxRouter,
        hostedInboxRegistry,
        inboxClaimRegistry,
        depositPolicyStore,
        depositRateLimitStore,
      })
    : createRelayRuntime({
        relayStore,
        inboxStore,
        identity: stableIdentity,
        metrics,
        meshCoordinator,
        meshConfig: resolved.mesh,
        gatewayLoop,

        inboxRouter,
        hostedInboxRegistry,
        inboxClaimRegistry,
        depositPolicyStore,
        depositRateLimitStore,
      });
  runtime.participateInRouting = resolved.mesh.participateInRouting;
  runtime.settlement = relay ? relay.settlement : null;
  runtime.attestationService = relay ? relay.attestationService : null;
  runtime.reputationScorer = relay ? relay.reputationScorer : null;
  runtime.handleRegistry = relay ? relay.handleRegistry : null;
  runtime.handleExchange = relay ? relay.handleExchange : null;

  // --- WS Gateway ---
  let gateway = null;
  if (resolved.ws) {
    const protocolFactory = resolved.node.protocolFactory || createProtocolFactory({ nodeEnabled });
    const onInboundDeposit = resolved.node.onInboundDeposit
      || (nodeEnabled
        ? createDepositHandler({ crypto: new NodeCryptoProvider() })
        : createRelayOnlyDepositHandler());

    gateway = new WsGatewayServer({
      runtime,
      host: resolved.ws.host,
      port: resolved.ws.port,
      path: resolved.ws.path,
      metrics,
      protocolFactory,
      onInboundDeposit,
      storageProvider: nodeEnabled ? encryptedStorageProvider : null,
      nodeEnabled,
    });

    // Wire PersistentOutboundQueue status notifications out through the WS
    // gateway. Routed per-owner via the entry's ownerPublicKeyB64 so that
    // in a multi-tenant node, tenant A never sees tenant B's queue events.
    // Entries persisted before the ownerPublicKeyB64 field was added will
    // have a null owner and silently skip the broadcast — their retries
    // still work; only the status notification is degraded.
    if (outboundQueue && typeof outboundQueue.setOnStatusChange === "function") {
      const sessionRegistry = gateway.getSessionRegistry();
      outboundQueue.setOnStatusChange((queueId, status, entry) => {
        if (!sessionRegistry || typeof sessionRegistry.broadcastToOwner !== "function") return;
        const owner = entry && typeof entry.ownerPublicKeyB64 === "string"
          ? entry.ownerPublicKeyB64.trim()
          : "";
        if (!owner) return;
        const deliverInboxId = entry && typeof entry.deliverInboxId === "string"
          ? entry.deliverInboxId
          : "";
        const frame = {
          id: REZ_CONTRACT_TYPES.EVT_OUTBOUND_STATUS + ":" + Date.now() + ":" + randomUUID(),
          t: REZ_CONTRACT_TYPES.EVT_OUTBOUND_STATUS,
          v: CONTRACT_VERSION,
          body: {
            queueId,
            deliverInboxId,
            status,
            attemptedAtMs: Date.now(),
          },
        };
        sessionRegistry.broadcastToOwner(owner, frame);
      });
    }
  }

  // --- Relay-layer components (for start sequence) ---
  const relayRuntime = relay ? relay.relayRuntime : null;
  const relayTransport = relay ? relay.relayTransport : null;
  const relayConnectionPool = relay ? relay.relayConnectionPool : null;
  const onionKeyRotator = relay ? relay.onionKeyRotator : null;
  const selfDescriptorState = relay ? relay.selfDescriptorState : null;
  const publishPublicRelayIdentity = relay ? relay.publishPublicRelayIdentity : false;
  const getSelfDescriptorKeyRecords = relay ? relay.getSelfDescriptorKeyRecords : () => [];
  const descriptorExchange = relay ? relay.descriptorExchange : null;

  // --- Start sequence ---
  try {
    if (relayRuntime) {
      await relayRuntime.start();
      if (relayTransport && selfDescriptorState) {
        const relayAddr = relayTransport.getListenAddress();
        if (relayAddr) {
          selfDescriptorState.advertisedHost = resolved.relay.advertisedHost || relayAddr.host || "127.0.0.1";
          selfDescriptorState.endpoints = relayAddr;
          if (onionKeyRotator) {
            onionKeyRotator.start();
          }
        }
      }
      if (nodeEnabled && inboxRouter && stableIdentity.localInboxId) {
        inboxRouter.registerLocal([stableIdentity.localInboxId], null, {
          announce: publishPublicRelayIdentity,
        });
      }
      // Rehydrated hosted-inbox claimants don't fire setOnChange, so do an
      // initial sync into the routing layer at boot. Subsequent add/remove
      // calls go through HostedInboxRegistry.setOnChange.
      if (nodeEnabled && relay && typeof relay.syncHostedInboxesToRouter === "function") {
        relay.syncHostedInboxesToRouter();
      }
    }
    if (gateway) {
      await gateway.start();
    }

    // Connect to known relays
    if (relayConnectionPool) {
      const relayRecords = relay.relayStore.getAll();
      const descriptorCount = relayRecords.filter((r) => r.descriptor != null).length;
      const endpointCount = relayRecords.filter((r) => r.endpoint != null).length;
      console.log(
        "[NODE] relay pool connecting:",
        relayRecords.length,
        "records,",
        descriptorCount,
        "descriptors,",
        endpointCount,
        "with endpoints",
      );
      await relayConnectionPool.connectToKnownRelays(relayRecords);
      if (descriptorExchange) {
        descriptorExchange.announceToAllPeers();
      }
    }

    // Publish self descriptor AFTER pool connects
    if (relayTransport && selfDescriptorState) {
      const relayAddr = relayTransport.getListenAddress();
      if (relayAddr) {
        const advertisedHost = resolved.relay.advertisedHost || relayAddr.host || "127.0.0.1";
        selfDescriptorState.advertisedHost = advertisedHost;
        selfDescriptorState.endpoints = relayAddr;
        if (onionKeyRotator) {
          onionKeyRotator.start();
        }
        const keyRecords = getSelfDescriptorKeyRecords();
        const descriptor = publishSelfDescriptor({
          relayStore: relay.relayStore,
          selfDescriptorState,
          relayAddr,
          keyRecords,
        });
        if (descriptor && descriptorExchange) {
          descriptorExchange.announceSelfToAllPeers(descriptor);
          console.log(
            `[NODE] published relay descriptor: relayKeyId=${selfDescriptorState.relayKeyId} endpoint=${advertisedHost}:${selfDescriptorState.advertisedPort || relayAddr.port}`,
          );
        }
      }
      // Hook key rotation to announce updated self descriptors to peers
      if (onionKeyRotator && onionKeyRotator._onDescriptorUpdate) {
        const originalUpdate = onionKeyRotator._onDescriptorUpdate;
        onionKeyRotator._onDescriptorUpdate = (keyRecords) => {
          originalUpdate(keyRecords);
          const selfDesc = relay.relayStore.getSelfDescriptor({ nowMs: Date.now() });
          if (selfDesc && descriptorExchange) {
            descriptorExchange.announceSelfToAllPeers(
              typeof selfDesc.toJSON === "function" ? selfDesc.toJSON() : selfDesc,
            );
          }
        };
      }
    }
    if (meshCoordinator) {
      await meshCoordinator.start({ skipInitialConnect: true });
    }

    // Load persisted outbound queue and start retry scheduler
    if (outboundQueue) {
      await outboundQueue.loadAll();
    }
    if (retryScheduler) {
      retryScheduler.start();
    }
    const attestationService = relay ? relay.attestationService : null;
    if (attestationService) {
      attestationService.start();
    }

  } catch (err) {
    if (gateway) {
      await gateway.stop().catch(() => {});
    }
    if (relayConnectionPool) {
      await relayConnectionPool.close().catch(() => {});
    }
    if (relayRuntime && typeof relayRuntime.stop === "function") {
      await relayRuntime.stop().catch(() => {});
    }
    if (onionKeyRotator && typeof onionKeyRotator.stop === "function") {
      onionKeyRotator.stop();
    }
    if (retryScheduler) {
      retryScheduler.stop();
    }
    if (relay && relay.attestationService) {
      relay.attestationService.stop();
    }
    if (meshCoordinator) {
      await meshCoordinator.stop().catch(() => {});
    }
    throw err;
  }

  const relayAddr = relayTransport ? relayTransport.getListenAddress() : null;

  return {
    runtime,
    gateway,
    relayStore: relay ? relay.relayStore : null,
    storageProvider: encryptedStorageProvider,
    serverServices: node ? node.serverServices : null,
    metrics,
    config: resolved,
    relayAddress: relayAddr || null,
    async stop() {
      if (relay && relay.attestationService) {
        relay.attestationService.stop();
      }
      if (retryScheduler) {
        retryScheduler.stop();
      }
      if (onionKeyRotator && typeof onionKeyRotator.stop === "function") {
        onionKeyRotator.stop();
      }
      if (relayConnectionPool) {
        await relayConnectionPool.close().catch(() => {});
      }
      if (gateway) {
        await gateway.stop();
      }
      if (relayRuntime && typeof relayRuntime.stop === "function") {
        await relayRuntime.stop();
      }
      if (typeof runtime.stop === "function") {
        await runtime.stop();
      }
    },
  };
}

function publishSelfDescriptor({ relayStore, selfDescriptorState, relayAddr, keyRecords }) {
  if (!relayStore || !selfDescriptorState || !relayAddr || !Array.isArray(keyRecords) || keyRecords.length === 0) {
    return null;
  }
  const nowMs = Date.now();
  const descriptor = buildSignedRelayDescriptorJson({
    relayKeyId: selfDescriptorState.relayKeyId,
    advertisedHost: selfDescriptorState.advertisedHost,
    relayPort: selfDescriptorState.advertisedPort || relayAddr.port,
    tlsEnabled: selfDescriptorState.tlsEnabled === true,
    keyRecords,
    nodeKeyId: selfDescriptorState.nodeKeyId,
    nodePublicKeyB64: selfDescriptorState.nodePublicKeyB64,
    nodePrivateKeyB64: selfDescriptorState.nodePrivateKeyB64,
    nowMs,
  });
  if (!descriptor) {
    return null;
  }
  relayStore.upsertDescriptor(descriptor, { source: "self", receivedAtMs: nowMs });
  return descriptor;
}
