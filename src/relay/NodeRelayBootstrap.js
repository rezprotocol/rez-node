import { createHash, createPrivateKey, createPublicKey } from "node:crypto";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { RelayConnectionPool } from "../network/RelayConnectionPool.js";
import { InboxRouter } from "./InboxRouter.js";
import { RelayPeerDirectory } from "./RelayPeerDirectory.js";
import { SocketFrameRouter } from "./SocketFrameRouter.js";
import { RelayRuntime } from "./RelayRuntime.js";
import { TcpRelayTransport } from "./TcpRelayTransport.js";
import { loadOnionKeyringV1 } from "./RelayKeyringLoader.js";
import { OnionKeyRotator } from "./OnionKeyRotator.js";
import { OnionKeyRecordV1 } from "@rezprotocol/core";
import { buildSignedRelayDescriptorJson } from "./PeerAuthShared.js";
import { RelayRuntimeBridge } from "./RelayRuntimeBridge.js";
import { ControlMessageRegistry } from "../routing/ControlMessageRegistry.js";

/**
 * Constructs node-as-relay components for any node with relay config.
 * Does not start anything; caller is responsible for start order and publishing self descriptor.
 * When using onionKeyRotator, caller must set selfDescriptorState.endpoints (from relayTransport.getListenAddress())
 * before calling rotator.start(), and wire rotator.start() after transport is listening.
 *
 * @param {{ relayConfig: { listenHost: string, listenPort: number }, identity: { accountId: string, deviceId: string, localInboxId: string }, inboxStore: object, getInboxIds?: () => string[], maxPeers?: number, selfDescriptorState?: { relayStore: object, relayKeyId: string, advertisedHost: string, setEndpoints: (ep: { host: string, port: number }) => void } }} opts
 * @returns {{ transport: TcpRelayTransport, inboxRouter: InboxRouter, relayRuntime: RelayRuntime, connectionPool: RelayConnectionPool, onionKeyRotator: OnionKeyRotator, keyring: OnionKeyringV1 } | null}
 */
export function bootstrapNodeRelay({
  relayConfig,
  identity,
  inboxStore,
  relayStore = null,
  getInboxIds = null,
  getRegistrations = null,
  maxPeers = 32,
  selfDescriptorState = null,
} = {}) {
  if (!relayConfig || !identity || !inboxStore) {
    return null;
  }

  const relayCrypto = new NodeCryptoProvider();
  const onion = relayConfig && relayConfig.onion ? relayConfig.onion : null;
  const onionV2 = onion && onion.v2 ? onion.v2 : null;
  const configuredKeys = buildConfiguredRelayKeys(onionV2 && onionV2.keys ? onionV2.keys : []);
  const keyring = loadOnionKeyringV1({ keys: configuredKeys.keyringKeys });

  // relayKeyId is derived from the node signing key and carried on the
  // identity SSOT (ADR-RELAY-IDENTITY). Never derive it from deviceId or read
  // it from relay config here.
  const relayKeyId = typeof identity.relayKeyId === "string" && identity.relayKeyId.trim()
    ? identity.relayKeyId.trim()
    : "";
  if (!relayKeyId) {
    throw new Error("bootstrapNodeRelay requires identity.relayKeyId (derived by ensureNodeIdentity; see ADR-RELAY-IDENTITY)");
  }
  const deviceIdForRotator = relayKeyId;

  const onDescriptorUpdate =
    selfDescriptorState &&
    Boolean(selfDescriptorState.relayStore) && typeof selfDescriptorState.relayStore.upsertDescriptor === "function" &&
    selfDescriptorState.relayKeyId &&
    selfDescriptorState.advertisedHost != null
      ? (keyRecords) => {
          const ep = selfDescriptorState.endpoints;
          if (!ep) return;
          const nowMs = Date.now();
          const descriptor = buildSignedRelayDescriptorJson({
            relayKeyId: selfDescriptorState.relayKeyId,
            advertisedHost: selfDescriptorState.advertisedHost,
            relayPort: selfDescriptorState.advertisedPort || ep.port,
            tlsEnabled: selfDescriptorState.tlsEnabled === true,
            keyRecords,
            nodeKeyId: selfDescriptorState.nodeKeyId,
            nodePublicKeyB64: selfDescriptorState.nodePublicKeyB64,
            nodePrivateKeyB64: selfDescriptorState.nodePrivateKeyB64,
            nowMs,
          });
          if (!descriptor) return;
          selfDescriptorState.relayStore.upsertDescriptor(descriptor, {
            source: "self",
            receivedAtMs: nowMs,
          });
        }
      : () => {};

  const onionKeyRotator = configuredKeys.descriptorKeyRecords.length > 0
    ? null
    : new OnionKeyRotator({
        cryptoProvider: relayCrypto,
        keyring,
        onDescriptorUpdate,
        deviceId: deviceIdForRotator,
        ttlMs: 86_400_000 * 30,
        rotateAtFraction: 0.8,
      });

  const transport = new TcpRelayTransport({
    endpointId: identity.accountId,
    listenHost: relayConfig.listenHost,
    listenPort: relayConfig.listenPort,
    tlsOptions: relayConfig.tls,
  });

  const relayPeerDirectory = new RelayPeerDirectory();
  const inboxRouter = new InboxRouter({
    transport,
    inboxStore,
    relayPeerDirectory,
    logger: console,
    selfRelayKeyId: relayKeyId,
  });

  const bridge = new RelayRuntimeBridge();

  const relayRuntime = new RelayRuntime({
    transport,
    inboxStore,
    inboxRouter,
    relayDirectory: relayPeerDirectory,
    bridge,
    onion: {
      crypto: relayCrypto,
      v2: { keyring },
    },
  });
  const controlMessageRegistry = new ControlMessageRegistry();
  const stateRelayStore = selfDescriptorState && selfDescriptorState.relayStore !== undefined
    ? selfDescriptorState.relayStore
    : null;
  const sharedRelayStore = relayStore !== null && relayStore !== undefined
    ? relayStore
    : (stateRelayStore === undefined ? null : stateRelayStore);
  const frameRouter = new SocketFrameRouter({
    controlMessageRegistry,
    relayPeerDirectory: relayPeerDirectory,
    relayStore: sharedRelayStore,
    inboxRouter,
    inboxStore,
    relayRuntime,
    onRouteFailed: (obj, socket) => {
      if (relayRuntime && typeof relayRuntime.handleRouteFailed === "function") {
        relayRuntime.handleRouteFailed(obj, socket);
      }
    },
    isInboxLocal: getInboxIds
      ? (inboxId) => {
          const ids = getInboxIds();
          return Array.isArray(ids) && ids.includes(inboxId);
        }
      : null,
    getSelfDescriptor: () => buildSelfDescriptorJson({
      selfDescriptorState,
      configuredKeyRecords: configuredKeys.descriptorKeyRecords,
      onionKeyRotator,
    }),
    selfPeerAuth: {
      // Every node has a relayKeyId — that's a routing-layer identity, not
      // a "I have a public endpoint" claim. Descriptor publication
      // (broadcasting a reachable host:port) is what's gated on
      // advertisedHost. A NAT'd electron node STILL participates in the
      // relay mesh over its outbound TCP connections — see the
      // relay-network thesis. (Once a TCP socket is open, it's
      // bidirectional; reachability for incoming connections isn't a
      // prerequisite to relay traffic on an already-open socket.)
      relayKeyId: identity.relayKeyId,
      nodeKeyId: identity.nodeKeyId,
      nodePublicKeyB64: identity.nodePublicKeyB64,
      nodePrivateKeyB64: identity.nodePrivateKeyB64,
    },
    logger: console,
  });
  relayRuntime.frameRouter = frameRouter;

  const connectionPool = new RelayConnectionPool({
    inboxIds: !getInboxIds && identity.localInboxId ? [identity.localInboxId] : [],
    getInboxIds: getInboxIds || null,
    getRegistrations: typeof getRegistrations === "function" ? getRegistrations : null,
    inboxStore,
    inboxRouter,
    relayPeerDirectory,
    relayStore: sharedRelayStore,
    relayKeyId,
    advertisedRelayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    nodePrivateKeyB64: identity.nodePrivateKeyB64,
    getSelfDescriptor: () => buildSelfDescriptorJson({
      selfDescriptorState,
      configuredKeyRecords: configuredKeys.descriptorKeyRecords,
      onionKeyRotator,
    }),
    frameRouter,
    maxConnections: maxPeers,
  });
  // Inbound peers stay provisional (relay-provisional). Promotion to
  // relay-verified only happens through outbound connections we initiate
  // from configured relay lists or descriptor gossip from already-verified
  // relays. Route authority for hops=0 requires proof-carrying delegation
  // (owner-signed hosted-inbox-delegation bound to the relay identity).

  return {
    transport,
    inboxRouter,
    relayRuntime,
    connectionPool,
    relayPeerDirectory,
    frameRouter,
    controlMessageRegistry,
    onionKeyRotator,
    keyring,
    bridge,
    getSelfDescriptorKeyRecords() {
      if (configuredKeys.descriptorKeyRecords.length > 0) {
        return configuredKeys.descriptorKeyRecords.slice();
      }
      if (onionKeyRotator && typeof onionKeyRotator.getActiveKeyRecords === "function") {
        return onionKeyRotator.getActiveKeyRecords();
      }
      return [];
    },
  };
}

function buildSelfDescriptorJson({ selfDescriptorState, configuredKeyRecords, onionKeyRotator } = {}) {
  const endpoints = selfDescriptorState && selfDescriptorState.endpoints ? selfDescriptorState.endpoints : null;
  if (!selfDescriptorState || !selfDescriptorState.relayKeyId || !selfDescriptorState.advertisedHost
      || !endpoints || !endpoints.port) {
    return null;
  }
  const keyRecords = Array.isArray(configuredKeyRecords) && configuredKeyRecords.length > 0
    ? configuredKeyRecords
    : (onionKeyRotator && typeof onionKeyRotator.getActiveKeyRecords === "function" ? onionKeyRotator.getActiveKeyRecords() : []);
  if (!Array.isArray(keyRecords) || keyRecords.length === 0) {
    return null;
  }
  return buildSignedRelayDescriptorJson({
    relayKeyId: selfDescriptorState.relayKeyId,
    advertisedHost: selfDescriptorState.advertisedHost,
    relayPort: selfDescriptorState.advertisedPort || selfDescriptorState.endpoints.port,
    tlsEnabled: selfDescriptorState.tlsEnabled === true,
    keyRecords,
    nodeKeyId: selfDescriptorState.nodeKeyId,
    nodePublicKeyB64: selfDescriptorState.nodePublicKeyB64,
    nodePrivateKeyB64: selfDescriptorState.nodePrivateKeyB64,
  });
}

function buildConfiguredRelayKeys(keys) {
  const rawKeys = Array.isArray(keys) ? keys : [];
  const keyringKeys = [];
  const descriptorKeyRecords = [];
  const nowMs = Date.now();

  for (const key of rawKeys) {
    if (!key || typeof key !== "object") continue;
    const privateKeyBytes = decodeBase64(key.privateKeyBytes, "relay.onion.v2.keys[].privateKeyBytes");
    let publicKeyBytes;
    if (typeof key.publicKeyBytes === "string" && key.publicKeyBytes.trim()) {
      publicKeyBytes = decodeBase64(key.publicKeyBytes, "relay.onion.v2.keys[].publicKeyBytes");
    } else {
      publicKeyBytes = deriveX25519PublicKey(privateKeyBytes);
    }
    const onionKeyId = typeof key.onionKeyId === "string" && key.onionKeyId.trim()
      ? key.onionKeyId.trim()
      : onionKeyIdFromPublicKey(publicKeyBytes);
    const notBefore = Number(key.notBefore);
    const notAfter = Number(key.notAfter);
    const status = typeof key.status === "string" && key.status.trim() ? key.status.trim() : "active";

    keyringKeys.push({
      onionKeyId,
      privateKeyBytes,
      notBefore,
      notAfter,
      status,
    });
    descriptorKeyRecords.push(
      new OnionKeyRecordV1({
        onionKeyId,
        publicKeyBytes,
        format: "spki",
        createdAt: nowMs,
        notBefore,
        notAfter,
        status,
      }),
    );
  }

  return { keyringKeys, descriptorKeyRecords };
}

function decodeBase64(value, label) {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`Config ${label} must be base64 string`);
  }
  return new Uint8Array(Buffer.from(value, "base64"));
}

function deriveX25519PublicKey(privateKeyDerBytes) {
  const privKey = createPrivateKey({
    key: Buffer.from(privateKeyDerBytes),
    format: "der",
    type: "pkcs8",
  });
  const pubKey = createPublicKey(privKey);
  const pubDer = pubKey.export({ format: "der", type: "spki" });
  return new Uint8Array(pubDer);
}

function onionKeyIdFromPublicKey(publicKeyBytes) {
  return createHash("sha256").update(publicKeyBytes).digest("hex");
}

