import path from "node:path";
import { defaultControlSocketPath } from "../control/ControlServer.js";
import { PRICING_UNITS, ServicePricingV1 } from "@rezprotocol/core";
import { assertMultiDeviceFanoutReady } from "./deviceFanoutReadiness.js";

/**
 * Decode a base64 at-rest storage encryption key. Returns a 32-byte Uint8Array,
 * or null when absent/empty. Throws when present but not exactly 32 bytes — a
 * wrong-length key is a misconfiguration that must fail loud, never be padded.
 * The key is a SECRET: never log it, never persist it to shared storage.
 * @param {unknown} b64
 * @returns {Uint8Array|null}
 */
export function decodeStorageEncryptionKeyB64(b64) {
  if (typeof b64 !== "string" || b64.trim() === "") {
    return null;
  }
  const bytes = Buffer.from(b64.trim(), "base64");
  if (bytes.length !== 32) {
    throw new Error(`storage.encryptionKeyB64 must decode to exactly 32 bytes (got ${bytes.length})`);
  }
  return new Uint8Array(bytes);
}

export function validateConfig(config) {
  if (!config || typeof config !== "object") {
    throw new Error("rez-node requires config object");
  }

  const node = config.node;
  if (!node || typeof node !== "object") {
    throw new Error("rez-node requires config.node");
  }

  if (node.mode !== undefined && node.mode !== "full" && node.mode !== "relay-only") {
    throw new Error("rez-node requires config.node.mode in full|relay-only when provided");
  }
  const nodeMode = node.mode === "relay-only" ? "relay-only" : "full";

  // Derive relay.enabled / node.enabled from mode (backward compat)
  // relay-only → relay=true, node=false
  // full       → relay=true, node=true
  const nodeEnabled = nodeMode !== "relay-only";
  const relayEnabled = true; // relay always enabled for now

  const ws = node.ws && typeof node.ws === "object" ? node.ws : null;
  let normalizedWs = null;
  if (!ws) {
    if (nodeEnabled) {
      throw new Error("rez-node requires config.node.ws");
    }
  } else {
    const port = ws.port;
    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      throw new Error("rez-node requires integer config.node.ws.port");
    }

    const host = ws.host === undefined ? "127.0.0.1" : ws.host;
    if (typeof host !== "string" || !host.trim()) {
      throw new Error("rez-node requires string config.node.ws.host when provided");
    }

    const wsPath = ws.path;
    if (typeof wsPath !== "string" || !wsPath.trim()) {
      throw new Error("rez-node requires string config.node.ws.path");
    }

    // Track 2: optional TLS for the client-facing listener. A hosted node accepting stranger
    // registrations must not carry claim signatures and session traffic in the clear — but many
    // deployments terminate TLS at a load balancer, so this is CONFIGURED, not forced. What is not
    // acceptable is silence: an omitted/partial block is resolved explicitly below, and startRezNode
    // logs which mode it is running in, so "no TLS" is always a visible decision rather than an
    // unnoticed default.
    const tls = ws.tls === undefined || ws.tls === null ? null : ws.tls;
    let normalizedTls = null;
    if (tls !== null) {
      if (typeof tls !== "object" || Array.isArray(tls)) {
        throw new Error("rez-node requires object config.node.ws.tls when provided");
      }
      const keyPath = typeof tls.keyPath === "string" ? tls.keyPath.trim() : "";
      const certPath = typeof tls.certPath === "string" ? tls.certPath.trim() : "";
      // Both or neither: a half-configured TLS block is far more likely to be a deployment mistake
      // than an intention, and silently falling back to plaintext is the failure mode this whole
      // option exists to prevent.
      if (keyPath.length === 0 || certPath.length === 0) {
        throw new Error("rez-node requires both config.node.ws.tls.keyPath and .certPath when tls is configured");
      }
      const caPath = typeof tls.caPath === "string" && tls.caPath.trim().length > 0 ? tls.caPath.trim() : null;
      normalizedTls = { keyPath, certPath, caPath };
    }

    normalizedWs = {
      host,
      port,
      path: wsPath,
      tls: normalizedTls,
    };
  }

  const network = node.network;
  if (!network || typeof network !== "object") {
    throw new Error("rez-node requires config.node.network");
  }

  if (network.participateInRouting !== undefined && typeof network.participateInRouting !== "boolean") {
    throw new Error("rez-node requires boolean config.node.network.participateInRouting when provided");
  }
  if (network.participateInRouting === false) {
    throw new Error("rez-node full mesh is always enabled; use config.node.mode=relay-only instead of disabling network.participateInRouting");
  }

  if (!Array.isArray(network.knownRelays)) {
    throw new Error("rez-node requires array config.node.network.knownRelays");
  }
  for (const relay of network.knownRelays) {
    if (!relay || typeof relay !== "object") continue;
    if (relay.insecure !== undefined && typeof relay.insecure !== "boolean") {
      throw new Error("rez-node requires boolean config.node.network.knownRelays[].insecure when provided");
    }
    if (relay.tls !== undefined && typeof relay.tls !== "boolean") {
      throw new Error("rez-node requires boolean config.node.network.knownRelays[].tls when provided");
    }
  }

  const normalizedKnownRelays = normalizeKnownRelays(network.knownRelays);

  const mesh = node.mesh && typeof node.mesh === "object" ? node.mesh : {};
  const meshMode = mesh.mode === undefined ? "seeded-gossip" : String(mesh.mode);
  if (meshMode !== "seeded-gossip" && meshMode !== "seed-only") {
    throw new Error("rez-node requires config.node.mesh.mode in seeded-gossip|seed-only");
  }
  if (mesh.seeds !== undefined && !Array.isArray(mesh.seeds)) {
    throw new Error("rez-node requires array config.node.mesh.seeds when provided");
  }
  if (mesh.enabled !== undefined && typeof mesh.enabled !== "boolean") {
    throw new Error("rez-node requires boolean config.node.mesh.enabled when provided");
  }
  if (mesh.enabled === false) {
    throw new Error("rez-node full mesh is always enabled; remove config.node.mesh.enabled=false");
  }
  if (mesh.participateInRouting !== undefined && typeof mesh.participateInRouting !== "boolean") {
    throw new Error("rez-node requires boolean config.node.mesh.participateInRouting when provided");
  }
  if (mesh.participateInRouting === false) {
    throw new Error("rez-node full mesh always participates in routing; remove config.node.mesh.participateInRouting=false");
  }

  const relay = node.relay && typeof node.relay === "object" ? node.relay : {};
  const relayKeyId = typeof relay.relayKeyId === "string" && relay.relayKeyId.trim()
    ? relay.relayKeyId.trim()
    : null;
  if (nodeMode === "relay-only" && !relayKeyId) {
    throw new Error("rez-node requires config.node.relay.relayKeyId in relay-only mode");
  }
  const relayTls = relay.tls && typeof relay.tls === "object" && !Array.isArray(relay.tls) ? relay.tls : {};
  const relayListenHost = typeof relay.listenHost === "string" && relay.listenHost.trim()
    ? relay.listenHost.trim()
    : "127.0.0.1";
  const relayListenPort = Number.isInteger(Number(relay.listenPort)) && Number(relay.listenPort) >= 0
    ? Number(relay.listenPort)
    : 0;
  const relayAdvertisedHost = typeof relay.advertisedHost === "string" && relay.advertisedHost.trim()
    ? relay.advertisedHost.trim()
    : null;
  const relayAdvertisedPort = Number.isInteger(Number(relay.advertisedPort)) && Number(relay.advertisedPort) > 0
    ? Number(relay.advertisedPort)
    : null;
  const relayAdvertisedTls = relay.advertisedTls === true;
  if (isWildcardHost(relayListenHost) && !relayAdvertisedHost) {
    throw new Error("rez-node requires config.node.relay.advertisedHost when relay.listenHost is wildcard");
  }
  if (relay.tls !== undefined && (!relay.tls || typeof relay.tls !== "object" || Array.isArray(relay.tls))) {
    throw new Error("rez-node requires object config.node.relay.tls when provided");
  }
  if (relayTls.enabled !== undefined && typeof relayTls.enabled !== "boolean") {
    throw new Error("rez-node requires boolean config.node.relay.tls.enabled when provided");
  }
  const relayTlsEnabled = relayTls.enabled === true;
  const relayTlsCertPathRaw = typeof relayTls.certPath === "string" && relayTls.certPath.trim()
    ? relayTls.certPath.trim()
    : null;
  const relayTlsKeyPathRaw = typeof relayTls.keyPath === "string" && relayTls.keyPath.trim()
    ? relayTls.keyPath.trim()
    : null;
  if (relayTls.certPath !== undefined && !relayTlsCertPathRaw) {
    throw new Error("rez-node requires string config.node.relay.tls.certPath when provided");
  }
  if (relayTls.keyPath !== undefined && !relayTlsKeyPathRaw) {
    throw new Error("rez-node requires string config.node.relay.tls.keyPath when provided");
  }
  if (relayTlsEnabled && (!relayTlsCertPathRaw || !relayTlsKeyPathRaw)) {
    throw new Error("rez-node requires config.node.relay.tls.certPath/keyPath when relay tls is enabled");
  }

  const SETTLEMENT_MODES = new Set(["local", "chain"]);
  const relaySettlement = relay.settlement && typeof relay.settlement === "object" ? relay.settlement : {};
  const settlementMode = typeof relaySettlement.mode === "string" && SETTLEMENT_MODES.has(relaySettlement.mode)
    ? relaySettlement.mode
    : "local";
  if (relaySettlement.mode !== undefined && !SETTLEMENT_MODES.has(relaySettlement.mode)) {
    throw new Error("rez-node requires config.node.relay.settlement.mode in local|chain");
  }

  const relayPricing = relay.pricing && typeof relay.pricing === "object" ? relay.pricing : {};
  const pricingEnabled = relayPricing.enabled === true;
  // networkId is the immutable settlement-network binding stamped into every
  // signed debit receipt (anti cross-network replay). Defaults to a personal-
  // relay network; a hosted cluster sets its own (e.g. rez:testnet:v1).
  if (relayPricing.networkId !== undefined
      && (typeof relayPricing.networkId !== "string" || relayPricing.networkId.trim() === "")) {
    throw new Error("rez-node requires non-empty string config.node.relay.pricing.networkId when provided");
  }
  const pricingNetworkId = typeof relayPricing.networkId === "string" && relayPricing.networkId.trim()
    ? relayPricing.networkId.trim()
    : "rez:local:v1";
  const pricingServices = {};
  if (relayPricing.services && typeof relayPricing.services === "object") {
    for (const [serviceId, svcConfig] of Object.entries(relayPricing.services)) {
      if (!svcConfig || typeof svcConfig !== "object") continue;
      const costPerUnit = Number(svcConfig.costPerUnit);
      const unit = typeof svcConfig.unit === "string" ? svcConfig.unit : "operation";
      const currency = typeof svcConfig.currency === "string" && svcConfig.currency.trim() ? svcConfig.currency.trim() : "REZ";
      const description = typeof svcConfig.description === "string" ? svcConfig.description : "";
      try {
        new ServicePricingV1({ serviceId, costPerUnit, unit, currency, description });
      } catch (err) {
        throw new Error(`rez-node config.node.relay.pricing.services.${serviceId}: ${err.message}`);
      }
      pricingServices[serviceId] = { costPerUnit, unit, currency, description };
    }
  }

  const relayOnion = relay.onion && typeof relay.onion === "object" && !Array.isArray(relay.onion) ? relay.onion : {};
  if (relay.onion !== undefined && (!relay.onion || typeof relay.onion !== "object" || Array.isArray(relay.onion))) {
    throw new Error("rez-node requires object config.node.relay.onion when provided");
  }
  const relayOnionV2 = relayOnion.v2 && typeof relayOnion.v2 === "object" && !Array.isArray(relayOnion.v2) ? relayOnion.v2 : {};
  if (relayOnion.v2 !== undefined && (!relayOnion.v2 || typeof relayOnion.v2 !== "object" || Array.isArray(relayOnion.v2))) {
    throw new Error("rez-node requires object config.node.relay.onion.v2 when provided");
  }
  if (relayOnionV2.keys !== undefined && !Array.isArray(relayOnionV2.keys)) {
    throw new Error("rez-node requires array config.node.relay.onion.v2.keys when provided");
  }
  const relayOnionKeys = (relayOnionV2.keys || []).map(normalizeRelayOnionKey);

  let identity;
  if (config.node && config.node.identity !== undefined) {
    if (!config.node.identity || typeof config.node.identity !== "object") {
      throw new Error("rez-node requires object config.node.identity when provided");
    }
    const accountId = String(config.node.identity.accountId || "").trim();
    const deviceId = String(config.node.identity.deviceId || "").trim();
    const localInboxId = String(config.node.identity.localInboxId || "").trim();
    if (!accountId || !deviceId || !localInboxId) {
      throw new Error("rez-node requires non-empty config.node.identity.accountId/deviceId/localInboxId");
    }
    identity = {
      accountId,
      deviceId,
      localInboxId,
    };
    // Preserve node key material (the mesh keypair) when supplied. Dropping it
    // forced ensureIdentityShape to regenerate the node key on every boot, which
    // rotated the fs-mode at-rest storage key (derived from it) and broke
    // decryption of prior storage. Node keys are all-or-nothing.
    const nodeKeyId = String(config.node.identity.nodeKeyId || "").trim();
    const nodePublicKeyB64 = String(config.node.identity.nodePublicKeyB64 || "").trim();
    const nodePrivateKeyB64 = String(config.node.identity.nodePrivateKeyB64 || "").trim();
    const someKeys = nodeKeyId || nodePublicKeyB64 || nodePrivateKeyB64;
    const allKeys = nodeKeyId && nodePublicKeyB64 && nodePrivateKeyB64;
    if (someKeys && !allKeys) {
      throw new Error(
        "rez-node config.node.identity node key material must be complete "
          + "(nodeKeyId + nodePublicKeyB64 + nodePrivateKeyB64) or fully omitted",
      );
    }
    if (allKeys) {
      identity.nodeKeyId = nodeKeyId;
      identity.nodePublicKeyB64 = nodePublicKeyB64;
      identity.nodePrivateKeyB64 = nodePrivateKeyB64;
    }
  }

  const storage = node.storage && typeof node.storage === "object" ? node.storage : {};
  const dataDir = typeof storage.dataDir === "string" && storage.dataDir.trim().length > 0
    ? path.resolve(storage.dataDir.trim())
    : path.resolve(process.cwd(), ".local", "rez-node-data");
  const defaultThreadIdRaw = typeof storage.defaultThreadId === "string" ? storage.defaultThreadId.trim() : "";
  const defaultThreadId = /^th_[A-Za-z0-9_-]{22}$/.test(defaultThreadIdRaw) ? defaultThreadIdRaw : null;
  const controlSocketPathRaw = typeof storage.controlSocketPath === "string" && storage.controlSocketPath.trim().length > 0
    ? storage.controlSocketPath.trim()
    : defaultControlSocketPath(dataDir);

  // Storage backend selector. "fs" (default) is the single-node filesystem
  // store; "pg" is the shared-state Postgres backend for a hosted cluster.
  const storageBackend = typeof storage.backend === "string" ? storage.backend.trim().toLowerCase() : "fs";
  if (storageBackend !== "fs" && storageBackend !== "pg") {
    throw new Error("rez-node requires config.node.storage.backend in fs|pg");
  }
  const storagePg = storage.pg && typeof storage.pg === "object" ? storage.pg : {};
  const pgConnectionString = typeof storagePg.connectionString === "string" ? storagePg.connectionString.trim() : "";
  if (storageBackend === "pg" && pgConnectionString === "") {
    throw new Error("rez-node requires config.node.storage.pg.connectionString when storage.backend=pg");
  }
  // Run pending migrations during boot. Safe for concurrent node starts (the
  // runner takes a Postgres advisory lock and is forward-only + version-gated).
  // Default on so a fresh cluster `up` just works; operators running migrations
  // out-of-band can set it false.
  const pgMigrateOnBoot = storagePg.migrateOnBoot === undefined ? true : storagePg.migrateOnBoot === true;

  // At-rest storage encryption key. fs mode derives it from the node identity
  // (single-node). pg mode REQUIRES an explicit cluster key: distinct nodes have
  // distinct identities, so a derived key would make each node write rows the
  // others cannot read — a split-brain that looks like corruption. All trusted
  // home-cluster nodes must share one explicit key. (throws on wrong length)
  const storageEncryptionKeyB64 = typeof storage.encryptionKeyB64 === "string" ? storage.encryptionKeyB64.trim() : "";
  const decodedStorageKey = decodeStorageEncryptionKeyB64(storageEncryptionKeyB64);
  if (storageBackend === "pg" && !decodedStorageKey) {
    throw new Error(
      "rez-node requires config.node.storage.encryptionKeyB64 (or REZ_STORAGE_ENCRYPTION_KEY), "
        + "a 32-byte base64 cluster key, when storage.backend=pg — refusing to derive a per-node key",
    );
  }

  // Redis liveness bus (OPTIONAL, pg clusters only). When unset, the cluster is
  // still correct via reconnect-drain (Slice 2); Redis only adds real-time
  // cross-node live delivery. shardCount MUST be a cluster-wide constant — every
  // node subscribes to the same shard set, so a mismatch would split the bus.
  const redis = node.redis && typeof node.redis === "object" ? node.redis : {};
  const redisUrl = typeof redis.url === "string" ? redis.url.trim() : "";
  if (redis.url !== undefined && typeof redis.url !== "string") {
    throw new Error("rez-node requires string config.node.redis.url when provided");
  }
  const redisShardCount = clampInt(redis.shardCount, 1, 4096, 64);
  const redisPresenceTtlMs = clampInt(redis.presenceTtlMs, 1000, 600_000, 30_000);

  // S2.5 E6 fan-out GATE. Per-device durable fan-out (the durable inbox delivering
  // to MORE than one device of an account) stays OFF until the multi-device E2EE
  // suite (S12) is green AND the audit-R4 revocation-boundary release blockers
  // (F2 legacy-cursor migration, F3 durable admission control) land. Fanning one
  // ciphertext to two devices on a shared ratchet breaks the ratchet, and opening
  // fan-out before the revocation work exists is a security regression.
  //
  // A config flip is the operator's INTENT; the interlock is the readiness policy in
  // deviceFanoutReadiness.js (the SSOT, shared by the runtime factories + bootstrap
  // so it can't be bypassed by an embedding app — audit R4 L2c review P1).
  // Requesting fan-out before every blocker is ready FAILS LOUD (assertMultiDevice-
  // FanoutReady throws, naming the unmet blockers) — never a silent downgrade — so a
  // node cannot even boot with multiDeviceFanout=true until the work ships.
  const device = node.device && typeof node.device === "object" ? node.device : {};
  if (device.multiDeviceFanout !== undefined && typeof device.multiDeviceFanout !== "boolean") {
    throw new Error("rez-node requires boolean config.node.device.multiDeviceFanout when provided");
  }
  const multiDeviceFanoutRequested = device.multiDeviceFanout === true;
  const fanoutReady = assertMultiDeviceFanoutReady(multiDeviceFanoutRequested);
  const DEVICE_FANOUT_MAX = 8;
  // Effective gate state = operator intent AND the readiness interlock.
  const multiDeviceFanout = multiDeviceFanoutRequested && fanoutReady;
  const maxDevices = multiDeviceFanout ? DEVICE_FANOUT_MAX : 1;

  const meshPolicy = mesh.policy && typeof mesh.policy === "object" && !Array.isArray(mesh.policy) ? mesh.policy : {};

  const backup = node.backup && typeof node.backup === "object" ? node.backup : {};
  const retentionDaysRaw = Number(backup.retentionDays);
  const retentionDays = Number.isFinite(retentionDaysRaw)
    ? Math.max(1, Math.min(3650, Math.floor(retentionDaysRaw)))
    : 90;

  return {
    ws: normalizedWs,
    network: {
      participateInRouting: true,
      knownRelays: normalizedKnownRelays,
    },
    mesh: {
      enabled: true,
      mode: meshMode,
      seeds: normalizeStringArray(mesh.seeds),
      minPeers: clampInt(mesh.minPeers, 1, 1000, 3),
      maxPeers: clampInt(mesh.maxPeers, 1, 1000, 32),
      discoveryIntervalMs: clampInt(mesh.discoveryIntervalMs, 1_000, 300_000, 30_000),
      discoveryTimeoutMs: clampInt(mesh.discoveryTimeoutMs, 200, 30_000, 3_000),
      limitPerSource: clampInt(mesh.limitPerSource, 1, 500, 200),
      participateInRouting: true,
      policy: {
        rateLimit: clampInt(meshPolicy.rateLimit, 1, 100_000, 120),
        payloadMaxBytes: clampInt(meshPolicy.payloadMaxBytes, 1024, 64 * 1024 * 1024, 1_048_576),
        failureThreshold: clampInt(meshPolicy.failureThreshold, 1, 1000, 8),
        defaultHops: clampInt(meshPolicy.defaultHops, 1, 3, 1),
        forceOnionRouting: meshPolicy.forceOnionRouting === true,
      },
      allowRelayKeyIds: normalizeStringArray(mesh.allowRelayKeyIds),
      denyRelayKeyIds: normalizeStringArray(mesh.denyRelayKeyIds),
    },
    storage: {
      dataDir,
      defaultThreadId,
      controlSocketPath: controlSocketPathRaw,
      backend: storageBackend,
      // SECRET — present only when configured; do not log app.config wholesale.
      encryptionKeyB64: storageEncryptionKeyB64,
      pg: {
        connectionString: pgConnectionString,
        migrateOnBoot: pgMigrateOnBoot,
      },
    },
    redis: {
      // "" = disabled (cluster stays correct via reconnect-drain; Redis adds
      // real-time cross-node push only).
      url: redisUrl,
      shardCount: redisShardCount,
      presenceTtlMs: redisPresenceTtlMs,
    },
    backup: {
      retentionDays,
    },
    device: {
      // E6 fan-out gate (see above): false ⇒ one active device per inbox.
      multiDeviceFanout,
      maxDevices,
    },
    relay: {
      enabled: relayEnabled,
      listenHost: relayListenHost,
      listenPort: relayListenPort,
      advertisedHost: relayAdvertisedHost,
      advertisedPort: relayAdvertisedPort,
      advertisedTls: relayAdvertisedTls,
      relayKeyId,
      tls: {
        enabled: relayTlsEnabled,
        certPath: relayTlsCertPathRaw ? path.resolve(relayTlsCertPathRaw) : null,
        keyPath: relayTlsKeyPathRaw ? path.resolve(relayTlsKeyPathRaw) : null,
      },
      onion: {
        v2: {
          keys: relayOnionKeys,
        },
      },
      settlement: {
        mode: settlementMode,
      },
      pricing: {
        enabled: pricingEnabled,
        services: pricingServices,
        networkId: pricingNetworkId,
      },
    },
    node: {
      enabled: nodeEnabled,
      mode: nodeMode,
      identity,
      serverServicesFactory: typeof node.serverServicesFactory === "function" ? node.serverServicesFactory : null,
      serviceCacheFactory: typeof node.serviceCacheFactory === "function" ? node.serviceCacheFactory : null,
      groupLookupClass: node.groupLookupClass || null,
      protocolFactory: typeof node.protocolFactory === "function" ? node.protocolFactory : null,
      onInboundDeposit: typeof node.onInboundDeposit === "function" ? node.onInboundDeposit : null,
    },
  };
}

function normalizeKnownRelays(value) {
  if (!Array.isArray(value)) return [];
  return value.map((relay) => {
    if (!relay || typeof relay !== "object") return relay;
    return {
      ...relay,
      insecure: relay.insecure === true,
      tls: relay.tls === true,
    };
  });
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) return [];
  const out = [];
  const seen = new Set();
  for (const item of value) {
    const text = typeof item === "string" ? item.trim() : "";
    if (!text || seen.has(text)) continue;
    seen.add(text);
    out.push(text);
  }
  return out;
}

function normalizeRelayOnionKey(key) {
  if (!key || typeof key !== "object" || Array.isArray(key)) {
    throw new Error("rez-node requires object config.node.relay.onion.v2.keys[]");
  }
  const privateKeyBytes = typeof key.privateKeyBytes === "string" && key.privateKeyBytes.trim()
    ? key.privateKeyBytes.trim()
    : null;
  if (!privateKeyBytes) {
    throw new Error("rez-node requires string config.node.relay.onion.v2.keys[].privateKeyBytes");
  }
  const onionKeyId = typeof key.onionKeyId === "string" && key.onionKeyId.trim()
    ? key.onionKeyId.trim()
    : null;
  const publicKeyBytes = typeof key.publicKeyBytes === "string" && key.publicKeyBytes.trim()
    ? key.publicKeyBytes.trim()
    : null;
  const notBefore = Number(key.notBefore);
  const notAfter = Number(key.notAfter);
  if (!Number.isFinite(notBefore) || !Number.isFinite(notAfter)) {
    throw new Error("rez-node requires numeric config.node.relay.onion.v2.keys[].notBefore/notAfter");
  }
  if (notBefore > notAfter) {
    throw new Error("rez-node requires config.node.relay.onion.v2.keys[].notBefore <= notAfter");
  }
  const status = typeof key.status === "string" && key.status.trim()
    ? key.status.trim()
    : "active";
  if (!["active", "draining", "revoked"].includes(status)) {
    throw new Error("rez-node requires config.node.relay.onion.v2.keys[].status in active|draining|revoked");
  }
  return {
    ...(onionKeyId ? { onionKeyId } : {}),
    ...(publicKeyBytes ? { publicKeyBytes } : {}),
    privateKeyBytes,
    notBefore,
    notAfter,
    status,
  };
}

function clampInt(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const rounded = Math.floor(num);
  return Math.max(min, Math.min(max, rounded));
}

function isWildcardHost(host) {
  const value = typeof host === "string" ? host.trim().toLowerCase() : "";
  return value === "0.0.0.0" || value === "::" || value === "::0";
}
