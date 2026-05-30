import path from "node:path";
import { defaultControlSocketPath } from "../control/ControlServer.js";
import { PRICING_UNITS, ServicePricingV1 } from "@rezprotocol/core";

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

    normalizedWs = {
      host,
      port,
      path: wsPath,
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
  if (config.node?.identity !== undefined) {
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
        rateLimit: clampInt(mesh?.policy?.rateLimit, 1, 100_000, 120),
        payloadMaxBytes: clampInt(mesh?.policy?.payloadMaxBytes, 1024, 64 * 1024 * 1024, 1_048_576),
        failureThreshold: clampInt(mesh?.policy?.failureThreshold, 1, 1000, 8),
        defaultHops: clampInt(mesh?.policy?.defaultHops, 1, 3, 1),
        forceOnionRouting: mesh?.policy?.forceOnionRouting === true,
      },
      allowRelayKeyIds: normalizeStringArray(mesh.allowRelayKeyIds),
      denyRelayKeyIds: normalizeStringArray(mesh.denyRelayKeyIds),
    },
    storage: {
      dataDir,
      defaultThreadId,
      controlSocketPath: controlSocketPathRaw,
    },
    backup: {
      retentionDays,
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
