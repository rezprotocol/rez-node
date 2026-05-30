import { descriptorHasUsableOnionKey } from "@rezprotocol/core";
import { resolveDeliveryDescriptor } from "./resolveDeliveryDescriptor.js";

const STORE_KEY = "substrate:relayStore:descriptors:v1";

export class RelayStore {
  constructor({ metrics = null, storageProvider = null, logger = console, nowMs = () => Date.now() } = {}) {
    this._relays = new Map();
    this.metrics = metrics;
    this._kv = storageProvider && typeof storageProvider.getKeyValueStore === "function"
      ? storageProvider.getKeyValueStore()
      : null;
    this._logger = logger ?? console;
    this._nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
    this._persistChain = Promise.resolve();
    this._suspendPersist = false;
  }

  load(relayList) {
    this._relays.clear();
    const list = Array.isArray(relayList) ? relayList : [];
    for (const relay of list) {
      if (!relay || typeof relay !== "object") continue;
      if (relay.relayKeyId && Array.isArray(relay.endpoints) && Array.isArray(relay.onionKeys)) {
        this.upsertDescriptor(relay, { source: "config", receivedAtMs: this._nowMs(), skipPersist: true });
        continue;
      }
      const id = normalizeRelayId(relay);
      if (!id) continue;
      this._relays.set(id, buildLegacyRecord(relay, id, this._nowMs()));
    }
    this.metrics?.setGauge("activePeers", this._relays.size);
  }

  async hydratePersistentDescriptors() {
    if (!this._kv || typeof this._kv.get !== "function") return;
    const snapshot = await this._kv.get(STORE_KEY);
    const entries = Array.isArray(snapshot?.descriptors) ? snapshot.descriptors : [];
    const nowMs = this._nowMs();
    this._suspendPersist = true;
    try {
      for (const entry of entries) {
        const descriptor = entry?.descriptor && typeof entry.descriptor === "object"
          ? entry.descriptor
          : entry;
        const expiresAt = Number(descriptor?.expiresAt);
        if (!Number.isFinite(expiresAt) || expiresAt <= nowMs) continue;
        const source = typeof entry?.source === "string" && entry.source.trim()
          ? entry.source.trim()
          : "persisted";
        const receivedAtMs = Number(entry?.receivedAtMs);
        const bindingTrust = normalizeBindingTrust(entry?.bindingTrust, source);
        this.upsertDescriptor(descriptor, {
          source,
          receivedAtMs: Number.isFinite(receivedAtMs) ? receivedAtMs : nowMs,
          bindingTrust,
          skipPersist: true,
        });
      }
    } finally {
      this._suspendPersist = false;
    }
  }

  async flushPersistence() {
    await this._persistChain;
  }

  getAll() {
    return Array.from(this._relays.values()).map((relay) => ({ ...relay }));
  }

  upsertDescriptor(descriptor, { source = "discovery", receivedAtMs = this._nowMs(), bindingTrust = undefined, skipPersist = false } = {}) {
    const relayKeyId = normalizeRelayKeyId(descriptor);
    if (!relayKeyId) return { accepted: false, reason: "missing-relayKeyId" };

    const expiresAt = Number(descriptor?.expiresAt);
    if (!Number.isFinite(expiresAt)) return { accepted: false, reason: "missing-expiresAt" };

    const record = this._relays.get(relayKeyId);
    if (record?.source === "self" && source !== "self") {
      return { accepted: false, reason: "self-authoritative" };
    }
    const nextNodeKeyId = normalizeNodeKeyId(descriptor?.meta?.node?.keyId);
    const nextNodePublicKeyB64 = normalizeNodePublicKey(descriptor?.meta?.node?.publicKeyB64);
    const requestedTrust = normalizeBindingTrust(bindingTrust, source);
    const trust = strongerBindingTrust(record?.bindingTrust, requestedTrust);
    const gossipEligible = trust !== "tofu";
    if (record?.nodeKeyId && nextNodeKeyId && record.nodeKeyId !== nextNodeKeyId) {
      return { accepted: false, reason: "relay-rebind" };
    }
    if (record?.descriptor) {
      const existingExpires = Number(record.descriptor?.expiresAt);
      const existingSeen = Number(record.receivedAtMs || 0);
      if (existingExpires > expiresAt) return { accepted: false, reason: "older-expiresAt" };
      if (existingExpires === expiresAt && existingSeen >= receivedAtMs) return { accepted: false, reason: "older-receivedAt" };
    }

    const endpoint = selectPrimaryEndpoint(descriptor);
    this._relays.set(relayKeyId, {
      id: relayKeyId,
      relayKeyId,
      source: String(source || "discovery"),
      descriptor,
      endpoint,
      transport: inferTransport(descriptor, endpoint),
      receivedAtMs,
      expiresAt,
      bindingTrust: trust,
      nodeKeyId: nextNodeKeyId || record?.nodeKeyId || null,
      nodePublicKeyB64: nextNodePublicKeyB64 || record?.nodePublicKeyB64 || null,
      verifiedAtMs: trust === "verified" || trust === "self" || trust === "config" ? receivedAtMs : (record?.verifiedAtMs ?? null),
      gossipEligible,
      lastSeen: record?.lastSeen ?? null,
      failures: record?.failures ?? 0,
    });
    this.metrics?.setGauge("activePeers", this._relays.size);
    if (!skipPersist) this._schedulePersist();
    return { accepted: true };
  }

  mergeDescriptors(descriptors, { source = "discovery", receivedAtMs = this._nowMs(), maxPeers = Infinity } = {}) {
    const list = Array.isArray(descriptors) ? descriptors : [];
    const acceptedRelayKeyIds = [];
    const rejected = [];
    for (const descriptor of list) {
      if (this._relays.size >= maxPeers) {
        rejected.push({ relayKeyId: normalizeRelayKeyId(descriptor), reason: "max-peers" });
        continue;
      }
      const result = this.upsertDescriptor(descriptor, { source, receivedAtMs, skipPersist: true });
      if (result.accepted) {
        acceptedRelayKeyIds.push(normalizeRelayKeyId(descriptor));
      } else {
        rejected.push({ relayKeyId: normalizeRelayKeyId(descriptor), reason: result.reason || "rejected" });
      }
    }
    if (acceptedRelayKeyIds.length > 0) this._schedulePersist();
    return {
      accepted: acceptedRelayKeyIds.length,
      rejected: rejected.length,
      acceptedRelayKeyIds,
      rejected,
    };
  }

  listDescriptors({ nowMs = Date.now() } = {}) {
    const out = [];
    for (const relay of this._relays.values()) {
      if (!relay?.descriptor) continue;
      if (relay.gossipEligible === false) continue;
      if (Number(relay.descriptor.expiresAt) <= nowMs) continue;
      if (!descriptorHasUsableOnionKey(relay.descriptor, nowMs)) continue;
      out.push(relay.descriptor);
    }
    return out;
  }

  /**
   * Remove relay entries that have no usable onion key at nowMs (or no valid descriptor).
   * Returns the number of entries removed.
   */
  evictExpired({ nowMs = Date.now() } = {}) {
    const toRemove = [];
    for (const [relayKeyId, relay] of this._relays.entries()) {
      if (!relay?.descriptor) continue;
      const expiresAt = Number(relay.descriptor?.expiresAt);
      if (Number.isFinite(expiresAt) && expiresAt <= nowMs) {
        toRemove.push(relayKeyId);
        continue;
      }
      if (!descriptorHasUsableOnionKey(relay.descriptor, nowMs)) {
        toRemove.push(relayKeyId);
      }
    }
    for (const id of toRemove) {
      this._relays.delete(id);
    }
    if (toRemove.length > 0) {
      this.metrics?.setGauge("activePeers", this._relays.size);
      this._schedulePersist();
    }
    return toRemove.length;
  }

  /**
   * Return the descriptor for the relay record with source "self" (this node's relay), or null.
   */
  getSelfDescriptor({ nowMs = Date.now() } = {}) {
    for (const relay of this._relays.values()) {
      if (relay?.source !== "self" || !relay?.descriptor) continue;
      if (Number(relay.expiresAt) <= nowMs) continue;
      if (!descriptorHasUsableOnionKey(relay.descriptor, nowMs)) continue;
      return relay.descriptor;
    }
    return null;
  }

  /**
   * Return the descriptor for a relay by relayKeyId if present and valid (not expired, usable onion key).
   */
  getDescriptor(relayKeyId, { nowMs = Date.now() } = {}) {
    const id = typeof relayKeyId === "string" ? relayKeyId.trim() : "";
    if (!id) return null;
    const relay = this._relays.get(id);
    if (!relay?.descriptor) return null;
    if (Number(relay.expiresAt) <= nowMs) return null;
    if (!descriptorHasUsableOnionKey(relay.descriptor, nowMs)) return null;
    return relay.descriptor;
  }

  /**
   * Resolve a route entry (from InboxRouter.getRouteTo) to the delivery relay descriptor.
   * ID-based only. Single place for route→descriptor rules.
   * @param {object} routeEntry - { direct?, deliveryRelayKeyId?, relayKeyId? }
   * @param {{ descriptors?: object[], nowMs?: number }} opts - descriptors = merged list (discovery + store) for ID fallback
   * @returns {object|null} descriptor or null
   */
  getDeliveryDescriptorForRoute(routeEntry, { descriptors = [], nowMs = Date.now() } = {}) {
    return resolveDeliveryDescriptor(routeEntry, { descriptors, relayStore: this, nowMs });
  }

  snapshotPeers({ nowMs = Date.now(), failureThreshold = 8 } = {}) {
    const peers = [];
    for (const relay of this._relays.values()) {
      const nodeId = String(relay?.relayKeyId || relay?.id || "").trim();
      if (!nodeId) continue;
      const expired = Number.isFinite(Number(relay?.expiresAt)) && Number(relay.expiresAt) <= nowMs;
      const failures = Number(relay?.failures || 0);
      peers.push({
        nodeId,
        transport: String(relay?.transport || "unknown"),
        lastSeenAtMs: Number(relay?.lastSeen) || null,
        health: expired
          ? "stale"
          : (failures >= failureThreshold ? "degraded" : "healthy"),
        source: String(relay?.source || "unknown"),
      });
    }
    peers.sort((a, b) => String(a.nodeId).localeCompare(String(b.nodeId)));
    return peers;
  }

  _schedulePersist() {
    if (this._suspendPersist || !this._kv || typeof this._kv.set !== "function") {
      return this._persistChain;
    }
    this._persistChain = this._persistChain
      .then(async () => {
        const nowMs = this._nowMs();
        const descriptors = [];
        for (const relay of this._relays.values()) {
          if (!relay?.descriptor) continue;
          if (relay.source === "self") continue;
          const expiresAt = Number(relay.descriptor?.expiresAt);
          if (Number.isFinite(expiresAt) && expiresAt <= nowMs) continue;
          descriptors.push({
            descriptor: relay.descriptor,
            source: relay.source,
            receivedAtMs: relay.receivedAtMs,
            bindingTrust: relay.bindingTrust,
          });
        }
        await this._kv.set(STORE_KEY, { descriptors });
      })
      .catch((err) => {
        this._logger?.warn?.("RelayStore persist failed", err?.message ?? err);
      });
    return this._persistChain;
  }
}

function normalizeRelayId(relay) {
  const relayKeyId = normalizeRelayKeyId(relay);
  if (relayKeyId) return relayKeyId;
  const id = typeof relay?.id === "string" ? relay.id.trim() : "";
  return id || "";
}

function normalizeRelayKeyId(value) {
  return typeof value?.relayKeyId === "string" && value.relayKeyId.trim()
    ? value.relayKeyId.trim()
    : "";
}

function buildLegacyRecord(relay, id, nowMs = Date.now()) {
  // Extract endpoint from explicit field, or fall back to host/port
  const explicitEndpoint = normalizeEndpoint(relay?.endpoint || null);
  let endpoint = explicitEndpoint;
  if (!endpoint) {
    const host = typeof relay?.host === "string" ? relay.host.trim() : "";
    const port = Number(relay?.port);
    if (host && Number.isInteger(port) && port > 0) {
      endpoint = {
        host,
        port,
        ...(relay?.tls === true ? { tls: true } : {}),
      };
    }
  }
  return {
    ...relay,
    id,
    relayKeyId: String(relay?.relayKeyId || id),
    source: String(relay?.source || "config"),
    descriptor: null,
    endpoint,
    transport: String(relay?.transport || "unknown"),
    receivedAtMs: nowMs,
    expiresAt: Number(relay?.expiresAt) || Number.MAX_SAFE_INTEGER,
    bindingTrust: "config",
    nodeKeyId: null,
    nodePublicKeyB64: null,
    verifiedAtMs: nowMs,
    gossipEligible: true,
    lastSeen: null,
    failures: 0,
  };
}

function normalizeBindingTrust(value, source = "") {
  const requested = typeof value === "string" ? value.trim() : "";
  if (requested === "config" || requested === "self" || requested === "verified" || requested === "tofu") {
    return requested;
  }
  const normalizedSource = typeof source === "string" ? source.trim() : "";
  if (normalizedSource === "config") return "config";
  if (normalizedSource === "self") return "self";
  if (normalizedSource === "peer-bind-tofu") return "tofu";
  if (normalizedSource === "peer-bind-verified") return "verified";
  return "verified";
}

function strongerBindingTrust(current, next) {
  const rank = new Map([
    ["tofu", 0],
    ["verified", 1],
    ["config", 2],
    ["self", 3],
  ]);
  const currentRank = rank.get(current) ?? -1;
  const nextRank = rank.get(next) ?? -1;
  return currentRank > nextRank ? current : next;
}

function normalizeNodeKeyId(value) {
  const keyId = typeof value === "string" ? value.trim() : "";
  return keyId || null;
}

function normalizeNodePublicKey(value) {
  const publicKey = typeof value === "string" ? value.trim() : "";
  return publicKey || null;
}

function selectPrimaryEndpoint(descriptor) {
  if (!Array.isArray(descriptor?.endpoints) || descriptor.endpoints.length === 0) return null;
  return normalizeEndpoint(descriptor.endpoints[0]);
}

function normalizeEndpoint(endpoint) {
  if (!endpoint || typeof endpoint !== "object") return null;
  const host = String(endpoint.host || "").trim();
  const port = Number(endpoint.port);
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  return {
    host,
    port,
    ...(endpoint.tls === true ? { tls: true } : {}),
  };
}

function inferTransport(descriptor, endpoint) {
  if (descriptor?.meta?.capabilities?.transports && Array.isArray(descriptor.meta.capabilities.transports)) {
    const first = descriptor.meta.capabilities.transports.find((item) => typeof item === "string" && item.trim().length > 0);
    if (first) return first;
  }
  return endpoint ? "tcp" : "unknown";
}
