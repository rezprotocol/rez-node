import { randomUUID } from "node:crypto";
import { RouteEnvelopeV1 } from "../contracts/records/RouteEnvelopeV1.js";
import { RouteQueryV1 } from "../contracts/records/RouteQueryV1.js";
import { RouteReplyV1 } from "../contracts/records/RouteReplyV1.js";
import { bytesToBase64, base64ToBytes, isNonEmptyString } from "@rezprotocol/core";

const DEFAULT_POLICY = Object.freeze({
  routeQueryTtl: 3,
  routeQueryTimeoutMs: 600,
  routeCacheTtlMs: 60_000,
  packetTtl: 20,
  minHops: 2,
  maxHops: 6,
  seenCacheTtlMs: 5 * 60_000,
  maxCacheEntries: 10_000,
});

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      try {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve(text ? JSON.parse(text) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(payload));
}

function normalizeUrl(value) {
  const text = String(value || "").trim().replace(/\/+$/, "");
  return text || null;
}

function clampInt(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const rounded = Math.floor(num);
  return Math.max(min, Math.min(max, rounded));
}

function firstHttpEndpoint(descriptor) {
  if (!descriptor || !Array.isArray(descriptor.endpoints) || descriptor.endpoints.length === 0) return null;
  for (const endpoint of descriptor.endpoints) {
    const host = String((endpoint && endpoint.host) || "").trim();
    const port = Number(endpoint && endpoint.port);
    if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) continue;
    return `http://${host}:${port}`;
  }
  return null;
}

export class RoutingEngine {
  constructor({
    nodeId,
    localInboxId,
    relayStore = null,
    routeTable = null,
    gatewayLoop = null,
    inboxStore = null,
    logger = console,
    nowMs = () => Date.now(),
    fetchImpl = fetch,
    policy = {},
  } = {}) {
    this.nodeId = String(nodeId || "").trim() || `node:${Math.random().toString(16).slice(2, 10)}`;
    this.localInboxId = String(localInboxId || "").trim();
    this.relayStore = relayStore;
    this.routeTable = routeTable || null;
    this.gatewayLoop = gatewayLoop;
    this.inboxStore = inboxStore;
    this.logger = logger || console;
    this.nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
    this.fetchImpl = typeof fetchImpl === "function" ? fetchImpl : fetch;
    this.policy = {
      ...DEFAULT_POLICY,
      ...(policy && typeof policy === "object" ? policy : {}),
    };
    this.publicRouteBaseUrl = null;
    this.localHandles = new Set();
    if (this.localInboxId) {
      this.localHandles.add(this.localInboxId);
    }
    this.localPayloadHandlers = new Map();

    this.routeCache = new Map();
    this.seenPackets = new Map();
    this.seenQueries = new Map();
  }

  setPublicRouteBaseUrl(value) {
    this.publicRouteBaseUrl = normalizeUrl(value);
  }

  registerLocalHandle(handle) {
    const target = String(handle || "").trim();
    if (!target) return;
    this.localHandles.add(target);
  }

  unregisterLocalHandle(handle) {
    const target = String(handle || "").trim();
    if (!target) return;
    if (target !== this.localInboxId) {
      this.localHandles.delete(target);
    }
    this.localPayloadHandlers.delete(target);
  }

  registerLocalPayloadHandler(handle, handler) {
    const target = String(handle || "").trim();
    if (!target || typeof handler !== "function") {
      return () => {};
    }
    this.localHandles.add(target);
    let handlers = this.localPayloadHandlers.get(target);
    if (!handlers) {
      handlers = new Set();
      this.localPayloadHandlers.set(target, handlers);
    }
    handlers.add(handler);
    return () => this.unregisterLocalPayloadHandler(target, handler);
  }

  unregisterLocalPayloadHandler(handle, handler) {
    const target = String(handle || "").trim();
    if (!target || typeof handler !== "function") return;
    const handlers = this.localPayloadHandlers.get(target);
    if (!handlers) return;
    handlers.delete(handler);
    if (handlers.size === 0) {
      this.localPayloadHandlers.delete(target);
    }
  }

  isLocalTarget(targetHandle) {
    const target = String(targetHandle || "").trim();
    if (!target) return false;
    if (this.localHandles.has(target)) return true;
    return this.localInboxId.length > 0 && target === this.localInboxId;
  }

  getStatus() {
    return {
      nodeId: this.nodeId,
      localHandleCount: this.localHandles.size,
      localHandlerHandleCount: this.localPayloadHandlers.size,
      cachedRoutes: this.routeCache.size,
      seenPackets: this.seenPackets.size,
      seenQueries: this.seenQueries.size,
      publicRouteBaseUrl: this.publicRouteBaseUrl,
    };
  }

  async handleHttpRequest(req, res) {
    const url = new URL(req.url || "/", `http://${req.headers.host || "127.0.0.1"}`);
    const ingressBaseUrl = normalizeUrl(`http://${req.headers.host || ""}`) || this.publicRouteBaseUrl;

    if (req.method === "POST" && url.pathname === "/v1/routing/query") {
      let body;
      try {
        body = await parseJsonBody(req);
      } catch {
        sendJson(res, 400, { error: "invalid_json" });
        return true;
      }
      try {
        const reply = await this.handleRouteQuery({
          query: (body && body.query) || body,
          fromPeerUrl: normalizeUrl(body && body.fromPeerUrl),
          ingressBaseUrl,
        });
        sendJson(res, 200, { reply: reply.toJSON() });
      } catch (err) {
        sendJson(res, 400, { error: (err && err.message) || "invalid_query" });
      }
      return true;
    }

    if (req.method === "POST" && url.pathname === "/v1/routing/forward") {
      let body;
      try {
        body = await parseJsonBody(req);
      } catch {
        sendJson(res, 400, { error: "invalid_json" });
        return true;
      }
      try {
        const result = await this.forwardEnvelope({
          envelope: (body && body.envelope) || body,
          fromPeerUrl: normalizeUrl(body && body.fromPeerUrl),
        });
        sendJson(res, 202, { ok: true, mode: result.mode });
      } catch (err) {
        if (this.logger && this.logger.warn) this.logger.warn("RoutingEngine forward failed", err);
        sendJson(res, 400, { error: (err && err.message) || "forward_failed" });
      }
      return true;
    }

    return false;
  }

  async routePayload({ targetHandle, payloadBytes, packetId = null, ttl = null } = {}) {
    if (!(payloadBytes instanceof Uint8Array)) {
      throw new Error("route payloadBytes must be Uint8Array");
    }
    const target = String(targetHandle || "").trim();
    if (!target) throw new Error("route targetHandle is required");

    if (this.isLocalTarget(target)) {
      await this._deliverLocal({ targetHandle: target, payloadBytes });
      return { mode: "local", packetId: packetId || null };
    }

    const envelope = new RouteEnvelopeV1({
      packetId: packetId || randomUUID(),
      targetHandle: target,
      payloadB64: bytesToBase64(payloadBytes),
      ttl: Number.isInteger(ttl) ? ttl : clampInt(this.policy.packetTtl, 1, 255, DEFAULT_POLICY.packetTtl),
      originNodeId: this.nodeId,
      hops: [this.nodeId],
      createdAtMs: this.nowMs(),
    });
    envelope.validate();

    return this.forwardEnvelope({ envelope });
  }

  async handleRouteQuery({ query, fromPeerUrl = null, ingressBaseUrl = null } = {}) {
    const record = query instanceof RouteQueryV1 ? query : new RouteQueryV1(query);
    record.validate();
    this._markSeenQuery(record.queryId);

    if (this.isLocalTarget(record.targetHandle)) {
      const localUrl = normalizeUrl(ingressBaseUrl) || this.publicRouteBaseUrl;
      if (localUrl) {
        return new RouteReplyV1({
          queryId: record.queryId,
          targetHandle: record.targetHandle,
          responderNodeId: this.nodeId,
          found: true,
          nextHopNodeId: this.nodeId,
          nextHopUrl: localUrl,
          path: [this.nodeId],
          cacheTtlMs: clampInt(this.policy.routeCacheTtlMs, 1_000, 3_600_000, DEFAULT_POLICY.routeCacheTtlMs),
          createdAtMs: this.nowMs(),
        });
      }
    }

    const cached = this._getCachedRoute(record.targetHandle, [fromPeerUrl]);
    if (cached) {
      return new RouteReplyV1({
        queryId: record.queryId,
        targetHandle: record.targetHandle,
        responderNodeId: this.nodeId,
        found: true,
        nextHopNodeId: cached.nodeId,
        nextHopUrl: cached.routeBaseUrl,
        path: [this.nodeId, cached.nodeId].filter(Boolean),
        cacheTtlMs: Math.max(0, cached.expiresAtMs - this.nowMs()),
        createdAtMs: this.nowMs(),
      });
    }

    if (record.ttl <= 0) {
      return new RouteReplyV1({
        queryId: record.queryId,
        targetHandle: record.targetHandle,
        responderNodeId: this.nodeId,
        found: false,
        createdAtMs: this.nowMs(),
      });
    }

    const visited = new Set([...(record.visited || []), this.nodeId]);
    const discovered = await this._queryPeers({
      targetHandle: record.targetHandle,
      queryId: record.queryId,
      ttl: record.ttl - 1,
      visited,
      excludePeerUrls: [fromPeerUrl],
    });
    if (!discovered) {
      return new RouteReplyV1({
        queryId: record.queryId,
        targetHandle: record.targetHandle,
        responderNodeId: this.nodeId,
        found: false,
        createdAtMs: this.nowMs(),
      });
    }

    this._cacheRoute(record.targetHandle, discovered.peer);
    return new RouteReplyV1({
      queryId: record.queryId,
      targetHandle: record.targetHandle,
      responderNodeId: this.nodeId,
      found: true,
      nextHopNodeId: discovered.peer.nodeId,
      nextHopUrl: discovered.peer.routeBaseUrl,
      path: [this.nodeId, ...(Array.isArray(discovered.reply.path) ? discovered.reply.path : [])],
      cacheTtlMs: clampInt(discovered.reply.cacheTtlMs, 1_000, 3_600_000, DEFAULT_POLICY.routeCacheTtlMs),
      createdAtMs: this.nowMs(),
    });
  }

  async forwardEnvelope({ envelope, fromPeerUrl = null } = {}) {
    const record = envelope instanceof RouteEnvelopeV1 ? envelope : new RouteEnvelopeV1(envelope);
    record.validate();

    if (this._isSeenPacket(record.packetId)) {
      return { mode: "duplicate", packetId: record.packetId };
    }
    this._markSeenPacket(record.packetId);

    if (this.isLocalTarget(record.targetHandle)) {
      await this._deliverLocal({ targetHandle: record.targetHandle, payloadBytes: base64ToBytes(record.payloadB64) });
      return { mode: "local-deliver", packetId: record.packetId };
    }
    if (record.ttl <= 0) {
      return { mode: "dropped-ttl", packetId: record.packetId };
    }

    const nextHop = await this.resolveNextHop({
      targetHandle: record.targetHandle,
      excludePeerUrls: [fromPeerUrl],
    });
    if (nextHop) {
      const forwarded = new RouteEnvelopeV1({
        ...record.toJSON(),
        ttl: record.ttl - 1,
        hops: [...record.hops, this.nodeId],
      });
      await this._postJson(`${nextHop.routeBaseUrl}/v1/routing/forward`, {
        envelope: forwarded.toJSON(),
        fromPeerUrl: this.publicRouteBaseUrl,
      }, clampInt(this.policy.routeQueryTimeoutMs, 100, 30_000, DEFAULT_POLICY.routeQueryTimeoutMs));
      return { mode: "forwarded", packetId: record.packetId, nextHopUrl: nextHop.routeBaseUrl };
    }

    const fallbackOk = await this._forwardViaGateway(record);
    if (fallbackOk) {
      return { mode: "fallback-gateway", packetId: record.packetId };
    }
    return { mode: "unresolved", packetId: record.packetId };
  }

  async resolveNextHop({ targetHandle, excludePeerUrls = [] } = {}) {
    const cached = this._getCachedRoute(targetHandle, excludePeerUrls);
    if (cached) return cached;

    // Check the shared RouteTable — if the relay layer already knows a route,
    // derive the peer info from the relay store descriptor and skip HTTP query.
    if (this.routeTable && this.relayStore) {
      const tcpRoute = this.routeTable.get(targetHandle);
      if (tcpRoute) {
        const deliveryRelayKeyId = tcpRoute.deliveryRelayKeyId || tcpRoute.relayKeyId || "";
        if (deliveryRelayKeyId) {
          const descriptor = typeof this.relayStore.getDescriptorByKeyId === "function"
            ? this.relayStore.getDescriptorByKeyId(deliveryRelayKeyId)
            : null;
          if (descriptor) {
            const routeBaseUrl = normalizeUrl(
              (descriptor.meta && descriptor.meta.node && descriptor.meta.node.routeBaseUrl)
              || firstHttpEndpoint(descriptor),
            );
            if (routeBaseUrl) {
              const peer = {
                nodeId: deliveryRelayKeyId,
                routeBaseUrl,
                expiresAtMs: this.nowMs() + clampInt(this.policy.routeCacheTtlMs, 1_000, 3_600_000, DEFAULT_POLICY.routeCacheTtlMs),
              };
              this._cacheRoute(targetHandle, peer);
              return peer;
            }
          }
        }
      }
    }

    const discovered = await this._queryPeers({
      targetHandle,
      queryId: randomUUID(),
      ttl: clampInt(this.policy.routeQueryTtl, 1, 32, DEFAULT_POLICY.routeQueryTtl),
      visited: new Set([this.nodeId]),
      excludePeerUrls,
    });
    if (!discovered) return null;
    this._cacheRoute(targetHandle, discovered.peer);
    return discovered.peer;
  }

  async _queryPeers({ targetHandle, queryId, ttl, visited, excludePeerUrls = [] } = {}) {
    const peers = this._candidatePeers().filter((peer) => {
      if (!peer.routeBaseUrl) return false;
      if (excludePeerUrls.includes(peer.routeBaseUrl)) return false;
      if (visited && visited.has(peer.nodeId)) return false;
      return true;
    });
    if (peers.length === 0) return null;

    const query = new RouteQueryV1({
      queryId,
      targetHandle,
      requesterNodeId: this.nodeId,
      ttl: Math.max(0, Number(ttl) || 0),
      visited: [...(visited || [])],
      createdAtMs: this.nowMs(),
    });

    const tasks = peers.map(async (peer) => {
      const res = await this._postJson(
        `${peer.routeBaseUrl}/v1/routing/query`,
        { query: query.toJSON(), fromPeerUrl: this.publicRouteBaseUrl },
        clampInt(this.policy.routeQueryTimeoutMs, 100, 30_000, DEFAULT_POLICY.routeQueryTimeoutMs),
      );
      const reply = new RouteReplyV1((res && res.reply) || {});
      reply.validate();
      if (!reply.found) {
        throw new Error("route_not_found");
      }
      return { peer, reply };
    });

    try {
      return await Promise.any(tasks);
    } catch {
      return null;
    }
  }

  _candidatePeers() {
    const peers = [];
    const seen = new Set();
    const relays = (this.relayStore && typeof this.relayStore.getAll === "function") ? this.relayStore.getAll() : [];
    for (const relay of relays) {
      const descriptor = (relay && relay.descriptor) || relay;
      const meta = descriptor && descriptor.meta;
      const metaNode = meta && meta.node;
      const nodeId = String((metaNode && metaNode.nodeId) || (descriptor && descriptor.relayKeyId) || (relay && relay.relayKeyId) || (relay && relay.id) || "").trim();
      if (!nodeId || nodeId === this.nodeId) continue;
      let routeBaseUrl = normalizeUrl(metaNode && metaNode.routeBaseUrl);
      if (!routeBaseUrl) {
        const capabilities = meta && meta.capabilities;
        const transports = capabilities && capabilities.transports;
        if (Array.isArray(transports) && transports.includes("http")) {
          routeBaseUrl = normalizeUrl(firstHttpEndpoint(descriptor));
        }
      }
      if (!routeBaseUrl || seen.has(routeBaseUrl)) continue;
      seen.add(routeBaseUrl);
      peers.push({ nodeId, routeBaseUrl });
    }
    return peers;
  }

  async _deliverLocal({ targetHandle, payloadBytes } = {}) {
    const handled = await this._dispatchLocalPayloadHandlers({
      targetHandle,
      payloadBytes,
    });
    if (handled) return;
    if (this.inboxStore && typeof this.inboxStore.deposit === "function" && isNonEmptyString(targetHandle)) {
      await this.inboxStore.depositFromWire(targetHandle, payloadBytes);
      return;
    }
    throw new Error("local delivery unavailable");
  }

  async _dispatchLocalPayloadHandlers({ targetHandle, payloadBytes } = {}) {
    const target = String(targetHandle || "").trim();
    if (!target) return false;
    const handlers = this.localPayloadHandlers.get(target);
    if (!handlers || handlers.size === 0) return false;
    for (const handler of [...handlers]) {
      try {
        const handled = await handler({ targetHandle: target, payloadBytes });
        if (handled === true) return true;
      } catch (err) {
        if (this.logger && this.logger.warn) {
          this.logger.warn("RoutingEngine local payload handler failed", {
            handle: target,
            err: (err && err.message) || err,
          });
        }
      }
    }
    return false;
  }

  _getCachedRoute(targetHandle, excludePeerUrls = []) {
    this._pruneCaches();
    const key = String(targetHandle || "").trim();
    const entry = this.routeCache.get(key);
    if (!entry) return null;
    if (entry.expiresAtMs <= this.nowMs()) {
      this.routeCache.delete(key);
      return null;
    }
    if (excludePeerUrls.includes(entry.routeBaseUrl)) return null;
    return entry;
  }

  _cacheRoute(targetHandle, peer) {
    const key = String(targetHandle || "").trim();
    if (!key || !peer || !peer.routeBaseUrl) return;
    const ttlMs = clampInt(this.policy.routeCacheTtlMs, 1_000, 3_600_000, DEFAULT_POLICY.routeCacheTtlMs);
    this.routeCache.set(key, {
      nodeId: String(peer.nodeId || "").trim() || "unknown",
      routeBaseUrl: peer.routeBaseUrl,
      expiresAtMs: this.nowMs() + ttlMs,
    });
    this._pruneCaches();
  }

  async _forwardViaGateway(envelope) {
    if (!this.gatewayLoop || typeof this.gatewayLoop.sendToInbox !== "function") return false;
    const innerBytes = base64ToBytes(envelope.payloadB64);
    await this.gatewayLoop.sendToInbox({
      innerBytes,
      deliverInboxId: envelope.targetHandle,
      minHops: clampInt(this.policy.minHops, 1, 16, DEFAULT_POLICY.minHops),
      maxHops: clampInt(this.policy.maxHops, 1, 16, DEFAULT_POLICY.maxHops),
    });
    return true;
  }

  _isSeenPacket(packetId) {
    const entry = this.seenPackets.get(packetId);
    if (!entry) return false;
    if (entry <= this.nowMs()) {
      this.seenPackets.delete(packetId);
      return false;
    }
    return true;
  }

  _markSeenPacket(packetId) {
    if (!packetId) return;
    const ttlMs = clampInt(this.policy.seenCacheTtlMs, 1_000, 3_600_000, DEFAULT_POLICY.seenCacheTtlMs);
    this.seenPackets.set(packetId, this.nowMs() + ttlMs);
    this._pruneCaches();
  }

  _markSeenQuery(queryId) {
    if (!queryId) return;
    const ttlMs = clampInt(this.policy.seenCacheTtlMs, 1_000, 3_600_000, DEFAULT_POLICY.seenCacheTtlMs);
    this.seenQueries.set(queryId, this.nowMs() + ttlMs);
    this._pruneCaches();
  }

  _pruneCaches() {
    const nowMs = this.nowMs();
    for (const [key, expiresAtMs] of this.seenPackets.entries()) {
      if (expiresAtMs <= nowMs) this.seenPackets.delete(key);
    }
    for (const [key, expiresAtMs] of this.seenQueries.entries()) {
      if (expiresAtMs <= nowMs) this.seenQueries.delete(key);
    }
    for (const [key, entry] of this.routeCache.entries()) {
      if (Number(entry && entry.expiresAtMs) <= nowMs) this.routeCache.delete(key);
    }

    const maxEntries = clampInt(this.policy.maxCacheEntries, 100, 1_000_000, DEFAULT_POLICY.maxCacheEntries);
    while (this.seenPackets.size > maxEntries) {
      const first = this.seenPackets.keys().next().value;
      this.seenPackets.delete(first);
    }
    while (this.seenQueries.size > maxEntries) {
      const first = this.seenQueries.keys().next().value;
      this.seenQueries.delete(first);
    }
    while (this.routeCache.size > maxEntries) {
      const first = this.routeCache.keys().next().value;
      this.routeCache.delete(first);
    }
  }

  async _postJson(url, payload, timeoutMs) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await this.fetchImpl(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload || {}),
        signal: controller.signal,
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      return await res.json();
    } finally {
      clearTimeout(timer);
    }
  }
}
