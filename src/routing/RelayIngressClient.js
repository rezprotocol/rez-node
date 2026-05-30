import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";

const DEFAULT_STREAM_TIMEOUT_MS = 10_000;
const DEFAULT_RECONNECT_MS = 1_000;
const DEFAULT_CLAIM_LIMIT = 200;

function normalizeUrl(value) {
  const text = String(value || "").trim().replace(/\/+$/, "");
  return text || null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampInt(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const rounded = Math.floor(num);
  return Math.max(min, Math.min(max, rounded));
}

async function hashBodyHex(crypto, bodyBytes) {
  const digest = await crypto.hashSha256(bodyBytes || new Uint8Array());
  return Buffer.from(digest).toString("hex");
}

export class RelayIngressClient {
  constructor({
    relaySources,
    localInboxId,
    signer,
    inboxStore,
    nodeId,
    logger = console,
    nowMs = () => Date.now(),
    fetchImpl = fetch,
    streamTimeoutMs = DEFAULT_STREAM_TIMEOUT_MS,
    reconnectDelayMs = DEFAULT_RECONNECT_MS,
  } = {}) {
    this.relaySources = Array.isArray(relaySources)
      ? relaySources.map((value) => normalizeUrl(value)).filter(Boolean)
      : [];
    this.localInboxId = String(localInboxId || "").trim();
    this.signer = signer;
    this.inboxStore = inboxStore;
    this.nodeId = String(nodeId || "").trim();
    this.logger = logger || console;
    this.nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
    this.fetchImpl = typeof fetchImpl === "function" ? fetchImpl : fetch;
    this.streamTimeoutMs = clampInt(streamTimeoutMs, 1_000, 60_000, DEFAULT_STREAM_TIMEOUT_MS);
    this.reconnectDelayMs = clampInt(reconnectDelayMs, 200, 60_000, DEFAULT_RECONNECT_MS);
    this.crypto = new NodeCryptoProvider();

    this._started = false;
    this._workers = [];
    this._seen = new Map();
  }

  async start() {
    if (this._started) return;
    if (this.relaySources.length === 0) return;
    if (!this.localInboxId || !this.inboxStore || typeof this.inboxStore.deposit !== "function") return;
    if (!this.signer || typeof this.signer.sign !== "function" || typeof this.signer.getSignerRef !== "function") return;

    this._started = true;
    for (const source of this.relaySources) {
      const worker = this._runSourceLoop(source).catch((err) => {
        console.error("[RelayIngressClient] source loop failed for", source, err && err.message ? err.message : err);
      });
      this._workers.push(worker);
    }
  }

  async stop() {
    this._started = false;
    await Promise.allSettled(this._workers);
    this._workers = [];
  }

  async _runSourceLoop(source) {
    let cursor = null;
    while (this._started) {
      try {
        cursor = await this._claimAndProcess(source, cursor);
        const streamUrl = new URL("/v1/ingress/stream", source);
        streamUrl.searchParams.set("inboxId", this.localInboxId);
        streamUrl.searchParams.set("timeoutMs", String(this.streamTimeoutMs));
        if (cursor) streamUrl.searchParams.set("cursor", cursor);
        const response = await this._request({
          source,
          method: "GET",
          url: streamUrl,
          bodyObj: null,
        });
        const item = response && response.item && typeof response.item === "object" ? response.item : null;
        const itemId = item ? String(item.id || "").trim() : "";
        if (!itemId) continue;
        cursor = await this._processMessage(source, itemId, cursor);
      } catch (err) {
        console.error("[RelayIngressClient] stream error for", source, err && err.message ? err.message : err);
        if (!this._started) return;
        await sleep(this.reconnectDelayMs);
      }
    }
  }

  async _claimAndProcess(source, cursor) {
    const claimUrl = new URL("/v1/ingress/claim", source);
    const claimed = await this._request({
      source,
      method: "POST",
      url: claimUrl,
      bodyObj: {
        inboxId: this.localInboxId,
        cursor: cursor || null,
        limit: DEFAULT_CLAIM_LIMIT,
      },
    }).catch((claimErr) => {
      console.error("[RelayIngressClient] claim request failed for source=" + source + ": " + (claimErr && claimErr.message ? claimErr.message : claimErr));
      return null;
    });
    const items = claimed && Array.isArray(claimed.items) ? claimed.items : [];
    let nextCursor = cursor;
    for (const item of items) {
      const itemId = item && item.id ? String(item.id).trim() : "";
      if (!itemId) continue;
      nextCursor = await this._processMessage(source, itemId, nextCursor);
    }
    return nextCursor;
  }

  async _processMessage(source, messageId, cursor) {
    const seenKey = `${source}|${messageId}`;
    if (this._seen.has(seenKey)) {
      return messageId;
    }
    const fetchUrl = new URL("/v1/ingress/fetch", source);
    const fetched = await this._request({
      source,
      method: "POST",
      url: fetchUrl,
      bodyObj: {
        inboxId: this.localInboxId,
        messageId,
      },
    });
    const payloadB64 = fetched && typeof fetched.payloadB64 === "string" ? fetched.payloadB64 : "";
    if (!payloadB64) return cursor;

    const payloadBytes = new Uint8Array(Buffer.from(payloadB64, "base64"));
    await this.inboxStore.depositFromWire(this.localInboxId, payloadBytes);
    this._seen.set(seenKey, this.nowMs());
    this._pruneSeen();

    const ackUrl = new URL("/v1/ingress/ack", source);
    await this._request({
      source,
      method: "POST",
      url: ackUrl,
      bodyObj: {
        inboxId: this.localInboxId,
        messageId,
      },
    }).catch(() => {});
    return messageId;
  }

  _pruneSeen() {
    const cutoff = this.nowMs() - 5 * 60_000;
    for (const [key, ts] of this._seen.entries()) {
      if (ts < cutoff) this._seen.delete(key);
    }
    while (this._seen.size > 10_000) {
      const first = this._seen.keys().next().value;
      this._seen.delete(first);
    }
  }

  async _request({ source, method, url, bodyObj }) {
    const bodyBytes = bodyObj == null
      ? new Uint8Array()
      : new Uint8Array(Buffer.from(JSON.stringify(bodyObj), "utf8"));
    const headers = await this._buildAuthHeaders({
      method,
      path: url.pathname,
      bodyBytes,
    });
    if (bodyObj != null) {
      headers["content-type"] = "application/json";
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.streamTimeoutMs + 2_000);
    try {
      const res = await this.fetchImpl(url, {
        method,
        headers,
        body: bodyObj == null ? undefined : Buffer.from(bodyBytes).toString("utf8"),
        signal: controller.signal,
      });
      if (!res.ok) {
        throw new Error(`ingress_${method.toLowerCase()}_http_${res.status}@${source}`);
      }
      return await res.json();
    } finally {
      clearTimeout(timeout);
    }
  }

  async _buildAuthHeaders({ method, path, bodyBytes }) {
    const signerRef = typeof this.signer.getSignerRef === "function" ? this.signer.getSignerRef() : null;
    if (!signerRef || typeof signerRef !== "object") {
      throw new Error("RelayIngressClient signer.getSignerRef() returned invalid result");
    }
    const publicKeyB64 = String(signerRef.publicKeyB64 || "").trim();
    if (!publicKeyB64) {
      throw new Error("RelayIngressClient signer.getSignerRef().publicKeyB64 required");
    }
    const ts = this.nowMs();
    const bodyHashHex = await hashBodyHex(this.crypto, bodyBytes);
    const canonical = [
      String(method || "").toUpperCase(),
      String(path || ""),
      this.nodeId,
      String(ts),
      bodyHashHex,
    ].join("\n");
    const canonicalBytes = new Uint8Array(Buffer.from(canonical, "utf8"));
    const sigBytes = await this.signer.sign(canonicalBytes);

    return {
      "x-rez-node-id": this.nodeId,
      "x-rez-ts": String(ts),
      "x-rez-pub": publicKeyB64,
      "x-rez-sig": Buffer.from(sigBytes).toString("base64url"),
    };
  }
}
