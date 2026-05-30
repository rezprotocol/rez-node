import { TcpConnectionManager } from "./tcp/TcpConnectionManager.js";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { base64ToBytes } from "@rezprotocol/core";
import {
  PEER_AUTH_PROTOCOL_VERSION,
  derivePeerAuth,
  meshPeerAcceptPayload,
  meshPeerAuthPayload,
  meshPeerChallengePayload,
  signedPayloadBytes,
} from "../relay/PeerAuthShared.js";

const PEER_AUTH_CRYPTO = new NodeCryptoProvider();

function timestampText() {
  const now = new Date();
  const pad = (value, size = 2) => String(value).padStart(size, "0");
  return `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}.${pad(now.getMilliseconds(), 3)}`;
}

function poolLog(method, ...args) {
  const writer = console && typeof console[method] === "function" ? console[method] : console.log;
  writer(`${timestampText()} [POOL]`, ...args);
}

function isLocalhostHost(host) {
  return host === "localhost" || host === "127.0.0.1" || host === "::1";
}

function parseEndpoint(endpoint) {
  if (!endpoint || typeof endpoint !== "object") return null;
  const host = typeof endpoint.host === "string" ? endpoint.host.trim() : "";
  const port = Number(endpoint.port);
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  // Explicit tls flag takes precedence.
  // For non-localhost endpoints with no explicit tls flag, default to TLS.
  const tlsExplicit = endpoint.tls === true || endpoint.tls === false;
  const useTls = tlsExplicit ? endpoint.tls : !isLocalhostHost(host);
  return { host, port, tls: useTls, tlsAuto: !tlsExplicit && useTls };
}

function endpointKey(endpoint) {
  if (!endpoint) return null;
  const protocol = endpoint.tls === true ? "tls" : "tcp";
  return `${protocol}://${endpoint.host}:${endpoint.port}`;
}

function parseConnectionKey(endpointStr) {
  if (typeof endpointStr !== "string") return null;
  const text = endpointStr.trim();
  if (!text) return null;
  if (text.startsWith("tls://") || text.startsWith("tcp://")) {
    const protocol = text.startsWith("tls://") ? "tls" : "tcp";
    const body = text.slice(protocol.length + 3);
    const idx = body.lastIndexOf(":");
    if (idx <= 0) return null;
    const host = body.slice(0, idx);
    const port = Number(body.slice(idx + 1));
    if (!host || !Number.isInteger(port) || port <= 0) return null;
    return { host, port, tls: protocol === "tls", tlsAuto: protocol === "tls" && !isLocalhostHost(host) };
  }
  const idx = text.lastIndexOf(":");
  if (idx <= 0) return null;
  const host = text.slice(0, idx);
  const port = Number(text.slice(idx + 1));
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  return { host, port, tls: false, tlsAuto: false };
}

export class RelayConnectionPool {
  constructor({
    inboxIds = [],
    getInboxIds = null,
    getRegistrations = null,
    inboxStore = null,
    inboxRouter = null,
    relayPeerDirectory = null,
    relayStore = null,
    relayKeyId = null,
    advertisedRelayKeyId = null,
    nodeKeyId = null,
    nodePublicKeyB64 = null,
    nodePrivateKeyB64 = null,
    getSelfDescriptor = null,
    frameRouter = null,
    onInboundFrame = null,
    maxConnections = 32,
    idleTimeoutMs = 600_000,
    keepAliveInitialDelayMs = 30_000,
  } = {}) {
    this.#inboxIds = Array.isArray(inboxIds) ? inboxIds.filter((id) => typeof id === "string" && id.trim()) : [];
    this.#getInboxIds = typeof getInboxIds === "function" ? getInboxIds : null;
    this.#getRegistrations = typeof getRegistrations === "function" ? getRegistrations : null;
    this.#inboxStore = inboxStore;
    this.#inboxRouter = inboxRouter ?? null;
    this.#relayKeyId = typeof relayKeyId === "string" && relayKeyId.trim() ? relayKeyId.trim() : null;
    this.#advertisedRelayKeyId = typeof advertisedRelayKeyId === "string" && advertisedRelayKeyId.trim()
      ? advertisedRelayKeyId.trim()
      : null;
    this.#relayPeerDirectory = relayPeerDirectory ?? null;
    this.#relayStore = relayStore ?? null;
    this.#nodeKeyId = typeof nodeKeyId === "string" ? nodeKeyId.trim() : "";
    this.#nodePublicKeyB64 = typeof nodePublicKeyB64 === "string" ? nodePublicKeyB64.trim() : "";
    this.#nodePrivateKey = typeof nodePrivateKeyB64 === "string" && nodePrivateKeyB64.trim()
      ? base64ToBytes(nodePrivateKeyB64.trim())
      : null;
    this.#getSelfDescriptor = typeof getSelfDescriptor === "function" ? getSelfDescriptor : null;
    this.#frameRouter = frameRouter ?? null;
    this.#descriptorExchange = null;
    this.#relayIdByKey = new Map();
    this.#expectedRelayIdByKey = new Map();
    this.#registrations = new Map();
    this.#peerAuthStates = new Map();
    this.#closed = false;
    this.#lastRegisteredInboxIds = new Set();

    const self = this;
    function dispatchInbound(bytes, socket) {
      if (self.#capturePeerAuthFrame(bytes, socket)) {
        return;
      }
      if (self.#frameRouter) {
        self.#frameRouter.dispatch(bytes, socket).catch((err) => {
          poolLog("error", "frame router dispatch error", err && err.message ? err.message : err);
        });
        return;
      }
      // Legacy path: no frame router — only handle route/withdraw for inboxRouter, else deposit
      if (self.#inboxRouter) {
        try {
          const text = new TextDecoder().decode(bytes);
          const obj = JSON.parse(text);
          if (obj && typeof obj === "object" && typeof obj._ctl === "string") {
            if (obj._ctl === "inbox.route" || obj._ctl === "inbox.withdraw") {
              self.#inboxRouter.handleControlMessage(obj, socket);
              return;
            }
          }
        } catch {
          // not JSON or not route/withdraw — fall through
        }
      }
      const depositCallback = typeof onInboundFrame === "function"
        ? onInboundFrame
        : (b) => {
            if (!self.#getInboxIds && self.#inboxIds.length > 0 && self.#inboxStore) {
              self.#inboxStore.depositFromWire(self.#inboxIds[0], b).catch((err) => {
                poolLog("error", "deposit error", err && err.message ? err.message : err);
              });
              return;
            }
            self.#defaultDemux(b);
          };
      depositCallback(bytes);
    }

    this.#manager = new TcpConnectionManager({
      resolve: (endpointStr) => parseConnectionKey(endpointStr),
      maxConnections,
      idleTimeoutMs,
      keepAliveInitialDelayMs: Number.isFinite(Number(keepAliveInitialDelayMs)) && keepAliveInitialDelayMs > 0 ? keepAliveInitialDelayMs : 0,
      onConnectionOpen: (_key, _socket) => {},
      onConnectionClose: (key, socket) => {
        this.#dropRelayMappingsForConnectionKey(key);
        const peerAuthState = this.#peerAuthStates.get(key);
        if (peerAuthState && peerAuthState.timeout) clearTimeout(peerAuthState.timeout);
        this.#peerAuthStates.delete(key);
        if (this.#inboxRouter) this.#inboxRouter.removeConnection(socket);
        if (this.#descriptorExchange) this.#descriptorExchange.removePeer(socket);
      },
      onInboundFrame: (bytes, socket) => {
        this.#touchConnectionForSocket(socket);
        dispatchInbound(bytes, socket);
      },
    });
  }

  #inboxIds;
  #getInboxIds;
  #getRegistrations;
  #inboxStore;
  #inboxRouter;
  #relayKeyId;
  #advertisedRelayKeyId;
  #relayPeerDirectory;
  #relayStore;
  #nodeKeyId;
  #nodePublicKeyB64;
  #nodePrivateKey;
  #getSelfDescriptor;
  #frameRouter;
  #descriptorExchange;
  #manager;
  #relayIdByKey;
  #expectedRelayIdByKey;
  #registrations;
  #peerAuthStates;
  #closed;
  #lastRegisteredInboxIds;

  setDescriptorExchange(exchange) {
    this.#descriptorExchange = exchange ?? null;
  }

  #currentSelfDescriptor() {
    if (this.#getSelfDescriptor) {
      const descriptor = this.#getSelfDescriptor();
      return descriptor && typeof descriptor === "object" ? descriptor : null;
    }
    const descriptor = this.#relayStore && typeof this.#relayStore.getSelfDescriptor === "function"
      ? this.#relayStore.getSelfDescriptor({ nowMs: Date.now() })
      : null;
    if (!descriptor) return null;
    return typeof descriptor.toJSON === "function" ? descriptor.toJSON() : descriptor;
  }

  #findConnectionKeyForRelayId(relayKeyId) {
    const direct = this.#relayIdByKey.get(relayKeyId);
    if (direct && this.#manager.connections.has(direct)) {
      return direct;
    }
    for (const [key, conn] of this.#manager.connections.entries()) {
      const auth = this.#relayPeerDirectory && typeof this.#relayPeerDirectory.getAuth === "function"
        ? this.#relayPeerDirectory.getAuth(conn.socket)
        : null;
      if (auth && auth.authLevel === "relay-verified" && auth.relayKeyId === relayKeyId) {
        this.#relayIdByKey.set(relayKeyId, key);
        return key;
      }
    }
    return null;
  }

  #currentInboxIds() {
    if (this.#getInboxIds) {
      const ids = this.#getInboxIds();
      return Array.isArray(ids) ? ids.filter((id) => typeof id === "string" && id.trim()) : [];
    }
    return this.#inboxIds;
  }

  #currentRegistrations() {
    if (!this.#getRegistrations) return [];
    const registrations = this.#getRegistrations();
    return Array.isArray(registrations)
      ? registrations.filter((entry) => entry && typeof entry === "object")
      : [];
  }

  #defaultDemux(bytes) {
    if (!this.#inboxStore || !(bytes instanceof Uint8Array) || bytes.length === 0) return;
    let text;
    try {
      text = new TextDecoder().decode(bytes);
    } catch {
      return;
    }
    let obj;
    try {
      obj = JSON.parse(text);
    } catch {
      return;
    }
    if (!obj || typeof obj !== "object" || obj._ctl !== "inbox.deposit") return;
    const inboxId = typeof obj.inboxId === "string" && obj.inboxId.trim() ? obj.inboxId.trim() : null;
    if (!inboxId) return;
    const hosted = new Set(this.#currentInboxIds());
    if (!hosted.has(inboxId)) return;
    if (typeof obj.inner !== "string") return;
    let innerBytes;
    try {
      innerBytes = new Uint8Array(Buffer.from(obj.inner, "base64"));
    } catch {
      return;
    }
      this.#inboxStore.depositFromWire(inboxId, innerBytes).catch((err) => {
      poolLog("error", "deposit error", err && err.message ? err.message : err);
    });
  }

  get connectionCount() {
    return this.#manager.connections.size;
  }

  listActiveConnectionEndpoints() {
    const endpoints = [];
    for (const conn of this.#manager.connections.values()) {
      if (!conn || !conn.socket || conn.socket.destroyed) continue;
      endpoints.push({
        host: conn.host,
        port: conn.port,
        tls: conn.tls === true,
      });
    }
    return endpoints;
  }

  #touchConnectionForSocket(socket) {
    if (!socket) return;
    for (const conn of this.#manager.connections.values()) {
      if (conn.socket === socket) {
        conn.lastUsed = Date.now();
        return;
      }
    }
  }

  #connectionKeyForSocket(socket) {
    if (!socket) return null;
    for (const [key, conn] of this.#manager.connections.entries()) {
      if (conn.socket === socket) return key;
    }
    return null;
  }

  #dropRelayMappingsForConnectionKey(key) {
    if (!key) return;
    this.#expectedRelayIdByKey.delete(key);
    for (const [relayKeyId, mappedKey] of this.#relayIdByKey.entries()) {
      if (mappedKey === key) this.#relayIdByKey.delete(relayKeyId);
    }
  }

  #capturePeerAuthFrame(bytes, socket) {
    if (!(bytes instanceof Uint8Array) || bytes.length === 0 || !socket) return false;
    let obj = null;
    try {
      obj = JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      return false;
    }
    if (!obj || (obj._ctl !== "peer.challenge" && obj._ctl !== "peer.accept")) return false;
    const key = this.#connectionKeyForSocket(socket);
    if (!key) return false;
    const state = this.#peerAuthStates.get(key);
    if (!state) return true;
    if (obj._ctl === "peer.challenge" && state.identifySent === true) return true;
    if (obj._ctl === "peer.accept" && state.accepted === true) return true;
    try {
      if (obj._ctl === "peer.challenge") {
        const protocolVersion = Number(obj.protocolVersion);
        const challengeId = typeof obj.challengeId === "string" ? obj.challengeId.trim() : "";
        const nonceB64 = typeof obj.nonceB64 === "string" ? obj.nonceB64.trim() : "";
        const nodeKeyId = typeof obj.nodeKeyId === "string" ? obj.nodeKeyId.trim() : "";
        const nodePublicKeyB64 = typeof obj.nodePublicKeyB64 === "string" ? obj.nodePublicKeyB64.trim() : "";
        const signatureB64 = typeof obj.signatureB64 === "string" ? obj.signatureB64.trim() : "";
        const relayKeyId = typeof obj.relayKeyId === "string" && obj.relayKeyId.trim() ? obj.relayKeyId.trim() : null;
        const expiresAtMs = Number(obj.expiresAtMs);
        if (
          protocolVersion !== PEER_AUTH_PROTOCOL_VERSION
          || !challengeId
          || !nonceB64
          || !nodeKeyId
          || !nodePublicKeyB64
          || !signatureB64
          || !Number.isFinite(expiresAtMs)
          || expiresAtMs <= Date.now()
        ) {
          throw new Error("peer challenge invalid");
        }
        const remotePublicKey = base64ToBytes(nodePublicKeyB64);
        const remoteSignature = base64ToBytes(signatureB64);
        const expectedRelayKeyId = this.#expectedRelayIdByKey.get(key) || null;
        if (expectedRelayKeyId && relayKeyId !== expectedRelayKeyId) {
          throw new Error("peer challenge relay mismatch");
        }
        let remoteKnown = false;
        if (relayKeyId) {
          const descriptor = this.#relayStore && typeof this.#relayStore.getDescriptor === "function"
            ? this.#relayStore.getDescriptor(relayKeyId, { nowMs: Date.now() })
            : null;
          if (descriptor) {
            const meta = descriptor.meta && typeof descriptor.meta === "object" ? descriptor.meta : {};
            const node = meta.node && typeof meta.node === "object" ? meta.node : {};
            const descriptorKeyId = typeof node.keyId === "string" ? node.keyId.trim() : "";
            const descriptorPublicKeyB64 = typeof node.publicKeyB64 === "string" ? node.publicKeyB64.trim() : "";
            if (descriptorKeyId !== nodeKeyId || descriptorPublicKeyB64 !== nodePublicKeyB64) {
              throw new Error("peer challenge descriptor mismatch");
            }
            remoteKnown = true;
          }
        }
        const verified = PEER_AUTH_CRYPTO.verify({
          publicKey: remotePublicKey,
          msg: signedPayloadBytes(meshPeerChallengePayload({
            challengeId,
            nonceB64,
            relayKeyId,
            nodeKeyId,
          })),
          sig: remoteSignature,
        });
        if (verified !== true) {
          throw new Error("peer challenge signature invalid");
        }
        state.challengeId = challengeId;
        state.nonceB64 = nonceB64;
        state.remote = {
          relayKeyId,
          nodeKeyId,
          nodePublicKeyB64,
          knownRelay: remoteKnown,
        };
        const identifyBytes = new TextEncoder().encode(JSON.stringify({
          _ctl: "peer.identify",
          protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
          relayKeyId: this.#advertisedRelayKeyId || undefined,
          nodeKeyId: this.#nodeKeyId,
          nodePublicKeyB64: this.#nodePublicKeyB64,
          challengeId,
          signatureB64: Buffer.from(PEER_AUTH_CRYPTO.sign({
            privateKey: this.#nodePrivateKey,
            msg: signedPayloadBytes(meshPeerAuthPayload({
              challengeId,
              nonceB64,
              relayKeyId: this.#advertisedRelayKeyId,
              nodeKeyId: this.#nodeKeyId,
            })),
          })).toString("base64"),
        }));
        this.#manager.send(key, identifyBytes)
          .then(() => {
            state.identifySent = true;
          })
          .catch((err) => {
            clearTimeout(state.timeout);
            state.reject(err);
            this.#peerAuthStates.delete(key);
            try { socket.destroy(); } catch {}
          });
        return true;
      }

      const protocolVersion = Number(obj.protocolVersion);
      const challengeId = typeof obj.challengeId === "string" ? obj.challengeId.trim() : "";
      const acceptedAs = typeof obj.acceptedAs === "string" ? obj.acceptedAs.trim() : "";
      const relayKeyId = typeof obj.relayKeyId === "string" && obj.relayKeyId.trim() ? obj.relayKeyId.trim() : null;
      const nodeKeyId = typeof obj.nodeKeyId === "string" ? obj.nodeKeyId.trim() : "";
      const nodePublicKeyB64 = typeof obj.nodePublicKeyB64 === "string" ? obj.nodePublicKeyB64.trim() : "";
      const trustLevel = typeof obj.trustLevel === "string" ? obj.trustLevel.trim() : "";
      const signatureB64 = typeof obj.signatureB64 === "string" ? obj.signatureB64.trim() : "";
      if (
        protocolVersion !== PEER_AUTH_PROTOCOL_VERSION
        || !challengeId
        || !nodeKeyId
        || !nodePublicKeyB64
        || !signatureB64
        || !state.challengeId
        || state.challengeId !== challengeId
        || !state.remote
      ) {
        throw new Error("peer accept invalid");
      }
      if (nodeKeyId !== state.remote.nodeKeyId || nodePublicKeyB64 !== state.remote.nodePublicKeyB64) {
        throw new Error("peer accept node mismatch");
      }
      if ((state.remote.relayKeyId || null) !== (relayKeyId || null)) {
        throw new Error("peer accept relay mismatch");
      }
      if (acceptedAs !== "leaf" && acceptedAs !== "relay-known" && acceptedAs !== "relay-provisional") {
        throw new Error("peer accept mode invalid");
      }
      if (trustLevel !== "verified" && trustLevel !== "tofu") {
        throw new Error("peer accept trust invalid");
      }
      const verified = PEER_AUTH_CRYPTO.verify({
        publicKey: base64ToBytes(nodePublicKeyB64),
        msg: signedPayloadBytes(meshPeerAcceptPayload({
          challengeId,
          acceptedAs,
          relayKeyId,
          nodeKeyId,
          trustLevel,
        })),
        sig: base64ToBytes(signatureB64),
      });
      if (verified !== true) {
        throw new Error("peer accept signature invalid");
      }
      const remotePeerAuth = derivePeerAuth({ relayKeyId: state.remote.relayKeyId, knownRelay: state.remote.knownRelay });
      const auth = this.#relayPeerDirectory
        ? this.#relayPeerDirectory.authenticate(socket, {
            relayKeyId: state.remote.relayKeyId,
            nodeKeyId: state.remote.nodeKeyId,
            nodePublicKeyB64: state.remote.nodePublicKeyB64,
            source: "outbound",
            authLevel: remotePeerAuth.authLevel,
          })
        : null;
      if (auth && auth.authLevel === "relay-verified" && state.remote.relayKeyId) {
        this.#relayIdByKey.set(state.remote.relayKeyId, key);
        if (this.#inboxRouter) this.#inboxRouter.addPeer(socket);
        if (this.#descriptorExchange) this.#descriptorExchange.addPeer(socket);
      }
      const selfDescriptor = this.#currentSelfDescriptor();
      if (selfDescriptor && this.#advertisedRelayKeyId) {
        const bindBytes = new TextEncoder().encode(JSON.stringify({
          _ctl: "peer.bind",
          descriptor: selfDescriptor,
        }));
        this.#manager.send(key, bindBytes).catch(() => {});
      }
      state.accepted = true;
      clearTimeout(state.timeout);
      state.resolve(true);
    } catch (err) {
      clearTimeout(state.timeout);
      state.reject(err);
      this.#peerAuthStates.delete(key);
      try { socket.destroy(); } catch {}
    }
    return true;
  }

  async ensureConnection(endpoint) {
    if (this.#closed) return;
    const parsed = parseEndpoint(endpoint);
    if (!parsed) throw new Error("RelayConnectionPool.ensureConnection requires { host, port }");
    await this.#ensureRegistered(parsed);
  }

  async #ensurePeerAuthenticated(parsed) {
    const key = endpointKey(parsed);
    const existing = this.#peerAuthStates.get(key);
    if (existing && existing.accepted === true && this.#manager.connections.has(key)) {
      return;
    }
    if (existing && existing.promise) {
      await existing.promise;
      return;
    }
    if (!this.#nodeKeyId || !this.#nodePublicKeyB64 || !(this.#nodePrivateKey instanceof Uint8Array)) {
      throw new Error("RelayConnectionPool peer auth keys unavailable");
    }
    let resolveAuth;
    let rejectAuth;
    const promise = new Promise((resolve, reject) => {
      resolveAuth = resolve;
      rejectAuth = reject;
    });
    const timeout = setTimeout(() => {
      const state = this.#peerAuthStates.get(key);
      if (!state || state.identifySent === true) return;
      state.reject(new Error("peer auth timeout"));
      this.#peerAuthStates.delete(key);
      const conn = this.#manager.connections.get(key);
      if (conn && conn.socket) {
        try { conn.socket.destroy(); } catch (destroyErr) {
          poolLog("error", "socket destroy failed during peer auth timeout for key=" + key + ":", destroyErr && destroyErr.message ? destroyErr.message : destroyErr);
        }
      }
    }, 5_000);
    if (timeout.unref) timeout.unref();
    this.#peerAuthStates.set(key, {
      identifySent: false,
      accepted: false,
      challengeId: null,
      nonceB64: null,
      remote: null,
      promise,
      timeout,
      resolve: resolveAuth,
      reject: rejectAuth,
    });
    const helloBytes = new TextEncoder().encode(JSON.stringify({
      _ctl: "peer.hello",
      protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
      relayKeyId: this.#advertisedRelayKeyId || undefined,
      nodeKeyId: this.#nodeKeyId,
      nodePublicKeyB64: this.#nodePublicKeyB64,
    }));
    try {
      await this.#manager.send(key, helloBytes);
      await promise;
    } catch (err) {
      clearTimeout(timeout);
      this.#peerAuthStates.delete(key);
      throw err;
    }
  }

  async sendBytes(endpoint, bytes) {
    if (this.#closed) throw new Error("RelayConnectionPool is closed");
    const parsed = parseEndpoint(endpoint);
    if (!parsed) throw new Error("RelayConnectionPool.sendBytes requires { host, port }");
    if (!(bytes instanceof Uint8Array)) throw new Error("RelayConnectionPool.sendBytes requires Uint8Array bytes");

    const key = endpointKey(parsed);

    // Ensure inbox registration is sent before (or with) the first data frame.
    // TcpConnectionManager lazily connects on first send, so we send the
    // registration frame first — it will trigger the connection, and the data
    // frame will be queued behind it on the same socket.
    await this.#ensureRegistered(parsed);

    await this.#manager.send(key, bytes);
  }

  /**
   * Send bytes to a relay by its relay key ID. Resolves the ID to a connection via the pool's directory.
   * Requires that the pool has connected to that relay (e.g. via connectToKnownRelays with descriptors).
   */
  async sendByRelayId(relayKeyId, bytes) {
    if (this.#closed) throw new Error("RelayConnectionPool is closed");
    if (typeof relayKeyId !== "string" || !relayKeyId.trim()) {
      throw new Error("RelayConnectionPool.sendByRelayId requires relayKeyId string");
    }
    if (!(bytes instanceof Uint8Array)) throw new Error("RelayConnectionPool.sendByRelayId requires Uint8Array bytes");
    const key = this.#findConnectionKeyForRelayId(relayKeyId.trim());
    if (!key) throw new Error("RelayConnectionPool.sendByRelayId: no connection for relay " + relayKeyId);
    await this.#manager.send(key, bytes);
  }

  /**
   * Query all connected relays for a route to the given inbox.
   * Sends inbox.query to each relay and waits for the first affirmative reply
   * (which installs the route into the local route table via InboxRouter).
   *
   * @param {string} inboxId
   * @param {number} [timeoutMs=3000]
   * @returns {Promise<boolean>} true if a route was found and installed
   */
  async queryRoute(inboxId, timeoutMs = 3000) {
    if (this.#closed) return false;
    if (typeof inboxId !== "string" || !inboxId.trim()) return false;
    const keys = Array.from(this.#manager.connections.keys());
    if (keys.length === 0) return false;
    const queryId = "rq_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8);
    // Register the pending query with InboxRouter so it resolves when reply arrives
    const replyPromise = this.#inboxRouter && typeof this.#inboxRouter.waitForQueryReply === "function"
      ? this.#inboxRouter.waitForQueryReply(queryId, timeoutMs)
      : null;
    if (!replyPromise) return false;
    const ctlMsg = JSON.stringify({ _ctl: "inbox.query", queryId, inboxIds: [inboxId] });
    const ctlBytes = new TextEncoder().encode(ctlMsg);
    const sends = keys.map((key) => this.#manager.send(key, ctlBytes).catch(() => {}));
    await Promise.allSettled(sends);
    return replyPromise;
  }

  /**
   * Send an inbox.deposit control message to all currently connected relays.
   * Returns the count of successful sends. Logs failures per connection.
   * @returns {{ sent: number, failed: number, total: number }}
   */
  async sendDepositToAllConnections(deliverInboxId, innerBytes) {
    if (this.#closed) {
      throw new Error("sendDepositToAllConnections: pool is closed");
    }
    if (typeof deliverInboxId !== "string" || !deliverInboxId.trim()) {
      throw new Error("sendDepositToAllConnections: deliverInboxId is required");
    }
    if (!(innerBytes instanceof Uint8Array)) {
      throw new Error("sendDepositToAllConnections: innerBytes must be Uint8Array");
    }
    const ctl = {
      _ctl: "inbox.deposit",
      inboxId: deliverInboxId,
      inner: Buffer.from(innerBytes).toString("base64"),
    };
    const ctlBytes = new TextEncoder().encode(JSON.stringify(ctl));
    const keys = Array.from(this.#manager.connections.keys());
    if (keys.length === 0) {
      throw new Error("sendDepositToAllConnections: no relay connections available");
    }
    const results = await Promise.allSettled(
      keys.map((key) => this.#manager.send(key, new Uint8Array(ctlBytes))),
    );
    let sent = 0;
    let failed = 0;
    for (let i = 0; i < results.length; i++) {
      if (results[i].status === "fulfilled") {
        sent += 1;
      } else {
        failed += 1;
        poolLog("error", "sendDepositToAllConnections failed for relay", keys[i], results[i].reason);
      }
    }
    if (sent === 0) {
      throw new Error("sendDepositToAllConnections: all " + failed + " relay sends failed for deliverInboxId=" + deliverInboxId);
    }
    return { sent, failed, total: keys.length };
  }

  async connectToKnownRelays(relayRecords) {
    if (this.#closed) return;
    const list = Array.isArray(relayRecords) ? relayRecords : [];
    const seen = new Set();
    const promises = [];
    let skipped = 0;
    for (const rec of list) {
      const descriptor = rec && typeof rec === "object" ? rec.descriptor : null;
      const ep = (rec && rec.endpoint) || (descriptor && Array.isArray(descriptor.endpoints) ? descriptor.endpoints[0] : null);
      const parsed = parseEndpoint(ep);
      if (!parsed) {
        skipped += 1;
        continue;
      }
      const key = endpointKey(parsed);
      const relayKeyId = descriptor && typeof descriptor.relayKeyId === "string" ? descriptor.relayKeyId : null;
      if (typeof relayKeyId === "string" && relayKeyId.trim()) {
        this.#relayIdByKey.set(relayKeyId.trim(), key);
        this.#expectedRelayIdByKey.set(key, relayKeyId.trim());
      }
      if (seen.has(key)) continue;
      seen.add(key);
      promises.push(
        this.ensureConnection(parsed)
          .then(() => { poolLog("log", "registered inbox on relay", key); })
          .catch((err) => {
            if (relayKeyId) this.#relayIdByKey.delete(relayKeyId.trim());
            poolLog("warn", "failed to connect to relay", key, err && err.message ? err.message : err);
          }),
      );
    }
    poolLog("log", "connectToKnownRelays:", list.length, "records,", seen.size, "unique endpoints,", skipped, "skipped (no endpoint)");
    await Promise.all(promises);
    poolLog("log", "connectToKnownRelays done, active connections:", this.#manager.connections.size);
  }

  async close() {
    this.#closed = true;
    this.#registrations.clear();
    this.#relayIdByKey.clear();
    this.#expectedRelayIdByKey.clear();
    this.#peerAuthStates.clear();
    await this.#manager.close();
  }

  async #ensureRegistered(parsed) {
    const key = endpointKey(parsed);
    if (this.#registrations.has(key) && this.#manager.connections.has(key)) return;

    this.#registrations.set(key, { registeredAtMs: Date.now() });
    try {
      await this.#ensurePeerAuthenticated(parsed);
    } catch (err) {
      this.#registrations.delete(key);
      throw err;
    }

    const registrations = this.#currentRegistrations();
    const inboxDebug = process.env.REZ_INBOX_DEBUG === "1";
    if (inboxDebug) {
      const ids = registrations.map((r) => (r && typeof r.inboxId === "string") ? r.inboxId : "?");
      console.log("[INBOX-DEBUG] RelayConnectionPool.#ensureRegistered sending inbox.register",
        { relayEndpoint: key, count: registrations.length, inboxIds: ids });
    }
    if (registrations.length > 0) {
      const ctlMsg = JSON.stringify({ _ctl: "inbox.register", registrations });
      const ctlBytes = new TextEncoder().encode(ctlMsg);
      try {
        await this.#manager.send(key, ctlBytes);
      } catch (err) {
        this.#registrations.delete(key);
        throw err;
      }
      // Track registered inbox IDs for withdrawal detection
      for (const reg of registrations) {
        if (reg && typeof reg.inboxId === "string" && reg.inboxId.trim()) {
          this.#lastRegisteredInboxIds.add(reg.inboxId.trim());
        }
      }
    } else if (inboxDebug) {
      console.warn("[INBOX-DEBUG] RelayConnectionPool.#ensureRegistered: NO registrations to send (HostedInboxRegistry empty?)",
        { relayEndpoint: key });
    }
    if (this.#descriptorExchange && this.#relayPeerDirectory) {
      const conn = this.#manager.connections.get(key);
      if (conn && conn.socket && this.#relayPeerDirectory.isAuthenticatedRelaySocket(conn.socket)) {
        const announceBytes = this.#descriptorExchange.buildAnnounceBytes();
        if (announceBytes) {
          await this.#manager.send(key, announceBytes).catch((announceErr) => {
            poolLog("error", "descriptor announce send failed for key=" + key + ":", announceErr && announceErr.message ? announceErr.message : announceErr);
          });
        }
      }
    }
  }

  /**
   * Re-register current inbox records on all existing relay connections (e.g. after HostedInboxRegistry add/remove).
   * Also sends inbox.withdraw for any inboxes that were previously registered but are no longer present.
   */
  async updateInboxIds() {
    if (this.#closed) return;
    const registrations = this.#currentRegistrations();

    // Detect withdrawn inboxes by diffing current vs previously registered
    const currentInboxIds = new Set();
    for (const reg of registrations) {
      if (reg && typeof reg.inboxId === "string" && reg.inboxId.trim()) {
        currentInboxIds.add(reg.inboxId.trim());
      }
    }
    const withdrawnIds = [];
    for (const prevId of this.#lastRegisteredInboxIds) {
      if (!currentInboxIds.has(prevId)) {
        withdrawnIds.push(prevId);
      }
    }
    this.#lastRegisteredInboxIds = currentInboxIds;

    // Send withdrawal for removed inboxes
    if (withdrawnIds.length > 0) {
      const withdrawMsg = JSON.stringify({ _ctl: "inbox.withdraw", inboxIds: withdrawnIds });
      const withdrawBytes = new TextEncoder().encode(withdrawMsg);
      for (const key of this.#manager.connections.keys()) {
        try {
          await this.#manager.send(key, withdrawBytes);
        } catch {
          // connection may be dead
        }
      }
    }

    // Re-register current inboxes
    const inboxDebug = process.env.REZ_INBOX_DEBUG === "1";
    if (inboxDebug) {
      const ids = registrations.map((r) => (r && typeof r.inboxId === "string") ? r.inboxId : "?");
      console.log("[INBOX-DEBUG] RelayConnectionPool.updateInboxIds re-broadcasting inbox.register",
        { connectionCount: this.#manager.connections.size, count: registrations.length, inboxIds: ids, withdrawnIds });
    }
    if (registrations.length === 0) return;
    const ctlMsg = JSON.stringify({ _ctl: "inbox.register", registrations });
    const ctlBytes = new TextEncoder().encode(ctlMsg);
    for (const key of this.#manager.connections.keys()) {
      try {
        const parsed = parseConnectionKey(key);
        if (parsed) {
          await this.#ensurePeerAuthenticated(parsed);
        }
        await this.#manager.send(key, ctlBytes);
        const conn2 = this.#manager.connections.get(key);
        if (this.#descriptorExchange && this.#relayPeerDirectory && conn2 && conn2.socket && this.#relayPeerDirectory.isAuthenticatedRelaySocket(conn2.socket)) {
          const announceBytes = this.#descriptorExchange.buildAnnounceBytes();
          if (announceBytes) {
            await this.#manager.send(key, announceBytes).catch((announceErr) => {
              poolLog("error", "descriptor announce send failed for key=" + key + ":", announceErr && announceErr.message ? announceErr.message : announceErr);
            });
          }
        }
      } catch {
        // connection may be dead; next ensureConnection will retry
      }
    }
  }
}
