/**
 * Socket-level frame router. Classifies each incoming frame and dispatches to the correct handler.
 * Used by both inbound connections (TcpRelayTransport) and outbound connections (RelayConnectionPool)
 * so that onion packets and control messages are handled the same regardless of connection direction.
 */

import { JsonCodec, Envelope, base64ToBytes, isNonEmptyString, validateRelayIdentityBinding } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { encodeFrame, sendControlMessage } from "../network/tcp/TcpFraming.js";
import {
  PEER_AUTH_PROTOCOL_VERSION,
  derivePeerAuth,
  meshPeerAcceptPayload,
  meshPeerAuthPayload,
  meshPeerChallengePayload,
  signedPayloadBytes,
  verifyRelayDescriptorSignature,
} from "./PeerAuthShared.js";

const CONTROL_PEER_HELLO = "peer.hello";
const CONTROL_PEER_CHALLENGE = "peer.challenge";
const CONTROL_PEER_IDENTIFY = "peer.identify";
const CONTROL_PEER_ACCEPT = "peer.accept";
const CONTROL_PEER_BIND = "peer.bind";
const CONTROL_INBOX_REGISTER = "inbox.register";
const CONTROL_INBOX_ROUTE = "inbox.route";
const CONTROL_INBOX_WITHDRAW = "inbox.withdraw";
const CONTROL_INBOX_DEPOSIT = "inbox.deposit";
const CONTROL_INBOX_QUERY = "inbox.query";
const CONTROL_INBOX_QUERY_REPLY = "inbox.query.reply";
const CONTROL_ROUTE_FAILED = "route.failed";
const CONTROL_DESCRIPTOR_ANNOUNCE = "descriptor.announce";
const CONTROL_DESCRIPTOR_EXCHANGE = "descriptor.exchange";

const ONION_TYPE_V2 = "rez.onion.v2";
const PEER_AUTH_CRYPTO = new NodeCryptoProvider();

const RATE_LIMIT_WINDOW_MS = 1000;
const RATE_LIMIT_MAX_FRAMES = 200;

function looksBinary(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.length === 0) return false;
  const len = Math.min(32, bytes.length);
  for (let i = 0; i < len; i += 1) {
    const b = bytes[i];
    if (b < 0x20 && b !== 0x09 && b !== 0x0a && b !== 0x0d) return true;
    if (b > 0x7e) return true;
  }
  return false;
}

export class SocketFrameRouter {
  constructor({
    relayPeerDirectory = null,
    relayStore = null,
    inboxRouter = null,
    inboxStore = null,
    relayRuntime = null,
    onRouteFailed = null,
    isInboxLocal = null,
    selfPeerAuth = null,
    getSelfDescriptor = null,
    controlMessageRegistry = null,
    logger = console,
  } = {}) {
    this._relayPeerDirectory = relayPeerDirectory ?? null;
    this._relayStore = relayStore ?? null;
    this._inboxRouter = inboxRouter ?? null;
    this._inboxStore = inboxStore ?? null;
    this._relayRuntime = relayRuntime ?? null;
    this._onRouteFailed = typeof onRouteFailed === "function" ? onRouteFailed : null;
    this._isInboxLocal = typeof isInboxLocal === "function" ? isInboxLocal : null;
    this._descriptorExchange = null;
    this._controlMessageRegistry = controlMessageRegistry || null;
    this._selfPeerAuth = normalizeSelfPeerAuth(selfPeerAuth);
    this._getSelfDescriptor = typeof getSelfDescriptor === "function" ? getSelfDescriptor : null;
    this._logger = logger ?? console;
    this._decoder = new JsonCodec();
    /** @type {WeakMap<object, {count: number, windowStart: number}>} */
    this._socketRateLimit = new WeakMap();
    /** @type {WeakMap<object, number>} per-socket last peer.hello timestamp for rate limiting */
    this._peerHelloLastMs = new WeakMap();
  }

  setDescriptorExchange(exchange) {
    this._descriptorExchange = exchange ?? null;
  }

  /**
   * Process one frame. Dispatches to the appropriate handler based on frame content.
   * @param {Uint8Array} bytes - Raw frame payload (after length-prefix strip)
   * @param {object} socket - The socket the frame arrived on (for reply/context)
   * @returns {Promise<boolean>} true if the frame was handled, false if dropped/unknown
   */
  async dispatch(bytes, socket) {
    if (!(bytes instanceof Uint8Array) || bytes.length === 0) return false;

    // Per-socket rate limiting
    const nowMs = Date.now();
    let rl = this._socketRateLimit.get(socket);
    if (!rl || nowMs - rl.windowStart > RATE_LIMIT_WINDOW_MS) {
      rl = { count: 0, windowStart: nowMs };
      this._socketRateLimit.set(socket, rl);
    }
    rl.count += 1;
    if (rl.count > RATE_LIMIT_MAX_FRAMES) {
      this._logDroppedFrame("rate_limited", { count: rl.count });
      return false;
    }

    let obj = null;
    try {
      const text = new TextDecoder().decode(bytes);
      obj = JSON.parse(text);
    } catch {
      this._logDroppedFrame("frame_not_json", {
        frameLength: bytes.length,
        looksBinary: looksBinary(bytes),
      });
      return false;
    }

    if (obj && typeof obj === "object" && isNonEmptyString(obj._ctl)) {
      const ctl = obj._ctl;
      switch (ctl) {
        case CONTROL_PEER_HELLO:
          return this._handlePeerHello(obj, socket);
        case CONTROL_PEER_CHALLENGE:
          this._logDroppedFrame("control_rejected", { ctl });
          return false;
        case CONTROL_PEER_IDENTIFY:
          return this._handlePeerIdentify(obj, socket);
        case CONTROL_PEER_ACCEPT:
          this._logDroppedFrame("control_rejected", { ctl });
          return false;
        case CONTROL_PEER_BIND:
          return this._handlePeerBind(obj, socket);
        case CONTROL_INBOX_REGISTER:
        case CONTROL_INBOX_ROUTE:
        case CONTROL_INBOX_WITHDRAW:
        case CONTROL_INBOX_QUERY:
        case CONTROL_INBOX_QUERY_REPLY:
          if (this._inboxRouter) {
            const result = this._inboxRouter.handleControlMessage(obj, socket);
            const ok = await Promise.resolve(result);
            if (!ok) this._logDroppedFrame("control_rejected", { ctl });
            return ok;
          }
          this._logDroppedFrame("control_rejected", { ctl });
          return false;
        case CONTROL_INBOX_DEPOSIT:
          // Require authenticated socket before accepting deposits to prevent
          // unauthenticated TCP clients from injecting packets into inboxes.
          if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedSocket(socket)) {
            this._logDroppedFrame("control_rejected", { ctl, reason: "unauthenticated" });
            return false;
          }
          if (this._inboxRouter) {
            const result = this._inboxRouter.handleControlMessage(obj, socket);
            const ok = await Promise.resolve(result);
            if (ok) {
              return true;
            }
          }
          const depositOk = this._handleDeposit(obj);
          if (!depositOk) this._logDroppedFrame("control_rejected", { ctl });
          return depositOk;
        case CONTROL_ROUTE_FAILED:
          if (this._relayPeerDirectory && this._relayPeerDirectory.isAuthenticatedRelaySocket(socket) && this._onRouteFailed) {
            this._onRouteFailed(obj, socket);
            return true;
          }
          this._logDroppedFrame("control_rejected", { ctl });
          return false;
        case CONTROL_DESCRIPTOR_ANNOUNCE:
          if (this._relayPeerDirectory && this._relayPeerDirectory.isAuthenticatedRelaySocket(socket) && this._descriptorExchange) {
            return this._descriptorExchange.handleAnnounce(obj, socket);
          }
          this._logDroppedFrame("control_rejected", { ctl });
          return false;
        case CONTROL_DESCRIPTOR_EXCHANGE:
          if (this._relayPeerDirectory && this._relayPeerDirectory.isAuthenticatedRelaySocket(socket) && this._descriptorExchange) {
            return this._descriptorExchange.handleExchange(obj, socket);
          }
          this._logDroppedFrame("control_rejected", { ctl });
          return false;
        default:
          if (this._controlMessageRegistry) {
            // DHT messages land here. End-user nodes (NAT'd electron
            // clients running a local rez-node) authenticate as
            // relay-provisional — they assert a routing-layer identity
            // but don't publish a descriptor. They MUST be able to
            // participate in routing, since "every node is a relay" is
            // load-bearing for the network thesis. Content trust on
            // dht.store is enforced separately by HIGH-8 claimant-
            // signed registrations, not by descriptor-trust.
            if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedRoutingSocket(socket)) {
              this._logDroppedFrame("control_rejected", { ctl, reason: "unauthenticated" });
              return false;
            }
            const handled = await this._controlMessageRegistry.dispatch(ctl, obj, socket);
            if (handled) return true;
          }
          this._logDroppedFrame("unknown_ctl", { ctl });
          return false;
      }
    }

    if (obj && typeof obj === "object" && this._relayRuntime
      && this._relayPeerDirectory && this._relayPeerDirectory.isAuthenticatedSocket(socket)) {
      try {
        const ctx = await this._decoder.decode({ bytes });
        const envelope = ctx.envelope;
        if (envelope instanceof Envelope) {
          const type = envelope.header && envelope.header.type;
          if (type === ONION_TYPE_V2) {
            await this._relayRuntime.handleInboundEnvelope(bytes, socket);
            return true;
          }
          this._logDroppedFrame("envelope_type", { type: type ?? "missing" });
          return false;
        }
      } catch (err) {
        this._logDroppedFrame("envelope_decode", { errMessage: (err && err.message) || String(err) });
        return false;
      }
    }

    this._logDroppedFrame("no_handler", {});
    return false;
  }

  _logDroppedFrame(reason, context) {
    const ctx = context && typeof context === "object" ? context : {};
    if (this._logger && typeof this._logger.warn === "function") {
      this._logger.warn("SocketFrameRouter frame dropped", { reason, ...ctx });
    }
  }

  _handleDeposit(ctlObj) {
    if (!this._inboxStore || !isNonEmptyString(ctlObj.inboxId)) return false;
    if (typeof ctlObj.inner !== "string") return false;
    if (this._isInboxLocal && !this._isInboxLocal(ctlObj.inboxId)) return false;
    const innerBytes = new Uint8Array(Buffer.from(ctlObj.inner, "base64"));
    this._inboxStore.depositFromWire(ctlObj.inboxId, innerBytes).catch((err) => {
      if (this._logger && typeof this._logger.error === "function") {
        this._logger.error("SocketFrameRouter deposit error", (err && err.message) || String(err));
      }
    });
    return true;
  }

  _handlePeerHello(ctlObj, socket) {
    if (!this._relayPeerDirectory || !this._selfPeerAuth) return false;
    // Rate limit: max 1 peer.hello per socket per 10 seconds to prevent challenge flooding.
    const helloNow = Date.now();
    const lastHello = this._peerHelloLastMs.get(socket) || 0;
    if (helloNow - lastHello < 10_000) return false;
    this._peerHelloLastMs.set(socket, helloNow);
    const protocolVersion = Number(ctlObj.protocolVersion);
    const nodeKeyId = typeof ctlObj.nodeKeyId === "string" ? ctlObj.nodeKeyId.trim() : "";
    const nodePublicKeyB64 = typeof ctlObj.nodePublicKeyB64 === "string" ? ctlObj.nodePublicKeyB64.trim() : "";
    // TRUST-9: the connecting node must supply a fresh nonce we bind into the
    // signed challenge/accept. v4 peers always send it; fail closed if absent.
    const clientNonceB64 = typeof ctlObj.clientNonceB64 === "string" ? ctlObj.clientNonceB64.trim() : "";
    if (protocolVersion !== PEER_AUTH_PROTOCOL_VERSION || !nodeKeyId || !nodePublicKeyB64 || !clientNonceB64) return false;
    // ADR-RELAY-IDENTITY: a presented relayKeyId must be the self-certifying
    // identity of the presented node key. Reject before issuing a challenge so
    // ground relay IDs cannot even consume a handshake round trip. Leaf nodes
    // (no relayKeyId) are unaffected.
    const helloRelayKeyId = typeof ctlObj.relayKeyId === "string" && ctlObj.relayKeyId.trim()
      ? ctlObj.relayKeyId.trim()
      : null;
    if (helloRelayKeyId) {
      const binding = validateRelayIdentityBinding({
        relayKeyId: helloRelayKeyId,
        nodeKeyId,
        nodePublicKeyB64,
      });
      if (binding.ok !== true) {
        this._logger.warn("SocketFrameRouter peer.hello rejected", { reason: "relay-identity-binding:" + binding.reason });
        return false;
      }
    }
    const challenge = this._relayPeerDirectory.issueChallenge(socket, {
      expectedRelayKeyId: helloRelayKeyId,
      presentedNodeKeyId: nodeKeyId,
      presentedNodePublicKeyB64: nodePublicKeyB64,
      clientNonceB64,
    });
    if (!challenge) return false;
    const relayKeyId = this._selfPeerAuth.relayKeyId || null;
    const signature = PEER_AUTH_CRYPTO.sign({
      privateKey: this._selfPeerAuth.nodePrivateKey,
      msg: signedPayloadBytes(meshPeerChallengePayload({
        challengeId: challenge.challengeId,
        nonceB64: challenge.nonceB64,
        clientNonceB64,
        relayKeyId,
        nodeKeyId: this._selfPeerAuth.nodeKeyId,
        expiresAtMs: challenge.expiresAtMs,
      })),
    });
    this._sendCtl(socket, {
      _ctl: CONTROL_PEER_CHALLENGE,
      protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
      challengeId: challenge.challengeId,
      nonceB64: challenge.nonceB64,
      clientNonceB64,
      issuedAtMs: challenge.issuedAtMs,
      expiresAtMs: challenge.expiresAtMs,
      relayKeyId,
      nodeKeyId: this._selfPeerAuth.nodeKeyId,
      nodePublicKeyB64: this._selfPeerAuth.nodePublicKeyB64,
      signatureB64: Buffer.from(signature).toString("base64"),
    });
    return true;
  }

  async _handlePeerIdentify(ctlObj, socket) {
    if (!this._relayPeerDirectory) return false;
    const pending = this._relayPeerDirectory.getPendingChallenge(socket);
    if (!pending) return false;
    const protocolVersion = Number(ctlObj.protocolVersion);
    const challengeId = typeof ctlObj.challengeId === "string" ? ctlObj.challengeId.trim() : "";
    const nodeKeyId = typeof ctlObj.nodeKeyId === "string" ? ctlObj.nodeKeyId.trim() : "";
    const nodePublicKeyB64 = typeof ctlObj.nodePublicKeyB64 === "string" ? ctlObj.nodePublicKeyB64.trim() : "";
    const signatureB64 = typeof ctlObj.signatureB64 === "string" ? ctlObj.signatureB64.trim() : "";
    const relayKeyId = typeof ctlObj.relayKeyId === "string" && ctlObj.relayKeyId.trim() ? ctlObj.relayKeyId.trim() : null;
    if (
      protocolVersion !== PEER_AUTH_PROTOCOL_VERSION
      || !challengeId
      || !nodeKeyId
      || !nodePublicKeyB64
      || !signatureB64
      || challengeId !== pending.challengeId
      || Date.now() > pending.expiresAtMs
      || pending.presentedNodeKeyId !== nodeKeyId
      || pending.presentedNodePublicKeyB64 !== nodePublicKeyB64
      || (pending.expectedRelayKeyId || null) !== relayKeyId
    ) {
      this._rejectPeerSocket(socket);
      return false;
    }

    // ADR-RELAY-IDENTITY: enforce the self-certifying binding at the
    // authoritative gate — before any auth level is assigned. TOFU knowledge
    // can only strengthen a valid binding, never legitimize an invalid one.
    if (relayKeyId) {
      const binding = validateRelayIdentityBinding({ relayKeyId, nodeKeyId, nodePublicKeyB64 });
      if (binding.ok !== true) {
        this._logger.warn("SocketFrameRouter peer.identify rejected", { reason: "relay-identity-binding:" + binding.reason });
        this._rejectPeerSocket(socket);
        return false;
      }
    }

    let knownRelay = false;
    if (relayKeyId) {
      const descriptor = this._relayStore && typeof this._relayStore.getDescriptor === "function"
        ? this._relayStore.getDescriptor(relayKeyId, { nowMs: Date.now() })
        : null;
      if (descriptor) {
        const meta = descriptor.meta && typeof descriptor.meta === "object" ? descriptor.meta : {};
        const node = meta.node && typeof meta.node === "object" ? meta.node : {};
        const descriptorKeyId = typeof node.keyId === "string" ? node.keyId.trim() : "";
        const descriptorPublicKeyB64 = typeof node.publicKeyB64 === "string" ? node.publicKeyB64.trim() : "";
        if (descriptorKeyId !== nodeKeyId || descriptorPublicKeyB64 !== nodePublicKeyB64) {
          this._rejectPeerSocket(socket);
          return false;
        }
        knownRelay = true;
      }
    }
    const peerAuth = derivePeerAuth({ relayKeyId, knownRelay });
    const { authLevel, acceptedAs, wireTrustLevel } = peerAuth;

    let publicKey;
    let signature;
    try {
      publicKey = base64ToBytes(nodePublicKeyB64);
      signature = base64ToBytes(signatureB64);
    } catch {
      this._rejectPeerSocket(socket);
      return false;
    }

    const verified = await Promise.resolve(PEER_AUTH_CRYPTO.verify({
      publicKey,
      msg: signedPayloadBytes(meshPeerAuthPayload({
        challengeId,
        nonceB64: pending.nonceB64,
        relayKeyId,
        nodeKeyId,
      })),
      sig: signature,
    })).catch(() => false);
    if (verified !== true) {
      this._rejectPeerSocket(socket);
      return false;
    }

    const auth = this._relayPeerDirectory.authenticate(socket, {
      relayKeyId,
      nodeKeyId,
      nodePublicKeyB64,
      source: "inbound",
      authLevel,
    });
    if (auth && auth.authLevel === "relay-verified") {
      if (this._inboxRouter) this._inboxRouter.addPeer(socket);
      if (this._descriptorExchange) this._descriptorExchange.addPeer(socket);
    }
    // Leaf nodes (authLevel "node") and relay-provisional peers are NOT added
    // to route gossip. Only relay-verified peers participate in route
    // announcements. Leaf nodes register inboxes and receive deposits without
    // needing the full route table — exposing it would leak inbox topology.
    const acceptSignature = PEER_AUTH_CRYPTO.sign({
      privateKey: this._selfPeerAuth.nodePrivateKey,
      msg: signedPayloadBytes(meshPeerAcceptPayload({
        challengeId,
        acceptedAs,
        // TRUST-9: bind the accept to the same connecting-node nonce as the challenge.
        clientNonceB64: pending.clientNonceB64,
        relayKeyId: this._selfPeerAuth.relayKeyId,
        nodeKeyId: this._selfPeerAuth.nodeKeyId,
        trustLevel: wireTrustLevel,
      })),
    });
    this._sendCtl(socket, {
      _ctl: CONTROL_PEER_ACCEPT,
      protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
      challengeId,
      acceptedAs,
      relayKeyId: this._selfPeerAuth.relayKeyId || undefined,
      nodeKeyId: this._selfPeerAuth.nodeKeyId,
      nodePublicKeyB64: this._selfPeerAuth.nodePublicKeyB64,
      trustLevel: wireTrustLevel,
      signatureB64: Buffer.from(acceptSignature).toString("base64"),
    });
    const selfDescriptor = this._currentSelfDescriptor();
    if (selfDescriptor && this._selfPeerAuth.relayKeyId) {
      this._sendCtl(socket, {
        _ctl: CONTROL_PEER_BIND,
        descriptor: selfDescriptor,
      });
    }
    return true;
  }

  _handlePeerBind(ctlObj, socket) {
    if (!this._relayPeerDirectory) return false;
    const auth = this._relayPeerDirectory.getAuth(socket);
    if (!auth || (auth.authLevel !== "relay-provisional" && auth.authLevel !== "relay-verified")) {
      if (auth && auth.authLevel === "node") {
        this._logDroppedFrame("peer_bind_from_leaf", { nodeKeyId: auth.nodeKeyId });
      }
      return false;
    }
    const descriptor = ctlObj && ctlObj.descriptor && typeof ctlObj.descriptor === "object" ? ctlObj.descriptor : null;
    if (!descriptor || verifyRelayDescriptorSignature(descriptor) !== true) {
      this._rejectPeerSocket(socket);
      return false;
    }
    const relayKeyId = typeof descriptor.relayKeyId === "string" ? descriptor.relayKeyId.trim() : "";
    const descriptorMeta = descriptor.meta && typeof descriptor.meta === "object" ? descriptor.meta : {};
    const descriptorNode = descriptorMeta.node && typeof descriptorMeta.node === "object" ? descriptorMeta.node : {};
    const descriptorNodeKeyId = typeof descriptorNode.keyId === "string" ? descriptorNode.keyId.trim() : "";
    const descriptorNodePublicKeyB64 = typeof descriptorNode.publicKeyB64 === "string" ? descriptorNode.publicKeyB64.trim() : "";
    const descriptorProtocolVersion = Number(descriptorNode.protocolVersion);
    if (!relayKeyId || !descriptorNodeKeyId || !descriptorNodePublicKeyB64) {
      this._rejectPeerSocket(socket);
      return false;
    }
    if (descriptorProtocolVersion !== PEER_AUTH_PROTOCOL_VERSION) {
      this._rejectPeerSocket(socket);
      return false;
    }
    if (auth.relayKeyId && auth.relayKeyId !== relayKeyId) {
      this._rejectPeerSocket(socket);
      return false;
    }
    if (auth.nodeKeyId !== descriptorNodeKeyId || auth.nodePublicKeyB64 !== descriptorNodePublicKeyB64) {
      this._rejectPeerSocket(socket);
      return false;
    }
    const existing = this._relayStore && typeof this._relayStore.getDescriptor === "function"
      ? this._relayStore.getDescriptor(relayKeyId, { nowMs: Date.now() })
      : null;
    if (existing) {
      const existingMeta = existing.meta && typeof existing.meta === "object" ? existing.meta : {};
      const existingNode = existingMeta.node && typeof existingMeta.node === "object" ? existingMeta.node : {};
      const existingNodeKeyId = typeof existingNode.keyId === "string" ? existingNode.keyId.trim() : "";
      const existingNodePublicKeyB64 = typeof existingNode.publicKeyB64 === "string" ? existingNode.publicKeyB64.trim() : "";
      if (existingNodeKeyId !== descriptorNodeKeyId || existingNodePublicKeyB64 !== descriptorNodePublicKeyB64) {
        this._rejectPeerSocket(socket);
        return false;
      }
    }
    const isOutbound = auth.source === "outbound";
    const bindSource = isOutbound ? "peer-bind-verified" : "peer-bind-tofu";
    const bindTrust = isOutbound ? "verified" : "tofu";
    if (this._relayStore && typeof this._relayStore.upsertDescriptor === "function") {
      const admission = this._relayStore.upsertDescriptor(descriptor, {
        source: bindSource,
        bindingTrust: bindTrust,
        receivedAtMs: Date.now(),
      });
      // P2 canonical admission: a bind whose descriptor fails the canonical
      // validator must not proceed to promotion — the old code ignored the
      // verdict, which let an authenticated peer bind empty onion keys or a
      // past expiry straight into the store. Freshness dedup ("older-*") is
      // not a validity failure: the peer re-announced a descriptor we already
      // hold, which is fine.
      if (admission.accepted !== true
        && admission.reason !== "older-expiresAt" && admission.reason !== "older-receivedAt") {
        this._logDroppedFrame("peer_bind_rejected", { reason: admission.reason });
        this._rejectPeerSocket(socket);
        return false;
      }
    }
    // Only promote outbound-verified peers immediately. Inbound peers stay
    // provisional — promotion requires an outbound connection or descriptor
    // gossip from an already-verified relay.
    let promoted = null;
    if (isOutbound) {
      promoted = this._relayPeerDirectory.promoteRelay(socket, {
        relayKeyId,
      });
      if (promoted && promoted.authLevel === "relay-verified") {
        if (this._inboxRouter) this._inboxRouter.addPeer(socket);
        if (this._descriptorExchange) this._descriptorExchange.addPeer(socket);
      }
    }
    return true;
  }

  _currentSelfDescriptor() {
    if (!this._getSelfDescriptor) return null;
    const descriptor = this._getSelfDescriptor();
    return descriptor && typeof descriptor === "object" ? descriptor : null;
  }

  _sendCtl(socket, ctlObj) {
    const ok = sendControlMessage(socket, ctlObj);
    if (!ok) {
      const ctl = ctlObj && ctlObj._ctl ? ctlObj._ctl : "unknown";
      if (!socket || socket.destroyed) {
        this._logger.error("[SocketFrameRouter] _sendCtl: socket unavailable, dropping _ctl=" + ctl);
      } else {
        this._logger.error("[SocketFrameRouter] _sendCtl write failed for _ctl=" + ctl);
        this._rejectPeerSocket(socket);
      }
    }
  }

  _rejectPeerSocket(socket) {
    if (this._relayPeerDirectory) this._relayPeerDirectory.remove(socket);
    try {
      if (socket && typeof socket.destroy === "function") socket.destroy();
    } catch {
      // ignore
    }
  }
}

function normalizeSelfPeerAuth(input) {
  if (!input || typeof input !== "object") return null;
  const nodeKeyId = isNonEmptyString(input.nodeKeyId) ? input.nodeKeyId.trim() : "";
  const nodePublicKeyB64 = isNonEmptyString(input.nodePublicKeyB64) ? input.nodePublicKeyB64.trim() : "";
  const nodePrivateKeyB64 = isNonEmptyString(input.nodePrivateKeyB64) ? input.nodePrivateKeyB64.trim() : "";
  if (!nodeKeyId || !nodePublicKeyB64 || !nodePrivateKeyB64) return null;
  try {
    return {
      relayKeyId: isNonEmptyString(input.relayKeyId) ? input.relayKeyId.trim() : null,
      nodeKeyId,
      nodePublicKeyB64,
      nodePrivateKey: base64ToBytes(nodePrivateKeyB64),
    };
  } catch {
    return null;
  }
}
