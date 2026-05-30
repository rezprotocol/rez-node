import { randomBytes } from "node:crypto";
import { isNonEmptyString } from "@rezprotocol/core";

const CHALLENGE_TTL_MS = 60_000;

function randomHex(bytes = 8) {
  return Buffer.from(randomBytes(bytes)).toString("hex");
}

export class RelayPeerDirectory {
  constructor() {
    /** @type {Map<string, object>} relayKeyId -> socket */
    this._byId = new Map();
    /** @type {Map<object, object>} socket -> auth state */
    this._bySocket = new Map();
    /** @type {Map<string, number>} challengeId -> expiresAtMs (consumed challenges) */
    this._consumedChallenges = new Map();
  }

  issueChallenge(socket, { expectedRelayKeyId = null, presentedNodeKeyId = null, presentedNodePublicKeyB64 = null } = {}) {
    if (!socket || typeof socket !== "object") return null;
    const nowMs = Date.now();
    const current = this._bySocket.get(socket) || {};
    const challenge = {
      challengeId: `peer_challenge:${nowMs}:${randomHex(6)}`,
      nonceB64: Buffer.from(randomBytes(32)).toString("base64"),
      issuedAtMs: nowMs,
      expiresAtMs: nowMs + CHALLENGE_TTL_MS,
    };
    this._bySocket.set(socket, {
      ...current,
      pendingChallenge: {
        ...challenge,
        expectedRelayKeyId: isNonEmptyString(expectedRelayKeyId) ? expectedRelayKeyId.trim() : null,
        presentedNodeKeyId: isNonEmptyString(presentedNodeKeyId) ? presentedNodeKeyId.trim() : null,
        presentedNodePublicKeyB64: isNonEmptyString(presentedNodePublicKeyB64) ? presentedNodePublicKeyB64.trim() : null,
      },
      authenticated: false,
    });
    return challenge;
  }

  getPendingChallenge(socket) {
    const state = this._bySocket.get(socket);
    const challenge = (state && state.pendingChallenge) || null;
    if (!challenge) return null;

    // Consume the challenge immediately to prevent replay
    const challengeId = challenge.challengeId;
    if (this._consumedChallenges.has(challengeId)) {
      return null; // Already consumed -- reject replay
    }
    this._consumedChallenges.set(challengeId, challenge.expiresAtMs);
    state.pendingChallenge = null;
    this._purgeExpiredChallenges();

    return challenge;
  }

  _purgeExpiredChallenges() {
    const nowMs = Date.now();
    for (const [id, expiresAtMs] of this._consumedChallenges) {
      if (nowMs > expiresAtMs) {
        this._consumedChallenges.delete(id);
      }
    }
  }

  authenticate(socket, {
    relayKeyId = null,
    nodeKeyId,
    nodePublicKeyB64,
    source = "inbound",
    authLevel = "node",
  } = {}) {
    if (!socket || typeof socket !== "object") return null;
    const state = this._bySocket.get(socket) || {};
    const normalizedRelayKeyId = isNonEmptyString(relayKeyId) ? relayKeyId.trim() : null;
    const normalizedNodeKeyId = isNonEmptyString(nodeKeyId) ? nodeKeyId.trim() : null;
    const normalizedNodePublicKeyB64 = isNonEmptyString(nodePublicKeyB64) ? nodePublicKeyB64.trim() : null;
    if (!normalizedNodeKeyId || !normalizedNodePublicKeyB64) return null;
    const requestedAuthLevel = normalizeAuthLevel(authLevel, normalizedRelayKeyId);
    const relayVerified = requestedAuthLevel === "relay-verified";

    if (state.relayKeyId && state.relayKeyId !== normalizedRelayKeyId) {
      this._byId.delete(state.relayKeyId);
    }
    if (relayVerified && normalizedRelayKeyId) {
      const existingSocket = this._byId.get(normalizedRelayKeyId);
      if (existingSocket && existingSocket !== socket) {
        const existingState = this._bySocket.get(existingSocket);
        if (existingState) {
          existingState.authLevel = "node";
          existingState.relayKeyId = null;
        }
      }
      this._byId.set(normalizedRelayKeyId, socket);
    } else if (normalizedRelayKeyId) {
      this._byId.delete(normalizedRelayKeyId);
    }

    const nextState = {
      ...state,
      relayKeyId: normalizedRelayKeyId,
      nodeKeyId: normalizedNodeKeyId,
      nodePublicKeyB64: normalizedNodePublicKeyB64,
      authenticated: true,
      authLevel: requestedAuthLevel,
      pendingChallenge: null,
      source: source === "outbound" ? "outbound" : "inbound",
    };
    this._bySocket.set(socket, nextState);
    return { ...nextState };
  }

  promoteRelay(socket, { relayKeyId = null } = {}) {
    if (!socket || typeof socket !== "object") return null;
    const state = this._bySocket.get(socket);
    if (!state || !state.authenticated) return null;
    const normalizedRelayKeyId = isNonEmptyString(relayKeyId) ? relayKeyId.trim() : null;
    if (!normalizedRelayKeyId || (state.relayKeyId && state.relayKeyId !== normalizedRelayKeyId)) {
      return null;
    }
    this._byId.set(normalizedRelayKeyId, socket);
    const nextState = {
      ...state,
      relayKeyId: normalizedRelayKeyId,
      authLevel: "relay-verified",
    };
    this._bySocket.set(socket, nextState);
    return { ...nextState };
  }

  getSocket(relayKeyId) {
    if (!isNonEmptyString(relayKeyId)) return null;
    const socket = this._byId.get(relayKeyId.trim());
    if (!socket || socket.destroyed === true) return null;
    return socket;
  }

  getRelayKeyIdForSocket(socket) {
    if (!socket || typeof socket !== "object") return null;
    const state = this._bySocket.get(socket);
    if (!state || state.authLevel !== "relay-verified") return null;
    const relayKeyId = state.relayKeyId;
    return isNonEmptyString(relayKeyId) ? relayKeyId : null;
  }

  getAuth(socket) {
    if (!socket || typeof socket !== "object") return null;
    const state = this._bySocket.get(socket);
    if (!state || !state.authenticated) return null;
    return { ...state, pendingChallenge: null };
  }

  isAuthenticatedSocket(socket) {
    return !!this.getAuth(socket);
  }

  isAuthenticatedRelaySocket(socket) {
    const auth = this.getAuth(socket);
    return auth !== null && auth.authLevel === "relay-verified";
  }

  /**
   * Any peer that asserts a relay identity (verified or provisional).
   * Used to gate routing-layer participation (DHT messages, route
   * gossip) that does not itself require trusting the peer's published
   * descriptor — content trust is enforced separately by HIGH-8
   * claimant-signed delegations on stored route entries. Distinct from
   * `isAuthenticatedRelaySocket`, which is reserved for descriptor-
   * trust-dependent flows (descriptor.announce/exchange).
   */
  isAuthenticatedRoutingSocket(socket) {
    const auth = this.getAuth(socket);
    if (auth === null) return false;
    return auth.authLevel === "relay-verified" || auth.authLevel === "relay-provisional";
  }

  remove(socket) {
    if (!socket) return;
    const state = this._bySocket.get(socket);
    this._bySocket.delete(socket);
    if (state && state.relayKeyId) {
      this._byId.delete(state.relayKeyId);
    }
  }

  get size() {
    return this._byId.size;
  }
}

function normalizeAuthLevel(value, relayKeyId) {
  const requested = typeof value === "string" ? value.trim() : "";
  if (requested === "relay-verified" && relayKeyId) return requested;
  if (requested === "relay-provisional" && relayKeyId) return requested;
  return "node";
}

