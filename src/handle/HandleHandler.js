import { REZ_CONTRACT_TYPES, isNonEmptyString, verifyHandleOwnershipProof, RCapability } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;

function decodeCapChain(body) {
  if (!body || !Array.isArray(body.capChain) || body.capChain.length === 0) return null;
  return body.capChain.map((entry) => new RCapability(entry));
}
// Reject ownership-proof timestamps that drift more than 5 minutes from the
// relay's clock. Tight enough to make capture-replay impractical, loose
// enough to tolerate normal NTP skew. See docs/SECURITY_AUDIT.md CRITICAL-3.
const PROOF_SKEW_MS = 5 * 60 * 1000;

/**
 * Protocol handler for handle registration, resolution, and release.
 *
 * Mutating ops (register/renew/release) require an Ed25519 ownership proof:
 * the caller signs a canonical payload binding (kind, handle, keyId, tsMs,
 * relayKeyId) with the private key matching `keyId`. The handler verifies
 * the signature before touching state.
 *
 * `keyId` is interpreted as the base64 Ed25519 public key — owning the
 * handle is equivalent to owning the matching private key.
 *
 * handle.register is a paid service — goes through ServiceGate with
 * serviceId "handle.register". handle.resolve is free.
 */
export class HandleHandler {
  #ctx;
  #crypto;
  #clock;

  constructor(ctx, { crypto = null, clock = null } = {}) {
    this.#ctx = ctx;
    this.#crypto = crypto || new NodeCryptoProvider();
    this.#clock = typeof clock === "function" ? clock : () => Date.now();
  }

  async handleRegister(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const registry = this.#ctx.runtime.handleRegistry;
    if (!registry) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "Handle registry unavailable", retryable: false });
      return;
    }

    const proof = this.#extractProof({ body, kind: "handle.register", expectedRelayKeyId: registry.selfRelayKeyId });
    if (!proof) {
      this.#sendProofError(requestId, "register");
      return;
    }
    const verified = await verifyHandleOwnershipProof({
      ...proof,
      crypto: this.#crypto,
    });
    if (!verified) {
      this.#sendProofError(requestId, "register");
      return;
    }

    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "write",
      resource: "handles",
      requestId,
      serviceId: "handle.register",
      serviceParams: { quantity: 1 },
    });
    if (!cap) return;

    let claim;
    try {
      claim = await registry.register(proof.handle, proof.keyId);
    } catch (err) {
      const msg = err && err.message ? err.message : "Registration failed";
      this.#ctx.sendError({ id: requestId, code: "CONFLICT", message: msg, retryable: false });
      return;
    }

    const exchange = this.#ctx.runtime.handleExchange;
    if (exchange) {
      exchange.announceToAllPeers([claim]);
    }

    this.#ctx.sendResponse(requestId, T.HANDLE_REGISTER_RES, {
      handle: claim.handle,
      keyId: claim.keyId,
      expiresAtMs: claim.expiresAtMs,
    });
  }

  async handleResolve(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const { handle } = body;
    if (!isNonEmptyString(handle)) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "handle is required", retryable: false });
      return;
    }

    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "read",
      resource: "handles",
      requestId,
    });
    if (!cap) return;

    const registry = this.#ctx.runtime.handleRegistry;
    if (!registry) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "Handle registry unavailable", retryable: false });
      return;
    }

    // Resolve returns keyId publicly. That's safe: keyId IS a public Ed25519
    // key. Owning the handle requires the matching private key, which is
    // never revealed by this endpoint.
    const claim = await registry.resolve(handle);
    this.#ctx.sendResponse(requestId, T.HANDLE_RESOLVE_RES, {
      handle,
      keyId: claim ? claim.keyId : null,
      relayKeyId: claim ? claim.relayKeyId : null,
      expiresAtMs: claim ? claim.expiresAtMs : null,
      previousKeyId: claim ? claim.previousKeyId : null,
    });
  }

  async handleRelease(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const registry = this.#ctx.runtime.handleRegistry;
    if (!registry) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "Handle registry unavailable", retryable: false });
      return;
    }

    const proof = this.#extractProof({ body, kind: "handle.release", expectedRelayKeyId: registry.selfRelayKeyId });
    if (!proof) {
      this.#sendProofError(requestId, "release");
      return;
    }
    const verified = await verifyHandleOwnershipProof({
      ...proof,
      crypto: this.#crypto,
    });
    if (!verified) {
      this.#sendProofError(requestId, "release");
      return;
    }

    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "write",
      resource: "handles",
      requestId,
    });
    if (!cap) return;

    const released = await registry.release(proof.handle, proof.keyId);
    this.#ctx.sendResponse(requestId, T.HANDLE_RELEASE_RES, { handle: proof.handle, released });
  }

  /**
   * Pull and shape-check the ownership-proof fields from the wire body.
   * Returns null on any missing/malformed field or stale timestamp.
   */
  #extractProof({ body, kind, expectedRelayKeyId }) {
    if (!body || typeof body !== "object") return null;
    const handle = typeof body.handle === "string" ? body.handle.trim() : "";
    const keyId = typeof body.keyId === "string" ? body.keyId.trim() : "";
    const tsMs = Number(body.tsMs);
    const relayKeyId = typeof body.relayKeyId === "string" ? body.relayKeyId.trim() : "";
    const signatureB64 = typeof body.signatureB64 === "string" ? body.signatureB64.trim() : "";
    if (!handle || !keyId || !relayKeyId || !signatureB64) return null;
    if (!Number.isFinite(tsMs) || tsMs <= 0) return null;
    // Pin the proof to THIS relay so a signature captured at relay A can't
    // be forwarded to relay B (the two might gossip handles to each other).
    if (relayKeyId !== expectedRelayKeyId) return null;
    const now = this.#clock();
    if (Math.abs(now - tsMs) > PROOF_SKEW_MS) return null;
    return { kind, handle, keyId, tsMs, relayKeyId, signatureB64 };
  }

  #sendProofError(requestId, action) {
    this.#ctx.sendError({
      id: requestId,
      code: "UNAUTHORIZED",
      message: "handle." + action + " requires a valid ownership-proof signature (signatureB64, tsMs, relayKeyId)",
      retryable: false,
    });
  }
}
