import { HandleClaimV1, DEFAULT_TTL_MS, base64ToBytes, canonicalJSONStringify } from "@rezprotocol/core";

const KV_PREFIX = "handle:claim:";

/**
 * Stores and resolves handle claims.
 *
 * Handles are globally unique names (e.g., "alice") that map to a
 * publication key ID. Claims are persisted in the relay's KV store
 * and gossiped across the mesh via HandleExchange.
 *
 * Conflict resolution: first-come-first-served — the claim with the
 * earliest createdAtMs wins. Expired claims can be reclaimed by anyone.
 */
export class HandleRegistry {
  #kvStore;
  #receiptSigner;
  #selfRelayKeyId;
  #relayStore;
  #crypto;

  /**
   * @param {object} opts
   * @param {KeyValueStore} opts.kvStore — persistent storage for claims
   * @param {ReceiptSigner} opts.receiptSigner — signs new claims
   * @param {string} opts.selfRelayKeyId — this relay's key ID
   * @param {object} [opts.relayStore] — TRUST-5: resolves a registrar relayKeyId to
   *   its OPERATOR-PINNED node public key (getPinnedNodePublicKeyB64), used to
   *   verify gossiped claim signatures. Without it, gossiped claims fail closed.
   * @param {object} [opts.crypto] — Ed25519 verifier ({ verify({publicKey,msg,sig}) }).
   */
  constructor({ kvStore, receiptSigner, selfRelayKeyId, relayStore = null, crypto = null }) {
    if (!kvStore) throw new Error("HandleRegistry requires kvStore");
    if (!receiptSigner) throw new Error("HandleRegistry requires receiptSigner");
    if (!selfRelayKeyId || typeof selfRelayKeyId !== "string") throw new Error("HandleRegistry requires selfRelayKeyId");
    this.#kvStore = kvStore;
    this.#receiptSigner = receiptSigner;
    this.#selfRelayKeyId = selfRelayKeyId;
    this.#relayStore = relayStore;
    this.#crypto = crypto;
  }

  /**
   * TRUST-5: verify a gossiped claim's registrar signature. The claim is signed by
   * the registrar relay's NODE identity key (ReceiptSigner over the node key); we
   * resolve that relay's PINNED node public key by sig.relayKeyId and check the
   * Ed25519 signature over the canonical claim body. Fail-closed: a claim we cannot
   * cryptographically verify (no resolver/crypto, no pin for the registrar, or a bad
   * signature) is NEVER accepted — we never trust a gossiped handle->key mapping by
   * shape alone. Returns true only on a good signature.
   */
  async _verifyGossipedClaimSignature(claim) {
    if (!this.#relayStore || typeof this.#relayStore.getPinnedNodePublicKeyB64 !== "function") return false;
    if (!this.#crypto || typeof this.#crypto.verify !== "function") return false;
    const sig = claim && claim.sig && typeof claim.sig === "object" ? claim.sig : null;
    if (!sig || sig.alg !== "ed25519") return false;
    const sigRelayKeyId = typeof sig.relayKeyId === "string" ? sig.relayKeyId.trim() : "";
    if (!sigRelayKeyId) return false;
    // The trust anchor (which key to use) must live inside the SIGNED body: the
    // signed top-level relayKeyId must match the sig.relayKeyId that selects the key.
    const bodyRelayKeyId = typeof claim.relayKeyId === "string" ? claim.relayKeyId.trim() : "";
    if (bodyRelayKeyId && bodyRelayKeyId !== sigRelayKeyId) return false;
    const pubB64 = this.#relayStore.getPinnedNodePublicKeyB64(sigRelayKeyId);
    if (!pubB64) return false;
    let publicKey;
    let sigBytes;
    try {
      publicKey = base64ToBytes(pubB64);
      sigBytes = sig.sig instanceof Uint8Array ? sig.sig : Uint8Array.from(Array.isArray(sig.sig) ? sig.sig : []);
    } catch (err) {
      return false;
    }
    if (!(sigBytes instanceof Uint8Array) || sigBytes.length === 0) return false;
    const body = typeof claim.toJSON === "function" ? claim.toJSON() : { ...claim };
    delete body.sig;
    const msg = new TextEncoder().encode(canonicalJSONStringify(body));
    try {
      return (await this.#crypto.verify({ publicKey, msg, sig: sigBytes })) === true;
    } catch (err) {
      return false;
    }
  }

  /** This relay's keyId — exposed so HandleHandler can pin ownership-proof signatures. */
  get selfRelayKeyId() {
    return this.#selfRelayKeyId;
  }

  /**
   * Register a new handle claim. Fails if the handle is already claimed
   * by a different key and not expired.
   *
   * @param {string} handle — the handle to claim (e.g., "alice")
   * @param {string} keyId — the publication key ID
   * @returns {Promise<HandleClaimV1>} the signed claim
   * @throws {Error} if handle is already taken
   */
  async register(handle, keyId) {
    const normalized = this.#normalizeHandle(handle);
    const existing = await this.#getClaim(normalized);
    if (existing && !existing.isExpired() && existing.keyId !== keyId) {
      throw new Error("Handle already claimed: @" + normalized);
    }

    const previousKeyId = existing ? existing.keyId : null;
    const nowMs = Date.now();
    const body = {
      v: 1,
      handle: normalized,
      keyId,
      relayKeyId: this.#selfRelayKeyId,
      createdAtMs: nowMs,
      expiresAtMs: nowMs + DEFAULT_TTL_MS,
      previousKeyId,
    };
    const sig = await this.#receiptSigner.sign(body);
    const claim = new HandleClaimV1({ ...body, sig });

    await this.#kvStore.set(KV_PREFIX + normalized, claim.toJSON());
    return claim;
  }

  /**
   * Renew an existing handle claim. Only the current key holder can renew.
   *
   * @param {string} handle
   * @param {string} keyId — must match the existing claim's keyId
   * @returns {Promise<HandleClaimV1>}
   * @throws {Error} if handle not found or keyId doesn't match
   */
  async renew(handle, keyId) {
    const normalized = this.#normalizeHandle(handle);
    const existing = await this.#getClaim(normalized);
    if (!existing) {
      throw new Error("Handle not found: @" + normalized);
    }
    if (existing.keyId !== keyId) {
      throw new Error("Handle @" + normalized + " is not owned by this key");
    }

    const nowMs = Date.now();
    const body = {
      v: 1,
      handle: normalized,
      keyId,
      relayKeyId: this.#selfRelayKeyId,
      createdAtMs: nowMs,
      expiresAtMs: nowMs + DEFAULT_TTL_MS,
      previousKeyId: existing.previousKeyId,
    };
    const sig = await this.#receiptSigner.sign(body);
    const claim = new HandleClaimV1({ ...body, sig });

    await this.#kvStore.set(KV_PREFIX + normalized, claim.toJSON());
    return claim;
  }

  /**
   * Release a handle (voluntary). Only the current key holder can release.
   *
   * @param {string} handle
   * @param {string} keyId
   * @returns {Promise<boolean>}
   */
  async release(handle, keyId) {
    const normalized = this.#normalizeHandle(handle);
    const existing = await this.#getClaim(normalized);
    if (!existing) return false;
    if (existing.keyId !== keyId) return false;
    await this.#kvStore.delete(KV_PREFIX + normalized);
    return true;
  }

  /**
   * Resolve a handle to its current claim.
   *
   * @param {string} handle
   * @returns {Promise<HandleClaimV1|null>}
   */
  async resolve(handle) {
    const normalized = this.#normalizeHandle(handle);
    const claim = await this.#getClaim(normalized);
    if (claim && claim.isExpired()) return null;
    return claim;
  }

  /**
   * Accept a gossiped handle claim from another relay.
   * First-come-first-served: only accept if no existing claim or
   * the incoming claim is older (lower createdAtMs).
   *
   * @param {HandleClaimV1} claim
   * @returns {Promise<boolean>} true if accepted
   */
  async acceptGossipedClaim(claim) {
    if (!claim || claim.type !== "HandleClaimV1") return false;
    if (claim.isExpired()) return false;

    // TRUST-5: never accept a gossiped handle->key mapping by shape alone. The
    // registrar relay signs the claim with its node key; verify that signature
    // against the pinned registrar key before storing/re-serving it. Fail-closed.
    if (!(await this._verifyGossipedClaimSignature(claim))) {
      return false;
    }

    const existing = await this.#getClaim(claim.handle);
    if (existing && !existing.isExpired()) {
      // First-come-first-served: keep the older claim
      if (existing.createdAtMs <= claim.createdAtMs) return false;
    }

    await this.#kvStore.set(KV_PREFIX + claim.handle, claim.toJSON());
    return true;
  }

  /**
   * List all non-expired handle claims.
   * @returns {Promise<HandleClaimV1[]>}
   */
  async listClaims() {
    const keys = await this.#kvStore.keys(KV_PREFIX);
    const claims = [];
    for (const key of keys) {
      const json = await this.#kvStore.get(key);
      if (!json) continue;
      try {
        const claim = HandleClaimV1.fromJSON(json);
        if (!claim.isExpired()) {
          claims.push(claim);
        }
      } catch (_err) {
        // Skip corrupted entries
      }
    }
    return claims;
  }

  async #getClaim(handle) {
    const json = await this.#kvStore.get(KV_PREFIX + handle);
    if (!json) return null;
    try {
      return HandleClaimV1.fromJSON(json);
    } catch (_err) {
      return null;
    }
  }

  #normalizeHandle(handle) {
    if (typeof handle !== "string") throw new Error("Handle must be a string");
    return handle.trim().toLowerCase();
  }
}
