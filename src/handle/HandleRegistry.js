import { HandleClaimV1, DEFAULT_TTL_MS } from "@rezprotocol/core";

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

  /**
   * @param {object} opts
   * @param {KeyValueStore} opts.kvStore — persistent storage for claims
   * @param {ReceiptSigner} opts.receiptSigner — signs new claims
   * @param {string} opts.selfRelayKeyId — this relay's key ID
   */
  constructor({ kvStore, receiptSigner, selfRelayKeyId }) {
    if (!kvStore) throw new Error("HandleRegistry requires kvStore");
    if (!receiptSigner) throw new Error("HandleRegistry requires receiptSigner");
    if (!selfRelayKeyId || typeof selfRelayKeyId !== "string") throw new Error("HandleRegistry requires selfRelayKeyId");
    this.#kvStore = kvStore;
    this.#receiptSigner = receiptSigner;
    this.#selfRelayKeyId = selfRelayKeyId;
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
