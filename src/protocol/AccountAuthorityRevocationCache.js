/**
 * Bounded-staleness cache over the home's account authority-state (S2.5 S11, F4).
 *
 * A delegated session (or a device-set record signed by a delegated device) is
 * only as trustworthy as the account's CURRENT revocation state. The authority
 * home owns that state (PgAccountMutationSerializer.getAuthorityState); this cache
 * lets the hot verify paths consult it without a Postgres round-trip on every
 * check, accepting a bounded staleness window (a freshly-revoked cert can survive
 * up to `ttlMs`; the home is the eventual enforcement point).
 *
 * BYTE-COMPATIBILITY INVARIANT: `resolve()` returns `null` — never `{}` — when an
 * account has no revocations (no revoked certs AND no issued-at cutoff). The
 * downstream verifiers (`verifyAccountAuthority`, `verifyDurableRecordV2`) treat a
 * null `revocationState` as the untouched primary path, so a never-revoked account
 * stays byte-for-byte identical to the pre-S11 behavior.
 *
 * The cache is bounded (LRU-ish: oldest insertion evicted past `maxEntries`) so a
 * flood of distinct account identities cannot grow it without limit.
 */

const DEFAULT_TTL_MS = 30_000;
const DEFAULT_MAX_ENTRIES = 4096;

export class AccountAuthorityRevocationCache {
  #serializer;
  #ttlMs;
  #maxEntries;
  #nowMs;
  #entries; // Map<accountB64, { expiresAtMs, state }>  (state may be null)

  constructor({ serializer, ttlMs = DEFAULT_TTL_MS, maxEntries = DEFAULT_MAX_ENTRIES, nowMs = () => Date.now() } = {}) {
    if (!serializer || typeof serializer.getAuthorityState !== "function") {
      throw new Error("AccountAuthorityRevocationCache requires a serializer with getAuthorityState");
    }
    this.#serializer = serializer;
    this.#ttlMs = Number.isFinite(ttlMs) && ttlMs >= 0 ? ttlMs : DEFAULT_TTL_MS;
    this.#maxEntries = Number.isInteger(maxEntries) && maxEntries > 0 ? maxEntries : DEFAULT_MAX_ENTRIES;
    this.#nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
    this.#entries = new Map();
  }

  /**
   * The revocationState projection for an account, or null when it has none.
   * @param {string} accountIdentityPublicKeyB64
   * @returns {Promise<{revokedCertIds: string[], minValidIssuedAtMs: number}|null>}
   */
  async resolve(accountIdentityPublicKeyB64) {
    const account = typeof accountIdentityPublicKeyB64 === "string" && accountIdentityPublicKeyB64.trim().length > 0
      ? accountIdentityPublicKeyB64.trim()
      : null;
    if (!account) return null;

    const now = this.#nowMs();
    const hit = this.#entries.get(account);
    if (hit && hit.expiresAtMs > now) {
      return hit.state;
    }

    const authorityState = await this.#serializer.getAuthorityState(account);
    const state = this.#project(authorityState);
    this.#store(account, state, now, this.#epochOf(authorityState));
    return state;
  }

  /**
   * The account's current authority epoch — a cheap, ALWAYS-FRESH single-row read (passthrough to
   * the serializer). The per-dispatch L5 guard (review finding 1) calls this on every delegated
   * frame as its fast path: an epoch unchanged since admission proves authority is unchanged.
   * @returns {Promise<number>} epoch (0 for a blank account or one with no authority row)
   */
  async currentEpoch(accountIdentityPublicKeyB64) {
    const account = typeof accountIdentityPublicKeyB64 === "string" && accountIdentityPublicKeyB64.trim().length > 0
      ? accountIdentityPublicKeyB64.trim()
      : null;
    if (!account) return 0;
    return this.#serializer.getCurrentEpoch(account);
  }

  /**
   * Like resolve(), but ALWAYS reads the home — never serves a live cache entry — AND returns the
   * coherent epoch alongside the projected state. The L5 guard uses this on an epoch advance (and
   * at admission) both to re-verify against fresh revocation state and to reset its epoch watermark
   * to the epoch that state belongs to. getAuthorityState reads epoch + revoked set in one snapshot,
   * so `epoch` and `state` are mutually consistent. Still refreshes the warm entry (via #store, which
   * rejects a regressing epoch — review finding 2). null-when-empty invariant preserved on `state`.
   * @returns {Promise<{state: {revokedCertIds: string[], minValidIssuedAtMs: number}|null, epoch: number}>}
   */
  async resolveFreshWithEpoch(accountIdentityPublicKeyB64) {
    const account = typeof accountIdentityPublicKeyB64 === "string" && accountIdentityPublicKeyB64.trim().length > 0
      ? accountIdentityPublicKeyB64.trim()
      : null;
    if (!account) return { state: null, epoch: 0 };

    const authorityState = await this.#serializer.getAuthorityState(account);
    const state = this.#project(authorityState);
    const epoch = this.#epochOf(authorityState);
    this.#store(account, state, this.#nowMs(), epoch);
    return { state, epoch };
  }

  /**
   * Always-fresh projected revocation state (no epoch). Thin wrapper over resolveFreshWithEpoch for
   * callers that only need the state.
   * @returns {Promise<{revokedCertIds: string[], minValidIssuedAtMs: number}|null>}
   */
  async resolveFresh(accountIdentityPublicKeyB64) {
    return (await this.resolveFreshWithEpoch(accountIdentityPublicKeyB64)).state;
  }

  /** Drop a cached entry so the next resolve() re-reads the home (e.g. post-revoke). */
  invalidate(accountIdentityPublicKeyB64) {
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    if (account.length > 0) this.#entries.delete(account);
  }

  // null-when-empty: no revoked certs AND no issued-at cutoff ⇒ primary path.
  #project(authorityState) {
    if (!authorityState || typeof authorityState !== "object") return null;
    const revokedCertIds = Array.isArray(authorityState.revokedCertIds) ? authorityState.revokedCertIds : [];
    const minValidIssuedAtMs = Number.isFinite(Number(authorityState.minValidIssuedAtMs))
      ? Number(authorityState.minValidIssuedAtMs)
      : 0;
    if (revokedCertIds.length === 0 && minValidIssuedAtMs === 0) return null;
    return { revokedCertIds: [...revokedCertIds], minValidIssuedAtMs };
  }

  // The account authority epoch a snapshot was read at (monotonic per account). getAuthorityState
  // reads epoch + revoked set in one snapshot, so this epoch is coherent with `state`.
  #epochOf(authorityState) {
    if (!authorityState || typeof authorityState !== "object") return 0;
    const epoch = Number(authorityState.epoch);
    return Number.isFinite(epoch) && epoch >= 0 ? epoch : 0;
  }

  #store(account, state, now, sourceEpoch) {
    // Review finding 2: reject a REGRESSING store. Two reads (resolve/resolveFresh) can complete
    // out of order; an older read (lower epoch) finishing AFTER a newer one must NOT overwrite the
    // cache with stale revocation state (which would let a revoked cert re-authorize for the TTL).
    // The account epoch is monotonic, so a strictly-lower source epoch means a staler snapshot —
    // drop it (do not even refresh TTL/order). Equal-or-greater refreshes normally.
    const existing = this.#entries.get(account);
    if (existing && existing.sourceEpoch > sourceEpoch) return;
    // Refresh insertion order: delete then set so the account moves to the tail.
    if (this.#entries.has(account)) this.#entries.delete(account);
    this.#entries.set(account, { expiresAtMs: now + this.#ttlMs, state, sourceEpoch });
    while (this.#entries.size > this.#maxEntries) {
      const oldest = this.#entries.keys().next().value;
      if (oldest === undefined) break;
      this.#entries.delete(oldest);
    }
  }
}
