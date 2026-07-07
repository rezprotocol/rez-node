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
    this.#store(account, state, now);
    return state;
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

  #store(account, state, now) {
    // Refresh insertion order: delete then set so the account moves to the tail.
    if (this.#entries.has(account)) this.#entries.delete(account);
    this.#entries.set(account, { expiresAtMs: now + this.#ttlMs, state });
    while (this.#entries.size > this.#maxEntries) {
      const oldest = this.#entries.keys().next().value;
      if (oldest === undefined) break;
      this.#entries.delete(oldest);
    }
  }
}
