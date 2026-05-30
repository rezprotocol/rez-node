/**
 * Local store for DHT values (route entries) that this node is
 * responsible for storing on behalf of the network.
 *
 * Each value has a TTL. Expired values are not returned and are
 * cleaned up by periodic eviction.
 */
export class DhtValueStore {
  /** @type {Map<string, { routeEntry: object, storedAtMs: number, ttlMs: number }>} */
  #entries;

  /** @type {number} */
  #defaultTtlMs;

  /**
   * @param {{ defaultTtlMs?: number }} options
   */
  constructor({ defaultTtlMs = 86_400_000 } = {}) {
    if (!Number.isFinite(defaultTtlMs) || defaultTtlMs <= 0) {
      throw new Error("DhtValueStore defaultTtlMs must be a positive number");
    }
    this.#entries = new Map();
    this.#defaultTtlMs = defaultTtlMs;
  }

  /**
   * Store a route entry for the given inboxId. Pass null routeEntry to
   * remove (withdrawal).
   *
   * Monotonic-by-issuedAtMs: if an existing entry's registration carries
   * a `issuedAtMs` GREATER than the incoming entry's, the incoming is
   * silently rejected. Closes the "delegation-replay-overwrite"
   * observation in docs/SECURITY_AUDIT.md (pass 3) — a peer that observed
   * an old still-valid delegation could `dht.store` it on top of a newer
   * one and re-route to the previous (possibly dead) node. Equal
   * `issuedAtMs` is allowed (idempotent re-store keeps the freshest
   * TTL window).
   *
   * Returns `{ stored, reason }`. Callers may log on `stored === false`.
   *
   * @param {string} inboxId
   * @param {object|null} routeEntry - null removes the entry
   * @param {number} nowMs
   * @param {{ ttlMs?: number }} [options]
   * @returns {{ stored: boolean, reason: string|null }}
   */
  store(inboxId, routeEntry, nowMs, { ttlMs } = {}) {
    if (typeof inboxId !== "string" || inboxId.trim().length === 0) {
      throw new Error("DhtValueStore.store requires a non-empty inboxId");
    }
    if (!Number.isFinite(nowMs)) {
      throw new Error("DhtValueStore.store requires a finite nowMs");
    }
    // null routeEntry = withdrawal (tombstone removal)
    if (routeEntry === null) {
      this.#entries.delete(inboxId);
      return { stored: true, reason: null };
    }
    if (typeof routeEntry !== "object") {
      throw new Error("DhtValueStore.store requires a routeEntry object or null");
    }
    const incomingIssuedAtMs = readIssuedAtMs(routeEntry);
    const existing = this.#entries.get(inboxId);
    if (existing) {
      const existingIssuedAtMs = readIssuedAtMs(existing.routeEntry);
      // Only enforce when both sides actually carry an issuedAtMs.
      // Pre-HIGH-8 entries (none in production) or shape-incomplete
      // entries fall back to last-write-wins.
      if (
        incomingIssuedAtMs !== null
        && existingIssuedAtMs !== null
        && incomingIssuedAtMs < existingIssuedAtMs
      ) {
        return { stored: false, reason: "older-delegation" };
      }
    }
    const effectiveTtl = Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : this.#defaultTtlMs;
    this.#entries.set(inboxId, {
      routeEntry,
      storedAtMs: nowMs,
      ttlMs: effectiveTtl,
    });
    return { stored: true, reason: null };
  }

  /**
   * Retrieve a route entry. Returns null if not found or expired.
   *
   * @param {string} inboxId
   * @param {number} nowMs
   * @returns {object|null}
   */
  get(inboxId, nowMs) {
    const record = this.#entries.get(inboxId);
    if (!record) return null;
    if (nowMs - record.storedAtMs >= record.ttlMs) {
      this.#entries.delete(inboxId);
      return null;
    }
    return record.routeEntry;
  }

  /**
   * Remove a specific entry.
   * @param {string} inboxId
   * @returns {boolean}
   */
  remove(inboxId) {
    return this.#entries.delete(inboxId);
  }

  /**
   * Remove all expired entries. Returns the number evicted.
   * @param {number} nowMs
   * @returns {number}
   */
  evictExpired(nowMs) {
    let count = 0;
    for (const [inboxId, record] of this.#entries) {
      if (nowMs - record.storedAtMs >= record.ttlMs) {
        this.#entries.delete(inboxId);
        count += 1;
      }
    }
    return count;
  }

  /**
   * Return all non-expired entries.
   * @param {number} nowMs
   * @returns {Map<string, object>}
   */
  getAll(nowMs) {
    const result = new Map();
    for (const [inboxId, record] of this.#entries) {
      if (nowMs - record.storedAtMs < record.ttlMs) {
        result.set(inboxId, record.routeEntry);
      }
    }
    return result;
  }

  /** @returns {number} */
  get size() {
    return this.#entries.size;
  }
}

function readIssuedAtMs(routeEntry) {
  if (!routeEntry || typeof routeEntry !== "object") return null;
  const reg = routeEntry.registration;
  if (!reg || typeof reg !== "object") return null;
  const v = reg.issuedAtMs;
  return Number.isFinite(v) ? v : null;
}
