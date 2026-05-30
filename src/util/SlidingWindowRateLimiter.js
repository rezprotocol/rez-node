/**
 * Generic per-subject sliding-window rate limiter. Used by both:
 *
 *   - `DhtProtocol#handleStore` keyed on peer relayKeyId (docs/SECURITY_AUDIT.md LOW-6).
 *   - `GatewaySession` keyed on peer IP for `session.hello` (docs/SECURITY_AUDIT.md
 *     LOW observation: "session.hello has no direct per-IP rate limit").
 *
 * In-memory, ephemeral. No persistence; window state is reset on
 * restart. LRU-capped to bound memory under a sybil-keypair flood.
 *
 * A missing or empty subject key is treated as "skip rate-limit" (the
 * caller should already have shaped anonymous traffic upstream; this is
 * defensive against bugs that surface a null subject).
 */
const DEFAULT_WINDOW_MS = 60_000;
const DEFAULT_MAX_ATTEMPTS = 600;
const DEFAULT_LRU_CAP = 4096;

export class SlidingWindowRateLimiter {
  /** @type {Map<string, number[]>} subjectKey -> ascending timestamps within window */
  #buckets;

  /** @type {number} */
  #windowMs;

  /** @type {number} */
  #maxAttempts;

  /** @type {number} */
  #lruCap;

  /**
   * @param {object} [options]
   * @param {number} [options.windowMs] sliding-window width in ms (default 60s)
   * @param {number} [options.maxAttempts] max attempts per subject within the window (default 600)
   * @param {number} [options.lruCap] max distinct subject keys retained in memory (default 4096)
   */
  constructor({ windowMs, maxAttempts, lruCap } = {}) {
    this.#windowMs = typeof windowMs === "number" && windowMs > 0 ? windowMs : DEFAULT_WINDOW_MS;
    this.#maxAttempts = typeof maxAttempts === "number" && maxAttempts > 0 ? maxAttempts : DEFAULT_MAX_ATTEMPTS;
    this.#lruCap = typeof lruCap === "number" && lruCap > 0 ? lruCap : DEFAULT_LRU_CAP;
    this.#buckets = new Map();
  }

  get windowMs() {
    return this.#windowMs;
  }

  get maxAttempts() {
    return this.#maxAttempts;
  }

  /**
   * Atomically check the subject's budget and, if there's room, record
   * the attempt. Returns true if the attempt was admitted; false if the
   * cap is exhausted.
   *
   * @param {string|null|undefined} subjectKey
   * @param {number} nowMs
   * @returns {boolean}
   */
  record(subjectKey, nowMs) {
    if (typeof subjectKey !== "string" || subjectKey.length === 0) return true;
    if (!Number.isFinite(nowMs)) return true;
    const cutoff = nowMs - this.#windowMs;
    const existing = this.#buckets.get(subjectKey);
    const pruned = existing ? existing.filter((t) => t > cutoff) : [];
    if (pruned.length >= this.#maxAttempts) {
      this.#buckets.set(subjectKey, pruned);
      return false;
    }
    pruned.push(nowMs);
    this.#buckets.set(subjectKey, pruned);
    this.#enforceLru(subjectKey);
    return true;
  }

  /**
   * Drop a subject's bucket. Sliding-window pruning already bounds
   * growth, so this is optional cleanup.
   * @param {string} subjectKey
   */
  forget(subjectKey) {
    if (typeof subjectKey !== "string" || subjectKey.length === 0) return;
    this.#buckets.delete(subjectKey);
  }

  /** @returns {number} number of tracked subject keys (diagnostics) */
  get size() {
    return this.#buckets.size;
  }

  #enforceLru(currentKey) {
    if (this.#buckets.size <= this.#lruCap) return;
    for (const k of this.#buckets.keys()) {
      if (k === currentKey) continue;
      this.#buckets.delete(k);
      if (this.#buckets.size <= this.#lruCap) return;
    }
  }
}
