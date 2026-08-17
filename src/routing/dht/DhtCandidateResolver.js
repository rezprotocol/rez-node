/**
 * Authenticated candidate resolution for DHT lookups (ATLAS_PREREQUISITES
 * P3.1). One responsibility: given a discovered candidate relay ID, return an
 * already-authenticated (or newly authenticated) socket bound to that ID, or
 * a typed failure.
 *
 * Hard rules:
 * 1. requires a valid, unexpired, identity-bound descriptor already admitted
 *    to RelayStore (the pool enforces this at dial time);
 * 2. reuses/establishes through RelayConnectionPool only;
 * 3. verifies the authenticated peer matches the requested relay ID (pool
 *    enforces via the expected-relay handshake check + post-auth assert);
 * 4. NEVER dials an endpoint supplied by an untrusted DHT reply;
 * 5. exposes no plugin surface and no arbitrary-URL dial interface.
 *
 * Budgets: per-lookup dial cap, global concurrency cap, per-candidate dial
 * timeout, and a negative cache so a dead candidate is not re-dialed for
 * `negativeCacheMs`.
 */
import { isCanonicalRelayKeyId } from "@rezprotocol/core";
import { raceWithDeadline } from "../../util/raceWithDeadline.js";

export const CANDIDATE_RESOLUTION_FAILURES = Object.freeze([
  "invalid-relay-id",
  "no-descriptor",
  "dial-timeout",
  "dial-failed",
  "identity-mismatch",
  "budget-exhausted",
  "negative-cached",
  "resolver-closed",
]);

export class DhtCandidateResolver {
  #pool;
  #dialTimeoutMs;
  #negativeCacheMs;
  #maxConcurrentDials;
  #nowMs;
  /** @type {Map<string, number>} relayKeyId -> negative-cache expiry */
  #negativeCache;
  #inFlight;
  #closed;

  /**
   * @param {object} options
   * @param {import("../../network/RelayConnectionPool.js").RelayConnectionPool} options.pool
   * @param {number} [options.dialTimeoutMs]
   * @param {number} [options.negativeCacheMs]
   * @param {number} [options.maxConcurrentDials]
   * @param {() => number} [options.nowMs]
   */
  constructor({ pool, dialTimeoutMs = 3000, negativeCacheMs = 30_000, maxConcurrentDials = 4, nowMs = () => Date.now() } = {}) {
    if (!pool || typeof pool.getAuthenticatedRelaySocket !== "function") {
      throw new Error("DhtCandidateResolver requires a RelayConnectionPool with getAuthenticatedRelaySocket");
    }
    this.#pool = pool;
    this.#dialTimeoutMs = dialTimeoutMs;
    this.#negativeCacheMs = negativeCacheMs;
    this.#maxConcurrentDials = maxConcurrentDials;
    this.#nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
    this.#negativeCache = new Map();
    this.#inFlight = 0;
    this.#closed = false;
  }

  close() {
    this.#closed = true;
  }

  /**
   * @param {string} relayKeyId - canonical self-certifying relay id
   * @returns {Promise<{ ok: true, socket: object } | { ok: false, reason: string }>}
   */
  async resolve(relayKeyId) {
    if (this.#closed) return { ok: false, reason: "resolver-closed" };
    if (!isCanonicalRelayKeyId(relayKeyId)) {
      return { ok: false, reason: "invalid-relay-id" };
    }
    const now = this.#nowMs();
    const cachedUntil = this.#negativeCache.get(relayKeyId);
    if (Number.isFinite(cachedUntil)) {
      if (cachedUntil > now) return { ok: false, reason: "negative-cached" };
      this.#negativeCache.delete(relayKeyId);
    }
    if (this.#inFlight >= this.#maxConcurrentDials) {
      return { ok: false, reason: "budget-exhausted" };
    }

    this.#inFlight += 1;
    try {
      const socket = await raceWithDeadline(
        this.#pool.getAuthenticatedRelaySocket(relayKeyId),
        this.#dialTimeoutMs,
        DIAL_TIMEOUT,
      );
      if (socket === DIAL_TIMEOUT) {
        this.#negativeCache.set(relayKeyId, this.#nowMs() + this.#negativeCacheMs);
        return { ok: false, reason: "dial-timeout" };
      }
      return { ok: true, socket };
    } catch (err) {
      if (!(err instanceof Error)) throw err;
      this.#negativeCache.set(relayKeyId, this.#nowMs() + this.#negativeCacheMs);
      const message = err.message || "";
      if (message.includes("no admitted descriptor") || message.includes("no relay store")
        || message.includes("no dialable endpoint")) {
        return { ok: false, reason: "no-descriptor" };
      }
      if (message.includes("authenticated peer is not")) {
        return { ok: false, reason: "identity-mismatch" };
      }
      return { ok: false, reason: "dial-failed" };
    } finally {
      this.#inFlight -= 1;
    }
  }
}

const DIAL_TIMEOUT = Symbol("dht-candidate-dial-timeout");
