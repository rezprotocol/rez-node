import { randomBytes } from "node:crypto";

/**
 * Pending-query waiter shared by the DHT control protocols (DhtProtocol and
 * DurableRecordProtocol). Owns the queryId→pending map, the per-query
 * timeout, and the HIGH-9 same-socket reply guard so the two protocols can't
 * drift on this security-relevant machinery.
 *
 * Reply shape is the DHT lookup contract `{ value, nodes }`; the timeout (and
 * `clear`) resolve to `{ value: null, nodes: [] }`.
 */
export class DhtQueryWaiter {
  /** @type {Map<string, { resolve: Function, timer: ReturnType<typeof setTimeout>, expectedSocket: object|null }>} */
  #pending;

  /** @type {number} */
  #queryTimeoutMs;

  /** @type {string} */
  #idPrefix;

  /**
   * @param {{ queryTimeoutMs?: number, idPrefix?: string }} [options]
   */
  constructor({ queryTimeoutMs = 3000, idPrefix = "dht-q" } = {}) {
    this.#pending = new Map();
    this.#queryTimeoutMs = queryTimeoutMs;
    this.#idPrefix = idPrefix;
  }

  /**
   * Generate a cryptographically unguessable queryId — closes
   * SECURITY_AUDIT HIGH-9 (a predictable counter let an observer race-forge
   * replies).
   * @returns {string}
   */
  newQueryId() {
    return this.#idPrefix + "-" + randomBytes(16).toString("base64url");
  }

  /**
   * Register a pending query and return a promise that resolves when the
   * matching reply arrives or the timeout fires. `expectedSocket` is enforced
   * at reply time (HIGH-9).
   * @param {string} queryId
   * @param {object|null} [expectedSocket]
   * @returns {Promise<{ value: object|null, nodes: Array }>}
   */
  wait(queryId, expectedSocket = null) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#pending.delete(queryId);
        resolve({ value: null, nodes: [] });
      }, this.#queryTimeoutMs);
      this.#pending.set(queryId, { resolve, timer, expectedSocket });
    });
  }

  /**
   * Resolve a pending query from an incoming reply.
   *
   * Returns a status so callers can preserve their own per-protocol logging:
   *   - "resolved"        — the pending query was fulfilled
   *   - "no-pending"      — unknown/expired queryId (silently ignored)
   *   - "socket-mismatch" — reply arrived on a different socket (HIGH-9 drop)
   *
   * @param {string} queryId
   * @param {object} socket - socket the reply arrived on
   * @param {{ value: object|null, nodes: Array }} result
   * @returns {"resolved"|"no-pending"|"socket-mismatch"}
   */
  resolve(queryId, socket, result) {
    const pending = this.#pending.get(queryId);
    if (!pending) return "no-pending";
    if (pending.expectedSocket && socket !== pending.expectedSocket) return "socket-mismatch";
    clearTimeout(pending.timer);
    this.#pending.delete(queryId);
    pending.resolve(result);
    return "resolved";
  }

  /**
   * Cancel all pending queries, resolving them to the empty result.
   */
  clear() {
    for (const [, pending] of this.#pending) {
      clearTimeout(pending.timer);
      pending.resolve({ value: null, nodes: [] });
    }
    this.#pending.clear();
  }
}
