/**
 * Pending-acknowledgement tracker for dht.rec_store (ATLAS_PREREQUISITES
 * P4.2). A separate, narrowly owned waiter rather than a generalization of
 * DhtQueryWaiter: that waiter's resolution shape is hardcoded to the lookup
 * result `{ value, nodes }` (including on timeout and clear()), and an ack
 * additionally needs pre-consumption validation — a mismatched ack must be
 * IGNORED (leaving the timeout to fire), not consume the pending slot.
 *
 * Sender-side acceptance rules (all enforced here):
 * - same authenticated socket the request was sent on;
 * - same requestId;
 * - same slot key;
 * - same record digest.
 * Late, duplicate, mismatched-socket, wrong-key, wrong-digest, and
 * unknown-request acks are ignored with a typed verdict for the caller's log.
 */
import { randomBytes } from "node:crypto";

export class DhtRecordStoreAckWaiter {
  /** @type {Map<string, object>} requestId -> pending state */
  #pending;

  /** @type {number} */
  #ackTimeoutMs;

  constructor({ ackTimeoutMs = 3000 } = {}) {
    this.#pending = new Map();
    this.#ackTimeoutMs = ackTimeoutMs;
  }

  newRequestId() {
    // Unguessable id (same rationale as DhtQueryWaiter / HIGH-9): an off-path
    // peer must not be able to forge an ack for a request it never saw.
    return "rec-s-" + randomBytes(16).toString("base64url");
  }

  /**
   * Register a pending store request and get a promise for its outcome.
   * Resolves with one of:
   *   { outcome: "stored" | "refreshed" | "rejected", reason: string|null }
   *   { outcome: "timeout" }   — no acceptable ack within the deadline
   * @param {string} requestId
   * @param {object} socket - the socket the request is sent on
   * @param {string} key - slot id the request names
   * @param {string} recordDigestHex - digest the ack must echo
   * @returns {Promise<{ outcome: string, reason: string|null }>}
   */
  wait(requestId, socket, key, recordDigestHex) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#pending.delete(requestId);
        resolve({ outcome: "timeout", reason: null });
      }, this.#ackTimeoutMs);
      if (typeof timer.unref === "function") timer.unref();
      this.#pending.set(requestId, { socket, key, recordDigestHex, timer, resolve });
    });
  }

  /**
   * Attempt to resolve a pending request with a received ack.
   * @param {object} socket - socket the ack arrived on
   * @param {import("../../contracts/wireRecords/DhtRecordStore.js").DhtRecordStoreAckV1} ack
   * @returns {string} "ok" | "unknown-request" | "socket-mismatch" | "key-mismatch" | "digest-mismatch"
   */
  resolve(socket, ack) {
    const state = this.#pending.get(ack.requestId);
    if (!state) return "unknown-request";
    if (state.socket !== socket) return "socket-mismatch";
    if (state.key !== ack.key) return "key-mismatch";
    if (state.recordDigestHex !== ack.recordDigestHex) return "digest-mismatch";
    clearTimeout(state.timer);
    this.#pending.delete(ack.requestId);
    state.resolve({ outcome: ack.status, reason: ack.reason });
    return "ok";
  }

  /** Fail all pending waits (shutdown). Each resolves as a timeout. */
  clear() {
    for (const [requestId, state] of this.#pending) {
      clearTimeout(state.timer);
      state.resolve({ outcome: "timeout", reason: null });
      this.#pending.delete(requestId);
    }
  }

  get pendingCount() {
    return this.#pending.size;
  }
}
