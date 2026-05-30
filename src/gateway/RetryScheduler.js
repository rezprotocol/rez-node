const DEFAULT_POLL_INTERVAL_MS = 15_000; // 15 seconds
const DEFAULT_PRUNE_INTERVAL_MS = 5 * 60_000; // 5 minutes

/**
 * RetryScheduler — periodic timer that checks the PersistentOutboundQueue
 * for entries ready to retry, attempts delivery, and updates entry state.
 *
 * Also periodically prunes expired entries.
 *
 * Start/stop lifecycle: call start() after the gateway loop is ready,
 * stop() on shutdown.
 */
export class RetryScheduler {
  #queue;
  #sendFn;
  #pollIntervalMs;
  #pruneIntervalMs;
  #pollTimer = null;
  #pruneTimer = null;
  #running = false;

  /**
   * @param {object} opts
   * @param {PersistentOutboundQueue} opts.queue — the persistent queue to poll
   * @param {Function} opts.sendFn — async (entry) => void; attempts delivery
   * @param {number} [opts.pollIntervalMs=15000]
   * @param {number} [opts.pruneIntervalMs=300000]
   */
  constructor({ queue, sendFn, pollIntervalMs, pruneIntervalMs } = {}) {
    if (!queue || typeof queue.getRetryable !== "function") {
      throw new Error("RetryScheduler requires queue (PersistentOutboundQueue)");
    }
    if (typeof sendFn !== "function") {
      throw new Error("RetryScheduler requires sendFn");
    }
    this.#queue = queue;
    this.#sendFn = sendFn;
    this.#pollIntervalMs = typeof pollIntervalMs === "number" && pollIntervalMs > 0
      ? pollIntervalMs
      : DEFAULT_POLL_INTERVAL_MS;
    this.#pruneIntervalMs = typeof pruneIntervalMs === "number" && pruneIntervalMs > 0
      ? pruneIntervalMs
      : DEFAULT_PRUNE_INTERVAL_MS;
  }

  start() {
    if (this.#running) return;
    this.#running = true;
    this.#pollTimer = setInterval(() => this.#tick(), this.#pollIntervalMs);
    this.#pruneTimer = setInterval(() => this.#prune(), this.#pruneIntervalMs);
    // Unref timers so they don't prevent process exit
    if (this.#pollTimer && typeof this.#pollTimer.unref === "function") {
      this.#pollTimer.unref();
    }
    if (this.#pruneTimer && typeof this.#pruneTimer.unref === "function") {
      this.#pruneTimer.unref();
    }
  }

  stop() {
    this.#running = false;
    if (this.#pollTimer) {
      clearInterval(this.#pollTimer);
      this.#pollTimer = null;
    }
    if (this.#pruneTimer) {
      clearInterval(this.#pruneTimer);
      this.#pruneTimer = null;
    }
  }

  /**
   * Manually trigger a retry cycle (used by route-discovery flush).
   * @param {string} [deliverInboxId] — if provided, only retry entries for this inbox
   */
  async flushForInbox(deliverInboxId) {
    if (!deliverInboxId) return;
    const entries = this.#queue.getForInbox(deliverInboxId);
    for (const entry of entries) {
      await this.#attemptDelivery(entry);
    }
  }

  async #tick() {
    if (!this.#running) return;
    const retryable = this.#queue.getRetryable();
    for (const entry of retryable) {
      if (!this.#running) break;
      await this.#attemptDelivery(entry);
    }
  }

  async #attemptDelivery(entry) {
    try {
      await this.#sendFn(entry);
      await this.#queue.markDelivered(entry.queueId);
    } catch {
      await this.#queue.recordAttemptFailure(entry.queueId);
    }
  }

  async #prune() {
    if (!this.#running) return;
    try {
      await this.#queue.pruneExpired();
    } catch (err) {
      console.error("[RetryScheduler] prune failed: " + (err && err.message ? err.message : err));
    }
  }
}
