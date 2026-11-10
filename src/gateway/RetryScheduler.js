const DEFAULT_POLL_INTERVAL_MS = 15_000; // 15 seconds
const DEFAULT_PRUNE_INTERVAL_MS = 5 * 60_000; // 5 minutes

/**
 * RetryScheduler — periodic timer that checks the PersistentOutboundQueue
 * for entries ready to retry, attempts delivery, and updates entry state.
 *
 * Also periodically prunes expired entries.
 * Polls and route-discovery flushes share one attempt per queued record,
 * including its durable state update. This does not promise exactly-once
 * delivery across a crash or an ambiguous remote acknowledgement.
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
  #polling = false;
  #inFlight = new Map();

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
    this.#pollTimer = setInterval(() => {
      this.#tick().catch(() => {
        console.error("[RetryScheduler] retry cycle failed; queued work will be retried");
      });
    }, this.#pollIntervalMs);
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
   * Only entries whose backoff window has elapsed are attempted; a route that
   * flaps (repeated route-added events) therefore cannot re-blast — and
   * re-deposit — the same backed-off entry on every flap.
   * @param {string} [deliverInboxId] — if provided, only retry entries for this inbox
   */
  async flushForInbox(deliverInboxId) {
    if (!deliverInboxId) return;
    const entries = this.#queue.getRetryableForInbox(deliverInboxId);
    for (const entry of entries) {
      await this.#attemptDelivery(entry);
    }
  }

  async #tick() {
    if (!this.#running || this.#polling) return;
    this.#polling = true;
    try {
      const retryable = this.#queue.getRetryable();
      for (const entry of retryable) {
        if (!this.#running) break;
        try {
          await this.#attemptDelivery(entry);
        } catch {
          // A failed durable update must neither crash the node nor starve
          // unrelated packets. Do not log record keys, inboxes or backend paths.
          console.error("[RetryScheduler] retry state update failed; continuing queued work");
        }
      }
    } finally {
      this.#polling = false;
    }
  }

  async #attemptDelivery(entry) {
    const active = this.#inFlight.get(entry.queueId);
    if (active) return active;
    // Polls and route callbacks hold snapshots across awaits. A previous
    // attempt may have removed or replaced this record, or entered backoff.
    if (!this.#queue.getRetryableForInbox(entry.deliverInboxId).includes(entry)) return;
    // Defer sendFn until the shared guard is installed, including synchronous
    // re-entry from a sender. Hold it through the durable completion/failure.
    const attempt = Promise.resolve().then(async () => {
      try {
        await this.#sendFn(entry);
        await this.#queue.markDelivered(entry.queueId);
      } catch {
        await this.#queue.recordAttemptFailure(entry.queueId);
      }
    });
    this.#inFlight.set(entry.queueId, attempt);
    try {
      await attempt;
    } finally {
      this.#inFlight.delete(entry.queueId);
    }
  }

  async #prune() {
    if (!this.#running) return;
    try {
      await this.#queue.pruneExpired();
    } catch (err) {
      console.error("[RetryScheduler] prune failed; durable cleanup will retry");
    }
  }
}
