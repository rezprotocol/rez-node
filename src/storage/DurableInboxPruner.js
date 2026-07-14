/**
 * Periodic maintenance sweep for the durable home inbox.
 *
 * The durable log is append-only and bounded by per-inbox event/byte caps
 * (PgDurableInbox maxEvents/maxBytes). Nothing deletes consumed events on the
 * hot path — `cursorAck` only advances a cursor, it never removes a row — so
 * without a scheduled prune the caps fill with already-consumed events and
 * `append` throws `InboxCapExceededError` forever, permanently wedging the
 * inbox. This sweep reclaims:
 *   - events at/below the slowest LIVE device's cursor (the common case), and
 *   - the whole backlog of a fully-abandoned inbox via the TTL backstop.
 * `staleGraceMs` excludes silent devices from the prune watermark so an
 * abandoned cursor cannot pin an inbox's rows forever (plan threat-model P1#1).
 *
 * Lifecycle mirrors RetryScheduler: start() after the runtime is ready, stop()
 * on shutdown. The timer is unref'd so it never keeps the process alive, and a
 * sweep that is still running is not re-entered.
 */

// Reclaim consumed events frequently enough that a busy inbox never approaches
// its cap between sweeps, but cheaply (the sweep is a per-inbox advisory-locked
// DELETE below the live cursor — idle inboxes touch nothing).
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
// A device silent longer than this is excluded from the prune watermark (its
// un-advanced cursor no longer pins rows). It still keeps any mail at/above
// another live device's cursor; only the gap it alone was holding is reclaimed.
const DEFAULT_STALE_GRACE_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
// A fully-abandoned inbox (no live, non-revoked devices) reclaims events older
// than this. The home is the system of record, so the backstop is generous.
const DEFAULT_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

export class DurableInboxPruner {
  #durableInbox;
  #intervalMs;
  #ttlMs;
  #staleGraceMs;
  #logger;
  #timer;
  #running;
  #sweeping;
  #accountMutationSerializer;
  #journalTtlMs;

  constructor({ durableInbox, intervalMs, ttlMs, staleGraceMs, accountMutationSerializer = null, journalTtlMs, logger = console } = {}) {
    if (!durableInbox || typeof durableInbox.pruneAll !== "function") {
      throw new Error("DurableInboxPruner requires a durableInbox with pruneAll()");
    }
    this.#durableInbox = durableInbox;
    this.#intervalMs = typeof intervalMs === "number" && intervalMs > 0 ? intervalMs : DEFAULT_SWEEP_INTERVAL_MS;
    this.#ttlMs = typeof ttlMs === "number" && ttlMs > 0 ? ttlMs : DEFAULT_TTL_MS;
    this.#staleGraceMs = typeof staleGraceMs === "number" && staleGraceMs > 0 ? staleGraceMs : DEFAULT_STALE_GRACE_MS;
    // Audit R4 F3: the same sweep also prunes the account-mutation journal's replay
    // payload (result_json) past its retention window (the serializer keeps the audit
    // row). Optional — only a pg cluster node with the serializer wired supplies it.
    this.#accountMutationSerializer = accountMutationSerializer && typeof accountMutationSerializer.pruneExpiredReplayPayloads === "function"
      ? accountMutationSerializer
      : null;
    this.#journalTtlMs = typeof journalTtlMs === "number" && journalTtlMs > 0 ? journalTtlMs : DEFAULT_TTL_MS;
    this.#logger = logger;
    this.#timer = null;
    this.#running = false;
    this.#sweeping = false;
  }

  start() {
    if (this.#running) return;
    this.#running = true;
    this.#timer = setInterval(() => this.#sweep(), this.#intervalMs);
    // Unref so the sweep timer never holds the process (or a test runner) open.
    if (this.#timer && typeof this.#timer.unref === "function") {
      this.#timer.unref();
    }
  }

  stop() {
    this.#running = false;
    if (this.#timer) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
  }

  /**
   * Run one full sweep now (used by start()'s timer and directly by tests). A
   * sweep already in flight is not re-entered, so a slow Pg pass cannot stack.
   * @returns {Promise<{ inboxesSwept: number, deleted: number } | null>}
   */
  async sweep() {
    if (this.#sweeping) return null;
    this.#sweeping = true;
    try {
      const inbox = await this.#durableInbox.pruneAll({ ttlMs: this.#ttlMs, staleGraceMs: this.#staleGraceMs });
      let journalReplayExpired = 0;
      if (this.#accountMutationSerializer) {
        journalReplayExpired = await this.#accountMutationSerializer.pruneExpiredReplayPayloads(Date.now(), this.#journalTtlMs);
      }
      return { ...inbox, journalReplayExpired };
    } finally {
      this.#sweeping = false;
    }
  }

  async #sweep() {
    if (!this.#running) return;
    try {
      const result = await this.sweep();
      if (result && (result.deleted > 0 || result.journalReplayExpired > 0)) {
        this.#logger.log(
          "[DurableInboxPruner] swept " + result.inboxesSwept + " inbox(es), deleted "
            + result.deleted + " event(s), pruned " + (result.journalReplayExpired || 0) + " journal replay payload(s)",
        );
      }
    } catch (err) {
      this.#logger.error("[DurableInboxPruner] sweep failed: " + (err && err.message ? err.message : err));
    }
  }
}
