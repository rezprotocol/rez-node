import { randomUUID } from "node:crypto";
import { OutboundQueueEntryV1 } from "@rezprotocol/core";

const DEFAULT_MAX_PER_INBOX = 100;
const DEFAULT_MAX_TOTAL = 1000;
const DEFAULT_TTL_MS = 72 * 60 * 60 * 1000; // 72 hours
// Per-message attempt cap. Once an entry has failed this many times we give
// up and drop it (emitting "expired") instead of re-attempting until the 72h
// TTL. Each failed attempt can leave a deposit at the relay, so an uncapped
// retry of a permanently-stuck message is a deposit-amplification source.
// 12 attempts spans the full backoff schedule plus several 1h plateaus (~6h).
const DEFAULT_MAX_ATTEMPTS = 12;

// Exponential backoff schedule (ms)
const BACKOFF_SCHEDULE = [
  5_000,       // 5s
  15_000,      // 15s
  30_000,      // 30s
  60_000,      // 1m
  5 * 60_000,  // 5m
  15 * 60_000, // 15m
  30 * 60_000, // 30m
  60 * 60_000, // 1h (capped)
];

function backoffForAttempt(attempts) {
  const idx = Math.min(attempts, BACKOFF_SCHEDULE.length - 1);
  return BACKOFF_SCHEDULE[idx];
}

const KV_PREFIX = "outbound:queue:";
const KV_INDEX_PREFIX = "outbound:idx:";

/**
 * Persistent outbound message queue.
 *
 * Stores OutboundQueueEntryV1 records in an EncryptedKeyValueStore
 * so queued messages survive node restarts. Provides retry scheduling
 * with exponential backoff and TTL-based expiry.
 *
 * In-memory index mirrors KV store for fast inbox-level lookups.
 */
export class PersistentOutboundQueue {
  #kv;
  #maxPerInbox;
  #maxTotal;
  #maxAttempts;
  #ttlMs;
  #nowMs;
  #byInbox = new Map();   // deliverInboxId → Set<queueId>
  #entries = new Map();    // queueId → OutboundQueueEntryV1
  #onStatusChange = null;

  /**
   * @param {object} opts
   * @param {KeyValueStore} opts.keyValueStore — encrypted KV store for persistence
   * @param {number} [opts.maxPerInbox=100]
   * @param {number} [opts.maxTotal=1000]
   * @param {number} [opts.maxAttempts=12] — give up + drop after this many failures
   * @param {number} [opts.ttlMs=259200000] — time-to-live in ms (default 72h)
   * @param {Function} [opts.nowMs] — clock function
   */
  constructor({ keyValueStore, maxPerInbox, maxTotal, maxAttempts, ttlMs, nowMs } = {}) {
    if (!keyValueStore || typeof keyValueStore.set !== "function" || typeof keyValueStore.getStrict !== "function") {
      throw new Error("PersistentOutboundQueue requires keyValueStore with strict reads");
    }
    this.#kv = keyValueStore;
    this.#maxPerInbox = Math.max(1, Number(maxPerInbox) || DEFAULT_MAX_PER_INBOX);
    this.#maxTotal = Math.max(1, Number(maxTotal) || DEFAULT_MAX_TOTAL);
    this.#maxAttempts = Math.max(1, Number(maxAttempts) || DEFAULT_MAX_ATTEMPTS);
    this.#ttlMs = typeof ttlMs === "number" && ttlMs > 0 ? ttlMs : DEFAULT_TTL_MS;
    this.#nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
  }

  /**
   * Set callback for queue status changes.
   * Called with (queueId, status, entry) where status is "queued", "delivered", "expired".
   */
  setOnStatusChange(fn) {
    this.#onStatusChange = typeof fn === "function" ? fn : null;
  }

  /**
   * Load all queued entries from KV store into memory.
   * Call once at startup before processing.
   */
  async loadAll() {
    const keys = await this.#kv.keys(KV_PREFIX);
    const entries = new Map();
    const byInbox = new Map();
    const expired = [];
    for (const key of keys) {
      const json = await this.#kv.getStrict(key);
      if (json === undefined) continue;
      try {
        const entry = OutboundQueueEntryV1.fromJSON(json);
        if (key !== KV_PREFIX + entry.queueId) {
          throw new Error("queue key does not match queueId");
        }
        // Check TTL — prune expired entries on load
        if (this.#isExpired(entry)) {
          expired.push({ key, entry });
          continue;
        }
        if (entries.has(entry.queueId)) {
          throw new Error("duplicate queueId");
        }
        entries.set(entry.queueId, entry);
        let inboxEntries = byInbox.get(entry.deliverInboxId);
        if (!inboxEntries) {
          inboxEntries = new Set();
          byInbox.set(entry.deliverInboxId, inboxEntries);
        }
        inboxEntries.add(entry.queueId);
      } catch (err) {
        throw new Error("PersistentOutboundQueue durable entry is malformed: " + key, { cause: err });
      }
    }
    for (const item of expired) {
      await this.#kv.delete(item.key);
      this.#emitStatus(item.entry.queueId, "expired", item.entry);
    }
    this.#entries = entries;
    this.#byInbox = byInbox;
  }

  /**
   * Enqueue a message for delivery. Persists immediately.
   *
   * @param {object} opts
   * @param {string} opts.deliverInboxId
   * @param {Uint8Array} opts.innerBytes
   * @param {string} [opts.ownerPublicKeyB64] — originating session owner;
   *   used at status-change time to route notifications back to the right
   *   client. Nullable for callers that don't yet thread ownership through
   *   (older code paths, tests).
   * @returns {Promise<OutboundQueueEntryV1>}
   */
  async enqueue({ deliverInboxId, innerBytes, ownerPublicKeyB64 } = {}) {
    const now = this.#nowMs();
    const entry = new OutboundQueueEntryV1({
      queueId: randomUUID(),
      deliverInboxId,
      innerBytes,
      createdAtMs: now,
      attempts: 0,
      lastAttemptMs: 0,
      nextRetryMs: now, // immediate first attempt
      // DT-005 (hard retirement): receipts are retired and the return path is
      // no longer built, so a receipt inbox is inert metadata — and it names a
      // second inbox belonging to the sender, which is exactly the linkage the
      // queue should not persist at rest. NEW entries never carry it; the
      // field stays on the record and is still READ back for entries queued
      // before the retirement (OutboundQueueEntryV1.fromJSON, MeshBootstrap).
      receiptInboxId: null,
      ownerPublicKeyB64: ownerPublicKeyB64 || null,
    });

    // Enforce per-inbox limit — drop oldest
    await this.#enforcePerInboxLimit(deliverInboxId);
    // Enforce global limit — drop oldest across all inboxes
    await this.#enforceGlobalLimit();

    await this.#persist(entry);
    this.#entries.set(entry.queueId, entry);
    this.#indexAdd(deliverInboxId, entry.queueId);
    this.#emitStatus(entry.queueId, "queued", entry);
    return entry;
  }

  /**
   * Mark an entry as successfully delivered. Removes from queue.
   */
  async markDelivered(queueId) {
    const entry = this.#entries.get(queueId);
    if (!entry) return;
    await this.#kv.delete(KV_PREFIX + queueId);
    this.#entries.delete(queueId);
    this.#indexRemove(entry.deliverInboxId, queueId);
    this.#emitStatus(queueId, "delivered", entry);
  }

  /**
   * Record a failed delivery attempt. Updates backoff timing and persists.
   */
  async recordAttemptFailure(queueId) {
    const entry = this.#entries.get(queueId);
    if (!entry) return;
    const now = this.#nowMs();
    const nextAttempts = entry.attempts + 1;

    // Per-message attempt cap: stop re-attempting (and re-depositing) a
    // permanently-stuck message. Drop it and notify the owner as "expired"
    // (the same terminal status used for TTL expiry).
    if (nextAttempts >= this.#maxAttempts) {
      await this.#kv.delete(KV_PREFIX + queueId);
      this.#entries.delete(queueId);
      this.#indexRemove(entry.deliverInboxId, queueId);
      this.#emitStatus(queueId, "expired", entry);
      return;
    }

    const backoff = backoffForAttempt(nextAttempts);

    const updated = new OutboundQueueEntryV1({
      queueId: entry.queueId,
      deliverInboxId: entry.deliverInboxId,
      innerBytes: entry.innerBytes,
      createdAtMs: entry.createdAtMs,
      attempts: nextAttempts,
      lastAttemptMs: now,
      nextRetryMs: now + backoff,
      receiptInboxId: entry.receiptInboxId,
      ownerPublicKeyB64: entry.ownerPublicKeyB64,
    });

    await this.#persist(updated);
    this.#entries.set(queueId, updated);
  }

  /**
   * Get all entries ready for retry (past their nextRetryMs and not expired).
   * @returns {OutboundQueueEntryV1[]}
   */
  getRetryable() {
    const now = this.#nowMs();
    const results = [];
    for (const entry of this.#entries.values()) {
      if (this.#isExpired(entry)) continue;
      if (entry.nextRetryMs <= now) {
        results.push(entry);
      }
    }
    return results;
  }

  /**
   * Get entries for a specific inbox that are ready for retry (past their
   * nextRetryMs and not expired). Used by the route-discovery flush so that a
   * flapping route cannot re-attempt (and re-deposit) the same backed-off
   * entry on every route-added event — backoff governs the flush path too.
   * @param {string} deliverInboxId
   * @returns {OutboundQueueEntryV1[]}
   */
  getRetryableForInbox(deliverInboxId) {
    const ids = this.#byInbox.get(deliverInboxId);
    if (!ids) return [];
    const now = this.#nowMs();
    const results = [];
    for (const queueId of ids) {
      const entry = this.#entries.get(queueId);
      if (entry && !this.#isExpired(entry) && entry.nextRetryMs <= now) {
        results.push(entry);
      }
    }
    return results;
  }

  /**
   * Get all entries queued for a specific inbox.
   * @param {string} deliverInboxId
   * @returns {OutboundQueueEntryV1[]}
   */
  getForInbox(deliverInboxId) {
    const ids = this.#byInbox.get(deliverInboxId);
    if (!ids) return [];
    const results = [];
    for (const queueId of ids) {
      const entry = this.#entries.get(queueId);
      if (entry && !this.#isExpired(entry)) {
        results.push(entry);
      }
    }
    return results;
  }

  /**
   * Prune all expired entries from the queue.
   * @returns {Promise<number>} number of entries pruned
   */
  async pruneExpired() {
    let pruned = 0;
    for (const [queueId, entry] of this.#entries) {
      if (this.#isExpired(entry)) {
        await this.#kv.delete(KV_PREFIX + queueId);
        this.#entries.delete(queueId);
        this.#indexRemove(entry.deliverInboxId, queueId);
        this.#emitStatus(queueId, "expired", entry);
        pruned++;
      }
    }
    return pruned;
  }

  /** Total number of entries in the queue. */
  size() {
    return this.#entries.size;
  }

  /** Number of entries for a specific inbox. */
  sizeForInbox(deliverInboxId) {
    const ids = this.#byInbox.get(deliverInboxId);
    return ids ? ids.size : 0;
  }

  // --- Private helpers ---

  #isExpired(entry) {
    return this.#nowMs() - entry.createdAtMs > this.#ttlMs;
  }

  async #persist(entry) {
    await this.#kv.set(KV_PREFIX + entry.queueId, entry.toJSON());
  }

  #indexAdd(deliverInboxId, queueId) {
    let set = this.#byInbox.get(deliverInboxId);
    if (!set) {
      set = new Set();
      this.#byInbox.set(deliverInboxId, set);
    }
    set.add(queueId);
  }

  #indexRemove(deliverInboxId, queueId) {
    const set = this.#byInbox.get(deliverInboxId);
    if (!set) return;
    set.delete(queueId);
    if (set.size === 0) {
      this.#byInbox.delete(deliverInboxId);
    }
  }

  async #enforcePerInboxLimit(deliverInboxId) {
    const ids = this.#byInbox.get(deliverInboxId);
    if (!ids || ids.size < this.#maxPerInbox) return;

    // Find and remove oldest entries for this inbox
    const entries = [];
    for (const queueId of ids) {
      const e = this.#entries.get(queueId);
      if (e) entries.push(e);
    }
    entries.sort((a, b) => a.createdAtMs - b.createdAtMs);

    while (entries.length >= this.#maxPerInbox) {
      const oldest = entries.shift();
      await this.#kv.delete(KV_PREFIX + oldest.queueId);
      this.#entries.delete(oldest.queueId);
      this.#indexRemove(deliverInboxId, oldest.queueId);
    }
  }

  async #enforceGlobalLimit() {
    if (this.#entries.size < this.#maxTotal) return;

    // Find and remove oldest entry globally
    let oldest = null;
    for (const entry of this.#entries.values()) {
      if (!oldest || entry.createdAtMs < oldest.createdAtMs) {
        oldest = entry;
      }
    }
    if (oldest) {
      await this.#kv.delete(KV_PREFIX + oldest.queueId);
      this.#entries.delete(oldest.queueId);
      this.#indexRemove(oldest.deliverInboxId, oldest.queueId);
    }
  }

  #emitStatus(queueId, status, entry) {
    if (this.#onStatusChange) {
      try {
        this.#onStatusChange(queueId, status, entry);
      } catch {
        // ignore callback errors
      }
    }
  }
}
