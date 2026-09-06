/**
 * Persistent per-subject sliding-window rate-limit store for inbox deposits.
 *
 * Each deposit is bounded by TWO independent caps (HIGH-1 + LOW-4):
 *   1. per-(depositor pubkey, inbox) — pins a single authenticated
 *      identity's deposit rate.
 *   2. per-(source IP, inbox) — pins a network source's deposit rate,
 *      so an attacker who rotates `session.hello` keypairs to escape
 *      blocklists (docs/SECURITY_AUDIT.md LOW-4) still hits the IP cap.
 *
 * `record` accepts both subjects and atomically gates the deposit
 * behind both. If either cap is exhausted, NEITHER cap's counters are
 * incremented — so an honest user behind a NAT'd / proxied IP isn't
 * silently burning their pubkey budget for attempts denied by the shared
 * IP cap, and vice versa.
 *
 * Storage layout: one KV row per (subject-type, subject-key, inbox)
 * triple, with key prefix `node:deposit-ratelimit:v1:` and composite
 * form `<type>:<key>|<inboxId>` where `type` is `pk` (pubkey) or `ip`.
 * Pruned-but-still-full rows are persisted on the deny path so attackers
 * cannot flush counters by waiting for restart.
 *
 * An LRU cap on the in-memory cache prevents a flood of distinct subjects
 * from burning unbounded memory.
 */
const KEY_PREFIX = "node:deposit-ratelimit:v1:";

const DEFAULT_WINDOW_MS = 60_000;
const DEFAULT_MAX_DEPOSITS = 120;
const DEFAULT_LRU_CAP = 4096;

const SUBJECT_PK = "pk";
const SUBJECT_IP = "ip";

function compositeKey(subjectType, subjectKey, mailboxId) {
  return subjectType + ":" + subjectKey + "|" + mailboxId;
}

function rowKey(compositeId) {
  return KEY_PREFIX + compositeId;
}

export class DepositRateLimitStore {
  #kv;
  #cache;
  #hydrated;
  #windowMs;
  #maxDeposits;
  #lruCap;

  constructor({ storageProvider, windowMs, maxDeposits, lruCap } = {}) {
    if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
      throw new Error("DepositRateLimitStore requires storageProvider.getKeyValueStore()");
    }
    this.#kv = storageProvider.getKeyValueStore(null);
    this.#cache = new Map();
    this.#hydrated = false;
    this.#windowMs = typeof windowMs === "number" && windowMs > 0 ? windowMs : DEFAULT_WINDOW_MS;
    this.#maxDeposits = typeof maxDeposits === "number" && maxDeposits > 0 ? maxDeposits : DEFAULT_MAX_DEPOSITS;
    this.#lruCap = typeof lruCap === "number" && lruCap > 0 ? lruCap : DEFAULT_LRU_CAP;
  }

  get windowMs() {
    return this.#windowMs;
  }

  get maxDeposits() {
    return this.#maxDeposits;
  }

  async hydrate() {
    if (this.#hydrated) return;
    if (typeof this.#kv.getStrict !== "function") {
      throw new Error("DepositRateLimitStore requires strict durable reads");
    }
    const keys = await this.#kv.keys(KEY_PREFIX);
    const nowMs = Date.now();
    const cutoff = nowMs - this.#windowMs;
    const cache = new Map();
    const expiredKeys = [];
    for (const k of keys) {
      const compositeId = k.slice(KEY_PREFIX.length);
      const row = await this.#kv.getStrict(k);
      if (!row || !Array.isArray(row.timestamps)) {
        throw new Error("DepositRateLimitStore durable row is malformed: " + k);
      }
      if (row.timestamps.some((t) => !Number.isFinite(t) || t <= 0)) {
        throw new Error("DepositRateLimitStore durable timestamps are malformed: " + k);
      }
      const trimmed = row.timestamps.filter((t) => typeof t === "number" && t > cutoff);
      if (trimmed.length === 0) {
        expiredKeys.push(k);
        continue;
      }
      cache.set(compositeId, trimmed);
    }
    for (const k of expiredKeys) {
      await this.#kv.delete(k);
    }
    this.#cache = cache;
    this.#hydrated = true;
  }

  /**
   * Atomically check ALL subject budgets and, if every cap has room,
   * record this attempt against each. Returns `true` if the deposit was
   * recorded under both caps; `false` if any cap was exhausted.
   *
   * Missing subjects are silently skipped — anonymous-default traffic
   * without an authenticated pubkey is shaped elsewhere; a request from
   * an introspectable-IP-less source (eg unit tests) still passes the
   * pubkey gate.
   *
   * @param {object} args
   * @param {string} [args.depositorPubkeyB64]
   * @param {string} [args.sourceIp]
   * @param {string} args.mailboxId
   * @param {number} args.nowMs
   * @returns {Promise<boolean>} true = allowed and recorded, false = limited.
   */
  async record({ depositorPubkeyB64, sourceIp, mailboxId, nowMs }) {
    if (!this.#hydrated) {
      throw new Error("DepositRateLimitStore.record() called before hydrate()");
    }
    if (typeof mailboxId !== "string" || mailboxId.length === 0) return true;

    const subjects = [];
    if (typeof depositorPubkeyB64 === "string" && depositorPubkeyB64.length > 0) {
      subjects.push({ compositeId: compositeKey(SUBJECT_PK, depositorPubkeyB64, mailboxId) });
    }
    if (typeof sourceIp === "string" && sourceIp.length > 0) {
      subjects.push({ compositeId: compositeKey(SUBJECT_IP, sourceIp, mailboxId) });
    }
    if (subjects.length === 0) return true;

    const cutoff = nowMs - this.#windowMs;

    // Phase 1: check every subject's budget without mutating commit state.
    // If any cap is full, persist its pruned list (deny-path durability)
    // and short-circuit return false — no other cap is touched.
    for (const subject of subjects) {
      const existing = this.#cache.get(subject.compositeId);
      const pruned = existing ? existing.filter((t) => t > cutoff) : [];
      subject.pruned = pruned;
      if (pruned.length >= this.#maxDeposits) {
        this.#cache.set(subject.compositeId, pruned);
        if (pruned.length > 0) {
          await this.#kv.set(rowKey(subject.compositeId), { timestamps: pruned });
        } else {
          await this.#kv.delete(rowKey(subject.compositeId));
        }
        return false;
      }
    }

    // Phase 2: all caps have room — record the attempt against each.
    for (const subject of subjects) {
      subject.pruned.push(nowMs);
      this.#cache.set(subject.compositeId, subject.pruned);
      await this.#kv.set(rowKey(subject.compositeId), { timestamps: subject.pruned });
      await this.#enforceLru(subject.compositeId);
    }
    return true;
  }

  async #enforceLru(currentKey) {
    if (this.#cache.size <= this.#lruCap) return;
    for (const k of this.#cache.keys()) {
      if (k === currentKey) continue;
      this.#cache.delete(k);
      await this.#kv.delete(rowKey(k));
      if (this.#cache.size <= this.#lruCap) return;
    }
  }
}
