/**
 * Local store for durable signed records this node holds on behalf of the
 * network. Parallels DhtValueStore but blob-shaped (opaque signed records,
 * not route entries) and adds per-publisher quota accounting.
 *
 * The store trusts that callers have already verified the record
 * (signature + key-binding + expiry window) — exactly as DhtValueStore
 * trusts the inbound `validateStoredRouteEntry` gate. Its own jobs are:
 *   - immutability: a live slot cannot be overwritten with different content
 *   - TTL: honor the signed `expiresAtMs`, capped at `maxRecordTtlMs`
 *   - quota: bound per-publisher record count + total bytes (DoS surface)
 */
export class DurableRecordStore {
  /** @type {Map<string, { record: object, storedAtMs: number, ttlMs: number }>} */
  #records;

  /** @type {Map<string, { count: number, bytes: number }>} */
  #byPublisher;

  /** @type {number} */
  #maxRecordsPerPublisher;

  /** @type {number} */
  #maxBytesPerPublisher;

  /** @type {number} */
  #maxRecordTtlMs;

  /**
   * @param {{ maxRecordsPerPublisher?: number, maxBytesPerPublisher?: number, maxRecordTtlMs?: number }} [options]
   */
  constructor({ maxRecordsPerPublisher = 256, maxBytesPerPublisher = 4_194_304, maxRecordTtlMs = 86_400_000 * 30 } = {}) {
    if (!Number.isFinite(maxRecordsPerPublisher) || maxRecordsPerPublisher <= 0) {
      throw new Error("DurableRecordStore maxRecordsPerPublisher must be positive");
    }
    if (!Number.isFinite(maxBytesPerPublisher) || maxBytesPerPublisher <= 0) {
      throw new Error("DurableRecordStore maxBytesPerPublisher must be positive");
    }
    if (!Number.isFinite(maxRecordTtlMs) || maxRecordTtlMs <= 0) {
      throw new Error("DurableRecordStore maxRecordTtlMs must be positive");
    }
    this.#records = new Map();
    this.#byPublisher = new Map();
    this.#maxRecordsPerPublisher = maxRecordsPerPublisher;
    this.#maxBytesPerPublisher = maxBytesPerPublisher;
    this.#maxRecordTtlMs = maxRecordTtlMs;
  }

  /**
   * Store a verified record under its publisher-bound slot key.
   *
   * Idempotent: re-storing the byte-identical record (same `sigB64`) refreshes
   * the local TTL window — this is what storer-side re-replication relies on,
   * and crucially it does NOT re-broadcast or re-charge quota.
   *
   * Controlled mutability: a live slot holding *different* content from the
   * SAME publisher may be rolled strictly forward — a record whose
   * `issuedAtMs` is greater than the live one's supersedes it (and re-stores
   * with `reason: null` so it re-replicates). Within one issuance the record is
   * immutable (`reason: "immutable"`); an older issuance is rejected
   * (`reason: "older-record"`) so a stale rebroadcast can't roll the slot back.
   *
   * @param {string} localId - publisher-bound slot key (sha256 hex)
   * @param {object} record - a verified DurableRecordV1
   * @param {number} nowMs
   * @returns {{ stored: boolean, reason: string|null }}
   */
  store(localId, record, nowMs) {
    if (typeof localId !== "string" || localId.trim().length === 0) {
      throw new Error("DurableRecordStore.store requires a non-empty localId");
    }
    if (!record || typeof record !== "object") {
      throw new Error("DurableRecordStore.store requires a record object");
    }
    if (!Number.isFinite(nowMs)) {
      throw new Error("DurableRecordStore.store requires a finite nowMs");
    }

    const existing = this.#records.get(localId);
    if (existing && !this.#isExpired(existing, nowMs)) {
      if (existing.record.sigB64 === record.sigB64) {
        // Identical content — refresh the local retention window in place.
        existing.storedAtMs = nowMs;
        existing.ttlMs = this.#effectiveTtl(record, nowMs);
        return { stored: true, reason: "refreshed" };
      }
      // Same slot, different content. The slot key (localId) folds the
      // publisher key in, so two records here are the SAME publisher's — a
      // rotation of one logical record (e.g. a device-set add/remove bumps
      // `issuedAtMs` and re-signs). Allow controlled mutability: the publisher
      // may roll its OWN slot strictly forward (monotonic by `issuedAtMs`),
      // mirroring DhtValueStore's newer-delegation-wins rule. Within a single
      // issuance the record stays immutable, and an older issuance can never
      // overwrite a newer one (rollback / stale-rebroadcast defense).
      const samePublisher = typeof record.publisherPublicKeyB64 === "string"
        && record.publisherPublicKeyB64 === existing.record.publisherPublicKeyB64;
      const comparable = samePublisher
        && Number.isFinite(record.issuedAtMs)
        && Number.isFinite(existing.record.issuedAtMs);
      if (!comparable || record.issuedAtMs <= existing.record.issuedAtMs) {
        // Older issuance is a distinct, named rejection (a rolled-back or
        // replayed record); equal/unstamped/cross-publisher stays "immutable".
        const older = comparable && record.issuedAtMs < existing.record.issuedAtMs;
        return { stored: false, reason: older ? "older-record" : "immutable" };
      }
      // Strictly newer issuance from the same publisher — roll the slot
      // forward: release the superseded record's quota and re-insert below.
      this.#releaseQuota(existing.record.publisherPublicKeyB64, this.#recordBytes(existing.record));
      this.#records.delete(localId);
    } else if (existing) {
      // No live entry, but a stale one lingers — release its quota first so we
      // don't double-count when replacing.
      this.#releaseQuota(existing.record.publisherPublicKeyB64, this.#recordBytes(existing.record));
      this.#records.delete(localId);
    }

    const pub = typeof record.publisherPublicKeyB64 === "string" ? record.publisherPublicKeyB64 : "";
    const bytes = this.#recordBytes(record);
    const quota = this.#byPublisher.get(pub) || { count: 0, bytes: 0 };
    if (quota.count + 1 > this.#maxRecordsPerPublisher) {
      return { stored: false, reason: "publisher-record-quota" };
    }
    if (quota.bytes + bytes > this.#maxBytesPerPublisher) {
      return { stored: false, reason: "publisher-byte-quota" };
    }

    this.#records.set(localId, {
      record,
      storedAtMs: nowMs,
      ttlMs: this.#effectiveTtl(record, nowMs),
    });
    quota.count += 1;
    quota.bytes += bytes;
    this.#byPublisher.set(pub, quota);
    return { stored: true, reason: null };
  }

  /**
   * Retrieve a record. Returns null if absent or expired (and evicts on the
   * expired-read path).
   *
   * @param {string} localId
   * @param {number} nowMs
   * @returns {object|null}
   */
  get(localId, nowMs) {
    const entry = this.#records.get(localId);
    if (!entry) return null;
    if (this.#isExpired(entry, nowMs)) {
      this.#releaseQuota(entry.record.publisherPublicKeyB64, this.#recordBytes(entry.record));
      this.#records.delete(localId);
      return null;
    }
    return entry.record;
  }

  /**
   * Retrieve a record with its retention metadata (for persistence). Returns
   * null if absent or expired (evicting on the expired path).
   * @param {string} localId
   * @param {number} nowMs
   * @returns {{ record: object, storedAtMs: number, ttlMs: number }|null}
   */
  getEntry(localId, nowMs) {
    const entry = this.#records.get(localId);
    if (!entry) return null;
    if (this.#isExpired(entry, nowMs)) {
      this.#releaseQuota(entry.record.publisherPublicKeyB64, this.#recordBytes(entry.record));
      this.#records.delete(localId);
      return null;
    }
    return { record: entry.record, storedAtMs: entry.storedAtMs, ttlMs: entry.ttlMs };
  }

  /**
   * Remove a specific record.
   * @param {string} localId
   * @returns {boolean}
   */
  remove(localId) {
    const entry = this.#records.get(localId);
    if (!entry) return false;
    this.#releaseQuota(entry.record.publisherPublicKeyB64, this.#recordBytes(entry.record));
    return this.#records.delete(localId);
  }

  /**
   * Evict all expired records. Returns the evicted slot keys so callers can
   * mirror the removal to durable storage.
   * @param {number} nowMs
   * @returns {string[]} evicted localIds
   */
  evictExpired(nowMs) {
    const evicted = [];
    for (const [localId, entry] of this.#records) {
      if (this.#isExpired(entry, nowMs)) {
        this.#releaseQuota(entry.record.publisherPublicKeyB64, this.#recordBytes(entry.record));
        this.#records.delete(localId);
        evicted.push(localId);
      }
    }
    return evicted;
  }

  /**
   * Non-expired entries (with retention metadata) — used by re-replication
   * and persistence snapshotting.
   * @param {number} nowMs
   * @returns {Array<{ localId: string, record: object, storedAtMs: number, ttlMs: number }>}
   */
  getAllEntries(nowMs) {
    const out = [];
    for (const [localId, entry] of this.#records) {
      if (this.#isExpired(entry, nowMs)) continue;
      out.push({ localId, record: entry.record, storedAtMs: entry.storedAtMs, ttlMs: entry.ttlMs });
    }
    return out;
  }

  /**
   * Rebuild the store from a persisted snapshot, dropping any entry already
   * expired at load time and recomputing per-publisher quota from scratch (so
   * quota survives restart).
   *
   * @param {Array<{ localId: string, record: object, storedAtMs: number, ttlMs: number }>} entries
   * @param {number} nowMs
   */
  loadFromSnapshot(entries, nowMs) {
    this.#records.clear();
    this.#byPublisher.clear();
    if (!Array.isArray(entries)) return;
    for (const entry of entries) {
      if (!entry || typeof entry !== "object") continue;
      const { localId, record, storedAtMs, ttlMs } = entry;
      if (typeof localId !== "string" || !localId) continue;
      if (!record || typeof record !== "object") continue;
      if (!Number.isFinite(storedAtMs) || !Number.isFinite(ttlMs)) continue;
      const candidate = { record, storedAtMs, ttlMs };
      if (this.#isExpired(candidate, nowMs)) continue;
      this.#records.set(localId, candidate);
      const pub = typeof record.publisherPublicKeyB64 === "string" ? record.publisherPublicKeyB64 : "";
      const bytes = this.#recordBytes(record);
      const quota = this.#byPublisher.get(pub) || { count: 0, bytes: 0 };
      quota.count += 1;
      quota.bytes += bytes;
      this.#byPublisher.set(pub, quota);
    }
  }

  /** @returns {number} */
  get size() {
    return this.#records.size;
  }

  /**
   * Per-publisher quota usage (diagnostics/tests).
   * @param {string} publisherPublicKeyB64
   * @returns {{ count: number, bytes: number }}
   */
  publisherUsage(publisherPublicKeyB64) {
    const quota = this.#byPublisher.get(publisherPublicKeyB64);
    return quota ? { count: quota.count, bytes: quota.bytes } : { count: 0, bytes: 0 };
  }

  #effectiveTtl(record, nowMs) {
    const untilExpiry = Number.isFinite(record.expiresAtMs) ? record.expiresAtMs - nowMs : this.#maxRecordTtlMs;
    return Math.max(0, Math.min(untilExpiry, this.#maxRecordTtlMs));
  }

  #isExpired(entry, nowMs) {
    if (nowMs - entry.storedAtMs >= entry.ttlMs) return true;
    if (Number.isFinite(entry.record.expiresAtMs) && nowMs >= entry.record.expiresAtMs) return true;
    return false;
  }

  #recordBytes(record) {
    return typeof record.payloadB64 === "string" ? record.payloadB64.length : 0;
  }

  #releaseQuota(pub, bytes) {
    const quota = this.#byPublisher.get(pub);
    if (!quota) return;
    quota.count -= 1;
    quota.bytes -= bytes;
    if (quota.bytes < 0) quota.bytes = 0;
    // Count is the authoritative indicator — a publisher can legitimately
    // hold records whose total bytes is 0 (empty payloads), so never key
    // deletion on bytes or the count for still-held records is lost.
    if (quota.count <= 0) {
      this.#byPublisher.delete(pub);
    } else {
      this.#byPublisher.set(pub, quota);
    }
  }
}
