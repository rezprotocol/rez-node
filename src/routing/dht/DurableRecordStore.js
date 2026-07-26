import {
  DURABLE_RECORD_V2_VERSION,
  DEVICE_SET_RECORD_KIND,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  recordKindCarriesMonotonicEpoch,
  durableRecordMonotonicBinding,
} from "@rezprotocol/core";

// S2.5 S12: multi-device fan-out record kinds get a RESERVED per-publisher quota
// bucket, isolated from the general durable-record bucket, so a busy account's
// other records can never starve its peer-scoped device sets / authority state
// (each is published per peer under the SAME owner key). Recon Q7.
const RESERVED_RECORD_KINDS = new Set([DEVICE_SET_RECORD_KIND, ACCOUNT_AUTHORITY_STATE_RECORD_KIND]);

/**
 * Ceiling on remembered epoch floors. A floor OUTLIVES the record it came from (that is the whole
 * point — see #epochFloors), so it is not bounded by the record TTL or the per-publisher quota, and
 * an unbounded map in a network-facing process is a memory DoS. One entry per account whose
 * authority state this node has ever held is a small number in practice; this is the backstop, not
 * the expected regime. Eviction is a genuine safety regression for the evicted account, so it is
 * loud (see #evictOldestEpochFloor).
 */
const DEFAULT_MAX_EPOCH_FLOORS = 100_000;

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

  /** @type {Map<string, { count: number, bytes: number }>} general-kind quota */
  #byPublisher;

  /** @type {Map<string, { count: number, bytes: number }>} reserved-kind quota (device-set / authority-state) */
  #byPublisherReserved;

  /** @type {number} */
  #maxRecordsPerPublisher;

  /** @type {number} */
  #maxBytesPerPublisher;

  /** @type {number} */
  #maxReservedRecordsPerPublisher;

  /** @type {number} */
  #maxReservedBytesPerPublisher;

  /** @type {number} */
  #maxRecordTtlMs;

  /**
   * Highest-observed epoch per slot, for epoch-ordered record kinds — the ROLLBACK floor.
   *
   * @type {Map<string, { epoch: number, ownerPublicKeyB64: string, observedAtMs: number }>}
   *
   * Why this exists separately from the record map: slot replacement is ordered by `issuedAtMs`,
   * which only orders records that are BOTH present. Once a slot empties — TTL expiry, eviction,
   * a restart that dropped it — a genuinely root-signed OLDER authority state re-store wins the
   * empty slot outright, silently un-revoking a device for every off-home peer that reads it. So
   * the floor must survive the record: it is NEVER dropped on expiry, eviction, or removal, and
   * `loadFromSnapshot` raises it but never lowers it.
   *
   * It is node-LOCAL state, not a consensus value: a node that has never seen an account's slot has
   * no floor for it and accepts whatever it is first handed (then pins that). It bounds rollback on
   * the holders that actually witnessed the newer epoch, which is exactly the set that would
   * otherwise serve the stale one.
   */
  #epochFloors;

  /** @type {number} */
  #maxEpochFloors;

  /**
   * @param {{ maxRecordsPerPublisher?: number, maxBytesPerPublisher?: number, maxReservedRecordsPerPublisher?: number, maxReservedBytesPerPublisher?: number, maxRecordTtlMs?: number, maxEpochFloors?: number }} [options]
   */
  constructor({ maxRecordsPerPublisher = 256, maxBytesPerPublisher = 4_194_304, maxReservedRecordsPerPublisher = 256, maxReservedBytesPerPublisher = 4_194_304, maxRecordTtlMs = 86_400_000 * 30, maxEpochFloors = DEFAULT_MAX_EPOCH_FLOORS } = {}) {
    if (!Number.isFinite(maxRecordsPerPublisher) || maxRecordsPerPublisher <= 0) {
      throw new Error("DurableRecordStore maxRecordsPerPublisher must be positive");
    }
    if (!Number.isFinite(maxBytesPerPublisher) || maxBytesPerPublisher <= 0) {
      throw new Error("DurableRecordStore maxBytesPerPublisher must be positive");
    }
    if (!Number.isFinite(maxReservedRecordsPerPublisher) || maxReservedRecordsPerPublisher <= 0) {
      throw new Error("DurableRecordStore maxReservedRecordsPerPublisher must be positive");
    }
    if (!Number.isFinite(maxReservedBytesPerPublisher) || maxReservedBytesPerPublisher <= 0) {
      throw new Error("DurableRecordStore maxReservedBytesPerPublisher must be positive");
    }
    if (!Number.isFinite(maxRecordTtlMs) || maxRecordTtlMs <= 0) {
      throw new Error("DurableRecordStore maxRecordTtlMs must be positive");
    }
    if (!Number.isInteger(maxEpochFloors) || maxEpochFloors <= 0) {
      throw new Error("DurableRecordStore maxEpochFloors must be a positive integer");
    }
    this.#epochFloors = new Map();
    this.#maxEpochFloors = maxEpochFloors;
    this.#records = new Map();
    this.#byPublisher = new Map();
    this.#byPublisherReserved = new Map();
    this.#maxRecordsPerPublisher = maxRecordsPerPublisher;
    this.#maxBytesPerPublisher = maxBytesPerPublisher;
    this.#maxReservedRecordsPerPublisher = maxReservedRecordsPerPublisher;
    this.#maxReservedBytesPerPublisher = maxReservedBytesPerPublisher;
    this.#maxRecordTtlMs = maxRecordTtlMs;
  }

  // Route a record to its quota bucket by kind: reserved (fan-out kinds) or general.
  #bucketFor(record) {
    const kind = record && typeof record.recordKind === "string" ? record.recordKind : "";
    if (RESERVED_RECORD_KINDS.has(kind)) {
      return { map: this.#byPublisherReserved, maxRecords: this.#maxReservedRecordsPerPublisher, maxBytes: this.#maxReservedBytesPerPublisher };
    }
    return { map: this.#byPublisher, maxRecords: this.#maxRecordsPerPublisher, maxBytes: this.#maxBytesPerPublisher };
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
   * with `reason: null` so it re-replicates). An older issuance is rejected
   * (`reason: "older-record"`) so a stale rebroadcast can't roll the slot back.
   * Two DIFFERENT records sharing the SAME `issuedAtMs` (two honest publishes
   * in one millisecond) are broken by a DETERMINISTIC tie-break — the
   * lexicographically greater `sigB64` wins on every replica regardless of
   * arrival order, so the network converges instead of diverging (the loser is
   * `reason: "immutable"`). `issuedAtMs` is NOT a trust anchor here: the node
   * verifier (`verifyDurableRecord`) bounds it against `nowMs` so a far-future
   * stamp cannot poison the slot.
   *
   * @param {string} localId - publisher/owner-bound slot key (sha256 hex)
   * @param {object} record - a verified DurableRecordV1 or DurableRecordV2
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

    // ROLLBACK FLOOR, checked BEFORE anything is read out of or written to the slot. It must gate
    // the empty-slot path too — that is the case `issuedAtMs` ordering cannot see, because there is
    // no incumbent left to compare against.
    const gate = this.#epochGate(localId, record);
    if (!gate.ok) {
      return { stored: false, reason: gate.reason };
    }

    const existing = this.#records.get(localId);
    if (existing && !this.#isExpired(existing, nowMs)) {
      if (existing.record.sigB64 === record.sigB64) {
        // Identical content — refresh the local retention window in place.
        existing.storedAtMs = nowMs;
        existing.ttlMs = this.#effectiveTtl(record, nowMs);
        this.#raiseEpochFloor(localId, record, gate, nowMs);
        return { stored: true, reason: "refreshed" };
      }
      // Same slot, different content. The slot key (localId) folds the
      // publisher key in, so two records here are the SAME publisher's — a
      // rotation of one logical record (e.g. a device-set add/remove bumps
      // `issuedAtMs` and re-signs). Allow controlled mutability: the publisher
      // may roll its OWN slot strictly forward (monotonic by `issuedAtMs`),
      // mirroring DhtValueStore's newer-delegation-wins rule, and an older
      // issuance can never overwrite a newer one (rollback / stale-rebroadcast
      // defense).
      const incomingOwner = this.#accountingKey(record);
      const samePublisher = incomingOwner !== ""
        && incomingOwner === this.#accountingKey(existing.record);
      const comparable = samePublisher
        && Number.isFinite(record.issuedAtMs)
        && Number.isFinite(existing.record.issuedAtMs);
      if (!comparable) {
        // Cross-publisher (cannot happen — the slot folds the publisher) or an
        // unstamped record: with no orderable key, keep the incumbent.
        return { stored: false, reason: "immutable" };
      }
      if (record.issuedAtMs < existing.record.issuedAtMs) {
        return { stored: false, reason: "older-record" };
      }
      if (record.issuedAtMs === existing.record.issuedAtMs) {
        // Same issuance instant, different content. Two honest publishes in the
        // same millisecond would otherwise diverge across replicas (each keeps
        // whichever it saw first). Converge deterministically: the
        // lexicographically greater `sigB64` wins on EVERY replica regardless of
        // arrival order. The byte-identical case is handled above, so the two
        // sigs differ here. Loser rejected; winner falls through to re-store
        // (`reason: null`) so the agreed record re-replicates.
        const incumbentSig = typeof existing.record.sigB64 === "string" ? existing.record.sigB64 : "";
        const incomingSig = typeof record.sigB64 === "string" ? record.sigB64 : "";
        if (incomingSig <= incumbentSig) {
          return { stored: false, reason: "immutable" };
        }
      }
      // Strictly newer issuance, or the equal-issuance tie-break winner — roll
      // the slot forward: release the superseded record's quota and re-insert.
      this.#releaseQuota(existing.record);
      this.#records.delete(localId);
    } else if (existing) {
      // No live entry, but a stale one lingers — release its quota first so we
      // don't double-count when replacing.
      this.#releaseQuota(existing.record);
      this.#records.delete(localId);
    }

    const { map, maxRecords, maxBytes } = this.#bucketFor(record);
    const pub = this.#accountingKey(record);
    const bytes = this.#recordBytes(record);
    const quota = map.get(pub) || { count: 0, bytes: 0 };
    if (quota.count + 1 > maxRecords) {
      return { stored: false, reason: "publisher-record-quota" };
    }
    if (quota.bytes + bytes > maxBytes) {
      return { stored: false, reason: "publisher-byte-quota" };
    }

    this.#records.set(localId, {
      record,
      storedAtMs: nowMs,
      ttlMs: this.#effectiveTtl(record, nowMs),
    });
    quota.count += 1;
    quota.bytes += bytes;
    map.set(pub, quota);
    this.#raiseEpochFloor(localId, record, gate, nowMs);
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
      this.#releaseQuota(entry.record);
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
      this.#releaseQuota(entry.record);
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
    this.#releaseQuota(entry.record);
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
        this.#releaseQuota(entry.record);
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
    this.#byPublisherReserved.clear();
    // #epochFloors is deliberately NOT cleared. Floors outlive records by design, they are loaded
    // from their own snapshot (loadEpochFloors), and this method's contract is "rebuild the record
    // set" — clearing the floors here would make a restart the easiest way to erase them.
    if (!Array.isArray(entries)) return;
    for (const entry of entries) {
      if (!entry || typeof entry !== "object") continue;
      const { localId, record, storedAtMs, ttlMs } = entry;
      if (typeof localId !== "string" || !localId) continue;
      if (!record || typeof record !== "object") continue;
      if (!Number.isFinite(storedAtMs) || !Number.isFinite(ttlMs)) continue;
      const candidate = { record, storedAtMs, ttlMs };
      if (this.#isExpired(candidate, nowMs)) continue;
      // Self-healing: a held record re-establishes its own floor, so losing or corrupting the floor
      // file degrades to "the floor is whatever we still hold" rather than to no floor at all. This
      // only ever RAISES (see #raiseEpochFloor), so a snapshot record older than a loaded floor
      // cannot weaken it. A record whose payload is unreadable is refused outright by #epochGate,
      // so it never reaches here with a bad binding.
      const gate = this.#epochGate(localId, record);
      if (!gate.ok) {
        console.warn("[DHT] durable-record store: snapshot entry " + localId + " refused on load — " + gate.reason);
        continue;
      }
      this.#raiseEpochFloor(localId, record, gate, nowMs);
      this.#records.set(localId, candidate);
      const { map } = this.#bucketFor(record);
      const pub = this.#accountingKey(record);
      const bytes = this.#recordBytes(record);
      const quota = map.get(pub) || { count: 0, bytes: 0 };
      quota.count += 1;
      quota.bytes += bytes;
      map.set(pub, quota);
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
    const general = this.#byPublisher.get(publisherPublicKeyB64) || { count: 0, bytes: 0 };
    const reserved = this.#byPublisherReserved.get(publisherPublicKeyB64) || { count: 0, bytes: 0 };
    return { count: general.count + reserved.count, bytes: general.bytes + reserved.bytes };
  }

  // ---------------------------------------------------------------------------
  // Rollback floor (epoch-ordered record kinds)
  // ---------------------------------------------------------------------------

  /**
   * Decide whether a record may touch a slot at all, given the highest epoch this node has ever
   * accepted there. Returns `{ ok: true, epoch }` for an epoch-ordered kind that clears the floor,
   * `{ ok: true, epoch: null }` for a kind that carries no epoch (nothing to enforce), or
   * `{ ok: false, reason }`.
   *
   * EQUAL to the floor is admitted, not rejected: re-storing the current epoch is exactly what
   * storer-side re-replication does every cycle, and refusing it would break durability. Only a
   * STRICTLY lower epoch is a rollback. Content differences at the same epoch stay the existing
   * `issuedAtMs`/`sigB64` tie-break's job.
   *
   * @param {string} localId
   * @param {object} record
   * @returns {{ ok: true, epoch: number|null, ownerPublicKeyB64: string }|{ ok: false, reason: string }}
   */
  #epochGate(localId, record) {
    const kind = record && typeof record.recordKind === "string" ? record.recordKind.trim() : "";
    if (!recordKindCarriesMonotonicEpoch(kind)) {
      return { ok: true, epoch: null, ownerPublicKeyB64: "" };
    }
    let binding;
    try {
      binding = durableRecordMonotonicBinding(record);
    } catch (err) {
      // The record reached the store already verified, and verification rejects an unreadable
      // payload for this kind — so this is a caller that skipped the gate, not ordinary traffic.
      // Refuse the slot and say why; never fall through treating the epoch as absent.
      console.warn("[DHT] durable-record store: refused " + localId + " — epoch-ordered payload is unreadable: "
        + (err && err.message ? err.message : err));
      return { ok: false, reason: "epoch-unreadable" };
    }
    if (binding === null) {
      return { ok: false, reason: "epoch-unreadable" };
    }
    const floor = this.#epochFloors.get(localId);
    if (floor && binding.epoch < floor.epoch) {
      console.warn("[DHT] durable-record store: refused " + localId + " — epoch " + binding.epoch
        + " is below the highest observed epoch " + floor.epoch + " (rollback)");
      return { ok: false, reason: "epoch-floor" };
    }
    return { ok: true, epoch: binding.epoch, ownerPublicKeyB64: binding.accountIdentityPublicKeyB64 };
  }

  /**
   * Pin the floor for a slot we just accepted a record into. Monotonic: never lowers an existing
   * floor (a same-epoch re-store is a no-op), and never records anything for a kind that carries no
   * epoch.
   * @param {string} localId
   * @param {object} record
   * @param {{ epoch: number|null, ownerPublicKeyB64: string }} gate - the verdict from #epochGate
   * @param {number} nowMs
   */
  #raiseEpochFloor(localId, record, gate, nowMs) {
    if (!gate || gate.epoch === null) return;
    const existing = this.#epochFloors.get(localId);
    if (existing && existing.epoch >= gate.epoch) return;
    if (!existing && this.#epochFloors.size >= this.#maxEpochFloors) {
      this.#evictOldestEpochFloor();
    }
    this.#epochFloors.set(localId, {
      epoch: gate.epoch,
      ownerPublicKeyB64: gate.ownerPublicKeyB64,
      observedAtMs: nowMs,
    });
  }

  /**
   * Make room under the floor cap by dropping the least-recently-raised entry. This is a real
   * safety regression for that account on this node — it re-opens the rollback window the floor was
   * holding shut — so it is never silent.
   */
  #evictOldestEpochFloor() {
    let oldestKey = null;
    let oldestAt = Infinity;
    for (const [key, entry] of this.#epochFloors) {
      if (entry.observedAtMs < oldestAt) {
        oldestAt = entry.observedAtMs;
        oldestKey = key;
      }
    }
    if (oldestKey === null) return;
    const dropped = this.#epochFloors.get(oldestKey);
    this.#epochFloors.delete(oldestKey);
    console.warn("[DHT] durable-record store: EVICTED epoch floor for " + oldestKey + " (epoch "
      + dropped.epoch + ") — the floor cap of " + this.#maxEpochFloors
      + " was reached; that slot can now accept an older epoch until it observes a newer one again");
  }

  /**
   * Every remembered floor, for persistence snapshotting. Shape mirrors `loadEpochFloors`.
   * @returns {Array<{ localId: string, epoch: number, ownerPublicKeyB64: string, observedAtMs: number }>}
   */
  epochFloorEntries() {
    const out = [];
    for (const [localId, entry] of this.#epochFloors) {
      out.push({ localId, epoch: entry.epoch, ownerPublicKeyB64: entry.ownerPublicKeyB64, observedAtMs: entry.observedAtMs });
    }
    return out;
  }

  /**
   * One slot's remembered floor, or null. Used by the persistence hook to write through the entry
   * a just-accepted store raised.
   * @param {string} localId
   * @returns {{ localId: string, epoch: number, ownerPublicKeyB64: string, observedAtMs: number }|null}
   */
  epochFloorEntry(localId) {
    const entry = this.#epochFloors.get(localId);
    if (!entry) return null;
    return { localId, epoch: entry.epoch, ownerPublicKeyB64: entry.ownerPublicKeyB64, observedAtMs: entry.observedAtMs };
  }

  /**
   * Seed floors from a persisted snapshot. MERGES monotonically — an entry only raises a floor,
   * never lowers one — so load order is irrelevant and a stale file cannot weaken a floor the
   * running store already holds. Malformed entries are dropped LOUDLY: a floor is authorization-grade
   * state, and silently skipping a corrupt one would look identical to never having had it.
   *
   * The floor file is node-local trusted state, at the same level as the node's identity key. Unlike
   * a record it carries no signature, so it cannot be re-verified on load; a tampered file can only
   * raise a floor, which censors that account's publications ON THIS NODE (other replicas are
   * unaffected) — strictly weaker than the cluster-wide rollback the floor prevents.
   *
   * @param {Array<{ localId: string, epoch: number, ownerPublicKeyB64?: string, observedAtMs?: number }>} entries
   * @returns {number} entries applied
   */
  loadEpochFloors(entries) {
    if (!Array.isArray(entries)) return 0;
    let applied = 0;
    let malformed = 0;
    for (const entry of entries) {
      if (!entry || typeof entry !== "object") { malformed += 1; continue; }
      const localId = typeof entry.localId === "string" ? entry.localId.trim() : "";
      if (!localId) { malformed += 1; continue; }
      if (!Number.isSafeInteger(entry.epoch) || entry.epoch < 0) { malformed += 1; continue; }
      const existing = this.#epochFloors.get(localId);
      if (existing && existing.epoch >= entry.epoch) continue;
      if (!existing && this.#epochFloors.size >= this.#maxEpochFloors) {
        this.#evictOldestEpochFloor();
      }
      this.#epochFloors.set(localId, {
        epoch: entry.epoch,
        ownerPublicKeyB64: typeof entry.ownerPublicKeyB64 === "string" ? entry.ownerPublicKeyB64 : "",
        observedAtMs: Number.isFinite(entry.observedAtMs) ? entry.observedAtMs : 0,
      });
      applied += 1;
    }
    if (malformed > 0) {
      console.warn("[DHT] durable-record store: dropped " + malformed + " malformed epoch-floor entry(ies) on load"
        + " — those slots have no rollback floor until they observe a record again");
    }
    return applied;
  }

  /**
   * The key that slot-roll and quota accounting attribute a record to: V1
   * slots key the publisher; V2 slots key the OWNER (owner/signer split — the
   * signer may be a delegated device key, but the slot and its quota belong
   * to the owner, and the slot derivation is identical). Keying V2 off
   * `publisherPublicKeyB64` (absent on V2) would make every same-slot V2
   * republish read as cross-publisher ("immutable") and pool all V2 quota
   * under one "" bucket.
   *
   * @param {object} record
   * @returns {string}
   */
  #accountingKey(record) {
    if (record && record.v === DURABLE_RECORD_V2_VERSION) {
      return typeof record.ownerPublicKeyB64 === "string" ? record.ownerPublicKeyB64 : "";
    }
    return record && typeof record.publisherPublicKeyB64 === "string" ? record.publisherPublicKeyB64 : "";
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

  #releaseQuota(record) {
    const { map } = this.#bucketFor(record);
    const pub = this.#accountingKey(record);
    const bytes = this.#recordBytes(record);
    const quota = map.get(pub);
    if (!quota) return;
    quota.count -= 1;
    quota.bytes -= bytes;
    if (quota.bytes < 0) quota.bytes = 0;
    // Count is the authoritative indicator — a publisher can legitimately
    // hold records whose total bytes is 0 (empty payloads), so never key
    // deletion on bytes or the count for still-held records is lost.
    if (quota.count <= 0) {
      map.delete(pub);
    } else {
      map.set(pub, quota);
    }
  }
}
