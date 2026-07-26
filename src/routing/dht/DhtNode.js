import { durableRecordLocalId } from "@rezprotocol/core";
import { DhtNodeId } from "./DhtNodeId.js";
import { KBucketTable } from "./KBucketTable.js";
import { DhtValueStore } from "./DhtValueStore.js";
import { DhtLookup } from "./DhtLookup.js";
import { DhtProtocol } from "./DhtProtocol.js";
import { DhtRouteResolver } from "./DhtRouteResolver.js";
import { DhtRouteAnnouncer } from "./DhtRouteAnnouncer.js";
import { DurableRecordStore } from "./DurableRecordStore.js";
import { DurableRecordProtocol } from "./DurableRecordProtocol.js";
import { verifyDurableRecordDual, durableRecordTargetId, DEFAULT_MAX_RECORD_BYTES } from "./DurableRecord.js";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";

/**
 * Orchestrates the DHT subsystem. Owns the k-bucket table, value store,
 * lookup engine, protocol handlers, and the resolver/announcer strategies
 * that plug into the existing routing infrastructure.
 *
 * Usage:
 *   const dht = new DhtNode({ selfRelayKeyId, controlMessageRegistry, encodeCtl, trySendFrame });
 *   dht.install();  // register protocol handlers
 *   // Inject into routing:
 *   gatewayLoop.routeResolver = dht.routeResolver;
 *   inboxRouter._routeAnnouncer = dht.routeAnnouncer;
 *   // On peer connect/disconnect:
 *   dht.addPeer(relayKeyId, socket);
 *   dht.removePeer(relayKeyId);
 */
export class DhtNode {
  /** @type {DhtNodeId} */
  #selfNodeId;

  /** @type {KBucketTable} */
  #kBuckets;

  /** @type {DhtValueStore} */
  #valueStore;

  /** @type {DhtLookup} */
  #lookup;

  /** @type {DhtProtocol} */
  #protocol;

  /** @type {DhtRouteResolver} */
  #resolver;

  /** @type {DhtRouteAnnouncer} */
  #announcer;

  /** @type {DurableRecordStore} */
  #recordStore;

  /** @type {DurableRecordProtocol} */
  #recordProtocol;

  /** @type {import("./DurableRecordPersistence.js").DurableRecordPersistence|null} */
  #recordPersistence;

  /** @type {import("./DurableRecordEpochFloorPersistence.js").DurableRecordEpochFloorPersistence|null} */
  #epochFloorPersistence;

  /** @type {number} */
  #maxRecordBytes;

  /** @type {() => number} */
  #nowMs;

  /**
   * @param {object} options
   * @param {string} options.selfRelayKeyId
   * @param {import("../ControlMessageRegistry.js").ControlMessageRegistry} options.controlMessageRegistry
   * @param {(obj: object) => Uint8Array} options.encodeCtl
   * @param {(socket: object, bytes: Uint8Array) => void} options.trySendFrame
   * @param {import("../RouteResolver.js").RouteResolver|null} [options.fallbackResolver]
   * @param {{ k?: number, alpha?: number, queryTimeoutMs?: number, valueTtlMs?: number, republishIntervalMs?: number, storeRateLimitWindowMs?: number, storeRateLimitMax?: number }} [options.config]
   * @param {() => number} [options.nowMs]
   * @param {(socket: object) => string|null} [options.getPeerKey] LOW-6: maps socket -> peer rate-limit key (relayKeyId)
   * @param {(socket: object) => string|null} [options.getPeerIp] MED-13: maps socket -> /64-aggregated IP key
   */
  constructor({
    selfRelayKeyId,
    controlMessageRegistry,
    encodeCtl,
    trySendFrame,
    fallbackResolver = null,
    config = {},
    nowMs = () => Date.now(),
    getPeerKey = null,
    getPeerIp = null,
  }) {
    if (typeof selfRelayKeyId !== "string" || !selfRelayKeyId.trim()) {
      throw new Error("DhtNode requires selfRelayKeyId");
    }
    if (!controlMessageRegistry) {
      throw new Error("DhtNode requires controlMessageRegistry");
    }
    if (typeof encodeCtl !== "function") {
      throw new Error("DhtNode requires encodeCtl function");
    }
    if (typeof trySendFrame !== "function") {
      throw new Error("DhtNode requires trySendFrame function");
    }

    const k = config.k || 20;
    const alpha = config.alpha || 3;
    const queryTimeoutMs = config.queryTimeoutMs || 3000;
    const valueTtlMs = config.valueTtlMs || 86_400_000;
    const republishIntervalMs = config.republishIntervalMs || 3_600_000;
    const recordMaxBytes = config.recordMaxBytes || DEFAULT_MAX_RECORD_BYTES;

    this.#nowMs = nowMs;
    this.#maxRecordBytes = recordMaxBytes;
    this.#recordPersistence = null;
    this.#epochFloorPersistence = null;
    this.#selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
    this.#kBuckets = new KBucketTable(this.#selfNodeId, { k });
    this.#valueStore = new DhtValueStore({ defaultTtlMs: valueTtlMs });
    this.#lookup = new DhtLookup(this.#kBuckets, { alpha, k });

    const storeRateLimiter = new SlidingWindowRateLimiter({
      windowMs: config.storeRateLimitWindowMs,
      maxAttempts: config.storeRateLimitMax,
    });

    this.#protocol = new DhtProtocol({
      kBuckets: this.#kBuckets,
      valueStore: this.#valueStore,
      registry: controlMessageRegistry,
      selfNodeId: this.#selfNodeId,
      selfRelayKeyId,
      encodeCtl,
      trySendFrame,
      queryTimeoutMs,
      k,
      nowMs,
      storeRateLimiter,
      getPeerKey,
      getPeerIp,
    });

    this.#resolver = new DhtRouteResolver({
      lookup: this.#lookup,
      protocol: this.#protocol,
      valueStore: this.#valueStore,
      fallbackResolver,
      nowMs,
    });

    this.#announcer = new DhtRouteAnnouncer({
      protocol: this.#protocol,
      kBuckets: this.#kBuckets,
      k,
      republishIntervalMs,
      nowMs,
    });

    // Durable signed-record value-class on the same overlay. Records get
    // their own store budgets (durable disk is a bigger DoS surface than
    // ephemeral routes) and their own re-replication cadence.
    this.#recordStore = new DurableRecordStore({
      maxRecordsPerPublisher: config.recordMaxRecordsPerPublisher,
      maxBytesPerPublisher: config.recordMaxBytesPerPublisher,
      maxRecordTtlMs: config.recordMaxTtlMs,
    });
    this.#recordProtocol = new DurableRecordProtocol({
      kBuckets: this.#kBuckets,
      recordStore: this.#recordStore,
      registry: controlMessageRegistry,
      selfNodeId: this.#selfNodeId,
      encodeCtl,
      trySendFrame,
      queryTimeoutMs,
      k,
      nowMs,
      maxRecordBytes: recordMaxBytes,
      replicateIntervalMs: config.recordReplicateIntervalMs,
      maxRepublishPerTick: config.recordMaxRepublishPerTick,
      storeRateLimiter: new SlidingWindowRateLimiter({
        windowMs: config.recordStoreRateLimitWindowMs,
        maxAttempts: config.recordStoreRateLimitMax,
      }),
      getPeerKey,
      getPeerIp,
      onRecordStored: (localId, entry) => this.#persistRecord(localId, entry),
      onEpochFloorRaised: (entry) => this.#persistEpochFloor(entry),
      resolveAcrossOverlay: (localId) => this.#resolveRecordOverlay(localId),
      resolveRateLimiter: (config.recordResolveRateLimitMax || config.recordResolveRateLimitWindowMs)
        ? new SlidingWindowRateLimiter({
            windowMs: config.recordResolveRateLimitWindowMs,
            maxAttempts: config.recordResolveRateLimitMax,
          })
        : null,
    });
  }

  /**
   * Register DHT protocol handlers on the ControlMessageRegistry.
   */
  install() {
    this.#protocol.install();
    this.#recordProtocol.install();
  }

  /**
   * Unregister DHT protocol handlers.
   */
  uninstall() {
    this.#protocol.uninstall();
    this.#recordProtocol.uninstall();
  }

  /**
   * Attach a durable-record persistence backend. Records held on behalf of
   * the network are written through to it (so they survive relay restart).
   * @param {import("./DurableRecordPersistence.js").DurableRecordPersistence|null} persistence
   */
  setRecordPersistence(persistence) {
    this.#recordPersistence = persistence || null;
  }

  /**
   * Attach persistence for the durable-record ROLLBACK FLOOR (highest epoch ever accepted per
   * epoch-ordered slot). Separate from the record persistence because floors outlive records —
   * see DurableRecordEpochFloorPersistence.
   * @param {import("./DurableRecordEpochFloorPersistence.js").DurableRecordEpochFloorPersistence|null} persistence
   */
  setEpochFloorPersistence(persistence) {
    this.#epochFloorPersistence = persistence || null;
  }

  /**
   * Load previously-persisted records into the in-memory store (dropping any
   * already expired). Quota is recomputed from scratch.
   * @returns {Promise<number>} number of records loaded
   */
  async loadPersistedRecords() {
    // Floors FIRST, and inside this method rather than as a separate call the caller must remember
    // to sequence: loadFromSnapshot re-derives a floor from every record it loads, and those
    // re-derived floors must not be able to sit BELOW a persisted one. Because both paths only ever
    // raise, loading the persisted floors first makes the order irrelevant to the result — but
    // owning the order here means no caller can get it wrong.
    await this.#loadPersistedEpochFloors();
    if (!this.#recordPersistence) return 0;
    const entries = await this.#recordPersistence.loadAll();
    const now = this.#nowMs();
    const list = Array.isArray(entries) ? entries : [];
    // Persistence reload is an ingress path like any other — on-disk state is
    // NOT a trust root. A corrupted or tampered snapshot must not seed forged
    // records, so re-run every entry through the SAME gate the network ingress
    // uses (sig + publisher-key-binding + size) before loading. The store
    // stays dumb; verification lives here at the boundary, exactly as it does
    // for putRecord and the inbound rec_store handler.
    const verified = [];
    let suspicious = 0;
    for (const entry of list) {
      if (!entry || typeof entry !== "object" || !entry.record) continue;
      const verdict = await verifyDurableRecordDual(entry.record, now, { maxBytes: this.#maxRecordBytes });
      if (verdict.ok && verdict.localId === entry.localId) {
        verified.push(entry);
        continue;
      }
      // Expired entries are normal end-of-life (loadFromSnapshot drops them
      // too) — not corruption. Anything else failing the gate (bad signature,
      // size, or a record parked under the wrong slot) is a tampered or
      // corrupted snapshot: surface it loudly.
      if (verdict.reason !== "expired") suspicious += 1;
    }
    if (suspicious > 0) {
      console.warn("[DHT] durable-record reload: dropped " + suspicious + " of " + list.length
        + " persisted record(s) failing re-verification — possible snapshot tampering or corruption");
    }
    this.#recordStore.loadFromSnapshot(verified, now);
    return this.#recordStore.size;
  }

  /**
   * Publish a signed durable record: verify it, hold a local copy, and STORE
   * it on the k-closest nodes to its slot (located via an iterative
   * FIND_NODE so the true k-closest are reached even when local buckets are
   * sparse). The publisher must be online here (record creation time) — but
   * never again: holders re-replicate and serve it thereafter.
   *
   * @param {object} record - a signed DurableRecordV1
   * @returns {Promise<{ stored: boolean, reason: string|null, localId: string|null, replicas: number }>}
   */
  async putRecord(record) {
    const now = this.#nowMs();
    const verdict = await verifyDurableRecordDual(record, now, { maxBytes: this.#maxRecordBytes });
    if (!verdict.ok) {
      return { stored: false, reason: verdict.reason, localId: null, replicas: 0 };
    }
    const localId = verdict.localId;
    // Hold a local copy (and persist it) so an immediate read resolves and
    // the publisher's own node seeds the slot. Honor the store verdict: a live
    // slot may reject this record as immutable (same issuance, different
    // content) or as an older issuance (a rolled-back / stale rebroadcast).
    // Report that truthfully instead of claiming a store that did not happen,
    // and do NOT push a superseded record out to the k-closest replicas.
    const localResult = this.#recordProtocol.storeVerified(localId, record);
    if (!localResult.stored) {
      return { stored: false, reason: localResult.reason, localId, replicas: 0 };
    }

    const targetId = durableRecordTargetId(localId);
    const { closestNodes } = await this.#lookup.findNode(targetId, (entry, tid) => {
      return this.#protocol.queryFindNode(entry.socket, tid);
    });
    let replicas = 0;
    for (const node of closestNodes) {
      if (!node.socket || node.socket.destroyed === true) continue;
      this.#recordProtocol.queryRecStore(node.socket, localId, record);
      replicas += 1;
    }
    return { stored: true, reason: null, localId, replicas };
  }

  /**
   * Fetch a durable record by its publisher-bound coordinates. Local-first,
   * then an iterative FIND_VALUE over the overlay. The returned record is
   * re-verified (signature + slot-binding) before it is trusted, and
   * read-repaired into the local store.
   *
   * @param {{ recordKind: string, recordId: string, publisherPublicKeyB64: string }} coords
   * @returns {Promise<object|null>} the verified record, or null
   */
  async getRecord({ recordKind, recordId, publisherPublicKeyB64 } = {}) {
    const now = this.#nowMs();
    let localId;
    try {
      localId = durableRecordLocalId({ publisherPublicKeyB64, recordKind, recordId });
    } catch (err) {
      return null;
    }

    const local = this.#recordStore.get(localId, now);
    if (local) return local;

    return this.#resolveRecordOverlay(localId);
  }

  /**
   * Iterative FIND_VALUE for a durable record by its slot key, with re-verify
   * + read-repair. Shared by getRecord (after a local miss) and the
   * resolve-on-behalf path the record protocol invokes when a non-peer client
   * delegates its lookup to us.
   * @param {string} localId - 64-char publisher-bound slot key
   * @returns {Promise<object|null>}
   */
  async #resolveRecordOverlay(localId) {
    let targetId;
    try {
      targetId = durableRecordTargetId(localId);
    } catch (err) {
      return null;
    }
    const { value } = await this.#lookup.findValue(targetId, (entry, tid) => {
      return this.#recordProtocol.queryRecFind(entry.socket, localId);
    });
    if (!value) return null;

    const verdict = await verifyDurableRecordDual(value, this.#nowMs(), { maxBytes: this.#maxRecordBytes });
    if (!verdict.ok || verdict.localId !== localId) return null;

    // Read-repair: hold a copy locally (and persist) so subsequent reads are
    // fast and the slot gains a holder.
    this.#recordProtocol.storeVerified(localId, value);
    return value;
  }

  /**
   * Run one storer-side re-replication pass over held records. Driven by the
   * mesh tick.
   * @param {number} nowMs
   */
  republishHeldRecords(nowMs) {
    this.#recordProtocol.republishHeldRecords(nowMs);
  }

  /**
   * Evict expired durable records from memory and mirror the removal to
   * durable storage. Returns the number evicted.
   * @param {number} nowMs
   * @returns {number}
   */
  evictExpiredRecords(nowMs) {
    const evicted = this.#recordStore.evictExpired(nowMs);
    if (this.#recordPersistence) {
      for (const localId of evicted) {
        this.#recordPersistence.remove(localId).catch((err) => {
          console.warn("[DHT] durable-record persistence remove failed for " + localId + ": "
            + (err && err.message ? err.message : err));
        });
      }
    }
    return evicted.length;
  }

  /**
   * Add a peer to the k-bucket table.
   * @param {string} relayKeyId
   * @param {object} socket
   */
  addPeer(relayKeyId, socket) {
    if (typeof relayKeyId !== "string" || !relayKeyId.trim()) return;
    const nodeId = DhtNodeId.fromRelayKeyId(relayKeyId);
    this.#kBuckets.addOrUpdate(nodeId, relayKeyId, socket, this.#nowMs());
  }

  /**
   * Remove a peer from the k-bucket table by relayKeyId.
   * @param {string} relayKeyId
   */
  removePeer(relayKeyId) {
    this.#kBuckets.remove(relayKeyId);
  }

  /**
   * Remove all peers with the given socket from k-buckets.
   * @param {object} socket
   * @returns {string[]} removed relayKeyIds
   */
  removePeerBySocket(socket) {
    return this.#kBuckets.removeBySocket(socket);
  }

  /**
   * Evict expired values from the DHT value store.
   * @param {number} nowMs
   * @returns {number} count evicted
   */
  evictExpiredValues(nowMs) {
    return this.#valueStore.evictExpired(nowMs);
  }

  /** @returns {DhtRouteResolver} */
  get routeResolver() {
    return this.#resolver;
  }

  /** @returns {DhtRouteAnnouncer} */
  get routeAnnouncer() {
    return this.#announcer;
  }

  /** @returns {KBucketTable} for diagnostics */
  get kBuckets() {
    return this.#kBuckets;
  }

  /** @returns {DhtValueStore} for diagnostics */
  get valueStore() {
    return this.#valueStore;
  }

  /** @returns {DurableRecordStore} for diagnostics/tests */
  get recordStore() {
    return this.#recordStore;
  }

  /** @returns {DurableRecordProtocol} for diagnostics/tests */
  get recordProtocol() {
    return this.#recordProtocol;
  }

  /**
   * Write a held record through to durable storage (fire-and-forget; errors
   * are logged, never swallowed). Invoked by the record protocol on a
   * first-time accepted store.
   */
  #persistRecord(localId, entry) {
    if (!this.#recordPersistence) return;
    this.#recordPersistence.put(localId, entry).catch((err) => {
      console.warn("[DHT] durable-record persistence put failed for " + localId + ": "
        + (err && err.message ? err.message : err));
    });
  }

  /**
   * Write a raised rollback floor through to disk. A failure here is NOT fatal — the in-memory
   * floor still holds for this process, and a restart re-derives it from the record while that
   * record is still held — but it does shorten the floor's life to the record's, so it is logged
   * as the safety-relevant event it is.
   * @param {{ localId: string, epoch: number, ownerPublicKeyB64: string, observedAtMs: number }} entry
   */
  #persistEpochFloor(entry) {
    if (!this.#epochFloorPersistence) return;
    this.#epochFloorPersistence.put(entry).catch((err) => {
      console.warn("[DHT] durable-record epoch-floor persistence put failed for " + entry.localId
        + " (epoch " + entry.epoch + "): " + (err && err.message ? err.message : err)
        + " — the floor holds in memory but will not survive a restart past the record's own lifetime");
    });
  }

  /**
   * Seed the store's rollback floors from disk. Runs before records are loaded (see
   * loadPersistedRecords). A read failure is surfaced loudly and treated as "no persisted floors"
   * rather than aborting startup: the node still enforces every floor it observes from this point
   * on, and refusing to boot would trade a partial safety property for total unavailability.
   * @returns {Promise<number>} floors applied
   */
  async #loadPersistedEpochFloors() {
    if (!this.#epochFloorPersistence) return 0;
    let entries;
    try {
      entries = await this.#epochFloorPersistence.loadAll();
    } catch (err) {
      console.warn("[DHT] durable-record epoch-floor reload failed: "
        + (err && err.message ? err.message : err)
        + " — starting with only the floors re-derived from held records");
      return 0;
    }
    return this.#recordStore.loadEpochFloors(entries);
  }
}
