import { createHash } from "node:crypto";
import { DhtNodeId } from "./DhtNodeId.js";
import { verifyDurableRecordDual, durableRecordTargetId, DEFAULT_MAX_RECORD_BYTES } from "./DurableRecord.js";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";
import { DhtQueryWaiter } from "./DhtQueryWaiter.js";
import { DhtRecordStoreAckWaiter } from "./DhtRecordStoreAckWaiter.js";
import { peerRateLimitKey, peerRateLimitIpKey } from "./peerRateLimitKeys.js";
import { canonicalJSONStringify } from "../../util/canonicalize.js";
import {
  CTL_DHT_REC_STORE,
  CTL_DHT_REC_STORE_ACK,
  DHT_RECORD_STORE_PROTOCOL_VERSION,
  DHT_REC_STORE_ACK_STATUS,
  DhtRecordStoreRequestV1,
  DhtRecordStoreAckV1,
  boundedRejectReason,
} from "../../contracts/wireRecords/DhtRecordStore.js";

const CTL_REC_STORE = CTL_DHT_REC_STORE;
const CTL_REC_STORE_ACK = CTL_DHT_REC_STORE_ACK;
const CTL_REC_FIND = "dht.rec_find";
const CTL_REC_FIND_REPLY = "dht.rec_find.reply";

/** SHA-256 over the canonical JSON bytes of a signed record. */
export function durableRecordDigestHex(record) {
  return createHash("sha256").update(canonicalJSONStringify(record), "utf8").digest("hex");
}

/**
 * Durable signed-record protocol layer. A sibling of DhtProtocol that runs
 * on the same Kademlia overlay (shared k-buckets) but carries opaque signed
 * records instead of route entries, with publisher-bound slot keys and
 * storer-side re-replication (the durability mechanism the route DHT
 * deliberately omits because routes should die when their owner goes
 * offline — records must outlive an offline publisher).
 *
 * Control messages:
 *   dht.rec_store       { key, record }                     fire-and-forget
 *   dht.rec_find        { queryId, key }
 *   dht.rec_find.reply  { queryId, record|null, nodes }
 */
export class DurableRecordProtocol {
  /** @type {import("./KBucketTable.js").KBucketTable} */
  #kBuckets;

  /** @type {import("./DurableRecordStore.js").DurableRecordStore} */
  #recordStore;

  /** @type {import("../ControlMessageRegistry.js").ControlMessageRegistry} */
  #registry;

  /** @type {DhtNodeId} */
  #selfNodeId;

  /** @type {(obj: object) => Uint8Array} */
  #encodeCtl;

  /** @type {(socket: object, bytes: Uint8Array) => void} */
  #trySendFrame;

  /** @type {DhtQueryWaiter} */
  #queryWaiter;

  /** @type {DhtRecordStoreAckWaiter} */
  #ackWaiter;

  /** @type {{ attempted: number, acknowledgedStored: number, acknowledgedRefreshed: number, rejected: number, timedOut: number }} cumulative re-replication truth */
  #replicationStats;

  /** @type {number} */
  #k;

  /** @type {() => number} */
  #nowMs;

  /** @type {number} */
  #maxRecordBytes;

  /** @type {number} */
  #replicateIntervalMs;

  /** @type {number} */
  #maxRepublishPerTick;

  /** @type {Map<string, { lastPushMs: number }>} per-slot re-replication state */
  #replication;

  /** @type {SlidingWindowRateLimiter} */
  #storeRateLimiter;

  /** @type {SlidingWindowRateLimiter} */
  #storeIpRateLimiter;

  /** @type {(socket: object) => string|null} */
  #getPeerKey;

  /** @type {(socket: object) => string|null} */
  #getPeerIp;

  /** @type {((localId: string, entry: { record: object, storedAtMs: number, ttlMs: number }) => void)|null} */
  #onRecordStored;

  /** @type {((entry: { localId: string, epoch: number, ownerPublicKeyB64: string, observedAtMs: number }) => void)|null} */
  #onEpochFloorRaised;

  /** @type {((localId: string) => Promise<object|null>)|null} */
  #resolveAcrossOverlay;

  /** @type {SlidingWindowRateLimiter} */
  #resolveRateLimiter;

  /**
   * @param {object} options
   * @param {import("./KBucketTable.js").KBucketTable} options.kBuckets
   * @param {import("./DurableRecordStore.js").DurableRecordStore} options.recordStore
   * @param {import("../ControlMessageRegistry.js").ControlMessageRegistry} options.registry
   * @param {DhtNodeId} options.selfNodeId
   * @param {(obj: object) => Uint8Array} options.encodeCtl
   * @param {(socket: object, bytes: Uint8Array) => void} options.trySendFrame
   * @param {number} [options.queryTimeoutMs]
   * @param {number} [options.k]
   * @param {() => number} [options.nowMs]
   * @param {number} [options.maxRecordBytes]
   * @param {number} [options.replicateIntervalMs] re-replication cadence per record
   * @param {number} [options.maxRepublishPerTick] global per-tick push cap
   * @param {SlidingWindowRateLimiter} [options.storeRateLimiter]
   * @param {(socket: object) => string|null} [options.getPeerKey]
   * @param {SlidingWindowRateLimiter} [options.storeIpRateLimiter]
   * @param {(socket: object) => string|null} [options.getPeerIp]
   * @param {(localId: string, entry: { record: object, storedAtMs: number, ttlMs: number }) => void} [options.onRecordStored] called after a first-time accepted store AND after a refresh (keyed upsert — the refreshed retention window must survive restart)
   */
  constructor({
    kBuckets,
    recordStore,
    registry,
    selfNodeId,
    encodeCtl,
    trySendFrame,
    queryTimeoutMs = 3000,
    k = 20,
    nowMs = () => Date.now(),
    maxRecordBytes = DEFAULT_MAX_RECORD_BYTES,
    replicateIntervalMs = 600_000,
    maxRepublishPerTick = 64,
    storeRateLimiter = null,
    getPeerKey = null,
    storeIpRateLimiter = null,
    getPeerIp = null,
    onRecordStored = null,
    onEpochFloorRaised = null,
    resolveAcrossOverlay = null,
    resolveRateLimiter = null,
  }) {
    if (!kBuckets) throw new Error("DurableRecordProtocol requires kBuckets");
    if (!recordStore) throw new Error("DurableRecordProtocol requires recordStore");
    if (!registry) throw new Error("DurableRecordProtocol requires registry");
    if (!(selfNodeId instanceof DhtNodeId)) throw new Error("DurableRecordProtocol requires selfNodeId");
    if (typeof encodeCtl !== "function") throw new Error("DurableRecordProtocol requires encodeCtl function");
    if (typeof trySendFrame !== "function") throw new Error("DurableRecordProtocol requires trySendFrame function");

    this.#kBuckets = kBuckets;
    this.#recordStore = recordStore;
    this.#registry = registry;
    this.#selfNodeId = selfNodeId;
    this.#encodeCtl = encodeCtl;
    this.#trySendFrame = trySendFrame;
    this.#queryWaiter = new DhtQueryWaiter({ queryTimeoutMs, idPrefix: "rec-q" });
    this.#ackWaiter = new DhtRecordStoreAckWaiter({ ackTimeoutMs: queryTimeoutMs });
    this.#replicationStats = {
      attempted: 0,
      acknowledgedStored: 0,
      acknowledgedRefreshed: 0,
      rejected: 0,
      timedOut: 0,
    };
    this.#k = k;
    this.#nowMs = nowMs;
    this.#maxRecordBytes = maxRecordBytes;
    this.#replicateIntervalMs = replicateIntervalMs;
    this.#maxRepublishPerTick = maxRepublishPerTick;
    this.#replication = new Map();
    // Records are a bigger DoS surface than ephemeral routes (durable disk),
    // so they get their own per-peer + per-IP store budgets, mirroring the
    // route DHT's LOW-6 / MED-13 defenses.
    this.#storeRateLimiter = storeRateLimiter || new SlidingWindowRateLimiter();
    this.#getPeerKey = typeof getPeerKey === "function" ? getPeerKey : null;
    this.#storeIpRateLimiter = storeIpRateLimiter
      || new SlidingWindowRateLimiter({ windowMs: 60_000, maxAttempts: 5000 });
    this.#getPeerIp = typeof getPeerIp === "function" ? getPeerIp : null;
    this.#onRecordStored = typeof onRecordStored === "function" ? onRecordStored : null;
    this.#onEpochFloorRaised = typeof onEpochFloorRaised === "function" ? onEpochFloorRaised : null;
    // A non-peer requester (e.g. a NAT'd leaf whose only routing peer is us)
    // cannot iterate the overlay itself, so on a local miss we resolve the
    // record across the connected core on its behalf — the same way a relay
    // resolves a route for a gateway deposit. Recursion is bounded to one hop:
    // the resolve queries our k-bucket PEERS, and a peer requester is served
    // local-only/hints (never this resolve path), so it cannot loop.
    this.#resolveAcrossOverlay = typeof resolveAcrossOverlay === "function" ? resolveAcrossOverlay : null;
    // Client-triggered resolves are more expensive than a local lookup, so they
    // get their own per-peer budget on top of the store budgets.
    this.#resolveRateLimiter = resolveRateLimiter
      || new SlidingWindowRateLimiter({ windowMs: 60_000, maxAttempts: 300 });
  }

  /**
   * Store a verified record locally and notify the persistence hook on a
   * first-time insert. Shared by the inbound handler and the local PUT path
   * so both persist identically.
   * @param {string} localId
   * @param {object} record
   * @returns {{ stored: boolean, reason: string|null }}
   */
  storeVerified(localId, record) {
    const result = this.#recordStore.store(localId, record, this.#nowMs());
    if (result.stored && result.reason === "refreshed") {
      // Re-audit R6: a refresh moves the retention window (storedAtMs/ttlMs)
      // in memory — write it through, or a holder kept alive for weeks purely
      // by acked refreshes still has the original window on disk and DROPS
      // the record at restart (loadFromSnapshot sees it as expired). That is
      // exactly gate R2's holders-under-bounded-churn case: the holder acked
      // "refreshed" to publishers who count it as a replica.
      if (this.#onRecordStored) {
        const entry = this.#recordStore.getEntry(localId, this.#nowMs());
        if (entry) this.#onRecordStored(localId, entry);
      }
    }
    if (result.stored && result.reason === null) {
      if (this.#onRecordStored) {
        const entry = this.#recordStore.getEntry(localId, this.#nowMs());
        if (entry) this.#onRecordStored(localId, entry);
      }
      // Write the slot's rollback floor through on the SAME condition as the record (a first-time
      // insert or a slot roll — never a re-replication refresh, whose floor is already on disk).
      // Ordering note: the record lands in memory before either hook runs, so a crash between the
      // two leaves a held record with no persisted floor. That degrades safely — loadFromSnapshot
      // re-derives the floor from the record it loads.
      if (this.#onEpochFloorRaised) {
        const floor = this.#recordStore.epochFloorEntry(localId);
        if (floor) this.#onEpochFloorRaised(floor);
      }
    }
    return result;
  }

  install() {
    this.#registry.register(CTL_REC_STORE, (ctlObj, socket) => this.#handleRecStore(ctlObj, socket));
    this.#registry.register(CTL_REC_STORE_ACK, (ctlObj, socket) => this.#handleRecStoreAck(ctlObj, socket));
    this.#registry.register(CTL_REC_FIND, (ctlObj, socket) => this.#handleRecFind(ctlObj, socket));
    this.#registry.register(CTL_REC_FIND_REPLY, (ctlObj, socket) => this.#handleRecFindReply(ctlObj, socket));
  }

  uninstall() {
    this.#registry.unregister(CTL_REC_STORE);
    this.#registry.unregister(CTL_REC_STORE_ACK);
    this.#registry.unregister(CTL_REC_FIND);
    this.#registry.unregister(CTL_REC_FIND_REPLY);
    this.#queryWaiter.clear();
    this.#ackWaiter.clear();
  }

  // ---------------------------------------------------------------------------
  // Query primitives
  // ---------------------------------------------------------------------------

  /**
   * Send dht.rec_store to a peer and await its authenticated acknowledgement
   * (ATLAS_PREREQUISITES P4.2 — no more fire-and-forget).
   *
   * The pending wait is registered BEFORE the frame is sent, and an ack is
   * accepted only from the same socket, for the same requestId, slot key, and
   * record digest. Timeout is a LOCAL outcome.
   *
   * @param {object} socket
   * @param {string} key - publisher-bound slot key (sha256 hex)
   * @param {object} record
   * @returns {Promise<{ outcome: "stored"|"refreshed"|"rejected"|"timeout", reason: string|null }>}
   */
  queryRecStore(socket, key, record) {
    const requestId = this.#ackWaiter.newRequestId();
    // Validates our own outbound shape; throws loudly on a programming error.
    const request = new DhtRecordStoreRequestV1({
      protocolVersion: DHT_RECORD_STORE_PROTOCOL_VERSION,
      requestId,
      key,
      record,
    });
    const digestHex = durableRecordDigestHex(record);
    const pending = this.#ackWaiter.wait(requestId, socket, key, digestHex);
    const bytes = this.#encodeCtl({
      _ctl: CTL_REC_STORE,
      protocolVersion: request.protocolVersion,
      requestId: request.requestId,
      key: request.key,
      record: request.record,
    });
    this.#trySendFrame(socket, bytes);
    return pending;
  }

  /** Cumulative acknowledged-replication truth for repair/status surfaces. */
  getReplicationStats() {
    return { ...this.#replicationStats };
  }

  /**
   * Send dht.rec_find to a peer and wait for the reply.
   * @param {object} socket
   * @param {string} key - publisher-bound slot key (also the routing target hash)
   * @returns {Promise<{ value: object|null, nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>}
   */
  queryRecFind(socket, key) {
    const queryId = this.#queryWaiter.newQueryId();
    const bytes = this.#encodeCtl({ _ctl: CTL_REC_FIND, queryId, key });
    this.#trySendFrame(socket, bytes);
    return this.#queryWaiter.wait(queryId, socket);
  }

  // ---------------------------------------------------------------------------
  // Storer-side re-replication — the durability engine
  // ---------------------------------------------------------------------------

  /**
   * Push held records this node is responsible for (among the k-closest to
   * the slot) toward the current k-closest peers, refreshing their retention
   * windows. Records this node is no longer closest to are NOT pushed and age
   * out of their holders' stores naturally (TTL decay = implicit hand-off, no
   * false-drop risk). New backbone nodes near a slot acquire the record
   * within one replicate interval via an existing holder's push.
   *
   * Driven by the existing 30s mesh tick; each record is actually re-pushed
   * at most once per `replicateIntervalMs`, and at most `maxRepublishPerTick`
   * records are pushed per tick to smooth bursts.
   *
   * @param {number} nowMs
   */
  republishHeldRecords(nowMs) {
    const entries = this.#recordStore.getAllEntries(nowMs);
    const live = new Set();
    let pushed = 0;
    for (const { localId, record } of entries) {
      live.add(localId);
      if (pushed >= this.#maxRepublishPerTick) continue;
      const state = this.#replication.get(localId) || { lastPushMs: 0 };
      if (nowMs - state.lastPushMs < this.#replicateIntervalMs) continue;

      let targetId;
      try {
        targetId = durableRecordTargetId(localId);
      } catch (err) {
        continue;
      }
      const closest = this.#kBuckets.findClosest(targetId, this.#k);
      if (!this.#selfResponsible(targetId, closest)) {
        // Not our slot anymore — stop refreshing it; the record ages out of
        // our store at maxRecordTtl. Stamp the time so we don't recompute
        // every tick.
        state.lastPushMs = nowMs;
        this.#replication.set(localId, state);
        continue;
      }
      for (const peer of closest) {
        if (!peer.socket || peer.socket.destroyed === true) continue;
        // Attempted vs acknowledged are tracked separately (P4.4). The tick
        // never awaits: each promise self-resolves within the ack timeout and
        // the per-tick record cap bounds how many can be in flight, so there
        // is no unbounded promise collection and no tick overlap.
        this.#replicationStats.attempted += 1;
        this.queryRecStore(peer.socket, localId, record).then((res) => {
          if (res.outcome === DHT_REC_STORE_ACK_STATUS.STORED) {
            this.#replicationStats.acknowledgedStored += 1;
          } else if (res.outcome === DHT_REC_STORE_ACK_STATUS.REFRESHED) {
            this.#replicationStats.acknowledgedRefreshed += 1;
          } else if (res.outcome === DHT_REC_STORE_ACK_STATUS.REJECTED) {
            this.#replicationStats.rejected += 1;
          } else {
            this.#replicationStats.timedOut += 1;
          }
        });
      }
      state.lastPushMs = nowMs;
      this.#replication.set(localId, state);
      pushed += 1;
    }
    // Drop re-replication state for records we no longer hold.
    for (const key of this.#replication.keys()) {
      if (!live.has(key)) this.#replication.delete(key);
    }
  }

  // ---------------------------------------------------------------------------
  // Incoming message handlers
  // ---------------------------------------------------------------------------

  async #handleRecStore(ctlObj, socket) {
    // Typed request or nothing: the acked protocol has no legacy unacked
    // shape (the dev network upgrades in lockstep — ADR-RELAY-IDENTITY reset).
    const request = DhtRecordStoreRequestV1.tryCreate({
      protocolVersion: ctlObj.protocolVersion,
      requestId: ctlObj.requestId,
      key: typeof ctlObj.key === "string" ? ctlObj.key : "",
      record: ctlObj.record,
    });
    if (!request) return;
    const key = request.key;

    // Per-peer + per-IP store budgets (durable records are a juicier DoS
    // target than routes — see constructor note). Rate-limiting runs BEFORE
    // verifyDurableRecord by design: signature verification is the expensive
    // step, so the budget must gate it — otherwise a flood of invalid records
    // forces unbounded Ed25519 checks (CPU DoS). Do not reorder verify ahead
    // of the limiter. Rate-limited requests are dropped SILENTLY (no ack):
    // acking a flood would amplify it, and the sender's timeout is the
    // truthful local outcome.
    const peerKey = peerRateLimitKey(socket, this.#getPeerKey);
    if (!this.#storeRateLimiter.record(peerKey, this.#nowMs())) {
      console.warn("[DHT] dht.rec_store: rejected " + key + " — peer rate limit exceeded (peerKey=" + peerKey + ")");
      return;
    }
    const ipKey = peerRateLimitIpKey(socket, this.#getPeerIp);
    if (ipKey && !this.#storeIpRateLimiter.record(ipKey, this.#nowMs())) {
      console.warn("[DHT] dht.rec_store: rejected " + key + " — per-IP rate limit exceeded (ipKey=" + ipKey + ")");
      return;
    }

    const record = request.record;
    const verdict = await verifyDurableRecordDual(record, this.#nowMs(), { maxBytes: this.#maxRecordBytes });
    if (!verdict.ok) {
      console.warn("[DHT] dht.rec_store: rejected " + key + " — " + verdict.reason);
      this.#sendStoreAck(socket, request, DHT_REC_STORE_ACK_STATUS.REJECTED, boundedRejectReason(verdict.reason));
      return;
    }
    // Substitution guard: the announced slot key MUST equal the
    // publisher-bound slot derived from the record's own fields. Stops a
    // peer parking a (valid) record under someone else's slot.
    if (verdict.localId !== key) {
      console.warn("[DHT] dht.rec_store: rejected " + key + " — key/record mismatch");
      this.#sendStoreAck(socket, request, DHT_REC_STORE_ACK_STATUS.REJECTED, "slot-mismatch");
      return;
    }

    const result = this.storeVerified(key, record);
    if (result.stored && result.reason === null) {
      // `stored` means committed to the local store AND the persistence hook
      // ran (storeVerified invokes it on first-time inserts).
      this.#sendStoreAck(socket, request, DHT_REC_STORE_ACK_STATUS.STORED, null);
      return;
    }
    if (result.reason === "refreshed") {
      this.#sendStoreAck(socket, request, DHT_REC_STORE_ACK_STATUS.REFRESHED, null);
      return;
    }
    console.warn("[DHT] dht.rec_store: not stored for " + key + " — " + result.reason);
    this.#sendStoreAck(socket, request, DHT_REC_STORE_ACK_STATUS.REJECTED, boundedRejectReason(result.reason));
  }

  #sendStoreAck(socket, request, status, reason) {
    const ack = new DhtRecordStoreAckV1({
      protocolVersion: DHT_RECORD_STORE_PROTOCOL_VERSION,
      requestId: request.requestId,
      key: request.key,
      // stored/refreshed: digest of the exact bytes now held. rejected:
      // digest of the request's record, echoed for sender correlation only.
      recordDigestHex: durableRecordDigestHex(request.record),
      status,
      reason,
    });
    this.#trySendFrame(socket, this.#encodeCtl({
      _ctl: CTL_REC_STORE_ACK,
      protocolVersion: ack.protocolVersion,
      requestId: ack.requestId,
      key: ack.key,
      recordDigestHex: ack.recordDigestHex,
      status: ack.status,
      reason: ack.reason,
    }));
  }

  #handleRecStoreAck(ctlObj, socket) {
    const ack = DhtRecordStoreAckV1.tryCreate({
      protocolVersion: ctlObj.protocolVersion,
      requestId: ctlObj.requestId,
      key: typeof ctlObj.key === "string" ? ctlObj.key : "",
      recordDigestHex: ctlObj.recordDigestHex,
      status: ctlObj.status,
      reason: ctlObj.reason == null ? null : ctlObj.reason,
    });
    if (!ack) return;
    const verdict = this.#ackWaiter.resolve(socket, ack);
    if (verdict !== "ok") {
      // Late, duplicate, forged, or mismatched acks are IGNORED — they never
      // consume the pending wait (the timeout stays authoritative).
      console.warn("[DHT] dht.rec_store.ack: ignored (" + verdict + ") for " + ack.key);
    }
  }

  async #handleRecFind(ctlObj, socket) {
    const key = typeof ctlObj.key === "string" ? ctlObj.key.trim() : "";
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    if (!queryId) return;

    if (key) {
      const record = this.#recordStore.get(key, this.#nowMs());
      if (record) {
        // Defense-in-depth re-verification before serving (mirrors the route
        // DHT's HIGH-8 re-check on find_value).
        const verdict = await verifyDurableRecordDual(record, this.#nowMs(), { maxBytes: this.#maxRecordBytes });
        if (verdict.ok && verdict.localId === key) {
          this.#trySendFrame(socket, this.#encodeCtl({ _ctl: CTL_REC_FIND_REPLY, queryId, record, nodes: [] }));
          return;
        }
        this.#recordStore.remove(key);
      }

      // Local miss. If the requester is a non-peer client (not in our routing
      // table — e.g. a NAT'd leaf whose only DHT peer is us), it cannot iterate
      // the overlay itself, so resolve the record across the connected core on
      // its behalf and serve the result. A peer requester (a relay-core member)
      // iterates for itself, so it gets the k-closest hints below as usual —
      // which also bounds this to a single recursion hop.
      if (key.length === 64 && this.#resolveAcrossOverlay && !this.#kBuckets.hasSocket(socket)) {
        const peerKey = peerRateLimitKey(socket, this.#getPeerKey);
        if (!this.#resolveRateLimiter.record(peerKey, this.#nowMs())) {
          console.warn("[DHT] dht.rec_find: rejected recursive resolve for " + key + " — peer rate limit exceeded (peerKey=" + peerKey + ")");
        } else {
          let resolved = null;
          try {
            resolved = await this.#resolveAcrossOverlay(key);
          } catch (err) {
            console.warn("[DHT] dht.rec_find: recursive resolve failed for " + key + " — "
              + (err && err.message ? err.message : err));
          }
          if (resolved) {
            this.#trySendFrame(socket, this.#encodeCtl({ _ctl: CTL_REC_FIND_REPLY, queryId, record: resolved, nodes: [] }));
            return;
          }
        }
      }
    }

    // No value — respond with k-closest nodes (the slot key IS the target hash).
    const nodes = [];
    if (key.length === 64) {
      let targetId;
      try {
        targetId = DhtNodeId.fromHex(key);
      } catch (err) {
        targetId = null;
      }
      if (targetId) {
        const closest = this.#kBuckets.findClosest(targetId, this.#k);
        for (const entry of closest) {
          nodes.push({ nodeIdHex: entry.nodeId.hex, relayKeyId: entry.relayKeyId });
        }
      }
    }
    this.#trySendFrame(socket, this.#encodeCtl({ _ctl: CTL_REC_FIND_REPLY, queryId, record: null, nodes }));
  }

  #handleRecFindReply(ctlObj, socket) {
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    const value = ctlObj.record && typeof ctlObj.record === "object" ? ctlObj.record : null;
    const nodes = Array.isArray(ctlObj.nodes) ? ctlObj.nodes : [];
    // HIGH-9: reply must arrive on the same socket the query was sent to.
    if (this.#queryWaiter.resolve(queryId, socket, { value, nodes }) === "socket-mismatch") {
      console.warn("[DHT] dht.rec_find.reply: dropped reply on mismatched socket");
    }
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  #selfResponsible(targetId, closest) {
    if (closest.length < this.#k) return true;
    const kth = closest[closest.length - 1];
    return targetId.compareDistanceTo(this.#selfNodeId, kth.nodeId) < 0;
  }
}
