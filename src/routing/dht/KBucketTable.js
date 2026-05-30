import { DhtNodeId } from "./DhtNodeId.js";

/**
 * Kademlia k-bucket routing table.
 *
 * 256 buckets, one per bit of XOR distance from this node. Each bucket
 * holds up to k entries (default 20) sorted by last-seen time (LRU).
 * Oldest entries are at the front, most-recently-seen at the back.
 *
 * When a bucket is full and a new peer arrives:
 * - If the oldest entry's socket is destroyed → evict and insert new peer
 * - Otherwise → new peer is dropped (Kademlia: prefer long-lived nodes)
 */
export class KBucketTable {
  /** @type {DhtNodeId} */
  #selfId;

  /** @type {Array<Array<KBucketEntry>>} */
  #buckets;

  /** @type {number} */
  #k;

  /** @type {Map<string, number>} relayKeyId → bucket index for fast lookup */
  #relayKeyIndex;

  /**
   * @param {DhtNodeId} selfId
   * @param {{ k?: number }} options
   */
  constructor(selfId, { k = 20 } = {}) {
    if (!(selfId instanceof DhtNodeId)) {
      throw new Error("KBucketTable requires a DhtNodeId");
    }
    if (!Number.isInteger(k) || k < 1) {
      throw new Error("KBucketTable k must be a positive integer");
    }
    this.#selfId = selfId;
    this.#k = k;
    this.#buckets = new Array(256);
    for (let i = 0; i < 256; i += 1) {
      this.#buckets[i] = [];
    }
    this.#relayKeyIndex = new Map();
  }

  /**
   * Add a peer or update its last-seen time. Returns true if the peer
   * is now in the table (added or updated), false if rejected.
   *
   * @param {DhtNodeId} nodeId
   * @param {string} relayKeyId
   * @param {object} socket
   * @param {number} nowMs
   * @returns {boolean}
   */
  addOrUpdate(nodeId, relayKeyId, socket, nowMs) {
    if (!(nodeId instanceof DhtNodeId)) return false;
    if (typeof relayKeyId !== "string" || relayKeyId.trim().length === 0) return false;
    if (nodeId.equals(this.#selfId)) return false;

    const bucketIdx = this.#selfId.bucketIndex(nodeId);
    if (bucketIdx < 0) return false;

    const bucket = this.#buckets[bucketIdx];

    // Update existing entry
    const existingIdx = bucket.findIndex(function (e) { return e.relayKeyId === relayKeyId; });
    if (existingIdx >= 0) {
      const entry = bucket[existingIdx];
      entry.socket = socket;
      entry.lastSeenMs = nowMs;
      // Move to back (most recently seen)
      bucket.splice(existingIdx, 1);
      bucket.push(entry);
      return true;
    }

    // Bucket not full — insert at back
    if (bucket.length < this.#k) {
      const entry = { nodeId, relayKeyId, socket, lastSeenMs: nowMs };
      bucket.push(entry);
      this.#relayKeyIndex.set(relayKeyId, bucketIdx);
      return true;
    }

    // Bucket full — check if oldest entry's socket is dead
    const oldest = bucket[0];
    if (oldest.socket && oldest.socket.destroyed === true) {
      this.#relayKeyIndex.delete(oldest.relayKeyId);
      bucket.shift();
      const entry = { nodeId, relayKeyId, socket, lastSeenMs: nowMs };
      bucket.push(entry);
      this.#relayKeyIndex.set(relayKeyId, bucketIdx);
      return true;
    }

    // Oldest is live — reject new peer (prefer long-lived nodes)
    return false;
  }

  /**
   * Remove a peer by relayKeyId.
   * @param {string} relayKeyId
   * @returns {boolean}
   */
  remove(relayKeyId) {
    const bucketIdx = this.#relayKeyIndex.get(relayKeyId);
    if (bucketIdx === undefined) return false;

    const bucket = this.#buckets[bucketIdx];
    const idx = bucket.findIndex(function (e) { return e.relayKeyId === relayKeyId; });
    if (idx < 0) {
      this.#relayKeyIndex.delete(relayKeyId);
      return false;
    }

    bucket.splice(idx, 1);
    this.#relayKeyIndex.delete(relayKeyId);
    return true;
  }

  /**
   * Remove all entries whose socket matches. Returns removed relayKeyIds.
   * @param {object} socket
   * @returns {string[]}
   */
  removeBySocket(socket) {
    const removed = [];
    for (let i = 0; i < 256; i += 1) {
      const bucket = this.#buckets[i];
      for (let j = bucket.length - 1; j >= 0; j -= 1) {
        if (bucket[j].socket === socket) {
          removed.push(bucket[j].relayKeyId);
          this.#relayKeyIndex.delete(bucket[j].relayKeyId);
          bucket.splice(j, 1);
        }
      }
    }
    return removed;
  }

  /**
   * Find the count closest entries to targetId, sorted by XOR distance.
   * @param {DhtNodeId} targetId
   * @param {number} count
   * @returns {Array<KBucketEntry>}
   */
  findClosest(targetId, count) {
    if (!(targetId instanceof DhtNodeId) || count <= 0) return [];

    const all = [];
    for (let i = 0; i < 256; i += 1) {
      const bucket = this.#buckets[i];
      for (let j = 0; j < bucket.length; j += 1) {
        all.push(bucket[j]);
      }
    }

    all.sort(function (a, b) {
      return targetId.compareDistanceTo(a.nodeId, b.nodeId);
    });

    return all.slice(0, count);
  }

  /**
   * Look up an entry by relayKeyId.
   * @param {string} relayKeyId
   * @returns {KBucketEntry|null}
   */
  get(relayKeyId) {
    const bucketIdx = this.#relayKeyIndex.get(relayKeyId);
    if (bucketIdx === undefined) return null;
    const bucket = this.#buckets[bucketIdx];
    const entry = bucket.find(function (e) { return e.relayKeyId === relayKeyId; });
    return entry || null;
  }

  /** @returns {number} */
  get size() {
    let total = 0;
    for (let i = 0; i < 256; i += 1) {
      total += this.#buckets[i].length;
    }
    return total;
  }

  /**
   * Return all entries across all buckets.
   * @returns {Array<KBucketEntry>}
   */
  getAllEntries() {
    const all = [];
    for (let i = 0; i < 256; i += 1) {
      const bucket = this.#buckets[i];
      for (let j = 0; j < bucket.length; j += 1) {
        all.push(bucket[j]);
      }
    }
    return all;
  }
}

/**
 * @typedef {object} KBucketEntry
 * @property {DhtNodeId} nodeId
 * @property {string} relayKeyId
 * @property {object} socket
 * @property {number} lastSeenMs
 */
