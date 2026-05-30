import { DhtNodeId } from "./DhtNodeId.js";

/**
 * Iterative Kademlia lookup.
 *
 * Queries α closest known nodes in parallel, collects responses,
 * queries newly discovered closer nodes, and converges when no
 * closer nodes are returned.
 *
 * The sendQuery callback is injected by the caller — it handles
 * the actual control message send/receive over the wire.
 */
export class DhtLookup {
  /** @type {import("./KBucketTable.js").KBucketTable} */
  #kBuckets;

  /** @type {number} */
  #alpha;

  /** @type {number} */
  #k;

  /**
   * @param {import("./KBucketTable.js").KBucketTable} kBuckets
   * @param {{ alpha?: number, k?: number }} options
   */
  constructor(kBuckets, { alpha = 3, k = 20 } = {}) {
    if (!kBuckets || typeof kBuckets.findClosest !== "function") {
      throw new Error("DhtLookup requires a KBucketTable");
    }
    this.#kBuckets = kBuckets;
    this.#alpha = alpha;
    this.#k = k;
  }

  /**
   * Iterative FIND_NODE: find the k closest nodes to targetId.
   *
   * @param {DhtNodeId} targetId
   * @param {(entry: object, targetId: DhtNodeId) => Promise<{ nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>} sendQuery
   * @returns {Promise<{ closestNodes: Array<object> }>}
   */
  async findNode(targetId, sendQuery) {
    const result = await this.#iterativeLookup(targetId, sendQuery, false);
    return { closestNodes: result.closestNodes };
  }

  /**
   * Iterative FIND_VALUE: find a stored value or the k closest nodes.
   *
   * @param {DhtNodeId} targetId
   * @param {(entry: object, targetId: DhtNodeId) => Promise<{ value: object|null, nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>} sendQuery
   * @returns {Promise<{ value: object|null, closestNodes: Array<object> }>}
   */
  async findValue(targetId, sendQuery) {
    return this.#iterativeLookup(targetId, sendQuery, true);
  }

  /**
   * Core iterative lookup. Seeds from local k-buckets, queries α at a time,
   * adds discovered nodes, repeats until convergence.
   *
   * @param {DhtNodeId} targetId
   * @param {Function} sendQuery
   * @param {boolean} lookForValue
   * @returns {Promise<{ value: object|null, closestNodes: Array<object> }>}
   */
  async #iterativeLookup(targetId, sendQuery, lookForValue) {
    const seeds = this.#kBuckets.findClosest(targetId, this.#k);
    if (seeds.length === 0) {
      return { value: null, closestNodes: [] };
    }

    // Candidate list: all nodes we know about, sorted by distance
    /** @type {Map<string, { nodeId: DhtNodeId, relayKeyId: string, socket: object|null, queried: boolean }>} */
    const candidates = new Map();
    for (const seed of seeds) {
      candidates.set(seed.relayKeyId, {
        nodeId: seed.nodeId,
        relayKeyId: seed.relayKeyId,
        socket: seed.socket,
        queried: false,
      });
    }

    let foundValue = null;

    for (let round = 0; round < 10; round += 1) {
      // Pick α unqueried candidates closest to target
      const unqueried = [];
      for (const [, c] of candidates) {
        if (!c.queried) unqueried.push(c);
      }
      unqueried.sort(function (a, b) {
        return targetId.compareDistanceTo(a.nodeId, b.nodeId);
      });
      const batch = unqueried.slice(0, this.#alpha);
      if (batch.length === 0) break;

      // Query in parallel
      const promises = [];
      for (const candidate of batch) {
        candidate.queried = true;
        if (!candidate.socket || candidate.socket.destroyed === true) continue;
        promises.push(
          sendQuery(candidate, targetId).catch(function () {
            return { value: null, nodes: [] };
          })
        );
      }

      const results = await Promise.all(promises);

      let addedCloser = false;
      for (const result of results) {
        if (!result || typeof result !== "object") continue;

        // Check for value
        if (lookForValue && result.value && typeof result.value === "object") {
          foundValue = result.value;
          break;
        }

        // Merge discovered nodes
        const nodes = Array.isArray(result.nodes) ? result.nodes : [];
        for (const node of nodes) {
          if (!node || typeof node !== "object") continue;
          const relayKeyId = typeof node.relayKeyId === "string" ? node.relayKeyId : "";
          const nodeIdHex = typeof node.nodeIdHex === "string" ? node.nodeIdHex : "";
          if (!relayKeyId || !nodeIdHex || nodeIdHex.length !== 64) continue;
          if (candidates.has(relayKeyId)) continue;

          // LOW-5 (docs/SECURITY_AUDIT.md): nodeIdHex MUST be the
          // deterministic hash of relayKeyId. Otherwise a sybil peer
          // could claim a relayKeyId paired with an arbitrary nodeIdHex
          // chosen to land near the lookup target, monopolize the
          // iterative batch with socket-less candidates, and DoS the
          // lookup. Verifying the binding makes the sybil cost match
          // the cost of producing close-to-target relayKeyIds (which
          // requires brute-force key generation — Kademlia's
          // intended cost model).
          const derivedNodeId = DhtNodeId.fromRelayKeyId(relayKeyId);
          if (derivedNodeId.hex !== nodeIdHex) continue;

          let nodeId;
          try {
            nodeId = DhtNodeId.fromHex(nodeIdHex);
          } catch (err) {
            console.warn("[DHT] lookup: invalid nodeIdHex from peer:", err && err.message ? err.message : err);
            continue;
          }

          // Only add if closer than our current k-th closest
          const sorted = this.#sortedCandidates(candidates, targetId);
          if (sorted.length >= this.#k) {
            const kth = sorted[this.#k - 1];
            const cmp = targetId.compareDistanceTo(nodeId, kth.nodeId);
            if (cmp >= 0) continue;
          }

          candidates.set(relayKeyId, {
            nodeId,
            relayKeyId,
            socket: null, // remote node — no direct socket
            queried: false,
          });
          addedCloser = true;
        }
      }

      if (foundValue) break;
      if (!addedCloser) break;
    }

    const closestNodes = this.#sortedCandidates(candidates, targetId).slice(0, this.#k);
    return { value: foundValue, closestNodes };
  }

  /**
   * Sort candidates by distance to target.
   * @param {Map} candidates
   * @param {DhtNodeId} targetId
   * @returns {Array}
   */
  #sortedCandidates(candidates, targetId) {
    const list = [];
    for (const [, c] of candidates) {
      list.push(c);
    }
    list.sort(function (a, b) {
      return targetId.compareDistanceTo(a.nodeId, b.nodeId);
    });
    return list;
  }
}
