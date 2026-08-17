import { isCanonicalRelayKeyId } from "@rezprotocol/core";
import { DhtNodeId } from "./DhtNodeId.js";
import { DhtLookupReportV1 } from "../../contracts/records/DhtLookupReportV1.js";
import { raceWithDeadline } from "../../util/raceWithDeadline.js";

/**
 * Failure marks that mean "never attempted", not "attempted and failed".
 * "budget-exhausted" is the resolver's NODE-GLOBAL concurrency deferral
 * (re-audit R4): another lookup held the dial slots, so nothing was ever
 * attempted against the candidate — it stays a valid closest-node reference
 * and its reserved per-lookup dial is refunded.
 */
const UNATTEMPTED_FAILURES = new Set(["no-resolver", "dial-budget", "budget-exhausted"]);

/** Sentinel resolved by the deadline race when the clock wins (re-audit R2). */
const DEADLINE_EXPIRED = Symbol("dht-lookup-deadline-expired");

/**
 * Iterative Kademlia lookup (reworked per ATLAS_PREREQUISITES P3.2).
 *
 * Queries α closest known nodes in parallel, merges verified discovered
 * references, RESOLVES socket-less discovered candidates through an optional
 * authenticated candidate resolver (bounded dials), and converges when no
 * closer nodes are returned.
 *
 * Truthfulness/bounds contract:
 * - a socket-less candidate is never marked queried before resolution
 *   succeeds or a typed resolution failure is recorded — the pre-P3 slot-burn
 *   defect (marking queried before the socket check) is fixed and pinned by a
 *   regression test;
 * - unresolved candidates cannot displace every queryable candidate from a
 *   batch: each batch reserves a slot for an already-queryable candidate when
 *   one exists;
 * - dials are capped per lookup, the total deadline spans dial AND query
 *   time, and duplicate/reordered references cannot reset any budget
 *   (dedup by canonical relay ID and DHT node ID, counted once);
 * - with no resolver configured, only connected candidates are ever queried —
 *   the pre-P3 connected-peer behavior.
 *
 * The sendQuery callback is injected by the caller — it handles the actual
 * control message send/receive over the wire (with its own reply waiter and
 * same-socket guard).
 */
export class DhtLookup {
  /** @type {import("./KBucketTable.js").KBucketTable} */
  #kBuckets;

  #alpha;
  #k;
  #maxRounds;
  #maxNewDialsPerLookup;
  #totalDeadlineMs;
  /** @type {import("./DhtCandidateResolver.js").DhtCandidateResolver|null} */
  #candidateResolver;
  #nowMs;

  /**
   * @param {import("./KBucketTable.js").KBucketTable} kBuckets
   * @param {{ alpha?: number, k?: number, maxRounds?: number, maxNewDialsPerLookup?: number, totalDeadlineMs?: number, candidateResolver?: object|null, nowMs?: () => number }} options
   */
  constructor(kBuckets, {
    alpha = 3,
    k = 20,
    maxRounds = 10,
    maxNewDialsPerLookup = 4,
    totalDeadlineMs = 10_000,
    candidateResolver = null,
    nowMs = () => Date.now(),
  } = {}) {
    if (!kBuckets || typeof kBuckets.findClosest !== "function") {
      throw new Error("DhtLookup requires a KBucketTable");
    }
    this.#kBuckets = kBuckets;
    this.#alpha = alpha;
    this.#k = k;
    this.#maxRounds = maxRounds;
    this.#maxNewDialsPerLookup = maxNewDialsPerLookup;
    this.#totalDeadlineMs = totalDeadlineMs;
    this.#candidateResolver = candidateResolver && typeof candidateResolver.resolve === "function"
      ? candidateResolver
      : null;
    this.#nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
  }

  /**
   * Iterative FIND_NODE: find the k closest nodes to targetId.
   * @param {DhtNodeId} targetId
   * @param {(entry: object, targetId: DhtNodeId) => Promise<{ nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>} sendQuery
   * @returns {Promise<{ closestNodes: Array<object>, report: DhtLookupReportV1 }>}
   */
  async findNode(targetId, sendQuery, { deadlineAtMs = null } = {}) {
    const result = await this.#iterativeLookup(targetId, sendQuery, false, deadlineAtMs);
    return { closestNodes: result.closestNodes, report: result.report };
  }

  /**
   * Iterative FIND_VALUE: find a stored value or the k closest nodes.
   * @param {DhtNodeId} targetId
   * @param {(entry: object, targetId: DhtNodeId) => Promise<{ value: object|null, nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>} sendQuery
   * @returns {Promise<{ value: object|null, closestNodes: Array<object>, report: DhtLookupReportV1 }>}
   */
  async findValue(targetId, sendQuery, { deadlineAtMs = null } = {}) {
    return this.#iterativeLookup(targetId, sendQuery, true, deadlineAtMs);
  }

  /**
   * Validated clock read (repo fail-open lesson: NaN >= deadline is false,
   * so an invalid injected clock would silently DISABLE every deadline).
   */
  #now() {
    const t = this.#nowMs();
    if (!Number.isFinite(t)) {
      throw new Error("DhtLookup: injected nowMs returned a non-finite time");
    }
    return t;
  }

  /**
   * Race a promise against the remaining wall-clock budget (re-audit R2:
   * checking the clock BETWEEN awaits is not a deadline — a never-settling
   * dial or query must not hang the lookup). Resolves DEADLINE_EXPIRED when
   * the clock wins; the losing work is abandoned, not cancelled.
   *
   * An already-spent budget SKIPS the work entirely rather than handing
   * raceWithDeadline a 0 ms timer: once the deadline has passed there is
   * nothing left to spend, so starting a dial or query would overrun it.
   * (DhtNode's ack window makes the opposite call for its own reasons.)
   */
  #raceDeadline(promise, deadlineAtMs) {
    const remainingMs = deadlineAtMs - this.#now();
    if (remainingMs <= 0) return Promise.resolve(DEADLINE_EXPIRED);
    return raceWithDeadline(promise, remainingMs, DEADLINE_EXPIRED);
  }

  async #iterativeLookup(targetId, sendQuery, lookForValue, callerDeadlineAtMs) {
    const startedAtMs = this.#now();
    // A caller running under its own total budget (re-audit R3: putRecord)
    // may tighten — never extend — this lookup's deadline.
    const ownDeadlineAtMs = startedAtMs + this.#totalDeadlineMs;
    const deadlineAtMs = Number.isFinite(callerDeadlineAtMs)
      ? Math.min(callerDeadlineAtMs, ownDeadlineAtMs)
      : ownDeadlineAtMs;
    const counters = { queried: 0, dialAttempts: 0, timeouts: 0, rejected: 0 };
    let dialsUsed = 0;

    const seeds = this.#kBuckets.findClosest(targetId, this.#k);
    if (seeds.length === 0) {
      return this.#finish(null, new Map(), targetId, counters, "no-candidates");
    }

    /** @type {Map<string, { nodeId: DhtNodeId, relayKeyId: string, socket: object|null, queried: boolean, failed: string|null }>} */
    const candidates = new Map();
    /** @type {Set<string>} nodeIdHex dedup — a reference re-sent under a fresh relay id/order cannot re-enter */
    const seenNodeIdHex = new Set();
    for (const seed of seeds) {
      candidates.set(seed.relayKeyId, {
        nodeId: seed.nodeId,
        relayKeyId: seed.relayKeyId,
        socket: seed.socket,
        queried: false,
        failed: null,
      });
      seenNodeIdHex.add(seed.nodeId.hex);
    }

    let foundValue = null;
    let completionReason = "budget"; // rounds exhausted = bounded-work exit

    for (let round = 0; round < this.#maxRounds; round += 1) {
      if (this.#now() >= deadlineAtMs) {
        completionReason = "deadline";
        break;
      }

      // Pick a batch of α closest candidates that are queryable now or
      // resolvable within budget. Unqueryable candidates get a typed failure
      // recorded ONCE and never consume a slot.
      const unqueried = [];
      for (const [, c] of candidates) {
        if (!c.queried && !c.failed) unqueried.push(c);
      }
      unqueried.sort((a, b) => targetId.compareDistanceTo(a.nodeId, b.nodeId));

      const batch = [];
      for (const candidate of unqueried) {
        if (batch.length >= this.#alpha) break;
        const hasSocket = candidate.socket && candidate.socket.destroyed !== true;
        if (hasSocket) {
          batch.push(candidate);
          continue;
        }
        if (this.#candidateResolver && dialsUsed < this.#maxNewDialsPerLookup) {
          dialsUsed += 1; // reserved now — a malicious peer cannot refund it
          batch.push(candidate);
          continue;
        }
        candidate.failed = this.#candidateResolver ? "dial-budget" : "no-resolver";
      }

      // Anti-starvation: unresolved candidates must not displace EVERY
      // queryable candidate. If the batch is all dials, reserve one slot for
      // the closest already-queryable candidate outside it.
      if (batch.length > 0 && !batch.some((c) => c.socket && c.socket.destroyed !== true)) {
        const queryableOutside = unqueried.find((c) => !batch.includes(c) && c.socket && c.socket.destroyed !== true);
        if (queryableOutside) {
          const displaced = batch.pop();
          displaced.failed = null; // stays eligible for a later round
          dialsUsed -= 1; // its reserved dial is refunded — it was not attempted
          batch.push(queryableOutside);
        }
      }

      if (batch.length === 0) {
        completionReason = counters.queried > 0
          ? (dialsUsed >= this.#maxNewDialsPerLookup && this.#candidateResolver ? "budget" : "converged")
          : "no-candidates";
        break;
      }

      // Resolve socket-less batch members (bounded, parallel, deadline-aware).
      const dialTargets = batch.filter((c) => !(c.socket && c.socket.destroyed !== true));
      if (dialTargets.length > 0) {
        const dialWork = Promise.all(dialTargets.map(async (candidate) => {
          const resolution = await this.#candidateResolver.resolve(candidate.relayKeyId);
          if (resolution.ok === true) {
            counters.dialAttempts += 1;
            candidate.socket = resolution.socket;
          } else if (resolution.reason === "budget-exhausted") {
            // Re-audit R4: node-global deferral, NOT an attempt. Refund this
            // lookup's reserved dial so other candidates can use it, keep the
            // reference (UNATTEMPTED_FAILURES), and count no dial attempt.
            dialsUsed -= 1;
            candidate.failed = "budget-exhausted";
          } else {
            counters.dialAttempts += 1;
            candidate.failed = resolution.reason;
            if (resolution.reason === "dial-timeout") counters.timeouts += 1;
          }
        }));
        const dialOutcome = await this.#raceDeadline(dialWork, deadlineAtMs);
        if (dialOutcome === DEADLINE_EXPIRED || this.#now() >= deadlineAtMs) {
          completionReason = "deadline";
          break;
        }
      }

      const queryable = batch.filter((c) => !c.failed && c.socket && c.socket.destroyed !== true);
      if (queryable.length === 0) {
        continue; // next round picks other candidates; failed ones never return
      }

      const promises = [];
      for (const candidate of queryable) {
        candidate.queried = true;
        counters.queried += 1;
        promises.push(
          sendQuery(candidate, targetId).catch(() => ({ value: null, nodes: [] })),
        );
      }
      const results = await this.#raceDeadline(Promise.all(promises), deadlineAtMs);
      if (results === DEADLINE_EXPIRED || this.#now() >= deadlineAtMs) {
        // A query that never settles (or settles after the budget) cannot
        // hold the lookup open: the deadline race abandons it and the
        // partial state gathered so far is returned honestly.
        completionReason = "deadline";
        break;
      }

      let addedCloser = false;
      for (const result of results) {
        if (!result || typeof result !== "object") continue;

        if (lookForValue && result.value && typeof result.value === "object") {
          foundValue = result.value;
          break;
        }

        const nodes = Array.isArray(result.nodes) ? result.nodes : [];
        for (const node of nodes) {
          if (!node || typeof node !== "object") continue;
          const relayKeyId = typeof node.relayKeyId === "string" ? node.relayKeyId : "";
          const nodeIdHex = typeof node.nodeIdHex === "string" ? node.nodeIdHex : "";
          if (!relayKeyId || !nodeIdHex || nodeIdHex.length !== 64) {
            counters.rejected += 1;
            continue;
          }
          // ADR-RELAY-IDENTITY: discovered references must carry a canonical
          // self-certifying relay id — free strings can never authenticate.
          if (!isCanonicalRelayKeyId(relayKeyId)) {
            counters.rejected += 1;
            continue;
          }
          // Dedup by BOTH coordinates: a reference re-sent in another order
          // or round consumes no additional work and resets no budget.
          if (candidates.has(relayKeyId) || seenNodeIdHex.has(nodeIdHex)) continue;

          // LOW-5 (docs/SECURITY_AUDIT.md): nodeIdHex MUST be the
          // deterministic hash of relayKeyId, otherwise a sybil could pair a
          // cheap id with an arbitrary position near the target.
          const derivedNodeId = DhtNodeId.fromRelayKeyId(relayKeyId);
          if (derivedNodeId.hex !== nodeIdHex) {
            counters.rejected += 1;
            continue;
          }

          let nodeId;
          try {
            nodeId = DhtNodeId.fromHex(nodeIdHex);
          } catch (err) {
            counters.rejected += 1;
            console.warn("[DHT] lookup: invalid nodeIdHex from peer:", err && err.message ? err.message : err);
            continue;
          }

          // Only admit references closer than our current k-th closest.
          const sorted = this.#sortedCandidates(candidates, targetId);
          if (sorted.length >= this.#k) {
            const kth = sorted[this.#k - 1];
            if (targetId.compareDistanceTo(nodeId, kth.nodeId) >= 0) continue;
          }

          candidates.set(relayKeyId, {
            nodeId,
            relayKeyId,
            socket: null, // discovered — resolved via the candidate resolver
            queried: false,
            failed: null,
          });
          seenNodeIdHex.add(nodeIdHex);
          addedCloser = true;
        }
      }

      if (foundValue) {
        completionReason = "value-found";
        break;
      }
      if (!addedCloser) {
        completionReason = "converged";
        break;
      }
    }

    return this.#finish(foundValue, candidates, targetId, counters, completionReason);
  }

  #finish(value, candidates, targetId, counters, completionReason) {
    // A candidate whose RESOLUTION failed (dead endpoint, identity mismatch,
    // no admitted descriptor) is evidence against the reference — drop it.
    // A candidate we never attempted (no resolver configured, dial budget
    // spent) remains a valid closest-node REFERENCE, exactly as before P3.
    const closestNodes = this.#sortedCandidates(candidates, targetId)
      .filter((c) => !c.failed || UNATTEMPTED_FAILURES.has(c.failed))
      .slice(0, this.#k);
    const report = new DhtLookupReportV1({
      valueFound: value !== null && value !== undefined,
      closestRelayKeyIds: closestNodes.map((c) => c.relayKeyId),
      queriedCount: counters.queried,
      dialAttemptCount: counters.dialAttempts,
      timeoutCount: counters.timeouts,
      rejectedCandidateCount: counters.rejected,
      completionReason: value ? "value-found" : completionReason,
    });
    return { value: value || null, closestNodes, report };
  }

  #sortedCandidates(candidates, targetId) {
    const list = [];
    for (const [, c] of candidates) {
      list.push(c);
    }
    list.sort((a, b) => targetId.compareDistanceTo(a.nodeId, b.nodeId));
    return list;
  }
}
