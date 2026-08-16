/**
 * Private, truthful route-outcome stream (ATLAS_PREREQUISITES P5.3).
 *
 * Scope: ONION-SEND EXECUTION ONLY. GatewayLoop's shared-home deposit,
 * direct-route-cache, and routeDelivery fallback paths bypass the selector
 * and are deliberately NOT observed — this is not a general routing-outcome
 * feed, and widening it is a separately approved decision.
 *
 * Privacy: an outcome may carry process-local packet correlation, the PUBLIC
 * relay IDs of the executed path, the advisor mode, a coarse duration bucket,
 * a coarse reason class, and a timestamp. It must NEVER carry the destination
 * inbox, sender account, contact, payload, the non-executed candidate set, or
 * a discovery trace. Outcomes are memory-bounded, expire, and are never
 * persisted, gossiped, put in durable records, exported as metrics labels, or
 * sent to an SDK client by this prerequisite work.
 */
import { RRecord } from "@rezprotocol/core";

export const ROUTE_OUTCOME_CLASSES = Object.freeze([
  // The entry socket accepted the write. This is NOT delivery proof.
  "entry-send-accepted",
  // An authenticated route failure was correlated to the packet.
  "route-failed",
  "send-timeout",
  "send-disconnected",
  // Only when an existing authenticated end-to-end receipt actually proves it.
  "delivery-confirmed",
]);
const OUTCOME_CLASS_SET = new Set(ROUTE_OUTCOME_CLASSES);

export const ROUTE_OUTCOME_DURATION_BUCKETS = Object.freeze(["lt100ms", "lt1s", "lt10s", "gte10s"]);
const DURATION_BUCKET_SET = new Set(ROUTE_OUTCOME_DURATION_BUCKETS);

export function durationBucketFor(durationMs) {
  const ms = Number(durationMs);
  if (!Number.isFinite(ms) || ms < 0) return "lt100ms";
  if (ms < 100) return "lt100ms";
  if (ms < 1000) return "lt1s";
  if (ms < 10_000) return "lt10s";
  return "gte10s";
}

export class RouteOutcomeV1 extends RRecord {
  static type = "RouteOutcomeV1";

  constructor({
    packetId = null,
    outcomeClass,
    relayKeyIds = [],
    advisorMode = "off",
    durationBucket = "lt100ms",
    reasonClass = null,
    atMs,
  } = {}) {
    super();
    this.packetId = packetId == null ? null : String(packetId);
    this.outcomeClass = outcomeClass;
    this.relayKeyIds = Array.isArray(relayKeyIds) ? relayKeyIds.slice() : relayKeyIds;
    this.advisorMode = advisorMode;
    this.durationBucket = durationBucket;
    this.reasonClass = reasonClass == null ? null : String(reasonClass);
    this.atMs = Number(atMs);
    if (this.constructor === RouteOutcomeV1) this._seal();
  }

  validate() {
    this.assert(OUTCOME_CLASS_SET.has(this.outcomeClass), "RouteOutcomeV1.outcomeClass invalid", { outcomeClass: this.outcomeClass });
    this.assert(Array.isArray(this.relayKeyIds), "RouteOutcomeV1.relayKeyIds must be an array");
    for (const id of this.relayKeyIds) {
      this.assert(typeof id === "string" && id.trim().length > 0, "RouteOutcomeV1.relayKeyIds entries must be strings");
    }
    this.assert(["off", "shadow", "advisory"].includes(this.advisorMode), "RouteOutcomeV1.advisorMode invalid", { advisorMode: this.advisorMode });
    this.assert(DURATION_BUCKET_SET.has(this.durationBucket), "RouteOutcomeV1.durationBucket invalid", { durationBucket: this.durationBucket });
    if (this.reasonClass !== null) {
      this.assert(this.reasonClass.length <= 64, "RouteOutcomeV1.reasonClass too long");
    }
    this.assert(Number.isFinite(this.atMs), "RouteOutcomeV1.atMs must be finite");
  }
}

/**
 * Bounded in-process publish/subscribe for route outcomes. Default caps:
 * 1000 events or 15 minutes, whichever removes an event first.
 */
export class RouteOutcomeStream {
  #subscribers;
  #buffer;
  #maxEvents;
  #maxAgeMs;
  #nowMs;

  constructor({ maxEvents = 1000, maxAgeMs = 15 * 60_000, nowMs = () => Date.now() } = {}) {
    this.#subscribers = new Set();
    this.#buffer = [];
    this.#maxEvents = maxEvents;
    this.#maxAgeMs = maxAgeMs;
    this.#nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
  }

  /**
   * @param {(outcome: RouteOutcomeV1) => void} fn
   * @returns {() => void} unsubscribe
   */
  subscribe(fn) {
    if (typeof fn !== "function") {
      throw new Error("RouteOutcomeStream.subscribe requires a function");
    }
    this.#subscribers.add(fn);
    return () => { this.#subscribers.delete(fn); };
  }

  emit(outcome) {
    if (!(outcome instanceof RouteOutcomeV1)) {
      throw new Error("RouteOutcomeStream.emit requires a RouteOutcomeV1");
    }
    this.#evict();
    this.#buffer.push(outcome);
    if (this.#buffer.length > this.#maxEvents) {
      this.#buffer.splice(0, this.#buffer.length - this.#maxEvents);
    }
    for (const fn of this.#subscribers) {
      try {
        fn(outcome);
      } catch (err) {
        // A broken subscriber must not break routing or other subscribers,
        // but it is a real defect — surface it.
        console.warn("[RouteOutcomeStream] subscriber threw: " + (err && err.message ? err.message : err));
      }
    }
  }

  /** Recent, unexpired outcomes (oldest first). */
  getRecent() {
    this.#evict();
    return this.#buffer.slice();
  }

  get subscriberCount() {
    return this.#subscribers.size;
  }

  #evict() {
    const cutoff = this.#nowMs() - this.#maxAgeMs;
    let drop = 0;
    while (drop < this.#buffer.length && this.#buffer[drop].atMs <= cutoff) drop += 1;
    if (drop > 0) this.#buffer.splice(0, drop);
  }
}
