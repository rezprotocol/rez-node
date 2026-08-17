/**
 * Optional local route-advice seam (ATLAS_PREREQUISITES P5.1).
 *
 * An advisor receives ALREADY-ADMITTED, ALREADY-ELIGIBLE public relay
 * candidates and returns an ordering of their relay IDs. It has no network,
 * descriptor-store, route-table, inbox, crypto, or sender reference, and no
 * authority to add or remove candidates — the node validates the returned
 * permutation and falls back completely to the baseline selector on ANY
 * malformed advice. Partial trust of malformed advice is forbidden.
 *
 * Candidate input carries only public/local coarse fields. It must never
 * contain: inbox ID, account ID, contact ID, handle, payload bytes or size
 * class, plaintext query text, the full route-discovery trace, private key
 * material.
 */
import { RRecord } from "@rezprotocol/core";
import { raceWithDeadline } from "../util/raceWithDeadline.js";

export const ROUTE_ADVISOR_MODES = Object.freeze({
  OFF: "off",
  SHADOW: "shadow",
  ADVISORY: "advisory",
});

export const DEFAULT_ADVISOR_DEADLINE_MS = 50;

/** Public, coarse view of one eligible relay candidate. */
export class RouteAdvisorCandidateV1 extends RRecord {
  static type = "RouteAdvisorCandidateV1";

  constructor({ relayKeyId, endpointCount = 0, onionKeyCount = 0, expiresAtMs = null } = {}) {
    super();
    this.relayKeyId = typeof relayKeyId === "string" ? relayKeyId.trim() : "";
    this.endpointCount = Number(endpointCount);
    this.onionKeyCount = Number(onionKeyCount);
    this.expiresAtMs = expiresAtMs == null ? null : Number(expiresAtMs);
    if (this.constructor === RouteAdvisorCandidateV1) this._seal();
  }

  validate() {
    this.assert(this.relayKeyId.length > 0, "RouteAdvisorCandidateV1.relayKeyId required");
    this.assert(Number.isInteger(this.endpointCount) && this.endpointCount >= 0, "endpointCount invalid");
    this.assert(Number.isInteger(this.onionKeyCount) && this.onionKeyCount >= 0, "onionKeyCount invalid");
    if (this.expiresAtMs !== null) {
      this.assert(Number.isFinite(this.expiresAtMs), "expiresAtMs invalid");
    }
  }
}

/** The advisor's output: an ordering of the candidate relay IDs. */
export class RouteAdviceV1 extends RRecord {
  static type = "RouteAdviceV1";

  constructor({ orderedRelayKeyIds } = {}) {
    super();
    this.orderedRelayKeyIds = Array.isArray(orderedRelayKeyIds) ? orderedRelayKeyIds.slice() : orderedRelayKeyIds;
    if (this.constructor === RouteAdviceV1) this._seal();
  }

  validate() {
    this.assert(Array.isArray(this.orderedRelayKeyIds), "RouteAdviceV1.orderedRelayKeyIds must be an array");
    for (const id of this.orderedRelayKeyIds) {
      this.assert(typeof id === "string" && id.trim().length > 0, "RouteAdviceV1 entries must be non-empty strings");
    }
  }
}

/**
 * The one-method advisor interface. Implementations override adviseOrder.
 * Deterministic local components only — this is not a plugin framework.
 */
export class RouteAdvisor {
  /**
   * @param {RouteAdvisorCandidateV1[]} candidates - admitted, eligible relays
   * @returns {Promise<string[]>} ordered relay IDs (a permutation of the input set)
   */
  async adviseOrder(candidates) { // eslint-disable-line no-unused-vars
    throw new Error("RouteAdvisor.adviseOrder must be implemented");
  }
}

/**
 * Run an advisor over the eligible set with a strict deadline and validate
 * its advice. Returns `{ ok: true, orderedRelayKeyIds }` only for a valid
 * complete permutation delivered in time; every other case — timeout,
 * exception, duplicate ID, unknown ID, omitted eligible ID, malformed
 * response — returns `{ ok: false, reason }` and the caller MUST use the
 * baseline selector unchanged.
 *
 * @param {RouteAdvisor} advisor
 * @param {RouteAdvisorCandidateV1[]} candidates
 * @param {{ deadlineMs?: number }} [opts]
 */
export async function applyRouteAdvice(advisor, candidates, { deadlineMs = DEFAULT_ADVISOR_DEADLINE_MS } = {}) {
  if (!advisor || typeof advisor.adviseOrder !== "function") {
    return { ok: false, reason: "no-advisor" };
  }
  let advised;
  try {
    // adviseOrder() is called inside the try so a SYNCHRONOUS throw from a
    // hostile or broken advisor lands in the same isolation path as a
    // rejection, rather than escaping into routing selection.
    const advice = Promise.resolve(advisor.adviseOrder(candidates));
    advised = await raceWithDeadline(advice, deadlineMs, TIMEOUT_SENTINEL);
  } catch (err) {
    // Re-audit R5: isolation must be TOTAL. JavaScript permits rejecting
    // with strings, plain objects, or null — an instanceof filter here let
    // those escape into routing selection. Whatever the advisor threw, the
    // answer is the same: fall back to the baseline.
    return { ok: false, reason: "advisor-exception" };
  }
  if (advised === TIMEOUT_SENTINEL) {
    return { ok: false, reason: "advisor-timeout" };
  }
  if (!Array.isArray(advised)) {
    return { ok: false, reason: "malformed-advice" };
  }
  const eligible = new Set();
  for (const candidate of candidates) {
    eligible.add(candidate.relayKeyId);
  }
  if (advised.length !== eligible.size) {
    return { ok: false, reason: advised.length < eligible.size ? "omitted-candidate" : "extra-candidate" };
  }
  const seen = new Set();
  for (const id of advised) {
    if (typeof id !== "string") return { ok: false, reason: "malformed-advice" };
    if (seen.has(id)) return { ok: false, reason: "duplicate-candidate" };
    if (!eligible.has(id)) return { ok: false, reason: "unknown-candidate" };
    seen.add(id);
  }
  return { ok: true, orderedRelayKeyIds: advised.slice() };
}

const TIMEOUT_SENTINEL = Symbol("route-advisor-timeout");
