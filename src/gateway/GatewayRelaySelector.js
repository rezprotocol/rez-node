import { randomInt } from "node:crypto";
import { descriptorHasUsableOnionKey, isNonEmptyString } from "@rezprotocol/core";
import { ROUTE_ADVISOR_MODES, RouteAdvisorCandidateV1, applyRouteAdvice, DEFAULT_ADVISOR_DEADLINE_MS } from "./RouteAdvisor.js";

export class NotEnoughRelaysError extends Error {
  constructor(message = "Not enough relays") {
    super(message);
    this.name = "NotEnoughRelaysError";
  }
}

function hasTcpEndpoint(descriptor) {
  return Array.isArray(descriptor.endpoints)
    && descriptor.endpoints.some((ep) => ep && isNonEmptyString(ep.host) && Number.isInteger(ep.port));
}

export class GatewayRelaySelector {
  /**
   * @param {{ rng?: (max: number) => number, advisor?: import("./RouteAdvisor.js").RouteAdvisor|null, advisorMode?: string, advisorDeadlineMs?: number, onShadowComparison?: ((cmp: { advisedOrder: string[], baselineSelection: string[] }) => void)|null }} [opts]
   */
  constructor({ rng, advisor = null, advisorMode = ROUTE_ADVISOR_MODES.OFF, advisorDeadlineMs = DEFAULT_ADVISOR_DEADLINE_MS, onShadowComparison = null } = {}) {
    this.rng = rng || ((max) => randomInt(max));
    // P5 (ATLAS_PREREQUISITES): optional local advisor over the ELIGIBLE set
    // only. There is no `required`/`enforced` mode; any invalid advice falls
    // back completely to the baseline random selection below.
    const modes = Object.values(ROUTE_ADVISOR_MODES);
    if (!modes.includes(advisorMode)) {
      throw new Error("GatewayRelaySelector advisorMode must be one of " + modes.join("|"));
    }
    this.advisor = advisor;
    this.advisorMode = advisor ? advisorMode : ROUTE_ADVISOR_MODES.OFF;
    this.advisorDeadlineMs = advisorDeadlineMs;
    this.onShadowComparison = typeof onShadowComparison === "function" ? onShadowComparison : null;
  }

  /**
   * Baseline synchronous selection — eligibility filter + uniform random
   * choice. Behavior is pinned by the P0.1 no-extension baseline; the advisor
   * NEVER participates here.
   */
  select({ descriptors, minHops = 1, maxHops = 3, excludeRelayKeyIds = [], requireTcpEndpoint = true, nowMs } = {}) {
    const { eligible, actualHops } = this.#eligibleAndHops({ descriptors, minHops, maxHops, excludeRelayKeyIds, requireTcpEndpoint, nowMs });
    if (actualHops === 0) return [];
    return this.#randomPick(eligible, actualHops);
  }

  /**
   * P5.2: selection with the optional advisor consulted AFTER eligibility and
   * BEFORE the random choice. With no advisor (or mode `off`) this is
   * behaviorally identical to select(). `shadow` validates and records the
   * advice but executes baseline; `advisory` executes valid advice and falls
   * back to baseline on ANY invalid/missing/late advice.
   */
  async selectRanked(args = {}) {
    if (!this.advisor || this.advisorMode === ROUTE_ADVISOR_MODES.OFF) {
      return this.select(args);
    }
    const { eligible, actualHops } = this.#eligibleAndHops(args);
    if (actualHops === 0) return [];

    const nowMs = Number.isFinite(Number(args.nowMs)) ? Number(args.nowMs) : Date.now();
    const candidates = eligible.map((desc) => new RouteAdvisorCandidateV1({
      relayKeyId: desc.relayKeyId,
      endpointCount: Array.isArray(desc.endpoints) ? desc.endpoints.length : 0,
      onionKeyCount: Array.isArray(desc.onionKeys) ? desc.onionKeys.length : 0,
      expiresAtMs: Number.isFinite(Number(desc.expiresAt)) ? Number(desc.expiresAt) : null,
    }));
    const advice = await applyRouteAdvice(this.advisor, candidates, { deadlineMs: this.advisorDeadlineMs });

    if (this.advisorMode === ROUTE_ADVISOR_MODES.SHADOW) {
      const baseline = this.#randomPick(eligible, actualHops);
      if (this.onShadowComparison) {
        this.onShadowComparison({
          advisedOrder: advice.ok === true ? advice.orderedRelayKeyIds.slice() : null,
          adviceFailure: advice.ok === true ? null : advice.reason,
          baselineSelection: baseline.map((d) => d.relayKeyId),
        });
      }
      return baseline;
    }

    // advisory
    if (advice.ok !== true) {
      return this.#randomPick(eligible, actualHops);
    }
    const byId = new Map();
    for (const desc of eligible) {
      if (!byId.has(desc.relayKeyId)) byId.set(desc.relayKeyId, desc);
    }
    const ordered = [];
    for (const relayKeyId of advice.orderedRelayKeyIds) {
      const desc = byId.get(relayKeyId);
      if (desc) ordered.push(desc);
      if (ordered.length >= actualHops) break;
    }
    // The permutation was validated against the eligible set, so this cannot
    // come up short — but the node, not the advisor, is the execution
    // authority: fall back rather than trust a broken invariant.
    if (ordered.length < actualHops) {
      return this.#randomPick(eligible, actualHops);
    }
    return ordered;
  }

  #eligibleAndHops({ descriptors, minHops = 1, maxHops = 3, excludeRelayKeyIds = [], requireTcpEndpoint = true, nowMs } = {}) {
    if (!Array.isArray(descriptors)) {
      throw new Error("GatewayRelaySelector.select requires descriptors[]");
    }
    const exclude = new Set(excludeRelayKeyIds || []);
    const now = Number.isFinite(Number(nowMs)) ? Number(nowMs) : Date.now();
    const eligible = descriptors.filter((desc) => {
      if (!desc || !isNonEmptyString(desc.relayKeyId)) return false;
      if (exclude.has(desc.relayKeyId)) return false;
      if (requireTcpEndpoint && !hasTcpEndpoint(desc)) return false;
      if (!Array.isArray(desc.onionKeys) || desc.onionKeys.length === 0) return false;
      if (!descriptorHasUsableOnionKey(desc, now)) return false;
      return true;
    });

    const hops = Math.max(minHops, Math.min(maxHops, 3));
    if (hops === 0) return { eligible, actualHops: 0 };
    if (eligible.length === 0) {
      throw new NotEnoughRelaysError("No eligible relays available");
    }
    // Select up to `hops` relays, using whatever is available
    return { eligible, actualHops: Math.min(hops, eligible.length) };
  }

  #randomPick(eligible, actualHops) {
    const selected = [];
    const pool = [...eligible];
    for (let i = 0; i < actualHops; i += 1) {
      const idx = this.rng(pool.length);
      selected.push(pool.splice(idx, 1)[0]);
    }
    return selected;
  }
}
