/**
 * P5.1/P5.2 — optional local route advisor (ATLAS_PREREQUISITES).
 * The advisor can reorder eligible candidates but can never introduce an
 * ineligible relay, bypass node validation, fail routing, or delay it beyond
 * the strict deadline. Absence of an advisor is the exact baseline.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";
import {
  RouteAdvisor,
  RouteAdvisorCandidateV1,
  applyRouteAdvice,
  ROUTE_ADVISOR_MODES,
} from "../src/gateway/RouteAdvisor.js";
import { GatewayRelaySelector } from "../src/gateway/GatewayRelaySelector.js";
import { ReputationScorer } from "../src/settlement/ReputationScorer.js";

function makeDescriptor(relayKeyId, nowMs) {
  return new RelayDescriptorV1({
    relayKeyId,
    endpoints: [{ host: "127.0.0.1", port: 4800 }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: relayKeyId + "-onion",
        publicKeyBytes: new Uint8Array(32).fill(9),
        format: "raw",
        createdAt: nowMs - 1000,
        notBefore: nowMs - 1000,
        notAfter: nowMs + 60_000,
        status: "active",
      }),
    ],
    expiresAt: nowMs + 60_000,
    nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  });
}

class FixedOrderAdvisor extends RouteAdvisor {
  constructor(order) { super(); this.order = order; }
  async adviseOrder() { return this.order.slice(); }
}

const NOW = Date.now();
const DESCRIPTORS = ["r-a", "r-b", "r-c", "r-d"].map((id) => makeDescriptor(id, NOW));

test("applyRouteAdvice accepts only a complete valid permutation", async () => {
  const candidates = DESCRIPTORS.map((d) => new RouteAdvisorCandidateV1({ relayKeyId: d.relayKeyId }));
  const good = await applyRouteAdvice(new FixedOrderAdvisor(["r-d", "r-c", "r-b", "r-a"]), candidates);
  assert.equal(good.ok, true);
  assert.deepEqual(good.orderedRelayKeyIds, ["r-d", "r-c", "r-b", "r-a"]);

  const cases = [
    [["r-d", "r-c", "r-b"], "omitted-candidate"],
    [["r-d", "r-c", "r-b", "r-a", "r-x"], "extra-candidate"],
    [["r-d", "r-d", "r-b", "r-a"], "duplicate-candidate"],
    [["r-d", "r-c", "r-b", "r-INELIGIBLE"], "unknown-candidate"],
    ["not-an-array", "malformed-advice"],
  ];
  for (const [order, reason] of cases) {
    const verdict = await applyRouteAdvice(new FixedOrderAdvisor(order), candidates);
    assert.equal(verdict.ok, false, reason);
    assert.equal(verdict.reason, reason);
  }
});

test("advisor timeout and exception yield full fallback within the strict deadline", async () => {
  const candidates = [new RouteAdvisorCandidateV1({ relayKeyId: "r-a" })];
  class HangingAdvisor extends RouteAdvisor {
    async adviseOrder() { return new Promise(() => {}); }
  }
  const started = Date.now();
  const hung = await applyRouteAdvice(new HangingAdvisor(), candidates, { deadlineMs: 50 });
  assert.equal(hung.ok, false);
  assert.equal(hung.reason, "advisor-timeout");
  assert.ok(Date.now() - started < 500, "timeout is bounded");

  class ThrowingAdvisor extends RouteAdvisor {
    async adviseOrder() { throw new Error("advisor bug"); }
  }
  const threw = await applyRouteAdvice(new ThrowingAdvisor(), candidates);
  assert.deepEqual(threw, { ok: false, reason: "advisor-exception" });
});

test("advisory mode executes valid advice; the advisor cannot introduce an ineligible relay", async () => {
  const selector = new GatewayRelaySelector({
    rng: () => 0,
    advisor: new FixedOrderAdvisor(["r-c", "r-a", "r-d", "r-b"]),
    advisorMode: ROUTE_ADVISOR_MODES.ADVISORY,
  });
  const picked = await selector.selectRanked({ descriptors: DESCRIPTORS, minHops: 2, maxHops: 2, nowMs: NOW });
  assert.deepEqual(picked.map((d) => d.relayKeyId), ["r-c", "r-a"], "advised order executed");

  // Excluded (ineligible) relay: the advisor never even sees it, and a stale
  // advisor naming it produces full fallback — never an ineligible pick.
  const seen = [];
  class SpyAdvisor extends RouteAdvisor {
    async adviseOrder(candidates) {
      seen.push(candidates.map((c) => c.relayKeyId));
      return ["r-c", "r-a", "r-d", "r-b"]; // includes excluded r-b → invalid
    }
  }
  const selector2 = new GatewayRelaySelector({ rng: () => 0, advisor: new SpyAdvisor(), advisorMode: "advisory" });
  const picked2 = await selector2.selectRanked({
    descriptors: DESCRIPTORS, minHops: 2, maxHops: 2, excludeRelayKeyIds: ["r-b"], nowMs: NOW,
  });
  assert.deepEqual(seen[0], ["r-a", "r-c", "r-d"], "advisor input is the post-eligibility set only");
  assert.ok(!picked2.some((d) => d.relayKeyId === "r-b"), "ineligible relay can never be selected");
  assert.deepEqual(picked2.map((d) => d.relayKeyId), ["r-a", "r-c"], "fallback = baseline random (rng 0)");
});

test("shadow mode records the comparison but executes baseline", async () => {
  const comparisons = [];
  const selector = new GatewayRelaySelector({
    rng: () => 0,
    advisor: new FixedOrderAdvisor(["r-d", "r-c", "r-b", "r-a"]),
    advisorMode: ROUTE_ADVISOR_MODES.SHADOW,
    onShadowComparison: (cmp) => comparisons.push(cmp),
  });
  const picked = await selector.selectRanked({ descriptors: DESCRIPTORS, minHops: 2, maxHops: 2, nowMs: NOW });
  assert.deepEqual(picked.map((d) => d.relayKeyId), ["r-a", "r-b"], "baseline executed, not the advice");
  assert.equal(comparisons.length, 1);
  assert.deepEqual(comparisons[0].advisedOrder, ["r-d", "r-c", "r-b", "r-a"]);
  assert.deepEqual(comparisons[0].baselineSelection, ["r-a", "r-b"]);
});

test("there is no required/enforced mode", () => {
  assert.deepEqual(Object.values(ROUTE_ADVISOR_MODES).sort(), ["advisory", "off", "shadow"]);
  assert.throws(() => new GatewayRelaySelector({ advisor: new FixedOrderAdvisor([]), advisorMode: "required" }), Error);
});

test("the existing ReputationScorer works as a shadow advisor double without gaining routing authority", async () => {
  // Test-double proof only (P5.2): the seam can consume an existing local
  // scoring component; Atlas does not depend on it.
  const scorer = new ReputationScorer({
    attestationService: { getAttestationsFor: () => [] },
    relayStore: { getDescriptor: () => null },
  });
  class ScorerAdvisor extends RouteAdvisor {
    constructor(reputation) { super(); this.reputation = reputation; }
    async adviseOrder(candidates) {
      const scored = candidates.map((c) => {
        const summary = this.reputation.score(c.relayKeyId);
        return { id: c.relayKeyId, score: summary && Number.isFinite(summary.score) ? summary.score : 0 };
      });
      scored.sort((a, b) => b.score - a.score || (a.id < b.id ? -1 : 1));
      return scored.map((s) => s.id);
    }
  }
  const comparisons = [];
  const selector = new GatewayRelaySelector({
    rng: () => 0,
    advisor: new ScorerAdvisor(scorer),
    advisorMode: "shadow",
    onShadowComparison: (cmp) => comparisons.push(cmp),
  });
  const picked = await selector.selectRanked({ descriptors: DESCRIPTORS, minHops: 3, maxHops: 3, nowMs: NOW });
  assert.equal(picked.length, 3, "baseline routing is unaffected");
  assert.equal(comparisons.length, 1, "the scorer's ranking was observed in shadow");
  assert.ok(Array.isArray(comparisons[0].advisedOrder) || comparisons[0].adviceFailure,
    "shadow either recorded advice or a typed failure — never silence");
});

test("re-audit R5: an advisor rejecting with a NON-Error value (string/null/object) falls back, never escapes", async () => {
  const candidates = ["r-a", "r-b", "r-c"].map((id) => new RouteAdvisorCandidateV1({ relayKeyId: id }));
  const throwers = [
    { label: "sync string", adviseOrder() { throw "advisor exploded"; } }, // eslint-disable-line no-throw-literal
    { label: "async null", adviseOrder() { return Promise.reject(null); } },
    { label: "async object", adviseOrder() { return Promise.reject({ code: "E_ADVICE" }); } },
  ];
  for (const advisor of throwers) {
    const result = await applyRouteAdvice(advisor, candidates, { deadlineMs: 50 });
    assert.equal(result.ok, false, advisor.label);
    assert.equal(result.reason, "advisor-exception", advisor.label);
  }
});
