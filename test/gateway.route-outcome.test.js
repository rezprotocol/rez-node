/**
 * P5.3 — private, truthful route-outcome stream (ATLAS_PREREQUISITES).
 * Onion-send execution only; memory-bounded; expires; never exported.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  RouteOutcomeV1,
  RouteOutcomeStream,
  ROUTE_OUTCOME_CLASSES,
  durationBucketFor,
} from "../src/gateway/RouteOutcome.js";
import { GatewayLoop } from "../src/gateway/GatewayLoop.js";
import { GatewayRelaySelector } from "../src/gateway/GatewayRelaySelector.js";
import { GatewayPathPlanner } from "../src/gateway/GatewayPathPlanner.js";
import { GatewaySender } from "../src/gateway/GatewaySender.js";

function outcome(overrides = {}) {
  return new RouteOutcomeV1({
    packetId: "pkt-1",
    outcomeClass: "entry-send-accepted",
    relayKeyIds: ["rez:relay:" + "a".repeat(64)],
    advisorMode: "off",
    durationBucket: "lt100ms",
    reasonClass: null,
    atMs: 1_000,
    ...overrides,
  });
}

test("outcome classes and record fields are bounded; private identifiers are structurally impossible", () => {
  assert.deepEqual([...ROUTE_OUTCOME_CLASSES], [
    "entry-send-accepted", "route-failed", "send-timeout", "send-disconnected", "delivery-confirmed",
  ]);
  assert.throws(() => outcome({ outcomeClass: "delivered-for-sure" }), Error);
  assert.throws(() => outcome({ advisorMode: "required" }), Error);
  assert.throws(() => outcome({ durationBucket: "142ms-exactly" }), Error);
  // The record shape has no field for inbox/account/contact/payload — the
  // sealed RRecord drops nothing silently; unknown data simply has no home.
  const o = outcome();
  assert.deepEqual(Object.keys(o.toJSON()).sort(),
    ["advisorMode", "atMs", "durationBucket", "outcomeClass", "packetId", "reasonClass", "relayKeyIds"]);
});

test("durationBucketFor is coarse", () => {
  assert.equal(durationBucketFor(3), "lt100ms");
  assert.equal(durationBucketFor(500), "lt1s");
  assert.equal(durationBucketFor(5_000), "lt10s");
  assert.equal(durationBucketFor(60_000), "gte10s");
});

test("stream is bounded by count and age, supports unsubscribe, and isolates subscriber failures", () => {
  const clock = { now: 1_000 };
  const stream = new RouteOutcomeStream({ maxEvents: 3, maxAgeMs: 15 * 60_000, nowMs: () => clock.now });
  const seen = [];
  const unsubscribe = stream.subscribe((o) => seen.push(o.packetId));
  stream.subscribe(() => { throw new Error("broken subscriber"); });

  for (let i = 0; i < 5; i += 1) {
    stream.emit(outcome({ packetId: "pkt-" + i, atMs: clock.now }));
  }
  assert.deepEqual(seen, ["pkt-0", "pkt-1", "pkt-2", "pkt-3", "pkt-4"], "broken subscriber did not break delivery");
  assert.equal(stream.getRecent().length, 3, "count cap holds");
  assert.deepEqual(stream.getRecent().map((o) => o.packetId), ["pkt-2", "pkt-3", "pkt-4"]);

  clock.now += 15 * 60_000 + 1;
  assert.equal(stream.getRecent().length, 0, "age cap expires events");

  unsubscribe();
  stream.emit(outcome({ packetId: "pkt-after", atMs: clock.now }));
  assert.equal(seen.includes("pkt-after"), false, "unsubscribed");
});

test("GatewayLoop emits route-failed outcomes and stays silent for non-onion paths", async () => {
  const stream = new RouteOutcomeStream();
  const loop = new GatewayLoop({
    relaySelector: new GatewayRelaySelector({ rng: () => 0 }),
    pathPlanner: new GatewayPathPlanner(),
    sender: new GatewaySender({ pool: { sendByRelayId: async () => {} } }),
    crypto: { name: "stub" },
    inboxStore: { depositFromWire: async () => {} },
    isHostedHere: async () => true,
    routeOutcomes: stream,
  });

  // Shared-home deposit path (bypasses the selector): NO outcome emitted.
  await loop.sendToInbox({ innerBytes: new Uint8Array([1]), deliverInboxId: "inbox:home:1" });
  assert.equal(stream.getRecent().length, 0, "non-onion paths are out of scope");

  // Authenticated route failure correlated to a packet: emitted.
  loop.recordRouteFailure("pkt-x", "rez:relay:" + "b".repeat(64), "hop-unreachable");
  const recent = stream.getRecent();
  assert.equal(recent.length, 1);
  assert.equal(recent[0].outcomeClass, "route-failed");
  assert.equal(recent[0].packetId, "pkt-x");
  assert.equal(recent[0].reasonClass, "hop-unreachable");
});
