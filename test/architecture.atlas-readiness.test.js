/**
 * P0.1 — No-extension behavioral baseline (ATLAS_PREREQUISITES.md).
 *
 * These tests pin CURRENT behavior — including the ugly parts — so later
 * prerequisite tickets (P1–P7) can prove exactly what they changed. Do not
 * weaken an assertion here to make a later ticket pass; an intentional
 * behavior change must add a new explicit assertion beside the old one and
 * preserve the no-extension path.
 *
 * DurableRecordV2 publish / retrieve / persistence-reload / publisher-offline
 * baselines live in routing.durable-record-mesh.integration.test.js (the
 * canonical mesh harness) and are deliberately not duplicated here.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";
import { GatewayRelaySelector, NotEnoughRelaysError } from "../src/gateway/GatewayRelaySelector.js";
import { GatewayPathPlanner } from "../src/gateway/GatewayPathPlanner.js";
import { GatewaySender } from "../src/gateway/GatewaySender.js";
import { GatewayLoop, RoutingFailedError } from "../src/gateway/GatewayLoop.js";
import { MeshCoordinator } from "../src/gateway/MeshCoordinator.js";
import { RelayStore } from "../src/network/RelayStore.js";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
import { makeSignedRecord } from "./support/durableRecord.js";

const SRC_DIR = fileURLToPath(new URL("../src", import.meta.url));

function listSourceFiles(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) {
      out.push(...listSourceFiles(full));
    } else if (name.endsWith(".js")) {
      out.push(full);
    }
  }
  return out;
}

function makeDescriptor({ relayKeyId, nowMs, expiresAt, onionNotAfter, skipConstructorExpiryCheck = false }) {
  const args = {
    relayKeyId,
    endpoints: [{ host: "127.0.0.1", port: 4321 }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: `${relayKeyId}-onion`,
        publicKeyBytes: new Uint8Array(32).fill(7),
        format: "raw",
        createdAt: nowMs - 1000,
        notBefore: nowMs - 1000,
        notAfter: onionNotAfter != null ? onionNotAfter : nowMs + 60_000,
        status: "active",
      }),
    ],
    expiresAt,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  };
  // The class only enforces expiry when a finite nowMs is supplied at
  // construction; omitting it is how an expired instance can exist at all.
  if (!skipConstructorExpiryCheck) args.nowMs = nowMs;
  return new RelayDescriptorV1(args);
}

// ---------------------------------------------------------------------------
// Selector baseline
// ---------------------------------------------------------------------------

test("baseline: with no advisor seam, selection is uniform random over the eligible set via the injected rng", () => {
  const nowMs = Date.now();
  const descriptors = ["r-a", "r-b", "r-c", "r-d"].map((id) =>
    makeDescriptor({ relayKeyId: id, nowMs, expiresAt: nowMs + 60_000 }));
  const picks = [];
  const selector = new GatewayRelaySelector({ rng: (max) => { picks.push(max); return 0; } });
  const selected = selector.select({ descriptors, minHops: 3, maxHops: 3, nowMs });
  // rng(max) with 0 always picks the head, splicing without replacement.
  assert.deepEqual(selected.map((d) => d.relayKeyId), ["r-a", "r-b", "r-c"]);
  assert.deepEqual(picks, [4, 3, 2]);
  // Intentional P5 extension: selectRanked is the advisor seam. select()
  // remains the pinned no-extension baseline and the advisor NEVER
  // participates in it.
  assert.deepEqual(Object.getOwnPropertyNames(GatewayRelaySelector.prototype).sort(),
    ["constructor", "select", "selectRanked"]);
});

test("baseline: selectRanked with no advisor is behaviorally identical to select", async () => {
  const nowMs = Date.now();
  const descriptors = ["r-a", "r-b", "r-c", "r-d"].map((id) =>
    makeDescriptor({ relayKeyId: id, nowMs, expiresAt: nowMs + 60_000 }));
  const mkRng = () => { let i = 0; const seq = [2, 0, 1]; return () => seq[(i += 1) - 1] || 0; };
  const a = new GatewayRelaySelector({ rng: mkRng() })
    .select({ descriptors, minHops: 3, maxHops: 3, nowMs });
  const b = await new GatewayRelaySelector({ rng: mkRng() })
    .selectRanked({ descriptors, minHops: 3, maxHops: 3, nowMs });
  assert.deepEqual(b.map((d) => d.relayKeyId), a.map((d) => d.relayKeyId));
});

test("baseline (ugly, pinned): minHops overrides maxHops when larger and can exceed the 3-hop cap", () => {
  const nowMs = Date.now();
  const descriptors = ["r-a", "r-b", "r-c", "r-d"].map((id) =>
    makeDescriptor({ relayKeyId: id, nowMs, expiresAt: nowMs + 60_000 }));
  const selector = new GatewayRelaySelector({ rng: () => 0 });
  // hops = max(5, min(2, 3)) = 5 → actualHops = min(5, 4 eligible) = 4.
  const selected = selector.select({ descriptors, minHops: 5, maxHops: 2, nowMs });
  assert.equal(selected.length, 4);
});

test("baseline (ugly, pinned): hop count degrades to eligible-set size instead of failing", () => {
  const nowMs = Date.now();
  const descriptors = [makeDescriptor({ relayKeyId: "only", nowMs, expiresAt: nowMs + 60_000 })];
  const selector = new GatewayRelaySelector({ rng: () => 0 });
  const selected = selector.select({ descriptors, minHops: 3, maxHops: 3, nowMs });
  assert.equal(selected.length, 1);
});

test("baseline: zero eligible relays throws NotEnoughRelaysError", () => {
  const selector = new GatewayRelaySelector({ rng: () => 0 });
  assert.throws(
    () => selector.select({ descriptors: [], minHops: 1, maxHops: 3, nowMs: Date.now() }),
    NotEnoughRelaysError,
  );
});

test("baseline (ugly, pinned): the selector does NOT check descriptor expiry — that is enforced upstream", () => {
  const nowMs = Date.now();
  // Descriptor itself is expired, but its onion key window is still open.
  const expired = makeDescriptor({
    relayKeyId: "expired-desc",
    nowMs,
    expiresAt: nowMs - 5_000,
    onionNotAfter: nowMs + 60_000,
    skipConstructorExpiryCheck: true,
  });
  const selector = new GatewayRelaySelector({ rng: () => 0 });
  const selected = selector.select({ descriptors: [expired], minHops: 1, maxHops: 1, nowMs });
  assert.equal(selected.length, 1);
  assert.equal(selected[0].relayKeyId, "expired-desc");
  // The upstream filter that actually drops it:
  const store = new RelayStore();
  store.upsertDescriptor(expired.toJSON(), { source: "config", receivedAtMs: nowMs });
  assert.equal(store.listDescriptors({ nowMs }).length, 0);
});

// ---------------------------------------------------------------------------
// GatewayLoop failure-path baseline
// ---------------------------------------------------------------------------

test("baseline (ugly, pinned): NotEnoughRelaysError is not a routing failure — it bypasses the outbound retry queue", async () => {
  const nowMs = Date.now();
  assert.equal(new NotEnoughRelaysError() instanceof RoutingFailedError, false);

  const relayStore = new RelayStore();
  const delivery = makeDescriptor({ relayKeyId: "relay-delivery", nowMs, expiresAt: nowMs + 60_000 });
  relayStore.upsertDescriptor(delivery.toJSON(), { source: "config", receivedAtMs: nowMs });

  const enqueued = [];
  const loop = new GatewayLoop({
    relaySelector: new GatewayRelaySelector({ rng: () => 0 }),
    pathPlanner: new GatewayPathPlanner(),
    sender: new GatewaySender({ pool: { sendByRelayId: async () => {} } }),
    crypto: { name: "stub" },
    relayStore,
    outboundQueue: { enqueue: async (item) => { enqueued.push(item); } },
    routeResolver: {
      resolve: async () => ({ direct: false, deliveryRelayKeyId: "relay-delivery", hops: 1 }),
    },
    nowMs: () => nowMs,
  });

  // minHops 3 → 2 intermediates required; the only descriptor is the delivery
  // relay (excluded from intermediate selection) → selector throws.
  await assert.rejects(
    loop.sendToInbox({
      innerBytes: new Uint8Array([1, 2, 3]),
      deliverInboxId: "inbox:test:baseline:1",
      minHops: 3,
      maxHops: 3,
    }),
    NotEnoughRelaysError,
  );
  assert.equal(enqueued.length, 0, "selector exhaustion must not be queued for retry (current behavior)");
});

test("baseline (pinned): GatewayLoop.onRouteFailureCallback is a bare field with zero production subscribers", () => {
  // Behavioral half: null by default, invoked when set.
  const seen = [];
  const proto = { recordRouteFailure: GatewayLoop.prototype.recordRouteFailure, onRouteFailureCallback: null };
  proto.recordRouteFailure("pkt-1", "relay-x", "timeout"); // no throw when null
  proto.onRouteFailureCallback = (evt) => seen.push(evt);
  proto.recordRouteFailure("pkt-2", "relay-y", "disconnect");
  assert.deepEqual(seen, [{ packetId: "pkt-2", relayKeyId: "relay-y", reason: "disconnect" }]);

  // Source half: nothing in src/ assigns the callback (the seam is unsubscribed today).
  const assignments = [];
  for (const file of listSourceFiles(SRC_DIR)) {
    const text = readFileSync(file, "utf8");
    for (const line of text.split("\n")) {
      if (line.includes("onRouteFailureCallback") && line.includes("=")
        && !line.includes("=== ") && !line.trim().startsWith("//") && !line.trim().startsWith("*")) {
        assignments.push({ file: file.slice(SRC_DIR.length + 1), line: line.trim() });
      }
    }
  }
  assert.deepEqual(assignments, [
    { file: "gateway/GatewayLoop.js", line: "this.onRouteFailureCallback = null;" },
  ], "route-failure seam gained a subscriber or moved — update the baseline deliberately");
});

// ---------------------------------------------------------------------------
// Ownership baseline
// ---------------------------------------------------------------------------

test("baseline: MeshCoordinator owns only mesh lifecycle (pinned public surface, single sync-tick hook)", () => {
  const names = Object.getOwnPropertyNames(MeshCoordinator.prototype).sort();
  assert.deepEqual(names, [
    "_clearStartupRetry",
    "_emitStatusChanged",
    "_needsStartupRetry",
    "_scheduleStartupRetryIfNeeded",
    "_syncRouteState",
    "connectNewPeers",
    "constructor",
    "getStatus",
    "onStatusChanged",
    "refresh",
    "refreshSeedReachabilityFromConnections",
    "refreshSeedReachabilityFromStore",
    "setDescriptorExchange",
    "setOnSyncTick",
    "start",
    "stop",
  ], "MeshCoordinator public surface changed — it must not absorb advisor/scheduler/settlement duties");
});

test("baseline: InboxRouter is the only component that defines inbox delivery-route execution", () => {
  const definitions = [];
  for (const file of listSourceFiles(SRC_DIR)) {
    const text = readFileSync(file, "utf8");
    if (/^\s*(async\s+)?routeDelivery\s*\(/m.test(text)) {
      definitions.push(file.slice(SRC_DIR.length + 1));
    }
  }
  assert.deepEqual(definitions, ["relay/InboxRouter.js"]);
});

// ---------------------------------------------------------------------------
// Durable-record extensibility baseline
// ---------------------------------------------------------------------------

test("baseline: an unknown recordKind passes generic durable-record validation", async () => {
  const nowMs = 5_000;
  const { record, localId } = makeSignedRecord({
    recordKind: "future-unknown-kind-baseline",
    recordId: "x1",
    issuedAtMs: nowMs - 1000,
    expiresAtMs: nowMs + 3_600_000,
  });
  const verdict = await verifyDurableRecordDual(record, nowMs);
  assert.equal(verdict.ok, true, "generic checks alone must admit unknown kinds: " + (verdict.reason || ""));
  assert.equal(verdict.localId, localId);
});
