/**
 * P3.1/P3.2 — bounded DHT traversal beyond connected peers
 * (ATLAS_PREREQUISITES). Resolver unit tests + lookup traversal, budget,
 * starvation, and determinism tests under a fake clock.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DhtCandidateResolver } from "../src/routing/dht/DhtCandidateResolver.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";

function canonicalId(label) {
  return "rez:relay:" + createHash("sha256").update(label).digest("hex");
}

function fakePool(behaviors) {
  const calls = [];
  return {
    calls,
    getAuthenticatedRelaySocket: async (relayKeyId) => {
      calls.push(relayKeyId);
      const behavior = behaviors[relayKeyId];
      if (!behavior) throw new Error("getAuthenticatedRelaySocket: no admitted descriptor for " + relayKeyId);
      if (behavior.hang) return new Promise(() => {});
      if (behavior.error) throw new Error(behavior.error);
      return behavior.socket;
    },
  };
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

test("resolver returns an authenticated socket for a resolvable canonical candidate", async () => {
  const id = canonicalId("resolvable");
  const socket = { id: "s1", destroyed: false };
  const resolver = new DhtCandidateResolver({ pool: fakePool({ [id]: { socket } }) });
  const result = await resolver.resolve(id);
  assert.deepEqual(result, { ok: true, socket });
});

test("resolver rejects non-canonical ids without touching the pool", async () => {
  const pool = fakePool({});
  const resolver = new DhtCandidateResolver({ pool });
  assert.deepEqual(await resolver.resolve("relay-legacy"), { ok: false, reason: "invalid-relay-id" });
  assert.equal(pool.calls.length, 0);
});

test("resolver types failures and negative-caches them", async () => {
  const missing = canonicalId("missing");
  const mismatch = canonicalId("mismatch");
  const pool = fakePool({
    [mismatch]: { error: "getAuthenticatedRelaySocket: authenticated peer is not " + mismatch },
  });
  const resolver = new DhtCandidateResolver({ pool, negativeCacheMs: 30_000 });
  assert.deepEqual(await resolver.resolve(missing), { ok: false, reason: "no-descriptor" });
  assert.deepEqual(await resolver.resolve(mismatch), { ok: false, reason: "identity-mismatch" });
  // Second attempts hit the negative cache — no new dials.
  const callsBefore = pool.calls.length;
  assert.deepEqual(await resolver.resolve(missing), { ok: false, reason: "negative-cached" });
  assert.deepEqual(await resolver.resolve(mismatch), { ok: false, reason: "negative-cached" });
  assert.equal(pool.calls.length, callsBefore);
});

test("resolver dial timeout is bounded and negative-cached; cache expires deterministically", async () => {
  const hang = canonicalId("hang");
  const clock = { now: 1_000 };
  const resolver = new DhtCandidateResolver({
    pool: fakePool({ [hang]: { hang: true } }),
    dialTimeoutMs: 50,
    negativeCacheMs: 30_000,
    nowMs: () => clock.now,
  });
  assert.deepEqual(await resolver.resolve(hang), { ok: false, reason: "dial-timeout" });
  assert.deepEqual(await resolver.resolve(hang), { ok: false, reason: "negative-cached" });
  clock.now += 30_001;
  const third = await resolver.resolve(hang);
  assert.equal(third.reason, "dial-timeout", "expired negative cache allows a fresh (typed) attempt");
});

test("resolver concurrency budget yields typed budget-exhausted, not queuing", async () => {
  const a = canonicalId("slow-a");
  const b = canonicalId("slow-b");
  const resolver = new DhtCandidateResolver({
    pool: fakePool({ [a]: { hang: true }, [b]: { hang: true } }),
    dialTimeoutMs: 200,
    maxConcurrentDials: 1,
  });
  const first = resolver.resolve(a);
  const second = await resolver.resolve(b);
  assert.deepEqual(second, { ok: false, reason: "budget-exhausted" });
  assert.equal((await first).reason, "dial-timeout");
});

// ---------------------------------------------------------------------------
// Lookup traversal with the resolver
// ---------------------------------------------------------------------------

function lookupHarness({ resolver, clock, alpha = 3, k = 20, maxNewDialsPerLookup = 4, totalDeadlineMs = 10_000 }) {
  const selfId = DhtNodeId.fromRelayKeyId(canonicalId("self"));
  const kBuckets = new KBucketTable(selfId, { k });
  const lookup = new DhtLookup(kBuckets, {
    alpha,
    k,
    maxRounds: 10,
    maxNewDialsPerLookup,
    totalDeadlineMs,
    candidateResolver: resolver,
    nowMs: clock ? () => clock.now : undefined,
  });
  return { kBuckets, lookup };
}

function addSeed(kBuckets, label, socket, nowMs = 1000) {
  const id = canonicalId(label);
  kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId(id), id, socket, nowMs);
  return id;
}

test("a connected peer reveals a closer unconnected relay; the lookup authenticates and queries it next round", async () => {
  const discoveredId = canonicalId("discovered-holder");
  const discoveredSocket = { id: "sock-discovered", destroyed: false };
  const resolver = new DhtCandidateResolver({ pool: fakePool({ [discoveredId]: { socket: discoveredSocket } }) });
  const { kBuckets, lookup } = lookupHarness({ resolver });
  const seedId = addSeed(kBuckets, "seed-peer", { id: "sock-seed", destroyed: false });

  const targetId = DhtNodeId.fromRelayKeyId(discoveredId); // discovered node IS closest to target
  const queried = [];
  const result = await lookup.findValue(targetId, async (entry) => {
    queried.push(entry.relayKeyId);
    if (entry.relayKeyId === seedId) {
      return { value: null, nodes: [{ relayKeyId: discoveredId, nodeIdHex: DhtNodeId.fromRelayKeyId(discoveredId).hex }] };
    }
    if (entry.relayKeyId === discoveredId) {
      assert.equal(entry.socket, discoveredSocket, "query runs on the authenticated resolved socket");
      return { value: { found: "yes" }, nodes: [] };
    }
    return { value: null, nodes: [] };
  });

  assert.deepEqual(queried, [seedId, discoveredId]);
  assert.deepEqual(result.value, { found: "yes" });
  assert.equal(result.report.completionReason, "value-found");
  assert.equal(result.report.dialAttemptCount, 1);
  assert.equal(result.report.queriedCount, 2);
});

test("REGRESSION (slot burn): a socket-less candidate is never marked queried before resolution — and unreachable closers cannot starve a reachable one", async () => {
  // Four malicious unreachable closer candidates + one reachable further one.
  const reachableId = canonicalId("reachable-5th");
  const reachableSocket = { id: "sock-r", destroyed: false };
  const behaviors = { [reachableId]: { socket: reachableSocket } };
  const unreachable = [];
  for (let i = 0; i < 4; i += 1) {
    const id = canonicalId("unreachable-" + i);
    unreachable.push(id);
    behaviors[id] = { error: "dial failed" };
  }
  const resolver = new DhtCandidateResolver({ pool: fakePool(behaviors) });
  const { kBuckets, lookup } = lookupHarness({ resolver, alpha: 3 });
  const seedId = addSeed(kBuckets, "seed", { id: "sock-seed", destroyed: false });

  const targetId = DhtNodeId.fromRelayKeyId(canonicalId("target"));
  const queried = [];
  const result = await lookup.findValue(targetId, async (entry) => {
    queried.push(entry.relayKeyId);
    if (entry.relayKeyId === seedId) {
      const nodes = [...unreachable, reachableId].map((id) => ({
        relayKeyId: id,
        nodeIdHex: DhtNodeId.fromRelayKeyId(id).hex,
      }));
      return { value: null, nodes };
    }
    if (entry.relayKeyId === reachableId) {
      return { value: { got: "there" }, nodes: [] };
    }
    return { value: null, nodes: [] };
  });

  assert.deepEqual(result.value, { got: "there" }, "the reachable fifth candidate is reached despite 4 dead closers");
  assert.ok(queried.includes(reachableId));
  assert.ok(!queried.some((id) => unreachable.includes(id)), "unresolvable candidates are never queried");
  assert.equal(result.report.dialAttemptCount <= 4 + 1, true, "dial budget bounds attempts");
});

test("duplicate/reordered/malformed/mismatched references consume bounded work and never enter the verified set", async () => {
  const resolver = null; // connected-only mode
  const { kBuckets, lookup } = lookupHarness({ resolver });
  const seedId = addSeed(kBuckets, "seed-a", { id: "sa", destroyed: false });
  const targetId = DhtNodeId.fromRelayKeyId(canonicalId("t2"));

  const evilId = canonicalId("evil");
  let rounds = 0;
  const result = await lookup.findValue(targetId, async () => {
    rounds += 1;
    return {
      value: null,
      nodes: [
        { relayKeyId: "free-string", nodeIdHex: DhtNodeId.fromRelayKeyId("free-string").hex }, // non-canonical
        { relayKeyId: evilId, nodeIdHex: "f".repeat(64) }, // LOW-5 mismatch
        { relayKeyId: evilId }, // malformed
        { nodeIdHex: DhtNodeId.fromRelayKeyId(evilId).hex }, // malformed
      ],
    };
  });
  assert.equal(rounds, 1, "nothing new admitted → lookup converges after one round");
  assert.equal(result.report.completionReason, "converged");
  assert.equal(result.report.rejectedCandidateCount, 4);
  assert.deepEqual(result.report.closestRelayKeyIds, [seedId]);
});

test("deadline is deterministic under a fake clock and spans dial + query time", async () => {
  const clock = { now: 1_000 };
  const slowId = canonicalId("slow-resolve");
  const pool = {
    getAuthenticatedRelaySocket: async () => {
      clock.now += 20_000; // the dial itself blows the total deadline
      return { id: "late", destroyed: false };
    },
  };
  const resolver = new DhtCandidateResolver({ pool, nowMs: () => clock.now });
  const { kBuckets, lookup } = lookupHarness({ resolver, clock, totalDeadlineMs: 10_000 });
  const seedId = addSeed(kBuckets, "seed-b", { id: "sb", destroyed: false });
  const targetId = DhtNodeId.fromRelayKeyId(slowId);

  const result = await lookup.findValue(targetId, async (entry) => {
    if (entry.relayKeyId === seedId) {
      return { value: null, nodes: [{ relayKeyId: slowId, nodeIdHex: DhtNodeId.fromRelayKeyId(slowId).hex }] };
    }
    throw new Error("the post-deadline candidate must not be queried");
  });
  assert.equal(result.value, null);
  assert.equal(result.report.completionReason, "deadline");
});

test("no candidate resolver reproduces connected-peer-only behavior", async () => {
  const { kBuckets, lookup } = lookupHarness({ resolver: null });
  const seedId = addSeed(kBuckets, "only-seed", { id: "s-only", destroyed: false });
  const discovered = canonicalId("cannot-dial");
  const targetId = DhtNodeId.fromRelayKeyId(discovered);

  const queried = [];
  const result = await lookup.findValue(targetId, async (entry) => {
    queried.push(entry.relayKeyId);
    return { value: null, nodes: [{ relayKeyId: discovered, nodeIdHex: DhtNodeId.fromRelayKeyId(discovered).hex }] };
  });
  assert.deepEqual(queried, [seedId], "only the connected peer is ever queried");
  assert.equal(result.report.dialAttemptCount, 0);
  assert.equal(result.value, null);
});
