import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";

function makeSocket(label) {
  return { id: label, destroyed: false };
}

// ADR-RELAY-IDENTITY: discovered node references must carry a canonical
// self-certifying `rez:relay:<64 lowercase hex>` id. The lookup's format
// gate is structural, so a label-derived hash is sufficient for tests.
function canonicalId(label) {
  return "rez:relay:" + createHash("sha256").update(label).digest("hex");
}

function buildTable(selfId, peers) {
  const table = new KBucketTable(selfId);
  for (const peer of peers) {
    table.addOrUpdate(peer.nodeId, peer.relayKeyId, peer.socket, 1000);
  }
  return table;
}

describe("DhtLookup", () => {
  it("findNode returns seeded nodes when no query responses", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const peerB = { nodeId: DhtNodeId.fromRelayKeyId("peer-b"), relayKeyId: "peer-b", socket: makeSocket("b") };
    const table = buildTable(selfId, [peerA, peerB]);
    const lookup = new DhtLookup(table, { alpha: 2, k: 5 });

    const targetId = DhtNodeId.fromRelayKeyId("target");
    const result = await lookup.findNode(targetId, async function () {
      return { nodes: [] };
    });

    assert.ok(result.closestNodes.length <= 5);
    assert.ok(result.closestNodes.length >= 2);
  });

  it("findNode discovers closer nodes from responses", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 3, k: 5 });

    const targetId = DhtNodeId.fromRelayKeyId("target");
    const discoveredRelayKeyId = canonicalId("peer-closer");
    const discoveredNode = DhtNodeId.fromRelayKeyId(discoveredRelayKeyId);

    let queryCount = 0;
    const result = await lookup.findNode(targetId, async function () {
      queryCount += 1;
      if (queryCount === 1) {
        return {
          nodes: [{ nodeIdHex: discoveredNode.hex, relayKeyId: discoveredRelayKeyId }],
        };
      }
      return { nodes: [] };
    });

    assert.ok(queryCount >= 1);
    // The discovered node should appear in results
    const found = result.closestNodes.some(function (c) { return c.relayKeyId === discoveredRelayKeyId; });
    assert.ok(found, "discovered node should be in results");
  });

  it("findValue returns value when found", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 3, k: 5 });

    const targetId = DhtNodeId.fromRelayKeyId("inbox:test");
    const expectedRoute = { inboxId: "inbox:test", hops: 1, deliveryRelayKeyId: "relay-x" };

    const result = await lookup.findValue(targetId, async function () {
      return { value: expectedRoute, nodes: [] };
    });

    assert.deepStrictEqual(result.value, expectedRoute);
  });

  it("findValue returns null when not found", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 3, k: 5 });

    const targetId = DhtNodeId.fromRelayKeyId("inbox:unknown");
    const result = await lookup.findValue(targetId, async function () {
      return { value: null, nodes: [] };
    });

    assert.equal(result.value, null);
  });

  it("returns empty for empty k-bucket table", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const table = new KBucketTable(selfId);
    const lookup = new DhtLookup(table);

    const result = await lookup.findNode(DhtNodeId.fromRelayKeyId("target"), async function () {
      return { nodes: [] };
    });

    assert.equal(result.closestNodes.length, 0);
  });

  it("handles query errors gracefully", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 3, k: 5 });

    const result = await lookup.findNode(DhtNodeId.fromRelayKeyId("target"), async function () {
      throw new Error("network failure");
    });

    // Should not throw — errors are caught and treated as empty responses
    assert.ok(result.closestNodes.length >= 0);
  });

  it("skips peers with destroyed sockets", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const deadSocket = { id: "dead", destroyed: true };
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: deadSocket };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 3, k: 5 });

    let queryCalled = false;
    const result = await lookup.findNode(DhtNodeId.fromRelayKeyId("target"), async function () {
      queryCalled = true;
      return { nodes: [] };
    });

    assert.equal(queryCalled, false, "should not query destroyed socket");
    assert.ok(result.closestNodes.length >= 0);
  });

  it("converges within bounded rounds", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peers = [];
    for (let i = 0; i < 5; i += 1) {
      peers.push({
        nodeId: DhtNodeId.fromRelayKeyId("peer-" + i),
        relayKeyId: "peer-" + i,
        socket: makeSocket("p" + i),
      });
    }
    const table = buildTable(selfId, peers);
    const lookup = new DhtLookup(table, { alpha: 2, k: 3 });

    let rounds = 0;
    const result = await lookup.findNode(DhtNodeId.fromRelayKeyId("target"), async function () {
      rounds += 1;
      return { nodes: [] };
    });

    assert.ok(rounds <= 10, "should converge within 10 rounds, took " + rounds);
    assert.ok(result.closestNodes.length <= 3);
  });

  it("rejects invalid kBuckets", () => {
    assert.throws(function () { return new DhtLookup(null); }, /KBucketTable/);
  });
});

describe("DhtLookup re-audit deadlines and deferral (R2/R4)", () => {
  it("R2: a query that never settles cannot hold the lookup open past its total deadline", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 2, k: 5, totalDeadlineMs: 60 });

    const startedAt = Date.now();
    const result = await lookup.findValue(DhtNodeId.fromRelayKeyId("target"), function () {
      return new Promise(function () {}); // never settles
    });
    const elapsedMs = Date.now() - startedAt;

    assert.ok(elapsedMs < 1000, "deadline race must abandon the hung query (took " + elapsedMs + "ms)");
    assert.equal(result.value, null);
    assert.equal(result.report.completionReason, "deadline");
    // The queried-but-unanswered candidate is honest partial state.
    assert.equal(result.report.queriedCount, 1);
  });

  it("R2: a slow query does not extend the lookup past the deadline (partial results, not late results)", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 2, k: 5, totalDeadlineMs: 40 });

    const startedAt = Date.now();
    const result = await lookup.findValue(DhtNodeId.fromRelayKeyId("target"), function () {
      return new Promise(function (resolve) {
        setTimeout(function () { resolve({ value: { got: true }, nodes: [] }); }, 500);
      });
    });
    const elapsedMs = Date.now() - startedAt;

    assert.ok(elapsedMs < 400, "a 40ms lookup must not wait 500ms for a straggler (took " + elapsedMs + "ms)");
    assert.equal(result.value, null, "a past-deadline reply is not merged");
    assert.equal(result.report.completionReason, "deadline");
  });

  it("R2: a caller-supplied deadline tightens (never extends) the lookup budget", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 2, k: 5, totalDeadlineMs: 10_000 });

    const startedAt = Date.now();
    const result = await lookup.findNode(DhtNodeId.fromRelayKeyId("target"), function () {
      return new Promise(function () {}); // never settles
    }, { deadlineAtMs: Date.now() + 50 });
    const elapsedMs = Date.now() - startedAt;

    assert.ok(elapsedMs < 1000, "caller deadline wins over the 10s default (took " + elapsedMs + "ms)");
    assert.equal(result.report.completionReason, "deadline");
  });

  it("R2: an invalid injected clock fails LOUD instead of silently disabling the deadline", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);
    const lookup = new DhtLookup(table, { alpha: 2, k: 5, nowMs: function () { return NaN; } });
    await assert.rejects(
      lookup.findNode(DhtNodeId.fromRelayKeyId("target"), async function () { return { nodes: [] }; }),
      /non-finite/,
    );
  });

  it("R4: a globally-deferred dial (budget-exhausted) is refunded and the candidate stays a reference", async () => {
    const selfId = DhtNodeId.fromRelayKeyId("self");
    const peerA = { nodeId: DhtNodeId.fromRelayKeyId("peer-a"), relayKeyId: "peer-a", socket: makeSocket("a") };
    const table = buildTable(selfId, [peerA]);

    const discoveredX = canonicalId("deferred-x");
    const discoveredY = canonicalId("resolved-y");
    const resolverCalls = [];
    const resolver = {
      async resolve(relayKeyId) {
        resolverCalls.push(relayKeyId);
        if (resolverCalls.length === 1) {
          // Another lookup holds the node-global dial slots right now.
          return { ok: false, reason: "budget-exhausted" };
        }
        return { ok: true, socket: makeSocket("resolved") };
      },
    };
    // ONE per-lookup dial: only the refund can make the second reservation
    // possible. alpha 1 forces the two discovered candidates into separate
    // rounds so the refunded budget is observably reused.
    const lookup = new DhtLookup(table, {
      alpha: 1, k: 10, maxNewDialsPerLookup: 1, candidateResolver: resolver,
    });

    const queried = [];
    const result = await lookup.findNode(DhtNodeId.fromRelayKeyId("target"), async function (entry) {
      queried.push(entry.relayKeyId);
      if (entry.relayKeyId === "peer-a") {
        return {
          nodes: [
            { nodeIdHex: DhtNodeId.fromRelayKeyId(discoveredX).hex, relayKeyId: discoveredX },
            { nodeIdHex: DhtNodeId.fromRelayKeyId(discoveredY).hex, relayKeyId: discoveredY },
          ],
        };
      }
      return { nodes: [] };
    });

    assert.equal(resolverCalls.length, 2,
      "the deferred reservation was refunded — the second candidate got the lookup's single dial");
    const closestIds = result.closestNodes.map(function (c) { return c.relayKeyId; });
    assert.ok(closestIds.includes(resolverCalls[0]),
      "a never-attempted (deferred) candidate remains a valid closest-node reference");
    assert.ok(closestIds.includes(resolverCalls[1]), "the actually-resolved candidate is present too");
    assert.equal(result.report.dialAttemptCount, 1,
      "a global deferral is NOT counted as a dial attempt");
  });
});
