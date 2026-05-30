import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";

function makeSocket(label) {
  return { id: label, destroyed: false };
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
    const discoveredNode = DhtNodeId.fromRelayKeyId("peer-closer");

    let queryCount = 0;
    const result = await lookup.findNode(targetId, async function () {
      queryCount += 1;
      if (queryCount === 1) {
        return {
          nodes: [{ nodeIdHex: discoveredNode.hex, relayKeyId: "peer-closer" }],
        };
      }
      return { nodes: [] };
    });

    assert.ok(queryCount >= 1);
    // The discovered node should appear in results
    const found = result.closestNodes.some(function (c) { return c.relayKeyId === "peer-closer"; });
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
