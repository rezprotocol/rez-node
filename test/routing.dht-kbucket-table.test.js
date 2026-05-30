import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";

function makeId(seed) {
  return DhtNodeId.fromRelayKeyId("relay-" + seed);
}

function makeSocket(label) {
  return { id: label, destroyed: false };
}

describe("KBucketTable", () => {
  it("adds and retrieves a peer", () => {
    const self = makeId("self");
    const table = new KBucketTable(self, { k: 20 });
    const peerId = makeId("peer-a");
    const socket = makeSocket("a");

    const added = table.addOrUpdate(peerId, "relay-peer-a", socket, 1000);
    assert.equal(added, true);
    assert.equal(table.size, 1);

    const entry = table.get("relay-peer-a");
    assert.ok(entry);
    assert.equal(entry.relayKeyId, "relay-peer-a");
    assert.equal(entry.socket, socket);
    assert.equal(entry.lastSeenMs, 1000);
  });

  it("rejects self ID", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    const added = table.addOrUpdate(self, "relay-self", makeSocket("s"), 1000);
    assert.equal(added, false);
    assert.equal(table.size, 0);
  });

  it("updates lastSeenMs and moves to back on re-add", () => {
    const self = makeId("self");
    const table = new KBucketTable(self, { k: 20 });
    const peer = makeId("peer-a");
    const socket = makeSocket("a");

    table.addOrUpdate(peer, "relay-a", socket, 1000);
    const newSocket = makeSocket("a2");
    table.addOrUpdate(peer, "relay-a", newSocket, 2000);

    assert.equal(table.size, 1);
    const entry = table.get("relay-a");
    assert.equal(entry.lastSeenMs, 2000);
    assert.equal(entry.socket, newSocket);
  });

  it("evicts dead socket when bucket is full", () => {
    // All three IDs XOR with self to values 0x80, 0x81, 0x82 in last byte,
    // all sharing highest bit at position 7 → same k-bucket.
    const selfBytes = new Uint8Array(32);
    const selfId = DhtNodeId.fromBytes(selfBytes);
    const table = new KBucketTable(selfId, { k: 2 });

    const aBytes = new Uint8Array(32);
    aBytes[31] = 0x80;
    const bBytes = new Uint8Array(32);
    bBytes[31] = 0x81;
    const cBytes = new Uint8Array(32);
    cBytes[31] = 0x82;

    const deadSocket = { id: "dead", destroyed: true };
    const liveSocketB = makeSocket("b");
    const liveSocketC = makeSocket("c");

    table.addOrUpdate(DhtNodeId.fromBytes(aBytes), "relay-a", deadSocket, 100);
    table.addOrUpdate(DhtNodeId.fromBytes(bBytes), "relay-b", liveSocketB, 200);
    assert.equal(table.size, 2);

    // Bucket full, oldest (relay-a) has dead socket → should be evicted
    const added = table.addOrUpdate(DhtNodeId.fromBytes(cBytes), "relay-c", liveSocketC, 300);
    assert.equal(added, true);
    assert.equal(table.size, 2);
    assert.equal(table.get("relay-a"), null);
    assert.ok(table.get("relay-c"));
  });

  it("rejects new peer when bucket full and oldest is live", () => {
    const selfBytes = new Uint8Array(32);
    const selfId = DhtNodeId.fromBytes(selfBytes);
    const table = new KBucketTable(selfId, { k: 2 });

    // Same bucket: all XOR to 0x80, 0x81, 0x82 → highest bit at position 7
    const aBytes = new Uint8Array(32);
    aBytes[31] = 0x80;
    const bBytes = new Uint8Array(32);
    bBytes[31] = 0x81;
    const cBytes = new Uint8Array(32);
    cBytes[31] = 0x82;

    table.addOrUpdate(DhtNodeId.fromBytes(aBytes), "relay-a", makeSocket("a"), 100);
    table.addOrUpdate(DhtNodeId.fromBytes(bBytes), "relay-b", makeSocket("b"), 200);

    const added = table.addOrUpdate(DhtNodeId.fromBytes(cBytes), "relay-c", makeSocket("c"), 300);
    assert.equal(added, false);
    assert.equal(table.size, 2);
    assert.ok(table.get("relay-a"));
    assert.equal(table.get("relay-c"), null);
  });

  it("remove deletes entry", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    table.addOrUpdate(makeId("peer"), "relay-peer", makeSocket("p"), 1000);
    assert.equal(table.size, 1);

    const removed = table.remove("relay-peer");
    assert.equal(removed, true);
    assert.equal(table.size, 0);
    assert.equal(table.get("relay-peer"), null);
  });

  it("remove returns false for unknown relayKeyId", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    assert.equal(table.remove("unknown"), false);
  });

  it("removeBySocket removes all entries with matching socket", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    const sharedSocket = makeSocket("shared");
    const otherSocket = makeSocket("other");

    table.addOrUpdate(makeId("peer-1"), "relay-1", sharedSocket, 1000);
    table.addOrUpdate(makeId("peer-2"), "relay-2", sharedSocket, 1000);
    table.addOrUpdate(makeId("peer-3"), "relay-3", otherSocket, 1000);
    assert.equal(table.size, 3);

    const removed = table.removeBySocket(sharedSocket);
    assert.equal(removed.length, 2);
    assert.ok(removed.includes("relay-1"));
    assert.ok(removed.includes("relay-2"));
    assert.equal(table.size, 1);
    assert.ok(table.get("relay-3"));
  });

  it("findClosest returns entries sorted by XOR distance", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);

    const ids = [];
    for (let i = 0; i < 10; i += 1) {
      const nodeId = makeId("node-" + i);
      ids.push(nodeId);
      table.addOrUpdate(nodeId, "relay-node-" + i, makeSocket("n" + i), 1000);
    }
    assert.equal(table.size, 10);

    const target = makeId("target");
    const closest = table.findClosest(target, 5);
    assert.equal(closest.length, 5);

    // Verify sorted order: each entry should be closer than the next
    for (let i = 1; i < closest.length; i += 1) {
      const cmp = target.compareDistanceTo(closest[i - 1].nodeId, closest[i].nodeId);
      assert.ok(cmp <= 0, "entry " + (i - 1) + " should be closer than entry " + i);
    }
  });

  it("findClosest returns all when count exceeds table size", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    table.addOrUpdate(makeId("peer-a"), "relay-a", makeSocket("a"), 1000);
    table.addOrUpdate(makeId("peer-b"), "relay-b", makeSocket("b"), 1000);

    const closest = table.findClosest(makeId("target"), 100);
    assert.equal(closest.length, 2);
  });

  it("findClosest returns empty for empty table", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    const closest = table.findClosest(makeId("target"), 5);
    assert.equal(closest.length, 0);
  });

  it("getAllEntries returns all entries", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    table.addOrUpdate(makeId("a"), "relay-a", makeSocket("a"), 1000);
    table.addOrUpdate(makeId("b"), "relay-b", makeSocket("b"), 1000);
    table.addOrUpdate(makeId("c"), "relay-c", makeSocket("c"), 1000);

    const all = table.getAllEntries();
    assert.equal(all.length, 3);
  });

  it("rejects invalid constructor args", () => {
    assert.throws(() => new KBucketTable(null), /DhtNodeId/);
    assert.throws(() => new KBucketTable(makeId("self"), { k: 0 }), /positive integer/);
    assert.throws(() => new KBucketTable(makeId("self"), { k: -1 }), /positive integer/);
  });

  it("rejects invalid addOrUpdate args", () => {
    const self = makeId("self");
    const table = new KBucketTable(self);
    assert.equal(table.addOrUpdate(null, "relay", makeSocket("s"), 1000), false);
    assert.equal(table.addOrUpdate(makeId("peer"), "", makeSocket("s"), 1000), false);
    assert.equal(table.addOrUpdate(makeId("peer"), "   ", makeSocket("s"), 1000), false);
  });
});
