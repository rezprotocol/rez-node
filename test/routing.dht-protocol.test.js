import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

function makeSocket(label) {
  return { id: label, destroyed: false };
}

function createProtocol(opts = {}) {
  const selfRelayKeyId = opts.selfRelayKeyId || "relay-self";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = opts.kBuckets || new KBucketTable(selfNodeId);
  const valueStore = opts.valueStore || new DhtValueStore();
  const registry = opts.registry || new ControlMessageRegistry();
  const sent = [];
  const encodeCtl = function (obj) {
    return new TextEncoder().encode(JSON.stringify(obj));
  };
  const trySendFrame = function (socket, bytes) {
    const text = new TextDecoder().decode(bytes);
    sent.push({ socket, obj: JSON.parse(text) });
  };

  const protocol = new DhtProtocol({
    kBuckets,
    valueStore,
    registry,
    selfNodeId,
    selfRelayKeyId,
    encodeCtl,
    trySendFrame,
    queryTimeoutMs: opts.queryTimeoutMs || 500,
    nowMs: opts.nowMs || (() => 1000),
  });

  return { protocol, registry, kBuckets, valueStore, sent, selfNodeId };
}

describe("DhtProtocol", () => {
  it("install registers all DHT control message handlers", () => {
    const { protocol, registry } = createProtocol();
    protocol.install();

    assert.equal(registry.has("dht.find_node"), true);
    assert.equal(registry.has("dht.find_node.reply"), true);
    assert.equal(registry.has("dht.find_value"), true);
    assert.equal(registry.has("dht.find_value.reply"), true);
    assert.equal(registry.has("dht.store"), true);
  });

  it("uninstall removes all handlers", () => {
    const { protocol, registry } = createProtocol();
    protocol.install();
    protocol.uninstall();

    assert.equal(registry.has("dht.find_node"), false);
    assert.equal(registry.has("dht.store"), false);
  });

  it("dht.find_node responds with k-closest nodes", async () => {
    const { protocol, registry, kBuckets, sent, selfNodeId } = createProtocol();
    protocol.install();

    // Add a peer to k-buckets
    const peerId = DhtNodeId.fromRelayKeyId("peer-x");
    const peerSocket = makeSocket("px");
    kBuckets.addOrUpdate(peerId, "peer-x", peerSocket, 1000);

    const socket = makeSocket("requester");
    const targetId = DhtNodeId.fromRelayKeyId("target");
    await registry.dispatch("dht.find_node", {
      _ctl: "dht.find_node",
      queryId: "q1",
      targetIdHex: targetId.hex,
    }, socket);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].obj._ctl, "dht.find_node.reply");
    assert.equal(sent[0].obj.queryId, "q1");
    assert.ok(Array.isArray(sent[0].obj.nodes));
    assert.equal(sent[0].obj.nodes.length, 1);
    assert.equal(sent[0].obj.nodes[0].relayKeyId, "peer-x");
  });

  it("dht.find_value returns value when stored", async () => {
    const { protocol, registry, valueStore, sent } = createProtocol();
    protocol.install();

    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:a",
      deliveryRelayKeyId: "relay-remote",
      hops: 0,
    });
    valueStore.store("inbox:a", routeEntry, 1000);

    const socket = makeSocket("requester");
    await registry.dispatch("dht.find_value", {
      _ctl: "dht.find_value",
      queryId: "q2",
      targetIdHex: DhtNodeId.fromRelayKeyId("inbox:a").hex,
      inboxId: "inbox:a",
    }, socket);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].obj._ctl, "dht.find_value.reply");
    assert.deepStrictEqual(sent[0].obj.value, routeEntry);
    assert.deepStrictEqual(sent[0].obj.nodes, []);
  });

  it("dht.find_value returns k-closest when value not stored", async () => {
    const { protocol, registry, kBuckets, sent } = createProtocol();
    protocol.install();

    const peerId = DhtNodeId.fromRelayKeyId("peer-y");
    kBuckets.addOrUpdate(peerId, "peer-y", makeSocket("py"), 1000);

    const socket = makeSocket("requester");
    await registry.dispatch("dht.find_value", {
      _ctl: "dht.find_value",
      queryId: "q3",
      targetIdHex: DhtNodeId.fromRelayKeyId("inbox:unknown").hex,
      inboxId: "inbox:unknown",
    }, socket);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].obj._ctl, "dht.find_value.reply");
    assert.equal(sent[0].obj.value, null);
    assert.ok(sent[0].obj.nodes.length > 0);
  });

  it("dht.store saves value in value store", async () => {
    const nowMs = 5000;
    const { protocol, registry, valueStore } = createProtocol({ nowMs: () => nowMs });
    protocol.install();

    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:stored",
      deliveryRelayKeyId: "relay-z",
      hops: 0,
    });
    const socket = makeSocket("storer");
    await registry.dispatch("dht.store", {
      _ctl: "dht.store",
      inboxId: "inbox:stored",
      routeEntry,
    }, socket);

    const result = valueStore.get("inbox:stored", nowMs);
    assert.deepStrictEqual(result, routeEntry);
  });

  it("HIGH-8: dht.store with null routeEntry is rejected (no withdraw-proof schema yet)", async () => {
    const nowMs = 5000;
    const { protocol, registry, valueStore } = createProtocol({ nowMs: () => nowMs });
    protocol.install();

    // Seed a legit signed route.
    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:withdrawable",
      deliveryRelayKeyId: "relay-real",
      hops: 0,
    });
    valueStore.store("inbox:withdrawable", routeEntry, nowMs);
    assert.equal(valueStore.size, 1);

    // A hostile peer sends a tombstone — must be rejected so any peer
    // can't unilaterally evict any route from the DHT.
    const socket = makeSocket("withdrawer");
    await registry.dispatch("dht.store", {
      _ctl: "dht.store",
      inboxId: "inbox:withdrawable",
      routeEntry: null,
    }, socket);

    assert.equal(valueStore.size, 1, "tombstone must NOT evict the legit entry");
    assert.deepStrictEqual(valueStore.get("inbox:withdrawable", nowMs), routeEntry);
  });

  it("queryFindNode sends message and resolves on reply", async () => {
    const { protocol, registry, sent, selfNodeId } = createProtocol({ queryTimeoutMs: 1000 });
    protocol.install();

    const socket = makeSocket("peer");
    const targetId = DhtNodeId.fromRelayKeyId("target");
    const promise = protocol.queryFindNode(socket, targetId);

    // Simulate reply
    assert.equal(sent.length, 1);
    const queryId = sent[0].obj.queryId;
    await registry.dispatch("dht.find_node.reply", {
      _ctl: "dht.find_node.reply",
      queryId,
      nodes: [{ nodeIdHex: DhtNodeId.fromRelayKeyId("peer-z").hex, relayKeyId: "peer-z" }],
    }, socket);

    const result = await promise;
    assert.equal(result.nodes.length, 1);
    assert.equal(result.nodes[0].relayKeyId, "peer-z");
  });

  it("queryFindValue sends message and resolves on reply", async () => {
    const { protocol, registry, sent } = createProtocol({ queryTimeoutMs: 1000 });
    protocol.install();

    const socket = makeSocket("peer");
    const targetId = DhtNodeId.fromRelayKeyId("inbox:test");
    const promise = protocol.queryFindValue(socket, targetId, "inbox:test");

    assert.equal(sent.length, 1);
    assert.equal(sent[0].obj.inboxId, "inbox:test");

    const queryId = sent[0].obj.queryId;
    const routeEntry = { hops: 2, deliveryRelayKeyId: "relay-far" };
    await registry.dispatch("dht.find_value.reply", {
      _ctl: "dht.find_value.reply",
      queryId,
      value: routeEntry,
      nodes: [],
    }, socket);

    const result = await promise;
    assert.deepStrictEqual(result.value, routeEntry);
  });

  it("query times out and returns empty", async () => {
    const { protocol } = createProtocol({ queryTimeoutMs: 50 });
    protocol.install();

    const socket = makeSocket("peer");
    const result = await protocol.queryFindNode(socket, DhtNodeId.fromRelayKeyId("target"));

    // Should resolve after timeout with empty result
    assert.equal(result.value, null);
    assert.equal(result.nodes.length, 0);
  });

  it("sendStore fires and forgets", () => {
    const { protocol, sent } = createProtocol();
    protocol.install();

    const socket = makeSocket("peer");
    const entry = { hops: 0, deliveryRelayKeyId: "relay-self" };
    protocol.sendStore(socket, "inbox:local", entry);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].obj._ctl, "dht.store");
    assert.equal(sent[0].obj.inboxId, "inbox:local");
    assert.deepStrictEqual(sent[0].obj.routeEntry, entry);
  });

  it("ignores replies with unknown queryId", async () => {
    const { protocol, registry } = createProtocol();
    protocol.install();

    // Dispatch a reply with no matching pending query — should not throw
    await registry.dispatch("dht.find_node.reply", {
      _ctl: "dht.find_node.reply",
      queryId: "unknown-id",
      nodes: [],
    }, makeSocket("s"));
    // No assertion needed — just verify no error
  });

  it("rejects invalid constructor args", () => {
    assert.throws(function () { return new DhtProtocol({}); }, /kBuckets/);
  });

  it("uninstall resolves pending queries", async () => {
    const { protocol } = createProtocol({ queryTimeoutMs: 60000 });
    protocol.install();

    const socket = makeSocket("peer");
    const promise = protocol.queryFindNode(socket, DhtNodeId.fromRelayKeyId("target"));

    protocol.uninstall();

    const result = await promise;
    assert.equal(result.nodes.length, 0);
  });
});
