import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNode } from "../src/routing/dht/DhtNode.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

function makeSocket(label) {
  return { id: label, destroyed: false };
}

function createDhtNode(opts = {}) {
  const registry = new ControlMessageRegistry();
  const sent = [];
  const node = new DhtNode({
    selfRelayKeyId: opts.selfRelayKeyId || "relay-self",
    controlMessageRegistry: registry,
    encodeCtl: function (obj) { return new TextEncoder().encode(JSON.stringify(obj)); },
    trySendFrame: function (socket, bytes) {
      sent.push({ socket, obj: JSON.parse(new TextDecoder().decode(bytes)) });
    },
    fallbackResolver: opts.fallbackResolver || null,
    config: opts.config || {},
    nowMs: opts.nowMs || (() => 1000),
  });
  return { node, registry, sent };
}

describe("DhtNode", () => {
  it("installs and uninstalls protocol handlers", () => {
    const { node, registry } = createDhtNode();
    node.install();
    assert.equal(registry.has("dht.find_node"), true);
    assert.equal(registry.has("dht.store"), true);

    node.uninstall();
    assert.equal(registry.has("dht.find_node"), false);
    assert.equal(registry.has("dht.store"), false);
  });

  it("addPeer populates k-buckets", () => {
    const { node } = createDhtNode();
    node.install();

    node.addPeer("relay-peer-1", makeSocket("p1"));
    node.addPeer("relay-peer-2", makeSocket("p2"));

    assert.equal(node.kBuckets.size, 2);
    assert.ok(node.kBuckets.get("relay-peer-1"));
    assert.ok(node.kBuckets.get("relay-peer-2"));
  });

  it("removePeer removes from k-buckets", () => {
    const { node } = createDhtNode();
    node.install();

    node.addPeer("relay-peer-1", makeSocket("p1"));
    assert.equal(node.kBuckets.size, 1);

    node.removePeer("relay-peer-1");
    assert.equal(node.kBuckets.size, 0);
  });

  it("removePeerBySocket removes all matching", () => {
    const { node } = createDhtNode();
    node.install();

    const sharedSocket = makeSocket("shared");
    node.addPeer("relay-peer-1", sharedSocket);
    node.addPeer("relay-peer-2", makeSocket("other"));

    const removed = node.removePeerBySocket(sharedSocket);
    assert.equal(removed.length, 1);
    assert.equal(removed[0], "relay-peer-1");
    assert.equal(node.kBuckets.size, 1);
  });

  it("evictExpiredValues cleans up value store", () => {
    const { node } = createDhtNode({ config: { valueTtlMs: 5000 } });
    node.install();

    node.valueStore.store("inbox:a", { hops: 0 }, 1000);
    assert.equal(node.valueStore.size, 1);

    const evicted = node.evictExpiredValues(7000);
    assert.equal(evicted, 1);
    assert.equal(node.valueStore.size, 0);
  });

  it("exposes routeResolver and routeAnnouncer", () => {
    const { node } = createDhtNode();
    assert.ok(node.routeResolver);
    assert.ok(node.routeAnnouncer);
    assert.ok(typeof node.routeResolver.resolve === "function");
    assert.ok(typeof node.routeAnnouncer.announceRoutes === "function");
  });

  it("addPeer ignores empty relayKeyId", () => {
    const { node } = createDhtNode();
    node.addPeer("", makeSocket("s"));
    node.addPeer("  ", makeSocket("s"));
    assert.equal(node.kBuckets.size, 0);
  });

  it("handles dht.store via protocol when installed", async () => {
    const { node, registry } = createDhtNode();
    node.install();

    const socket = makeSocket("storer");
    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:stored-via-protocol",
      deliveryRelayKeyId: "relay-x",
      hops: 0,
    });

    await registry.dispatch("dht.store", {
      _ctl: "dht.store",
      inboxId: "inbox:stored-via-protocol",
      routeEntry,
    }, socket);

    const stored = node.valueStore.get("inbox:stored-via-protocol", 1000);
    assert.deepStrictEqual(stored, routeEntry);
  });

  it("rejects invalid constructor args", () => {
    assert.throws(function () {
      return new DhtNode({});
    }, /selfRelayKeyId/);

    assert.throws(function () {
      return new DhtNode({
        selfRelayKeyId: "relay-x",
        controlMessageRegistry: null,
        encodeCtl: function () {},
        trySendFrame: function () {},
      });
    }, /controlMessageRegistry/);
  });
});
