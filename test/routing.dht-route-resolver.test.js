import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { DhtRouteResolver } from "../src/routing/dht/DhtRouteResolver.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

function makeSocket(label) {
  return { id: label, destroyed: false };
}

function createResolver(opts = {}) {
  const selfRelayKeyId = "relay-self";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);
  const valueStore = new DhtValueStore();
  const registry = new ControlMessageRegistry();
  const sent = [];

  const protocol = new DhtProtocol({
    kBuckets,
    valueStore,
    registry,
    selfNodeId,
    selfRelayKeyId,
    encodeCtl: function (obj) { return new TextEncoder().encode(JSON.stringify(obj)); },
    trySendFrame: function (socket, bytes) {
      sent.push({ socket, obj: JSON.parse(new TextDecoder().decode(bytes)) });
    },
    queryTimeoutMs: 100,
    nowMs: opts.nowMs || (() => 1000),
  });
  protocol.install();

  const lookup = new DhtLookup(kBuckets, { alpha: 3, k: 20 });

  const resolver = new DhtRouteResolver({
    lookup,
    protocol,
    valueStore,
    fallbackResolver: opts.fallbackResolver || null,
    nowMs: opts.nowMs || (() => 1000),
  });

  return { resolver, kBuckets, valueStore, registry, sent, selfNodeId, protocol };
}

describe("DhtRouteResolver", () => {
  it("resolves from local RouteTable first", async () => {
    const { resolver } = createResolver();
    const routeEntry = { direct: true, hops: 0 };
    const routeTable = {
      get: function (id) { return id === "inbox:local" ? routeEntry : null; },
    };

    const result = await resolver.resolve("inbox:local", { routeTable, relayConnectionPool: null });
    assert.equal(result, routeEntry);
  });

  it("resolves from local DhtValueStore when RouteTable misses", async () => {
    const { resolver, valueStore } = createResolver();
    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:stored",
      deliveryRelayKeyId: "relay-host-a",
      hops: 0,
    });
    valueStore.store("inbox:stored", routeEntry, 1000);

    const routeTable = { get: function () { return null; } };
    const result = await resolver.resolve("inbox:stored", { routeTable, relayConnectionPool: null });
    assert.deepStrictEqual(result, routeEntry);
  });

  it("resolves via DHT FIND_VALUE when local stores miss", async () => {
    const { resolver, kBuckets, registry, sent } = createResolver();

    // Add a peer that will respond to FIND_VALUE
    const peerId = DhtNodeId.fromRelayKeyId("peer-a");
    const peerSocket = makeSocket("pa");
    kBuckets.addOrUpdate(peerId, "peer-a", peerSocket, 1000);

    const routeTable = { get: function () { return null; } };

    // Start resolution — it will send dht.find_value to peer
    const promise = resolver.resolve("inbox:remote", { routeTable, relayConnectionPool: null });

    // Wait a tick for the query to be sent
    await new Promise(function (r) { setTimeout(r, 10); });

    // Simulate peer response with a signed routeEntry — HIGH-8 requires
    // every value returned from FIND_VALUE to carry a claimant delegation.
    if (sent.length > 0) {
      const queryId = sent[0].obj.queryId;
      const { routeEntry: remoteRoute } = makeSignedRouteEntry({
        inboxId: "inbox:remote",
        deliveryRelayKeyId: "relay-far",
        hops: 0,
      });
      await registry.dispatch("dht.find_value.reply", {
        _ctl: "dht.find_value.reply",
        queryId,
        value: remoteRoute,
        nodes: [],
      }, peerSocket);
    }

    const result = await promise;
    assert.ok(result, "should resolve a route");
    assert.equal(result.inboxId, "inbox:remote");
  });

  it("HIGH-8: rejects an unsigned routeEntry returned by FIND_VALUE", async () => {
    const { resolver, kBuckets, registry, sent } = createResolver();
    const peerId = DhtNodeId.fromRelayKeyId("peer-evil");
    const peerSocket = makeSocket("pe");
    kBuckets.addOrUpdate(peerId, "peer-evil", peerSocket, 1000);

    const routeTable = { get: function () { return null; } };
    const promise = resolver.resolve("inbox:victim", { routeTable, relayConnectionPool: null });
    await new Promise(function (r) { setTimeout(r, 10); });

    if (sent.length > 0) {
      const queryId = sent[0].obj.queryId;
      // Hostile peer returns a routeEntry with NO registration.
      await registry.dispatch("dht.find_value.reply", {
        _ctl: "dht.find_value.reply",
        queryId,
        value: { inboxId: "inbox:victim", hops: 0, deliveryRelayKeyId: "relay-evil" },
        nodes: [],
      }, peerSocket);
    }

    const result = await promise;
    assert.equal(result, null, "unsigned routeEntry must be rejected");
  });

  it("HIGH-8: rejects a routeEntry whose delivery key doesn't match the delegation's nodeKey", async () => {
    const { resolver, valueStore } = createResolver();
    // Legit delegation for relay-real, then rewrap as delivering through relay-evil.
    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:rewrap",
      deliveryRelayKeyId: "relay-real",
      hops: 0,
    });
    const rewrapped = { ...routeEntry, deliveryRelayKeyId: "relay-evil", relayKeyId: "relay-evil" };
    valueStore.store("inbox:rewrap", rewrapped, 1000);

    const routeTable = { get: function () { return null; } };
    const result = await resolver.resolve("inbox:rewrap", { routeTable, relayConnectionPool: null });
    assert.equal(result, null, "rewrapped delegation must be rejected");
    // And the bad entry should be evicted to prevent re-serving.
    assert.equal(valueStore.get("inbox:rewrap", 1000), null);
  });

  it("falls back to gossip resolver when DHT lookup fails", async () => {
    let fallbackCalled = false;
    const fallbackRoute = { hops: 3, deliveryRelayKeyId: "relay-gossip" };
    const fallbackResolver = {
      resolve: async function () {
        fallbackCalled = true;
        return fallbackRoute;
      },
    };

    const { resolver } = createResolver({ fallbackResolver });

    const routeTable = { get: function () { return null; } };
    const result = await resolver.resolve("inbox:nowhere", { routeTable, relayConnectionPool: null });

    assert.ok(fallbackCalled);
    assert.deepStrictEqual(result, fallbackRoute);
  });

  it("returns null when no route found and no fallback", async () => {
    const { resolver } = createResolver();
    const routeTable = { get: function () { return null; } };
    const result = await resolver.resolve("inbox:unknown", { routeTable, relayConnectionPool: null });
    assert.equal(result, null);
  });

  it("handles null routeTable", async () => {
    const { resolver } = createResolver();
    const result = await resolver.resolve("inbox:x", { routeTable: null, relayConnectionPool: null });
    assert.equal(result, null);
  });

  it("rejects invalid constructor args", () => {
    assert.throws(function () { return new DhtRouteResolver({}); }, /lookup/);
  });
});
