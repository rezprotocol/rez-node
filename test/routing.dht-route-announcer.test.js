import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { DhtRouteAnnouncer } from "../src/routing/dht/DhtRouteAnnouncer.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

function makeSocket(label) {
  return { id: label, destroyed: false };
}

function createAnnouncer(opts = {}) {
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
    nowMs: () => 1000,
  });
  protocol.install();

  const announcer = new DhtRouteAnnouncer({
    protocol,
    kBuckets,
    k: opts.k || 20,
    republishIntervalMs: opts.republishIntervalMs || 3_600_000,
  });

  return { announcer, kBuckets, sent, selfNodeId };
}

function createCtx(routes) {
  const routeMap = new Map(Object.entries(routes || {}));
  return {
    routeTable: {
      get: function (id) { return routeMap.get(id) || null; },
      getAll: function () { return routeMap; },
    },
    selfRelayKeyId: "relay-self",
    createAnnouncedRouteEntry: function (id, route, hops) {
      if (!route) return null;
      const deliveryRelayKeyId = route.deliveryRelayKeyId || "relay-self";
      // For direct routes, attach a signed registration just like the
      // real `InboxRouter._createAnnouncedRouteEntry` does — HIGH-8
      // requires this for DHT-stored entries.
      if (route.direct === true) {
        const { routeEntry } = makeSignedRouteEntry({
          inboxId: id,
          deliveryRelayKeyId,
          hops: 0,
        });
        return routeEntry;
      }
      return {
        inboxId: id,
        hops,
        nextHopRelayKeyId: "relay-self",
        deliveryRelayKeyId,
      };
    },
  };
}

describe("DhtRouteAnnouncer", () => {
  it("announceRoutes STOREs on k-closest peers", () => {
    const { announcer, kBuckets, sent } = createAnnouncer();

    // Add peers to k-buckets
    const peerAId = DhtNodeId.fromRelayKeyId("peer-a");
    const peerBId = DhtNodeId.fromRelayKeyId("peer-b");
    kBuckets.addOrUpdate(peerAId, "peer-a", makeSocket("a"), 1000);
    kBuckets.addOrUpdate(peerBId, "peer-b", makeSocket("b"), 1000);

    const ctx = createCtx({
      "inbox:test": { hops: 0, direct: true, deliveryRelayKeyId: "relay-self" },
    });

    announcer.announceRoutes(["inbox:test"], 1, ctx);

    // Should have sent dht.store to both peers
    assert.ok(sent.length >= 1);
    for (const s of sent) {
      assert.equal(s.obj._ctl, "dht.store");
      assert.equal(s.obj.inboxId, "inbox:test");
    }
  });

  it("announceRoutes skips when route not in table", () => {
    const { announcer, kBuckets, sent } = createAnnouncer();
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-a"), "peer-a", makeSocket("a"), 1000);

    const ctx = createCtx({});
    announcer.announceRoutes(["inbox:missing"], 1, ctx);
    assert.equal(sent.length, 0);
  });

  it("announceRoutesExcept skips excluded socket", () => {
    const { announcer, kBuckets, sent } = createAnnouncer();
    const socketA = makeSocket("a");
    const socketB = makeSocket("b");
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-a"), "peer-a", socketA, 1000);
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-b"), "peer-b", socketB, 1000);

    const { routeEntry } = makeSignedRouteEntry({
      inboxId: "inbox:x",
      deliveryRelayKeyId: "relay-peer",
      hops: 0,
    });
    const ctx = createCtx({});
    announcer.announceRoutesExcept(socketA, [routeEntry], ctx);

    // Should send to socketB but not socketA
    assert.ok(sent.length > 0, "should have sent at least one store");
    for (const s of sent) {
      assert.notEqual(s.socket, socketA, "should not send to excluded socket");
    }
  });

  it("HIGH-8: announceRoutes skips routes WITHOUT a signed registration", () => {
    const { announcer, kBuckets, sent } = createAnnouncer();
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-a"), "peer-a", makeSocket("a"), 1000);

    // direct: false → createCtx returns an entry with no registration.
    const ctx = createCtx({
      "inbox:transitive": { hops: 2, direct: false, deliveryRelayKeyId: "relay-other" },
    });
    announcer.announceRoutes(["inbox:transitive"], 2, ctx);
    assert.equal(sent.length, 0, "transitive routes must not be DHT-stored");
  });

  it("announceRoutesExcept does nothing for empty entries", () => {
    const { announcer, sent } = createAnnouncer();
    announcer.announceRoutesExcept(null, [], createCtx({}));
    assert.equal(sent.length, 0);
  });

  it("announceWithdraw STOREs null on k-closest", () => {
    const { announcer, kBuckets, sent } = createAnnouncer();
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-a"), "peer-a", makeSocket("a"), 1000);

    announcer.announceWithdraw(["inbox:removed"], createCtx({}));

    assert.ok(sent.length >= 1);
    assert.equal(sent[0].obj._ctl, "dht.store");
    assert.equal(sent[0].obj.inboxId, "inbox:removed");
    assert.equal(sent[0].obj.routeEntry, null);
  });

  it("announceAllToPeer is a no-op", () => {
    const { announcer, sent } = createAnnouncer();
    const socket = makeSocket("new-peer");
    announcer.announceAllToPeer(socket, createCtx({}));
    assert.equal(sent.length, 0);
  });

  it("reannounceAll republishes direct routes only", () => {
    const { announcer, kBuckets, sent } = createAnnouncer({ republishIntervalMs: 0 });
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-a"), "peer-a", makeSocket("a"), 1000);

    const ctx = createCtx({
      "inbox:local": { hops: 0, direct: true, deliveryRelayKeyId: "relay-self" },
      "inbox:remote": { hops: 2, direct: false, deliveryRelayKeyId: "relay-other" },
    });

    announcer.reannounceAll(ctx);

    // Should only store the direct route
    const storeMessages = sent.filter(function (s) { return s.obj._ctl === "dht.store"; });
    const storedInboxIds = storeMessages.map(function (s) { return s.obj.inboxId; });
    assert.ok(storedInboxIds.includes("inbox:local"), "should republish direct route");
    assert.ok(!storedInboxIds.includes("inbox:remote"), "should not republish remote route");
  });

  it("reannounceAll rate-limits to republishIntervalMs", () => {
    const { announcer, kBuckets, sent } = createAnnouncer({ republishIntervalMs: 3_600_000 });
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-a"), "peer-a", makeSocket("a"), 1000);

    const ctx = createCtx({
      "inbox:local": { hops: 0, direct: true, deliveryRelayKeyId: "relay-self" },
    });

    announcer.reannounceAll(ctx);
    const firstCount = sent.length;
    assert.ok(firstCount > 0, "first reannounce should send");

    announcer.reannounceAll(ctx);
    assert.equal(sent.length, firstCount, "second reannounce should be throttled");
  });

  it("rejects invalid constructor args", () => {
    assert.throws(function () { return new DhtRouteAnnouncer({}); }, /protocol/);
  });
});
