import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { RouteTable } from "../src/routing/RouteTable.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { DhtRouteResolver } from "../src/routing/dht/DhtRouteResolver.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

/**
 * MED-9 regression suite — DHT-installed routes must not become
 * permanent next-hops. The three combined defenses:
 *   1. addRemote indexes peerSocket so socket-disconnect cleans up.
 *   2. addRemote accepts expiresAtMs; get() evicts on expiry.
 *   3. InboxRouter.routeDelivery evicts a remote entry on forward failure.
 */

function makeSocket(label) {
  return { id: label, destroyed: false };
}

describe("MED-9: RouteTable TTL eviction on get", () => {
  it("returns the entry before expiry", () => {
    const rt = new RouteTable();
    rt.addRemote("inbox:ttl", {
      hops: 1,
      nextHopRelayKeyId: "relay-a",
      deliveryRelayKeyId: "relay-a",
      nowMs: 1000,
      expiresAtMs: Date.now() + 60_000,
    });
    const entry = rt.get("inbox:ttl");
    assert.ok(entry, "non-expired entry must be returned");
    assert.equal(entry.deliveryRelayKeyId, "relay-a");
  });

  it("evicts and returns null when expiresAtMs is in the past", () => {
    const rt = new RouteTable();
    rt.addRemote("inbox:stale", {
      hops: 1,
      nextHopRelayKeyId: "relay-a",
      deliveryRelayKeyId: "relay-a",
      nowMs: 1000,
      expiresAtMs: Date.now() - 1, // already expired
    });
    assert.equal(rt.size, 1, "expired entry is initially present");
    const entry = rt.get("inbox:stale");
    assert.equal(entry, null, "expired entry must be evicted on read");
    assert.equal(rt.size, 0, "table is empty after eviction");
  });

  it("entries with no expiresAtMs are never evicted by TTL", () => {
    const rt = new RouteTable();
    rt.addRemote("inbox:noexpiry", {
      hops: 1,
      nextHopRelayKeyId: "relay-a",
      deliveryRelayKeyId: "relay-a",
      nowMs: 1000,
    });
    const entry = rt.get("inbox:noexpiry");
    assert.ok(entry, "entry without expiresAtMs survives");
    assert.equal(entry.expiresAtMs, null);
  });

  it("eviction cleans up reverse socket index", () => {
    const rt = new RouteTable();
    const peerSocket = makeSocket("p1");
    rt.addRemote("inbox:idx", {
      hops: 1,
      peerSocket,
      nextHopRelayKeyId: "relay-a",
      deliveryRelayKeyId: "relay-a",
      nowMs: 1000,
      expiresAtMs: Date.now() - 1,
    });
    rt.get("inbox:idx"); // triggers eviction
    // After eviction the reverse index must be empty — disconnecting the
    // peer socket later must not surface a phantom withdrawal for an
    // inbox that's already gone.
    const withdrawn = rt.removeAllForSocket(peerSocket);
    assert.equal(withdrawn.length, 0);
  });
});

describe("MED-9: addRemote indexes peerSocket for disconnect cleanup", () => {
  it("removeAllForSocket cleans up a DHT-installed route", () => {
    const rt = new RouteTable();
    const peerSocket = makeSocket("dht-responder");
    rt.addRemote("inbox:dht", {
      hops: 1,
      peerSocket,
      nextHopRelayKeyId: "relay-resp",
      deliveryRelayKeyId: "relay-host",
      nowMs: 1000,
    });
    assert.ok(rt.get("inbox:dht"), "route is installed");

    const withdrawn = rt.removeAllForSocket(peerSocket);
    assert.deepStrictEqual(withdrawn, ["inbox:dht"]);
    assert.equal(rt.get("inbox:dht"), null, "route removed after socket disconnect");
  });

  it("overwriting a remote route with a new peerSocket drops the old socket index", () => {
    const rt = new RouteTable();
    const oldSocket = makeSocket("old");
    const newSocket = makeSocket("new");

    rt.addRemote("inbox:over", {
      hops: 2,
      peerSocket: oldSocket,
      nextHopRelayKeyId: "relay-old",
      deliveryRelayKeyId: "relay-host",
      nowMs: 1000,
    });
    // Shorter path through a different socket overwrites.
    rt.addRemote("inbox:over", {
      hops: 1,
      peerSocket: newSocket,
      nextHopRelayKeyId: "relay-new",
      deliveryRelayKeyId: "relay-host",
      nowMs: 2000,
    });

    // The old socket index must NOT still claim inbox:over — otherwise
    // disconnecting the old socket would wrongly withdraw the route
    // that now flows through the new socket.
    const withdrawnOld = rt.removeAllForSocket(oldSocket);
    assert.deepStrictEqual(withdrawnOld, []);
    assert.ok(rt.get("inbox:over"), "route still present after old socket cleanup");

    const withdrawnNew = rt.removeAllForSocket(newSocket);
    assert.deepStrictEqual(withdrawnNew, ["inbox:over"]);
  });
});

describe("MED-9: DhtRouteResolver installs a TTL on resolved routes", () => {
  it("FIND_VALUE-installed remote route carries expiresAtMs ~5 min in the future", async () => {
    const selfRelayKeyId = "relay-self";
    const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
    const kBuckets = new KBucketTable(selfNodeId);
    const valueStore = new DhtValueStore();
    const registry = new ControlMessageRegistry();
    const sent = [];
    // Use real Date.now so RouteTable.get's expiry check (which itself
    // uses Date.now) agrees with the resolver's installation time.
    const nowMs = () => Date.now();

    const protocol = new DhtProtocol({
      kBuckets,
      valueStore,
      registry,
      selfNodeId,
      selfRelayKeyId,
      encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
      trySendFrame: (socket, bytes) => sent.push({ socket, obj: JSON.parse(new TextDecoder().decode(bytes)) }),
      queryTimeoutMs: 100,
      nowMs,
    });
    protocol.install();
    const lookup = new DhtLookup(kBuckets, { alpha: 3, k: 20 });
    const resolver = new DhtRouteResolver({ lookup, protocol, valueStore, nowMs });

    const peerSocket = makeSocket("responder");
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-resp"), "peer-resp", peerSocket, 1000);

    const realRouteTable = new RouteTable();
    const installBefore = Date.now();
    const promise = resolver.resolve("inbox:resolved", { routeTable: realRouteTable, relayConnectionPool: null });
    await new Promise((r) => setTimeout(r, 10));

    if (sent.length > 0) {
      const queryId = sent[0].obj.queryId;
      const { routeEntry } = makeSignedRouteEntry({
        inboxId: "inbox:resolved",
        deliveryRelayKeyId: "relay-host",
        hops: 0,
      });
      await registry.dispatch("dht.find_value.reply", {
        _ctl: "dht.find_value.reply",
        queryId,
        value: routeEntry,
        nodes: [],
      }, peerSocket);
    }

    await promise;
    const installAfter = Date.now();

    const installed = realRouteTable.get("inbox:resolved");
    assert.ok(installed, "route was installed in the local table");
    assert.equal(installed.direct, false);
    assert.ok(installed.expiresAtMs, "installed route carries an expiry");
    const ttl = installed.expiresAtMs - installBefore;
    const ttlUpper = installed.expiresAtMs - installAfter;
    // 5 minutes = 300_000 ms; allow slack on either side for test scheduling.
    assert.ok(ttl >= 4 * 60_000 && ttl <= 6 * 60_000, `TTL ~5min from installBefore, got ${ttl}ms`);
    assert.ok(ttlUpper >= 4 * 60_000 && ttlUpper <= 6 * 60_000, `TTL ~5min from installAfter, got ${ttlUpper}ms`);
  });
});

describe("MED-9: InboxRouter.routeDelivery evicts dead remote routes", () => {
  function buildRouter(getSocketImpl) {
    const directory = {
      getSocket: getSocketImpl,
      remove() {},
      isAuthenticatedSocket() { return true; },
      isAuthenticatedRelaySocket() { return true; },
      getAuth() { return null; },
      getRelayKeyIdForSocket() { return null; },
    };
    return new InboxRouter({
      transport: null,
      inboxStore: null,
      relayPeerDirectory: directory,
      logger: { error() {}, warn() {} },
      selfRelayKeyId: "relay-self",
    });
  }

  it("evicts when no peer socket is available for the next hop", async () => {
    const router = buildRouter(() => null);
    router.addRemoteRoute("inbox:dead", {
      hops: 1,
      peerSocket: makeSocket("ghost"),
      nextHopRelayKeyId: "relay-missing",
      deliveryRelayKeyId: "relay-host",
    });
    assert.ok(router.routeTable.get("inbox:dead"), "route is initially present");

    const ok = await router.routeDelivery("inbox:dead", new Uint8Array([1, 2, 3]));
    assert.equal(ok, false, "delivery fails when no socket");
    assert.equal(router.routeTable.get("inbox:dead"), null, "stale entry evicted so next deposit re-resolves");
  });

  it("does NOT evict a direct hosted-inbox entry when it has no socket", async () => {
    // Direct routes with null socket are local-hosted inboxes; routeDelivery
    // calls inboxStore.depositFromWire and must not evict the local route.
    const fakeStore = {
      depositFromWire: async () => {},
    };
    const directory = {
      getSocket: () => null,
      remove() {},
      isAuthenticatedSocket() { return true; },
      isAuthenticatedRelaySocket() { return true; },
      getAuth() { return null; },
      getRelayKeyIdForSocket() { return null; },
    };
    const router = new InboxRouter({
      transport: null,
      inboxStore: fakeStore,
      relayPeerDirectory: directory,
      logger: { error() {}, warn() {} },
      selfRelayKeyId: "relay-self",
    });
    router.registerLocal(["inbox:local"], null, { announce: false });
    const ok = await router.routeDelivery("inbox:local", new Uint8Array([9]));
    assert.equal(ok, true);
    assert.ok(router.routeTable.get("inbox:local"), "local hosted route survives delivery");
  });
});
