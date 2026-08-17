import test from "node:test";
import assert from "node:assert/strict";

import { DhtRouteAnnouncer } from "../src/routing/dht/DhtRouteAnnouncer.js";
import { DhtNode } from "../src/routing/dht/DhtNode.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeRelayIdentity } from "./support/relayIdentity.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

/**
 * A route announced while the k-buckets are empty is STOREd on zero peers.
 * That used to be silently indistinguishable from a successful publish: the
 * inbox was never stored anywhere, nothing retried it, and the only recovery
 * was `reannounceAll` — rate-limited to an hour in production. The inbox was
 * undiscoverable for that whole window.
 *
 * Found via CI, where the runner is slow enough to lose the registration/peering
 * race reliably. Across a full run the correlation was total: every inbox that
 * logged "STORE ... on 0 closest peers" failed every subsequent FIND_VALUE
 * (8/8), and every inbox stored on at least one peer resolved (8/8).
 *
 * The publish is now driven by the event that makes it possible — a peer
 * entering the k-bucket table — instead of a timer that assumes it already has.
 */

const SELF = makeRelayIdentity({ label: "self" });

function makeProtocol() {
  const stores = [];
  return {
    stores,
    sendStore(socket, inboxId, entry) { stores.push({ socket, inboxId, entry }); },
    storedIds() { return stores.map((s) => s.inboxId); },
  };
}

/** Minimal stand-in for the InboxRouter announcer context. */
function makeCtx() {
  const routes = new Map();
  return {
    routes,
    routeTable: {
      get(inboxId) {
        const route = routes.get(inboxId);
        return route === undefined ? null : route;
      },
      getAll() { return routes.entries(); },
    },
    createAnnouncedRouteEntry(inboxId, route, hops) {
      if (!route.registration) return null;
      return { inboxId, hops, registration: route.registration, deliveryRelayKeyId: route.deliveryRelayKeyId };
    },
  };
}

function addDirectRoute(ctx, inboxId, { registration = { inboxId, sig: "signed" } } = {}) {
  ctx.routes.set(inboxId, { direct: true, hops: 0, registration, deliveryRelayKeyId: SELF.relayKeyId });
}

function setup({ k = 20 } = {}) {
  const protocol = makeProtocol();
  const kBuckets = new KBucketTable(DhtNodeId.fromRelayKeyId(SELF.relayKeyId), { k });
  const announcer = new DhtRouteAnnouncer({ protocol, kBuckets, k });
  return { protocol, kBuckets, announcer, ctx: makeCtx() };
}

function addPeerToBuckets(kBuckets, label) {
  const peer = makeRelayIdentity({ label });
  const socket = { id: label, destroyed: false };
  const added = kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId(peer.relayKeyId), peer.relayKeyId, socket, 1_000);
  assert.equal(added, true, "precondition: the peer entered the k-bucket table");
  return socket;
}

test("an announce with no peers reaches nobody and is held for retry", () => {
  const { protocol, announcer, ctx } = setup();
  addDirectRoute(ctx, "inbox:alpha");

  announcer.announceRoutes(["inbox:alpha"], 1, ctx);

  assert.equal(protocol.stores.length, 0, "there was no peer to store on");
  assert.deepEqual(announcer.pendingAnnouncementIds, ["inbox:alpha"],
    "and the announcer must know the inbox is still unpublished");
});

test("gaining a peer publishes what the empty announce could not", () => {
  const { protocol, kBuckets, announcer, ctx } = setup();
  addDirectRoute(ctx, "inbox:alpha");
  announcer.announceRoutes(["inbox:alpha"], 1, ctx);

  const socket = addPeerToBuckets(kBuckets, "peer-1");
  const published = announcer.flushPendingAnnouncements();

  assert.equal(published, 1);
  assert.deepEqual(protocol.storedIds(), ["inbox:alpha"]);
  assert.equal(protocol.stores[0].socket, socket);
  assert.deepEqual(announcer.pendingAnnouncementIds, [], "and nothing is left pending");
});

test("a successful announce leaves nothing pending", () => {
  const { protocol, kBuckets, announcer, ctx } = setup();
  addPeerToBuckets(kBuckets, "peer-1");
  addDirectRoute(ctx, "inbox:alpha");

  announcer.announceRoutes(["inbox:alpha"], 1, ctx);

  assert.equal(protocol.stores.length, 1);
  assert.deepEqual(announcer.pendingAnnouncementIds, [],
    "a publish that reached a peer must not be retried");
});

test("flushing is a no-op in the steady state", () => {
  const { protocol, kBuckets, announcer } = setup();
  addPeerToBuckets(kBuckets, "peer-1");

  assert.equal(announcer.flushPendingAnnouncements(), 0);
  assert.equal(protocol.stores.length, 0);
});

test("a route withdrawn while pending is never published by the retry", () => {
  const { protocol, kBuckets, announcer, ctx } = setup();
  addDirectRoute(ctx, "inbox:alpha");
  announcer.announceRoutes(["inbox:alpha"], 1, ctx);
  assert.deepEqual(announcer.pendingAnnouncementIds, ["inbox:alpha"]);

  // Owner takes the inbox down before any peer ever arrived.
  ctx.routes.delete("inbox:alpha");
  announcer.announceWithdraw(["inbox:alpha"], ctx);

  addPeerToBuckets(kBuckets, "peer-1");
  const published = announcer.flushPendingAnnouncements();

  assert.equal(published, 0);
  assert.equal(protocol.stores.length, 0, "resurrecting a withdrawn route would be worse than never publishing it");
  assert.deepEqual(announcer.pendingAnnouncementIds, []);
});

test("a retry republishes live state, not the snapshot taken when it failed", () => {
  const { protocol, kBuckets, announcer, ctx } = setup();
  addDirectRoute(ctx, "inbox:alpha", { registration: { inboxId: "inbox:alpha", sig: "old" } });
  announcer.announceRoutes(["inbox:alpha"], 1, ctx);

  // The registration is re-issued while the announcement sits unpublished.
  addDirectRoute(ctx, "inbox:alpha", { registration: { inboxId: "inbox:alpha", sig: "renewed" } });

  addPeerToBuckets(kBuckets, "peer-1");
  announcer.flushPendingAnnouncements();

  assert.equal(protocol.stores.length, 1);
  assert.equal(protocol.stores[0].entry.registration.sig, "renewed",
    "publishing the stale snapshot would advertise a superseded registration");
});

test("a route that lost its claimant signature is dropped, not published unsigned (HIGH-8)", () => {
  const { protocol, kBuckets, announcer, ctx } = setup();
  addDirectRoute(ctx, "inbox:alpha");
  announcer.announceRoutes(["inbox:alpha"], 1, ctx);

  ctx.routes.set("inbox:alpha", { direct: true, hops: 0, registration: null, deliveryRelayKeyId: SELF.relayKeyId });

  addPeerToBuckets(kBuckets, "peer-1");
  announcer.flushPendingAnnouncements();

  assert.equal(protocol.stores.length, 0, "an unsigned entry must never reach the DHT");
  assert.deepEqual(announcer.pendingAnnouncementIds, []);
});

test("a route downgraded to transitively-learned is not republished as ours", () => {
  const { protocol, kBuckets, announcer, ctx } = setup();
  addDirectRoute(ctx, "inbox:alpha");
  announcer.announceRoutes(["inbox:alpha"], 1, ctx);

  // No longer directly hosted here — we cannot vouch for it any more.
  ctx.routes.set("inbox:alpha", {
    direct: false, hops: 2, registration: { inboxId: "inbox:alpha", sig: "signed" },
    deliveryRelayKeyId: SELF.relayKeyId,
  });

  addPeerToBuckets(kBuckets, "peer-1");
  announcer.flushPendingAnnouncements();

  assert.equal(protocol.stores.length, 0);
});

test("the pending buffer is bounded, and says so rather than dropping silently", () => {
  const { announcer, ctx } = setup();
  const warnings = [];
  const realWarn = console.warn;
  console.warn = (...args) => { warnings.push(args.map(String).join(" ")); };
  try {
    for (let i = 0; i < 300; i++) {
      const id = "inbox:" + i;
      addDirectRoute(ctx, id);
      announcer.announceRoutes([id], 1, ctx);
    }
  } finally {
    console.warn = realWarn;
  }

  assert.equal(announcer.pendingAnnouncementIds.length, 256);
  assert.ok(warnings.some((w) => w.includes("pending-announcement buffer full")),
    "an inbox we gave up on is unreachable — that must not be silent");
});

/**
 * The wiring test. Everything above exercises the announcer directly; this one
 * goes through `DhtNode.addPeer` — the single choke point where a peer enters
 * the k-bucket table, and the place the retry is actually triggered from. If
 * that call is ever dropped, the units above still pass and only this fails.
 */
test("DhtNode.addPeer publishes routes that were announced before it had peers", () => {
  const registry = new ControlMessageRegistry();
  const sent = [];
  const node = new DhtNode({
    selfRelayKeyId: SELF.relayKeyId,
    controlMessageRegistry: registry,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: (socket, bytes) => {
      sent.push({ socket, obj: JSON.parse(new TextDecoder().decode(bytes)) });
    },
    nowMs: () => 1000,
  });
  node.install();

  const peer = makeRelayIdentity({ label: "late-peer" });
  const { routeEntry } = makeSignedRouteEntry({
    inboxId: "inbox:published-late",
    deliveryRelayKeyId: SELF.relayKeyId,
  });
  const ctx = makeCtx();
  ctx.routes.set("inbox:published-late", {
    direct: true, hops: 0, registration: routeEntry.registration, deliveryRelayKeyId: SELF.relayKeyId,
  });

  // Registration wins the race against peering — the node has no peers yet.
  node.routeAnnouncer.announceRoutes(["inbox:published-late"], 1, ctx);
  assert.equal(sent.filter((f) => f.obj._ctl === "dht.store").length, 0,
    "precondition: nothing could be stored, there were no peers");
  assert.deepEqual(node.routeAnnouncer.pendingAnnouncementIds, ["inbox:published-late"]);

  // Peering completes.
  node.addPeer(peer.relayKeyId, { id: "late-peer-socket", destroyed: false });

  const storeFrames = sent.filter((f) => f.obj._ctl === "dht.store");
  assert.equal(storeFrames.length, 1, "the inbox must be published the moment a peer exists");
  assert.equal(storeFrames[0].obj.inboxId, "inbox:published-late");
  assert.deepEqual(node.routeAnnouncer.pendingAnnouncementIds, []);

  node.uninstall();
});

test("republish is still rate-limited, so the retry path is what actually recovers a lost publish", () => {
  let now = 0;
  const protocol = makeProtocol();
  const kBuckets = new KBucketTable(DhtNodeId.fromRelayKeyId(SELF.relayKeyId), { k: 20 });
  const announcer = new DhtRouteAnnouncer({
    protocol, kBuckets, k: 20, republishIntervalMs: 3_600_000, nowMs: () => now,
  });
  const ctx = makeCtx();
  addDirectRoute(ctx, "inbox:alpha");

  announcer.announceRoutes(["inbox:alpha"], 1, ctx);
  addPeerToBuckets(kBuckets, "peer-1");

  // The 30s mesh tick: the first reannounceAll republishes, but every
  // subsequent one inside the hour is a no-op. Without the flush, a publish
  // lost after that first tick waits out the full interval.
  now = 30_000;
  announcer.reannounceAll(ctx);
  const afterFirstTick = protocol.stores.length;
  now = 60_000;
  announcer.reannounceAll(ctx);

  assert.equal(protocol.stores.length, afterFirstTick,
    "republish is rate-limited to the hour — it is not a recovery path for a lost publish");
});
