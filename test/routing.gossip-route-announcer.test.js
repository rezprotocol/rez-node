import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { GossipRouteAnnouncer } from "../src/routing/GossipRouteAnnouncer.js";

function createMockCtx({ routes = new Map(), selfRelayKeyId = "relay-self" } = {}) {
  const sent = [];
  const peerSockets = new Set();

  return {
    ctx: {
      peerSockets,
      routeTable: {
        get(id) { return routes.get(id) || null; },
        getAll() { return routes; },
      },
      selfRelayKeyId,
      encodeCtl(obj) {
        return new TextEncoder().encode(JSON.stringify(obj));
      },
      trySendFrame(socket, bytes) {
        const text = new TextDecoder().decode(bytes);
        sent.push({ socket, obj: JSON.parse(text) });
      },
      createAnnouncedRouteEntry(id, route, hops) {
        if (!route) return null;
        return {
          inboxId: id,
          hops,
          nextHopRelayKeyId: selfRelayKeyId,
          deliveryRelayKeyId: route.deliveryRelayKeyId || selfRelayKeyId,
          relayKeyId: route.deliveryRelayKeyId || selfRelayKeyId,
        };
      },
    },
    sent,
    peerSockets,
  };
}

describe("GossipRouteAnnouncer", () => {
  it("announceRoutes broadcasts to all peers", () => {
    const announcer = new GossipRouteAnnouncer();
    const routes = new Map([
      ["inbox:a", { hops: 0, direct: true, deliveryRelayKeyId: "relay-self" }],
    ]);
    const { ctx, sent, peerSockets } = createMockCtx({ routes });
    const socketA = { id: "a" };
    const socketB = { id: "b" };
    peerSockets.add(socketA);
    peerSockets.add(socketB);

    announcer.announceRoutes(["inbox:a"], 1, ctx);

    assert.equal(sent.length, 2);
    assert.equal(sent[0].obj._ctl, "inbox.route");
    assert.equal(sent[0].obj.entries[0].inboxId, "inbox:a");
    assert.equal(sent[0].obj.entries[0].hops, 1);
    assert.equal(sent[1].socket, socketB);
  });

  it("announceRoutesExcept skips excluded socket", () => {
    const announcer = new GossipRouteAnnouncer();
    const { ctx, sent, peerSockets } = createMockCtx();
    const socketA = { id: "a" };
    const socketB = { id: "b" };
    peerSockets.add(socketA);
    peerSockets.add(socketB);

    const entries = [{ inboxId: "inbox:x", hops: 2, nextHopRelayKeyId: "relay-self", deliveryRelayKeyId: "relay-peer" }];
    announcer.announceRoutesExcept(socketA, entries, ctx);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].socket, socketB);
    assert.equal(sent[0].obj._ctl, "inbox.route");
  });

  it("announceRoutesExcept does nothing for empty entries", () => {
    const announcer = new GossipRouteAnnouncer();
    const { ctx, sent, peerSockets } = createMockCtx();
    peerSockets.add({ id: "a" });

    announcer.announceRoutesExcept(null, [], ctx);
    assert.equal(sent.length, 0);
  });

  it("announceWithdraw broadcasts withdrawal to all peers", () => {
    const announcer = new GossipRouteAnnouncer();
    const { ctx, sent, peerSockets } = createMockCtx();
    const socketA = { id: "a" };
    peerSockets.add(socketA);

    announcer.announceWithdraw(["inbox:removed"], ctx);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].obj._ctl, "inbox.withdraw");
    assert.deepStrictEqual(sent[0].obj.inboxIds, ["inbox:removed"]);
  });

  it("announceAllToPeer sends full route table to single peer", () => {
    const announcer = new GossipRouteAnnouncer();
    const routes = new Map([
      ["inbox:a", { hops: 0, direct: true, deliveryRelayKeyId: "relay-self" }],
      ["inbox:b", { hops: 1, direct: false, deliveryRelayKeyId: "relay-peer" }],
    ]);
    const { ctx, sent } = createMockCtx({ routes });
    const socket = { id: "target" };

    announcer.announceAllToPeer(socket, ctx);

    assert.equal(sent.length, 1);
    assert.equal(sent[0].socket, socket);
    assert.equal(sent[0].obj._ctl, "inbox.route");
    assert.equal(sent[0].obj.entries.length, 2);
  });

  it("announceAllToPeer does nothing for empty route table", () => {
    const announcer = new GossipRouteAnnouncer();
    const { ctx, sent } = createMockCtx();
    const socket = { id: "target" };

    announcer.announceAllToPeer(socket, ctx);
    assert.equal(sent.length, 0);
  });

  it("reannounceAll sends to all peers", () => {
    const announcer = new GossipRouteAnnouncer();
    const routes = new Map([
      ["inbox:a", { hops: 0, direct: true, deliveryRelayKeyId: "relay-self" }],
    ]);
    const { ctx, sent, peerSockets } = createMockCtx({ routes });
    peerSockets.add({ id: "peer1" });
    peerSockets.add({ id: "peer2" });

    announcer.reannounceAll(ctx);

    assert.equal(sent.length, 2);
    assert.equal(sent[0].obj._ctl, "inbox.route");
    assert.equal(sent[1].obj._ctl, "inbox.route");
  });

  it("skips routes where createAnnouncedRouteEntry returns null", () => {
    const announcer = new GossipRouteAnnouncer();
    const routes = new Map([
      ["inbox:a", null], // createAnnouncedRouteEntry will return null for null route
    ]);
    const { ctx, sent, peerSockets } = createMockCtx({ routes });
    peerSockets.add({ id: "peer" });

    announcer.announceRoutes(["inbox:a"], 1, ctx);

    // No frame sent — all entries resolved to null
    assert.equal(sent.length, 0);
  });
});
