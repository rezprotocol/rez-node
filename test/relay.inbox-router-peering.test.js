import test from "node:test";
import assert from "node:assert/strict";
import { RMailbox, MemoryDataStore, createDefaultRegistry, encodeOuterPacket, newRoutingKey } from "@rezprotocol/core";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { encodeFrame, createFrameDecoder } from "../src/network/tcp/TcpFraming.js";
import { createClaimantNodeDelegation, createSessionIdentity } from "./helpers/wsAuth.js";
import { makeRelayIdentity } from "./support/relayIdentity.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Create a pair of mock sockets that simulate a TCP connection.
 *
 * socketA represents Router A's end of the connection.
 * socketB represents Router B's end of the connection.
 *
 * When Router A writes to socketA (its end), data arrives at socketB._onData.
 * When Router B writes to socketB (its end), data arrives at socketA._onData.
 *
 * Usage:
 *   routerA.addPeer(socketA)  — A writes to socketA, data goes to B
 *   routerB.addPeer(socketB)  — B writes to socketB, data goes to A
 *   wireSocket(socketA, routerA) — data arriving at A is dispatched to routerA
 *   wireSocket(socketB, routerB) — data arriving at B is dispatched to routerB
 */
function createMockSocketPair() {
  const socketA = {
    destroyed: false,
    _onData: null,
    write(data) {
      if (socketA.destroyed) return false;
      // Data written to A arrives at B
      if (socketB._onData) socketB._onData(data);
      return true;
    },
  };
  const socketB = {
    destroyed: false,
    _onData: null,
    write(data) {
      if (socketB.destroyed) return false;
      // Data written to B arrives at A
      if (socketA._onData) socketA._onData(data);
      return true;
    },
  };
  return { socketA, socketB };
}

/**
 * Wire up a socket to decode incoming frames and dispatch control messages to a router.
 * The socket's _onData handler receives data written by the OTHER side of the connection.
 */
function wireSocket(socket, router) {
  const decoder = createFrameDecoder(async (bytes) => {
    try {
      const obj = JSON.parse(new TextDecoder().decode(bytes));
      const result = router.handleControlMessage(obj, socket);
      await Promise.resolve(result);
    } catch {
      // Not a valid control message
    }
  });
  socket._onData = (chunk) => decoder.push(chunk);
}

/**
 * Authenticate a socket as a relay peer. `identity` is a self-certifying
 * relay identity from makeRelayIdentity() — free-string ids no longer pass
 * RelayPeerDirectory's ADR-RELAY-IDENTITY binding check.
 */
function authenticateRelaySocket(directory, socket, identity) {
  return directory.authenticate(socket, {
    relayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
}

function authenticateNodeSocket(directory, socket, identity) {
  const id = identity || makeRelayIdentity();
  return directory.authenticate(socket, {
    nodeKeyId: id.nodeKeyId,
    nodePublicKeyB64: id.nodePublicKeyB64,
    relayKeyId: id.relayKeyId,
    authLevel: "node",
  });
}

function createNodeRegistration({ socketAuth, inboxId }) {
  const identity = createSessionIdentity();
  return createClaimantNodeDelegation({
    claimantIdentity: identity,
    inboxId,
    nodeKeyId: socketAuth.nodeKeyId,
    nodePublicKeyB64: socketAuth.nodePublicKeyB64,
    relayKeyId: socketAuth.relayKeyId,
  });
}

/** Claimant-signed registration naming the given relay identity's triple. */
function createRegistrationFor(identity, inboxId, extra) {
  const claimant = createSessionIdentity();
  return createClaimantNodeDelegation({
    claimantIdentity: claimant,
    inboxId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    relayKeyId: identity.relayKeyId,
    ...(extra || {}),
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test("inbox.deposit control message deposits bytes into inboxStore", async () => {
  const inboxStore = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayPeerDirectory = new RelayPeerDirectory();
  const authedSocket = { destroyed: false };
  authenticateNodeSocket(relayPeerDirectory, authedSocket);
  const router = new InboxRouter({ inboxStore, relayPeerDirectory });
  router.registerLocal(["inbox:target"], null);

  const innerBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([10, 20, 30]) });
  const inner = Buffer.from(innerBytes).toString("base64");

  const result = router.handleControlMessage({
    _ctl: "inbox.deposit",
    inboxId: "inbox:target",
    inner,
  }, authedSocket);
  const handled = await Promise.resolve(result);

  assert.equal(handled, true, "should handle inbox.deposit");

  const deposited = await inboxStore.list("inbox:target");
  assert.equal(deposited.items.length, 1, "should have one deposited message");
});

test("inbox.deposit returns false without authentication", async () => {
  const inboxStore = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayPeerDirectory = new RelayPeerDirectory();
  const router = new InboxRouter({ inboxStore, relayPeerDirectory });
  router.registerLocal(["inbox:target"], null);

  const unauthSocket = { destroyed: false };
  const result = router.handleControlMessage({
    _ctl: "inbox.deposit",
    inboxId: "inbox:target",
    inner: Buffer.from([1, 2, 3]).toString("base64"),
  }, unauthSocket);
  const handled = await Promise.resolve(result);

  assert.equal(handled, false, "should reject unauthenticated deposit");
});

test("inbox.deposit returns false with missing inboxId", () => {
  const inboxStore = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayPeerDirectory = new RelayPeerDirectory();
  const authedSocket = { destroyed: false };
  authenticateNodeSocket(relayPeerDirectory, authedSocket);
  const router = new InboxRouter({ inboxStore, relayPeerDirectory });

  assert.equal(
    router.handleControlMessage({ _ctl: "inbox.deposit", inner: "AQID" }, authedSocket),
    false,
    "should reject missing inboxId"
  );
});

test("inbox.deposit returns false with missing inner", () => {
  const inboxStore = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayPeerDirectory = new RelayPeerDirectory();
  const authedSocket = { destroyed: false };
  authenticateNodeSocket(relayPeerDirectory, authedSocket);
  const router = new InboxRouter({ inboxStore, relayPeerDirectory });

  assert.equal(
    router.handleControlMessage({ _ctl: "inbox.deposit", inboxId: "inbox:x" }, authedSocket),
    false,
    "should reject missing inner"
  );
});

test("peered routers propagate routes via addPeer", () => {
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const relayDirB = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: idA.relayKeyId, relayPeerDirectory: relayDirA });
  const routerB = new InboxRouter({ selfRelayKeyId: idB.relayKeyId, relayPeerDirectory: relayDirB });

  const { socketA, socketB } = createMockSocketPair();
  // Wire: data arriving at socketA dispatched to routerA, data at socketB to routerB
  wireSocket(socketA, routerA);
  wireSocket(socketB, routerB);
  authenticateRelaySocket(relayDirA, socketA, idB);
  authenticateRelaySocket(relayDirB, socketB, idA);

  // Register an inbox on Router B before peering. Post-MED-8, the
  // route must carry a claimant-signed registration so the peer's
  // gossip announcement (hops=0+registration) is accepted at A.
  const mockNodeSocket = { destroyed: false, write() { return true; } };
  const registration = createRegistrationFor(idB, "inbox:nodeB");
  routerB.registerLocal(["inbox:nodeB"], mockNodeSocket, { registrations: [registration] });

  // Router A writes to socketA, data arrives at socketB → dispatched to routerB
  // Router B writes to socketB, data arrives at socketA → dispatched to routerA
  routerA.addPeer(socketA);
  routerB.addPeer(socketB);

  // Router B called addPeer(socketB), which announces all routes to socketB.
  // socketB.write → socketA._onData → routerA receives the inbox.route message.
  const route = routerA.getRouteTo("inbox:nodeB");
  assert.ok(route, "Router A should have a route to inbox:nodeB");
  assert.equal(route.direct, false, "should be a remote route");
  // Post-MED-8: gossip only carries hops=0 entries (peer-direct, signed).
  assert.equal(route.hops, 0, "must be hops=0 with claimant proof");
});

test("peered routers propagate new registrations", () => {
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const relayDirB = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: idA.relayKeyId, relayPeerDirectory: relayDirA });
  const routerB = new InboxRouter({ selfRelayKeyId: idB.relayKeyId, relayPeerDirectory: relayDirB });

  const { socketA, socketB } = createMockSocketPair();
  wireSocket(socketA, routerA);
  wireSocket(socketB, routerB);
  authenticateRelaySocket(relayDirA, socketA, idB);
  authenticateRelaySocket(relayDirB, socketB, idA);

  // Peer first, register second
  routerA.addPeer(socketA);
  routerB.addPeer(socketB);

  // Now register inbox on Router B with a signed registration — should
  // announce to peer Router A as hops=0+proof (MED-8 requires the proof).
  const mockSocket = { destroyed: false, write() { return true; } };
  const registration = createRegistrationFor(idB, "inbox:late-register");
  routerB.registerLocal(["inbox:late-register"], mockSocket, { registrations: [registration] });

  // Router A should learn about it via the peer announcement
  const route = routerA.getRouteTo("inbox:late-register");
  assert.ok(route, "Router A should learn about late registration");
  assert.equal(route.direct, false, "should be remote");
});

test("reannounceAllRoutesToPeers replays the full route table to connected peers", () => {
  const router = new InboxRouter({ selfRelayKeyId: "relay-a" });
  const seen = [];
  const decoder = createFrameDecoder((bytes) => {
    seen.push(JSON.parse(new TextDecoder().decode(bytes)));
  });
  const peerSocket = {
    destroyed: false,
    write(data) {
      decoder.push(data);
      return true;
    },
  };

  router.addPeer(peerSocket);
  router.registerLocal(["inbox:local"], { destroyed: false, write() { return true; } });
  router.addRemoteRoute("inbox:remote", {
    hops: 1,
    nextHopRelayKeyId: "relay-b",
    deliveryRelayKeyId: "relay-b",
  });
  seen.length = 0;

  router.reannounceAllRoutesToPeers();

  assert.equal(seen.length, 1);
  assert.equal(seen[0]._ctl, "inbox.route");
  assert.deepEqual(
    seen[0].entries.map((entry) => entry.inboxId).sort(),
    ["inbox:local", "inbox:remote"],
  );
});

test("registerLocal can keep a local-only route without announcing to peers", () => {
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const relayDirB = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: idA.relayKeyId, relayPeerDirectory: relayDirA });
  const routerB = new InboxRouter({ selfRelayKeyId: idB.relayKeyId, relayPeerDirectory: relayDirB });

  const { socketA, socketB } = createMockSocketPair();
  wireSocket(socketA, routerA);
  wireSocket(socketB, routerB);
  authenticateRelaySocket(relayDirA, socketA, idB);
  authenticateRelaySocket(relayDirB, socketB, idA);

  routerB.registerLocal(["inbox:private-leaf"], null, { announce: false });
  routerA.addPeer(socketA);
  routerB.addPeer(socketB);

  assert.equal(routerB.isLocalHostedInbox("inbox:private-leaf"), true);
  assert.equal(routerA.getRouteTo("inbox:private-leaf"), null);
});

test("peered routers propagate explicit inbox.withdraw", () => {
  // Explicit inbox.withdraw is the only thing that removes a registered
  // local route — owner socket disconnect alone preserves the route so
  // deposits buffer at the relay until reconnect (see the survival
  // tests below).
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const relayDirB = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: idA.relayKeyId, relayPeerDirectory: relayDirA });
  const routerB = new InboxRouter({ selfRelayKeyId: idB.relayKeyId, relayPeerDirectory: relayDirB });

  const { socketA, socketB } = createMockSocketPair();
  wireSocket(socketA, routerA);
  wireSocket(socketB, routerB);
  authenticateRelaySocket(relayDirA, socketA, idB);
  authenticateRelaySocket(relayDirB, socketB, idA);

  const mockSocket = { destroyed: false, write() { return true; } };
  const registration = createRegistrationFor(idB, "inbox:will-withdraw");
  // Authenticate mockSocket as the node that registered the inbox so
  // _handleWithdraw's installerSocket check passes.
  relayDirB.authenticate(mockSocket, {
    relayKeyId: idB.relayKeyId,
    nodeKeyId: idB.nodeKeyId,
    nodePublicKeyB64: idB.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  routerB.registerLocal(["inbox:will-withdraw"], mockSocket, { registrations: [registration] });

  routerA.addPeer(socketA);
  routerB.addPeer(socketB);

  assert.ok(routerA.getRouteTo("inbox:will-withdraw"), "A should have route before withdraw");

  // Owner sends an explicit inbox.withdraw — handled on B, propagated to A.
  routerB.handleControlMessage(
    { _ctl: "inbox.withdraw", inboxIds: ["inbox:will-withdraw"] },
    mockSocket,
  );

  assert.equal(routerB.getRouteTo("inbox:will-withdraw"), null, "B drops the route on explicit withdraw");
  assert.equal(routerA.getRouteTo("inbox:will-withdraw"), null, "A's cached route also drops via gossip");
});

test("owner socket disconnect preserves registered local route and does not propagate withdraw", () => {
  // The route survival contract: when an inbox owner's node disconnects
  // (crash, network drop, app close) without sending inbox.withdraw,
  // the relay must keep its "this inbox is hosted here" record so that
  // 1) subsequent deposits from peers buffer locally via the
  //    entry.direct && !entry.socket branch of routeDelivery, and
  // 2) peer relays keep their hops=1 route caches pointing at us.
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const relayDirB = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: idA.relayKeyId, relayPeerDirectory: relayDirA });
  const routerB = new InboxRouter({ selfRelayKeyId: idB.relayKeyId, relayPeerDirectory: relayDirB });

  const { socketA, socketB } = createMockSocketPair();
  wireSocket(socketA, routerA);
  wireSocket(socketB, routerB);
  authenticateRelaySocket(relayDirA, socketA, idB);
  authenticateRelaySocket(relayDirB, socketB, idA);

  const mockSocket = { destroyed: false, write() { return true; } };
  const registration = createRegistrationFor(idB, "inbox:offline-survive");
  routerB.registerLocal(["inbox:offline-survive"], mockSocket, { registrations: [registration] });

  routerA.addPeer(socketA);
  routerB.addPeer(socketB);

  assert.ok(routerA.getRouteTo("inbox:offline-survive"), "A should have route before owner disconnect");

  // Owner socket goes away with no inbox.withdraw — simulating a crash
  // or app close.
  routerB.removeConnection(mockSocket);

  const survived = routerB.getRouteTo("inbox:offline-survive");
  assert.ok(survived, "B must keep the route after owner socket disconnect");
  assert.equal(survived.direct, true, "preserved entry is still direct");
  assert.equal(survived.socket, null, "preserved entry has socket nulled — triggers buffering branch");
  assert.equal(routerB.isLocalHostedInbox("inbox:offline-survive"), true,
    "isLocalHostedInbox returns true so deposit fallback fires");

  assert.ok(routerA.getRouteTo("inbox:offline-survive"),
    "A's cached route must NOT be withdrawn — B is still the host");
});

test("routeDelivery sends inbox.deposit control message for remote routes", async () => {
  const sent = [];
  const peerSocket = {
    destroyed: false,
    write(data) {
      sent.push(Buffer.isBuffer(data) ? new Uint8Array(data) : data);
      return true;
    },
  };
  const relayPeerDirectory = new RelayPeerDirectory();
  const idNext = makeRelayIdentity();
  const idDelivery = makeRelayIdentity();
  relayPeerDirectory.authenticate(peerSocket, {
    relayKeyId: idNext.relayKeyId,
    nodeKeyId: idNext.nodeKeyId,
    nodePublicKeyB64: idNext.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  const routerA = new InboxRouter({ relayPeerDirectory, selfRelayKeyId: "relay-a" });

  // Add a remote route manually (simulating route learned from peer relay)
  routerA.addRemoteRoute("inbox:remote", {
    hops: 1,
    nextHopRelayKeyId: idNext.relayKeyId,
    deliveryRelayKeyId: idDelivery.relayKeyId,
    peerSocket,
  });

  const innerBytes = new Uint8Array([7, 8, 9]);
  const result = await routerA.routeDelivery("inbox:remote", innerBytes);

  assert.equal(result, true, "routeDelivery should succeed for remote route");
  assert.equal(sent.length, 1, "should have sent one message");

  // Verify the sent bytes are an inbox.deposit control message
  const payload = sent[0].length > 4 ? sent[0].subarray(4) : sent[0];
  const ctlText = new TextDecoder().decode(payload);
  const ctl = JSON.parse(ctlText);
  assert.equal(ctl._ctl, "inbox.deposit");
  assert.equal(ctl.inboxId, "inbox:remote");
  assert.equal(typeof ctl.inner, "string", "inner should be base64 string");

  // Verify the inner bytes round-trip correctly
  const decoded = new Uint8Array(Buffer.from(ctl.inner, "base64"));
  assert.deepEqual(decoded, innerBytes, "inner bytes should round-trip");

});

test("routeDelivery sends inbox.deposit control message for direct socket route", async () => {
  const writes = [];
  const mockSocket = {
    destroyed: false,
    write(data) {
      writes.push(Buffer.isBuffer(data) ? new Uint8Array(data) : data);
      return true;
    },
  };

  const router = new InboxRouter();
  router.registerLocal(["inbox:direct"], mockSocket);

  const innerBytes = new Uint8Array([1, 2, 3, 0xff]);
  const result = await router.routeDelivery("inbox:direct", innerBytes);

  assert.equal(result, true, "routeDelivery should succeed for direct route");
  assert.equal(writes.length, 1, "should have written one frame");

  // Frame is length-prefixed (4 bytes BE) + payload
  const frame = writes[0];
  assert.ok(frame.length >= 4, "frame should have length prefix");
  const payload = frame.length > 4 ? frame.subarray(4) : new Uint8Array(0);
  const ctlText = new TextDecoder().decode(payload);
  const ctl = JSON.parse(ctlText);
  assert.equal(ctl._ctl, "inbox.deposit");
  assert.equal(ctl.inboxId, "inbox:direct");
  assert.equal(typeof ctl.inner, "string", "inner should be base64 string");
  const decoded = new Uint8Array(Buffer.from(ctl.inner, "base64"));
  assert.deepEqual(decoded, innerBytes, "inner bytes should round-trip");
});

test("routeDelivery returns false when no route exists", async () => {
  const router = new InboxRouter();
  const result = await router.routeDelivery("inbox:nonexistent", new Uint8Array([1]));
  assert.equal(result, false);
});

test("registerLocal replaces stale direct socket route for same inbox", async () => {
  const router = new InboxRouter();
  const writesOld = [];
  const writesNew = [];
  const oldSocket = {
    destroyed: false,
    write(data) {
      writesOld.push(data);
      return true;
    },
  };
  const newSocket = {
    destroyed: false,
    write(data) {
      writesNew.push(data);
      return true;
    },
  };

  router.registerLocal(["inbox:same"], oldSocket);
  router.registerLocal(["inbox:same"], newSocket);

  const routed = await router.routeDelivery("inbox:same", new Uint8Array([1, 2, 3]));
  assert.equal(routed, true);
  assert.equal(writesOld.length, 0, "should not use stale socket route");
  assert.equal(writesNew.length, 1, "should route via latest socket");
});

test("InboxRouter constructor is backward-compatible without inboxStore", () => {
  // Existing code passes no inboxStore — should still work
  const router = new InboxRouter();
  assert.equal(router.size, 0);
  router.registerLocal(["inbox:test"], null);
  assert.equal(router.size, 1);
});

test("_handleRegister accepts signed registrations from node-authenticated sockets", () => {
  const relayPeerDirectory = new RelayPeerDirectory();
  const router = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory });
  const socket = {
    destroyed: false,
    write() {
      return true;
    },
  };
  const nodeIdentity = makeRelayIdentity();
  const auth = relayPeerDirectory.authenticate(socket, {
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    relayKeyId: nodeIdentity.relayKeyId,
    authLevel: "node",
  });
  const ok = router.handleControlMessage({
    _ctl: "inbox.register",
    registrations: [
      createNodeRegistration({
        socketAuth: auth,
        inboxId: "inbox:a",
      }),
    ],
  }, socket);
  assert.equal(ok, true);
  const route = router.getRouteTo("inbox:a");
  assert.ok(route, "route should be created");
  assert.equal(route.direct, true);
  assert.equal(route.socket, socket);
  assert.equal(route.nextHopRelayKeyId, "relay-a");
  assert.equal(route.deliveryRelayKeyId, nodeIdentity.relayKeyId,
    "the route must preserve the relay identity covered by the claimant signature");
  assert.equal(route.announceToPeers, false, "node-authenticated sockets should not gain relay gossip authority");
});

test("addRemoteRoute announces to other peers excluding source", () => {
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a" });
  const routerB = new InboxRouter({ selfRelayKeyId: "relay-b" });
  const { socketA, socketB } = createMockSocketPair();
  const toB = [];
  const toA = [];
  socketA.write = function (data) {
    toA.push(data);
    return true;
  };
  socketB.write = function (data) {
    toB.push(data);
    return true;
  };
  wireSocket(socketA, routerA);
  wireSocket(socketB, routerB);
  routerA.addPeer(socketA);
  routerA.addPeer(socketB);
  toA.length = 0;
  toB.length = 0;
  routerA.addRemoteRoute("inbox:remote-via-b", {
    hops: 1,
    peerSocket: socketB,
    nextHopRelayKeyId: "relay-b",
    deliveryRelayKeyId: "relay-c",
  });
  const toAAfterRemote = toA.filter((buf) => {
    try {
      const payload = buf.length > 4 ? buf.subarray(4) : buf;
      const obj = JSON.parse(new TextDecoder().decode(payload));
      return obj._ctl === "inbox.route" && Array.isArray(obj.entries)
        && obj.entries.some((e) => e.inboxId === "inbox:remote-via-b");
    } catch { return false; }
  });
  const toBAfterRemote = toB.filter((buf) => {
    try {
      const payload = buf.length > 4 ? buf.subarray(4) : buf;
      const obj = JSON.parse(new TextDecoder().decode(payload));
      return obj._ctl === "inbox.route" && Array.isArray(obj.entries)
        && obj.entries.some((e) => e.inboxId === "inbox:remote-via-b");
    } catch { return false; }
  });
  assert.ok(toAAfterRemote.length >= 1, "A should announce new remote route to its other peer");
  assert.equal(toBAfterRemote.length, 0, "should not send route back to source peer");
});

test("_handleDeposit tries routeDelivery first then local deposit", async () => {
  const inboxStore = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayPeerDirectory = new RelayPeerDirectory();
  const authedSocket = { destroyed: false };
  authenticateNodeSocket(relayPeerDirectory, authedSocket);
  const writes = [];
  const directSocket = {
    destroyed: false,
    write(data) {
      writes.push(data);
      return true;
    },
  };
  const router = new InboxRouter({ inboxStore, relayPeerDirectory });
  router.registerLocal(["inbox:direct"], directSocket);

  const result = router.handleControlMessage({
    _ctl: "inbox.deposit",
    inboxId: "inbox:direct",
    inner: Buffer.from([1, 2, 3]).toString("base64"),
  }, authedSocket);
  const handled = await Promise.resolve(result);
  assert.equal(handled, true);
  assert.equal(writes.length, 1, "should route to direct socket, not local store");
  const payload = writes[0].length > 4 ? writes[0].subarray(4) : writes[0];
  const ctl = JSON.parse(new TextDecoder().decode(payload));
  assert.equal(ctl._ctl, "inbox.deposit");
  assert.equal(ctl.inboxId, "inbox:direct");

  const noRouteResult = router.handleControlMessage({
    _ctl: "inbox.deposit",
    inboxId: "inbox:local-only",
    inner: Buffer.from([4, 5, 6]).toString("base64"),
  }, authedSocket);
  const noRouteHandled = await Promise.resolve(noRouteResult);
  assert.equal(noRouteHandled, false, "should reject unknown non-local inbox");

  router.registerLocal(["inbox:local-only"], null);
  const validLocalBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([4, 5, 6]) });
  const localResult = router.handleControlMessage({
    _ctl: "inbox.deposit",
    inboxId: "inbox:local-only",
    inner: Buffer.from(validLocalBytes).toString("base64"),
  }, authedSocket);
  const localHandled = await Promise.resolve(localResult);
  assert.equal(localHandled, true);
  const deposited = await inboxStore.list("inbox:local-only");
  assert.equal(deposited.items.length, 1, "should deposit to local hosted inbox");
});

// ---------------------------------------------------------------------------
// Proof-Carrying Route Authority Tests
// ---------------------------------------------------------------------------

test("_handleRoute rejects hops=0 without registration proof", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const peerSocket = { destroyed: false, write() { return true; } };
  authenticateRelaySocket(relayDirA, peerSocket, idB);

  const result = routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:hijack",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
    }],
  }, peerSocket);

  assert.equal(routerA.getRouteTo("inbox:hijack"), null, "hops=0 without proof should be rejected");
});

test("_handleRoute accepts hops=0 with valid registration proof", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const peerSocket = { destroyed: false, write() { return true; } };
  const peerAuth = authenticateRelaySocket(relayDirA, peerSocket, idB);
  assert.ok(peerAuth, "peer relay must authenticate with a bound identity");

  const registration = createRegistrationFor(idB, "inbox:proven");

  const result = routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:proven",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, peerSocket);

  assert.equal(result, true, "should accept hops=0 with valid proof");
  const route = routerA.getRouteTo("inbox:proven");
  assert.ok(route, "route should be created");
  assert.equal(route.hops, 0, "should be hops=0");
});

test("_handleRoute rejects hops=0 with expired registration", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const peerSocket = { destroyed: false, write() { return true; } };
  assert.ok(authenticateRelaySocket(relayDirA, peerSocket, idB));

  const pastMs = Date.now() - 100000;
  const registration = createRegistrationFor(idB, "inbox:expired", {
    issuedAtMs: pastMs - 100000,
    expiresAtMs: pastMs,
  });

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:expired",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, peerSocket);

  assert.equal(routerA.getRouteTo("inbox:expired"), null, "expired registration should be rejected");
});

test("_handleRoute rejects hops=0 with mismatched inboxId", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const peerSocket = { destroyed: false, write() { return true; } };
  assert.ok(authenticateRelaySocket(relayDirA, peerSocket, idB));

  const registration = createRegistrationFor(idB, "inbox:other");

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:target",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, peerSocket);

  assert.equal(routerA.getRouteTo("inbox:target"), null, "mismatched inboxId should be rejected");
});

test("_handleRoute rejects hops=0 with mismatched deliveryRelayKeyId", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const peerSocket = { destroyed: false, write() { return true; } };
  assert.ok(authenticateRelaySocket(relayDirA, peerSocket, idB));

  const registration = createRegistrationFor(idB, "inbox:mismatch");

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:mismatch",
      hops: 0,
      deliveryRelayKeyId: "relay-evil",
      registration,
    }],
  }, peerSocket);

  assert.equal(routerA.getRouteTo("inbox:mismatch"), null, "mismatched deliveryRelayKeyId should be rejected");
});

test("_handleWithdraw rejects withdrawal from non-installer socket", () => {
  const idB = makeRelayIdentity();
  const idEvil = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const installerSocket = { destroyed: false, write() { return true; } };
  authenticateRelaySocket(relayDirA, installerSocket, idB);

  const attackerSocket = { destroyed: false, write() { return true; } };
  authenticateRelaySocket(relayDirA, attackerSocket, idEvil);

  // Install a route via valid proof
  const registration = createRegistrationFor(idB, "inbox:protected");

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:protected",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, installerSocket);

  assert.ok(routerA.getRouteTo("inbox:protected"), "route should exist before withdrawal attempt");

  // Attacker tries to withdraw
  routerA.handleControlMessage({
    _ctl: "inbox.withdraw",
    inboxIds: ["inbox:protected"],
  }, attackerSocket);

  assert.ok(routerA.getRouteTo("inbox:protected"), "route should survive withdrawal from non-installer");
});

test("_handleWithdraw allows withdrawal from installer socket", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const installerSocket = { destroyed: false, write() { return true; } };
  authenticateRelaySocket(relayDirA, installerSocket, idB);

  const registration = createRegistrationFor(idB, "inbox:withdrawable");

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:withdrawable",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, installerSocket);

  assert.ok(routerA.getRouteTo("inbox:withdrawable"), "route should exist");

  routerA.handleControlMessage({
    _ctl: "inbox.withdraw",
    inboxIds: ["inbox:withdrawable"],
  }, installerSocket);

  assert.equal(routerA.getRouteTo("inbox:withdrawable"), null, "installer should be able to withdraw");
});

test("removeConnection cleans up installer routes", () => {
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const peerSocket = { destroyed: false, write() { return true; } };
  authenticateRelaySocket(relayDirA, peerSocket, idB);
  routerA.addPeer(peerSocket);

  const registration = createRegistrationFor(idB, "inbox:cleanup");

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:cleanup",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, peerSocket);

  assert.ok(routerA.getRouteTo("inbox:cleanup"), "route should exist before disconnect");

  routerA.removeConnection(peerSocket);

  assert.equal(routerA.getRouteTo("inbox:cleanup"), null, "route should be removed on disconnect");
});

test("announcements include registration proof for hops=0", () => {
  const nodeIdentity = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: "relay-a", relayPeerDirectory: relayDirA });

  const nodeSocket = { destroyed: false, write() { return true; } };
  const nodeAuth = relayDirA.authenticate(nodeSocket, {
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    relayKeyId: nodeIdentity.relayKeyId,
    authLevel: "node",
  });
  assert.ok(nodeAuth, "node socket must authenticate");

  const identity = createSessionIdentity();
  const registration = createClaimantNodeDelegation({
    claimantIdentity:identity,
    inboxId: "inbox:announced",
    nodeKeyId: nodeAuth.nodeKeyId,
    nodePublicKeyB64: nodeAuth.nodePublicKeyB64,
    relayKeyId: nodeAuth.relayKeyId,
  });

  // Register via _handleRegister so registration is stored
  routerA.handleControlMessage({
    _ctl: "inbox.register",
    registrations: [registration],
  }, nodeSocket);

  // Now add a relay peer and capture what gets announced
  const seen = [];
  const decoder = createFrameDecoder((bytes) => {
    seen.push(JSON.parse(new TextDecoder().decode(bytes)));
  });
  const peerSocket = {
    destroyed: false,
    write(data) {
      decoder.push(data);
      return true;
    },
  };
  authenticateRelaySocket(relayDirA, peerSocket, idB);
  routerA.addPeer(peerSocket);

  // Find the announcement for our inbox
  const routeMsg = seen.find(function (msg) { return msg._ctl === "inbox.route"; });
  assert.ok(routeMsg, "should announce routes to peer");
  const announcedEntry = routeMsg.entries.find(function (e) { return e.inboxId === "inbox:announced"; });
  assert.ok(announcedEntry, "should include the registered inbox");
  assert.equal(announcedEntry.hops, 0, "should announce at hops=0 with proof");
  assert.equal(announcedEntry.deliveryRelayKeyId, nodeIdentity.relayKeyId,
    "the announced delivery relay must match the signed registration");
  assert.ok(announcedEntry.registration, "should include registration proof");
  assert.equal(announcedEntry.registration.inboxId, "inbox:announced");
});

test("MED-8: re-gossip at hops>0 (transitive trust) is REJECTED by the receiver", () => {
  // Codifies the audit fix: A learned a route at hops=0 from B; when A
  // peers with C, A's re-gossip increments to hops=1 with no signed
  // registration. C must NOT install this transitive route — cross-mesh
  // discovery is the DHT's job (HIGH-8-anchored). Otherwise any
  // authenticated peer relay could advertise itself as next-hop for any
  // inbox it claims to have heard about.
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const idC = makeRelayIdentity();
  const relayDirA = new RelayPeerDirectory();
  const relayDirC = new RelayPeerDirectory();
  const routerA = new InboxRouter({ selfRelayKeyId: idA.relayKeyId, relayPeerDirectory: relayDirA });
  const routerC = new InboxRouter({ selfRelayKeyId: idC.relayKeyId, relayPeerDirectory: relayDirC });

  // Socket between A and B (installer)
  const peerSocketAB = { destroyed: false, write() { return true; } };
  authenticateRelaySocket(relayDirA, peerSocketAB, idB);

  // Install a verified hops=0 route on A
  const registration = createRegistrationFor(idB, "inbox:regossip");

  routerA.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:regossip",
      hops: 0,
      deliveryRelayKeyId: idB.relayKeyId,
      registration,
    }],
  }, peerSocketAB);

  assert.ok(routerA.getRouteTo("inbox:regossip"), "A should have the route");

  // Now peer A with C via mock socket pair
  const { socketA, socketB: socketC } = createMockSocketPair();
  wireSocket(socketA, routerA);
  wireSocket(socketC, routerC);
  authenticateRelaySocket(relayDirA, socketA, idC);
  authenticateRelaySocket(relayDirC, socketC, idA);

  routerA.addPeer(socketA);
  routerC.addPeer(socketC);

  // C must NOT install A's transitive (hops=1, no signed proof) re-gossip.
  assert.equal(routerC.getRouteTo("inbox:regossip"), null,
    "MED-8: transitive re-gossip without proof must not install");
});
