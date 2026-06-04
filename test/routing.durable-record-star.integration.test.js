import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNode } from "../src/routing/dht/DhtNode.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRecord } from "./support/durableRecord.js";

/**
 * Star-with-core durable-record topology — the REAL electron-first shape that
 * the fully-connected mesh integration test does NOT model.
 *
 * Production: each user runs a leaf node that connects egress-first to backbone
 * relays. The backbone relays form a connected core (relay-verified, mutual
 * k-bucket peers). A leaf adds its entry relay to its OWN k-buckets, but the
 * relay does NOT add the leaf (the leaf is relay-provisional from the relay's
 * view — SocketFrameRouter gates dhtNode.addPeer on relay-verified). So:
 *
 *   leafA ── relay0 ═══ relay1 ── leafB        (── leaf link, ═══ core link)
 *
 * with the asymmetry: leafX.addPeer(relayN) but NOT relayN.addPeer(leafX).
 *
 * A record published by leafA lands on leafA's entry relay (relay0). leafB can
 * only reach relay1. Today leafB.getRecord runs its OWN iterative lookup over
 * its sparse k-buckets ({relay1}); relay1 doesn't hold the record and only
 * returns socket-less node hints leafB can never dial — so the GET returns null
 * even though the record sits one core-hop away and the publisher is ONLINE.
 * This reproduces `acceptInvite: invite envelope not found`.
 *
 * The fix makes record traffic delegate to the connected core (the same way
 * chat deposits delegate to a relay): the entry relay resolves the lookup
 * across the core on the leaf's behalf. After the fix leafB.getRecord resolves.
 */

function makeNode(relayKeyId, clock, k, deliver) {
  const registry = new ControlMessageRegistry();
  const node = new DhtNode({
    selfRelayKeyId: relayKeyId,
    controlMessageRegistry: registry,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: deliver,
    nowMs: () => clock.now,
    config: { k, alpha: 3, queryTimeoutMs: 2000, recordReplicateIntervalMs: 0 },
  });
  node.install();
  return { relayKeyId, registry, node, nodeId: DhtNodeId.fromRelayKeyId(relayKeyId), alive: true };
}

// Directed-socket delivery identical to the mesh harness: a write returns
// immediately; the frame dispatches on the peer in a later tick.
function makeDeliver() {
  return function deliver(socket, bytes) {
    if (!socket || socket.destroyed === true) return;
    const peer = socket._peer;
    if (!peer || !peer.alive) return;
    const obj = JSON.parse(new TextDecoder().decode(bytes));
    queueMicrotask(() => {
      if (!peer.alive || socket.destroyed === true) return;
      peer.registry.dispatch(obj._ctl, obj, socket._peerSocket).catch(() => {});
    });
  };
}

// Wire a bidirectional directed-socket pair between a and b. `aAddsB`/`bAddsA`
// control which side inserts the other into its k-bucket table — the relay←leaf
// asymmetry is modelled by leaving bAddsA false for a leaf→relay link. Both
// endpoints still exist so the relay can REPLY on the arrival socket even when
// it has not added the leaf as a routing peer.
function connect(a, b, { aAddsB = true, bAddsA = true } = {}) {
  const epAB = { id: a.relayKeyId + "->" + b.relayKeyId, destroyed: false };
  const epBA = { id: b.relayKeyId + "->" + a.relayKeyId, destroyed: false };
  epAB._peer = b; epAB._peerSocket = epBA;
  epBA._peer = a; epBA._peerSocket = epAB;
  if (aAddsB) a.node.addPeer(b.relayKeyId, epAB);
  if (bAddsA) b.node.addPeer(a.relayKeyId, epBA);
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

const COORDS = (publicKeyB64, recordKind, recordId) => ({ recordKind, recordId, publisherPublicKeyB64: publicKeyB64 });

describe("durable-record star-with-core", () => {
  it("a record published by one leaf resolves at another leaf across the relay core (inviter ONLINE)", async () => {
    const clock = { now: 1_000 };
    const deliver = makeDeliver();
    const k = 20;

    // Two-relay connected core.
    const relay0 = makeNode("relay-core-0", clock, k, deliver);
    const relay1 = makeNode("relay-core-1", clock, k, deliver);
    connect(relay0, relay1, { aAddsB: true, bAddsA: true });

    // Leaves: each connects to ONE entry relay; the leaf adds the relay, the
    // relay does NOT add the leaf.
    const leafA = makeNode("leaf-A", clock, k, deliver);
    const leafB = makeNode("leaf-B", clock, k, deliver);
    connect(leafA, relay0, { aAddsB: true, bAddsA: false });
    connect(leafB, relay1, { aAddsB: true, bAddsA: false });

    // Topology invariants: leaves know only their entry relay; relays know only
    // each other (NOT the leaves).
    assert.equal(leafA.node.kBuckets.size, 1, "leafA knows only relay0");
    assert.equal(leafB.node.kBuckets.size, 1, "leafB knows only relay1");
    assert.equal(relay0.node.kBuckets.size, 1, "relay0 knows only relay1 (not leafA)");
    assert.equal(relay1.node.kBuckets.size, 1, "relay1 knows only relay0 (not leafB)");

    const { record, publicKeyB64, localId } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "star", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });

    // leafA (inviter, ONLINE) publishes. It pushes to its only reachable peer,
    // relay0.
    await leafA.node.putRecord(record);
    await flush();

    // Mechanism check: the record lands on leafA's entry relay only. leafB's
    // entry relay does NOT hold it, and leafB has no local copy — so resolving
    // it REQUIRES crossing the core.
    assert.ok(relay0.node.recordStore.get(localId, clock.now), "relay0 (leafA entry) holds the record");
    assert.equal(relay1.node.recordStore.get(localId, clock.now), null, "relay1 (leafB entry) does NOT hold it");
    assert.equal(leafB.node.recordStore.get(localId, clock.now), null, "leafB has no local copy");

    // The acceptor fetches with the inviter still online. This is the failing
    // case today; after the fix the entry relay resolves it across the core.
    const got = await leafB.node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "star"));
    assert.ok(got, "leafB resolves the record across the relay core");
    assert.equal(got.sigB64, record.sigB64, "resolved record is the published one");
  });

  it("resolves across a 3-relay core when the holder is neither leaf's entry relay", async () => {
    const clock = { now: 1_000 };
    const deliver = makeDeliver();
    const k = 20;

    // Three-relay connected core (fully meshed).
    const relay0 = makeNode("relay-core-0", clock, k, deliver);
    const relay1 = makeNode("relay-core-1", clock, k, deliver);
    const relay2 = makeNode("relay-core-2", clock, k, deliver);
    connect(relay0, relay1, { aAddsB: true, bAddsA: true });
    connect(relay0, relay2, { aAddsB: true, bAddsA: true });
    connect(relay1, relay2, { aAddsB: true, bAddsA: true });

    // leafA enters at relay0; leafB enters at relay2 — different entry relays.
    const leafA = makeNode("leaf-A", clock, k, deliver);
    const leafB = makeNode("leaf-B", clock, k, deliver);
    connect(leafA, relay0, { aAddsB: true, bAddsA: false });
    connect(leafB, relay2, { aAddsB: true, bAddsA: false });

    const { record, publicKeyB64, localId } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "tri", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    await leafA.node.putRecord(record);
    await flush();

    // The record sits on relay0 only — leafB's entry relay (relay2) must reach
    // it by iterating the core, not by a single direct query.
    assert.ok(relay0.node.recordStore.get(localId, clock.now), "relay0 holds the record");
    assert.equal(relay2.node.recordStore.get(localId, clock.now), null, "leafB entry relay does not hold it");

    const got = await leafB.node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "tri"));
    assert.ok(got, "leafB resolves a record held on a non-entry relay across the core");
    assert.equal(got.sigB64, record.sigB64);
  });

  it("a genuinely absent record returns null and terminates (no recursion loop)", async () => {
    const clock = { now: 1_000 };
    const deliver = makeDeliver();
    const k = 20;

    const relay0 = makeNode("relay-core-0", clock, k, deliver);
    const relay1 = makeNode("relay-core-1", clock, k, deliver);
    connect(relay0, relay1, { aAddsB: true, bAddsA: true });
    const leafB = makeNode("leaf-B", clock, k, deliver);
    connect(leafB, relay1, { aAddsB: true, bAddsA: false });

    const { publicKeyB64 } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "absent", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    // Never published anywhere. The entry relay resolves on the leaf's behalf,
    // its peer (relay0) answers a peer-miss with hints (does NOT re-recurse),
    // so the resolve converges to null rather than looping.
    const got = await leafB.node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "absent"));
    assert.equal(got, null, "absent record resolves to null and terminates");
  });

  it("rate-limits client resolve-on-behalf (no amplification): a throttled entry relay stops resolving", async () => {
    const clock = { now: 1_000 };
    const deliver = makeDeliver();
    const k = 20;

    // The entry relay (relay1) allows exactly ONE resolve-on-behalf per window.
    const relay0 = makeNode("relay-core-0", clock, k, deliver);
    const registry1 = new ControlMessageRegistry();
    const relay1Node = new DhtNode({
      selfRelayKeyId: "relay-core-1",
      controlMessageRegistry: registry1,
      encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
      trySendFrame: deliver,
      nowMs: () => clock.now,
      config: { k, alpha: 3, queryTimeoutMs: 2000, recordReplicateIntervalMs: 0, recordResolveRateLimitMax: 1 },
    });
    relay1Node.install();
    const relay1 = { relayKeyId: "relay-core-1", registry: registry1, node: relay1Node, nodeId: DhtNodeId.fromRelayKeyId("relay-core-1"), alive: true };
    connect(relay0, relay1, { aAddsB: true, bAddsA: true });

    const leafA = makeNode("leaf-A", clock, k, deliver);
    const leafB = makeNode("leaf-B", clock, k, deliver);
    connect(leafA, relay0, { aAddsB: true, bAddsA: false });
    connect(leafB, relay1, { aAddsB: true, bAddsA: false });

    const { record, publicKeyB64 } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "throttle", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    await leafA.node.putRecord(record);
    await flush();

    // First client resolve spends the only token (absent record → null).
    const decoy = makeSignedRecord({ recordKind: "peerlink-invite", recordId: "decoy", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000 });
    const first = await leafB.node.getRecord(COORDS(decoy.publicKeyB64, "peerlink-invite", "decoy"));
    assert.equal(first, null, "decoy resolve returns null and consumes the budget");

    // The real record is reachable ONLY via resolve-on-behalf, but the entry
    // relay is now throttled — so it returns hints instead of resolving, and
    // the leaf (which cannot dial relay0) gets null. Proves the limiter gates
    // the recursive resolve rather than letting a client amplify unboundedly.
    const throttled = await leafB.node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "throttle"));
    assert.equal(throttled, null, "second resolve is throttled (entry relay stops resolving)");

    // After the window resets, the same fetch resolves — throttle is temporary.
    clock.now += 61_000;
    const recovered = await leafB.node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "throttle"));
    assert.ok(recovered, "resolve recovers once the rate-limit window passes");
    assert.equal(recovered.sigB64, record.sigB64);
  });
});
