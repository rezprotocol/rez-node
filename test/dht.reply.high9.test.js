import test from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

/**
 * docs/SECURITY_AUDIT.md HIGH-9 — DHT reply handlers used to accept any
 * payload from any peer for any open queryId, and the queryId was a
 * predictable `${ms}-${counter}` string. Either alone made it trivial
 * for an authenticated peer relay to race-forge replies and hijack
 * iterative lookups + value resolution. Two defenses landed:
 *
 *   1. The reply handler enforces `socket === pending.expectedSocket`,
 *      so only the peer the query was actually sent to can satisfy it.
 *   2. The queryId is now `dht-q-${randomBytes(16).base64url}` — 128
 *      bits of entropy, unguessable.
 */

function makeSocket(label) {
  return { id: label, destroyed: false };
}

function createProtocol() {
  const selfRelayKeyId = "relay-self";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);
  const valueStore = new DhtValueStore();
  const registry = new ControlMessageRegistry();
  const sent = [];
  const protocol = new DhtProtocol({
    kBuckets, valueStore, registry, selfNodeId, selfRelayKeyId,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: (socket, bytes) => sent.push({ socket, obj: JSON.parse(new TextDecoder().decode(bytes)) }),
    queryTimeoutMs: 250,
    nowMs: () => Date.now(),
  });
  protocol.install();
  return { protocol, registry, valueStore, sent, kBuckets };
}

test("HIGH-9: dht.find_node.reply from a DIFFERENT socket than the query was sent to is dropped", async () => {
  const { protocol, registry, sent } = createProtocol();
  const intendedPeer = makeSocket("intended");
  const attacker = makeSocket("attacker");
  const targetId = DhtNodeId.fromRelayKeyId("target");

  const promise = protocol.queryFindNode(intendedPeer, targetId);
  assert.equal(sent.length, 1);
  const queryId = sent[0].obj.queryId;

  // Attacker races a reply on a different socket.
  await registry.dispatch("dht.find_node.reply", {
    _ctl: "dht.find_node.reply",
    queryId,
    nodes: [{ nodeIdHex: DhtNodeId.fromRelayKeyId("relay-evil").hex, relayKeyId: "relay-evil" }],
  }, attacker);

  // The intended peer now sends the real reply.
  await registry.dispatch("dht.find_node.reply", {
    _ctl: "dht.find_node.reply",
    queryId,
    nodes: [{ nodeIdHex: DhtNodeId.fromRelayKeyId("relay-real").hex, relayKeyId: "relay-real" }],
  }, intendedPeer);

  const result = await promise;
  assert.equal(result.nodes.length, 1);
  assert.equal(result.nodes[0].relayKeyId, "relay-real", "attacker's reply must not win the race");
});

test("HIGH-9: dht.find_value.reply from a DIFFERENT socket is dropped", async () => {
  const { protocol, registry, sent } = createProtocol();
  const intendedPeer = makeSocket("intended");
  const attacker = makeSocket("attacker");
  const targetId = DhtNodeId.fromRelayKeyId("inbox:victim");

  const promise = protocol.queryFindValue(intendedPeer, targetId, "inbox:victim");
  const queryId = sent[0].obj.queryId;

  // Attacker race-forges a routeEntry pointing at their relay. (Even if
  // they could win the race, validateStoredRouteEntry would reject it
  // downstream — but we want to assert the reply itself never resolves
  // the pending promise from a wrong socket.)
  const { routeEntry: evilEntry } = makeSignedRouteEntry({
    inboxId: "inbox:victim",
    deliveryRelayKeyId: "relay-evil",
    hops: 0,
  });
  await registry.dispatch("dht.find_value.reply", {
    _ctl: "dht.find_value.reply",
    queryId,
    value: evilEntry,
    nodes: [],
  }, attacker);

  // Real reply from the intended peer.
  const { routeEntry: realEntry } = makeSignedRouteEntry({
    inboxId: "inbox:victim",
    deliveryRelayKeyId: "relay-real",
    hops: 0,
  });
  await registry.dispatch("dht.find_value.reply", {
    _ctl: "dht.find_value.reply",
    queryId,
    value: realEntry,
    nodes: [],
  }, intendedPeer);

  const result = await promise;
  assert.ok(result.value, "intended peer's reply should resolve the promise");
  assert.equal(result.value.deliveryRelayKeyId, "relay-real");
});

test("HIGH-9: queryId is 128-bit-random, not a predictable counter", async () => {
  const { protocol, sent } = createProtocol();
  const peer = makeSocket("p");
  const targetId = DhtNodeId.fromRelayKeyId("target");

  // Issue several queries; collect queryIds.
  protocol.queryFindNode(peer, targetId);
  protocol.queryFindNode(peer, targetId);
  protocol.queryFindNode(peer, targetId);

  const ids = sent.map((s) => s.obj.queryId);
  // Shape: "dht-q-" prefix + base64url payload.
  for (const id of ids) {
    assert.match(id, /^dht-q-[A-Za-z0-9_-]{22}$/, `unexpected queryId shape: ${id}`);
  }
  // All distinct (very basic collision sanity).
  assert.equal(new Set(ids).size, ids.length, "queryIds must be unique");
  // Critically: NOT predictable. The old scheme produced incrementally
  // increasing counters; an attacker observing one query could derive the
  // next. With randomBytes(16), the probability of a predictable
  // relationship between consecutive IDs is negligible.
  for (let i = 1; i < ids.length; i++) {
    const prev = ids[i - 1].slice("dht-q-".length);
    const cur = ids[i].slice("dht-q-".length);
    // Crude check: the first 8 chars (≈48 bits) shouldn't match between
    // adjacent IDs. With the old counter scheme they'd share a prefix.
    assert.notEqual(prev.slice(0, 8), cur.slice(0, 8),
      "adjacent queryIds share entropy — randomness check failed");
  }
});

test("HIGH-9: a reply with a guessed/wrong queryId is ignored even from a peer we did query", async () => {
  const { protocol, registry, sent } = createProtocol();
  const peer = makeSocket("p");
  const targetId = DhtNodeId.fromRelayKeyId("target");

  const promise = protocol.queryFindNode(peer, targetId);
  // Note: don't use sent[0].obj.queryId — simulate an attacker guessing.
  await registry.dispatch("dht.find_node.reply", {
    _ctl: "dht.find_node.reply",
    queryId: "dht-q-AAAAAAAAAAAAAAAAAAAAAA",
    nodes: [{ nodeIdHex: DhtNodeId.fromRelayKeyId("relay-evil").hex, relayKeyId: "relay-evil" }],
  }, peer);

  // The real reply with the actual queryId.
  await registry.dispatch("dht.find_node.reply", {
    _ctl: "dht.find_node.reply",
    queryId: sent[0].obj.queryId,
    nodes: [{ nodeIdHex: DhtNodeId.fromRelayKeyId("relay-real").hex, relayKeyId: "relay-real" }],
  }, peer);

  const result = await promise;
  assert.equal(result.nodes[0].relayKeyId, "relay-real");
});

test("HIGH-9: concurrent queries to different peers each resolve only from their intended peer", async () => {
  const { protocol, registry, sent } = createProtocol();
  const peerA = makeSocket("pa");
  const peerB = makeSocket("pb");
  const targetId = DhtNodeId.fromRelayKeyId("inbox:t");

  const promiseA = protocol.queryFindValue(peerA, targetId, "inbox:t");
  const promiseB = protocol.queryFindValue(peerB, targetId, "inbox:t");
  const qA = sent[0].obj.queryId;
  const qB = sent[1].obj.queryId;
  assert.notEqual(qA, qB);

  // Cross-deliver: try to satisfy queryA with a reply from peerB, and
  // vice versa. Neither should succeed.
  const { routeEntry: entryX } = makeSignedRouteEntry({
    inboxId: "inbox:t",
    deliveryRelayKeyId: "relay-x",
    hops: 0,
  });
  await registry.dispatch("dht.find_value.reply", { _ctl: "dht.find_value.reply", queryId: qA, value: entryX, nodes: [] }, peerB);
  await registry.dispatch("dht.find_value.reply", { _ctl: "dht.find_value.reply", queryId: qB, value: entryX, nodes: [] }, peerA);

  // Now deliver correctly.
  const { routeEntry: entryFromA } = makeSignedRouteEntry({
    inboxId: "inbox:t",
    deliveryRelayKeyId: "relay-from-A",
    hops: 0,
  });
  const { routeEntry: entryFromB } = makeSignedRouteEntry({
    inboxId: "inbox:t",
    deliveryRelayKeyId: "relay-from-B",
    hops: 0,
  });
  await registry.dispatch("dht.find_value.reply", { _ctl: "dht.find_value.reply", queryId: qA, value: entryFromA, nodes: [] }, peerA);
  await registry.dispatch("dht.find_value.reply", { _ctl: "dht.find_value.reply", queryId: qB, value: entryFromB, nodes: [] }, peerB);

  const [resA, resB] = await Promise.all([promiseA, promiseB]);
  assert.equal(resA.value.deliveryRelayKeyId, "relay-from-A");
  assert.equal(resB.value.deliveryRelayKeyId, "relay-from-B");
});
