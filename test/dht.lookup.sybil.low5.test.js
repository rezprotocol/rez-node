import test from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";

/**
 * docs/SECURITY_AUDIT.md LOW-5 — DhtLookup's #iterativeLookup added any
 * `(nodeIdHex, relayKeyId)` pair returned by a peer to its candidate
 * list. The two fields are independent on the wire, but production
 * derives nodeIdHex from relayKeyId via `DhtNodeId.fromRelayKeyId`. A
 * sybil peer could exploit the gap by sending pairs where nodeIdHex
 * was hand-picked to be close to the lookup target, monopolizing the
 * α-batch with socket-less entries that the lookup can't query — a
 * cheap DoS on iterative discovery.
 *
 * The fix: drop any reply-node whose nodeIdHex doesn't equal
 * `DhtNodeId.fromRelayKeyId(relayKeyId).hex`. Sybils now have to brute-
 * force a relayKeyId that hashes near the target — the same cost model
 * Kademlia assumes.
 */

function makeSocket(label) {
  return { id: label, destroyed: false };
}

test("LOW-5: nodes whose nodeIdHex doesn't match DhtNodeId.fromRelayKeyId(relayKeyId).hex are dropped", async () => {
  const selfRelayKeyId = "relay-self";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);

  // Seed one real peer so the lookup has a starting point.
  const realSocket = makeSocket("real");
  const realNodeId = DhtNodeId.fromRelayKeyId("relay-real");
  kBuckets.addOrUpdate(realNodeId, "relay-real", realSocket, 1000);

  const lookup = new DhtLookup(kBuckets, { alpha: 3, k: 20 });

  // Lookup target: choose deterministically.
  const targetId = DhtNodeId.fromRelayKeyId("target");

  // The real peer's reply: include one valid node and one sybil whose
  // nodeIdHex was hand-picked to be close to the target but whose
  // relayKeyId hashes to a different (far) id.
  const validNodeId = DhtNodeId.fromRelayKeyId("relay-valid");
  const sybilRelayKeyId = "relay-sybil";
  const sybilFakeNodeIdHex = targetId.hex; // sybil claims to BE the target

  const sendQuery = async (entry) => {
    // Simulate the seed peer returning a sybil + a legit discovered node.
    if (entry.relayKeyId === "relay-real") {
      return {
        value: null,
        nodes: [
          { nodeIdHex: validNodeId.hex, relayKeyId: "relay-valid" },
          { nodeIdHex: sybilFakeNodeIdHex, relayKeyId: sybilRelayKeyId }, // mismatched pair
        ],
      };
    }
    return { value: null, nodes: [] };
  };

  const result = await lookup.findNode(targetId, sendQuery);
  const relayKeyIds = result.closestNodes.map((n) => n.relayKeyId);

  // The real peer + the legit discovered node should appear; the sybil
  // (whose nodeIdHex was a lie) must NOT.
  assert.ok(relayKeyIds.includes("relay-real"), "seed should still be in candidates");
  assert.ok(relayKeyIds.includes("relay-valid"), "honest discovered node should be in candidates");
  assert.equal(relayKeyIds.includes(sybilRelayKeyId), false,
    "sybil with mismatched (nodeIdHex, relayKeyId) must be dropped");
});

test("LOW-5: an honest discovered node (nodeIdHex matches relayKeyId hash) is still admitted", async () => {
  const selfRelayKeyId = "relay-self-2";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);
  kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("seed"), "seed", makeSocket("seed"), 1000);

  const lookup = new DhtLookup(kBuckets, { alpha: 3, k: 20 });
  const targetId = DhtNodeId.fromRelayKeyId("target-2");

  const honestRelayKeyId = "honest-peer";
  const honestNodeIdHex = DhtNodeId.fromRelayKeyId(honestRelayKeyId).hex;

  const sendQuery = async (entry) => {
    if (entry.relayKeyId === "seed") {
      return { value: null, nodes: [{ nodeIdHex: honestNodeIdHex, relayKeyId: honestRelayKeyId }] };
    }
    return { value: null, nodes: [] };
  };

  const result = await lookup.findNode(targetId, sendQuery);
  const relayKeyIds = result.closestNodes.map((n) => n.relayKeyId);
  assert.ok(relayKeyIds.includes(honestRelayKeyId), "honest peer must be admitted");
});

test("LOW-5: a sybil cannot monopolize the alpha batch by flooding fake-close nodeIds", async () => {
  const selfRelayKeyId = "relay-self-3";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);

  // Two real peers as seeds.
  kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("seed-A"), "seed-A", makeSocket("a"), 1000);
  kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("seed-B"), "seed-B", makeSocket("b"), 1000);

  const lookup = new DhtLookup(kBuckets, { alpha: 3, k: 20 });
  const targetId = DhtNodeId.fromRelayKeyId("target-3");

  // seed-A returns 10 sybils all claiming nodeIdHex == target.hex (the
  // closest possible distance) but with mismatched relayKeyIds.
  const sendQuery = async (entry) => {
    if (entry.relayKeyId === "seed-A") {
      const nodes = [];
      for (let i = 0; i < 10; i++) {
        nodes.push({ nodeIdHex: targetId.hex, relayKeyId: `sybil-${i}` });
      }
      return { value: null, nodes };
    }
    if (entry.relayKeyId === "seed-B") {
      // Returns one honest discovery.
      const honestRelayKeyId = "honest-q";
      return {
        value: null,
        nodes: [{ nodeIdHex: DhtNodeId.fromRelayKeyId(honestRelayKeyId).hex, relayKeyId: honestRelayKeyId }],
      };
    }
    return { value: null, nodes: [] };
  };

  const result = await lookup.findNode(targetId, sendQuery);
  const relayKeyIds = result.closestNodes.map((n) => n.relayKeyId);

  // Zero sybils admitted.
  for (let i = 0; i < 10; i++) {
    assert.equal(relayKeyIds.includes(`sybil-${i}`), false, `sybil-${i} must be dropped`);
  }
  // Honest peer admitted.
  assert.ok(relayKeyIds.includes("honest-q"));
});
