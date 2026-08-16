import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtLookup } from "../src/routing/dht/DhtLookup.js";

/**
 * ADR-RELAY-IDENTITY (rez-core/docs/adr/ADR-RELAY-IDENTITY.md): discovered
 * node references must carry a canonical self-certifying relay id of the
 * form `rez:relay:<64 lowercase hex>`. These tests don't need real keys —
 * the lookup's format gate is structural — so we derive canonical-format
 * ids deterministically from labels.
 */
function canonicalId(label) {
  return "rez:relay:" + createHash("sha256").update(label).digest("hex");
}

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
  // relayKeyId hashes to a different (far) id. Both use canonical-format
  // relay ids (ADR-RELAY-IDENTITY) so the LOW-5 hash-binding check — not
  // the format gate — is what rejects the sybil.
  const validRelayKeyId = canonicalId("relay-valid");
  const validNodeId = DhtNodeId.fromRelayKeyId(validRelayKeyId);
  const sybilRelayKeyId = canonicalId("relay-sybil");
  const sybilFakeNodeIdHex = targetId.hex; // sybil claims to BE the target

  const sendQuery = async (entry) => {
    // Simulate the seed peer returning a sybil + a legit discovered node.
    if (entry.relayKeyId === "relay-real") {
      return {
        value: null,
        nodes: [
          { nodeIdHex: validNodeId.hex, relayKeyId: validRelayKeyId },
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
  assert.ok(relayKeyIds.includes(validRelayKeyId), "honest discovered node should be in candidates");
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

  const honestRelayKeyId = canonicalId("honest-peer");
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
  // closest possible distance) but with mismatched (canonical-format)
  // relayKeyIds — so the LOW-5 hash binding, not the format gate, is
  // what must drop every one of them.
  const sybilRelayKeyIds = [];
  for (let i = 0; i < 10; i++) {
    sybilRelayKeyIds.push(canonicalId(`sybil-${i}`));
  }
  const honestRelayKeyId = canonicalId("honest-q");

  const sendQuery = async (entry) => {
    if (entry.relayKeyId === "seed-A") {
      const nodes = [];
      for (let i = 0; i < 10; i++) {
        nodes.push({ nodeIdHex: targetId.hex, relayKeyId: sybilRelayKeyIds[i] });
      }
      return { value: null, nodes };
    }
    if (entry.relayKeyId === "seed-B") {
      // Returns one honest discovery.
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
    assert.equal(relayKeyIds.includes(sybilRelayKeyIds[i]), false, `sybil-${i} must be dropped`);
  }
  // Honest peer admitted.
  assert.ok(relayKeyIds.includes(honestRelayKeyId));
});

test("ADR-RELAY-IDENTITY: a discovered node with a legacy free-string relayKeyId is rejected by the format gate even with a perfectly matching nodeIdHex", async () => {
  // rez-core/docs/adr/ADR-RELAY-IDENTITY.md: discovered node references
  // MUST carry a canonical self-certifying `rez:relay:<64 lowercase hex>`
  // id. A legacy free-string id can never authenticate, so it must not
  // enter the candidate set — even when its (nodeIdHex, relayKeyId)
  // binding is honest, i.e. nodeIdHex === fromRelayKeyId(relayKeyId).hex,
  // so the LOW-5 hash check alone would have admitted it.
  const selfNodeId = DhtNodeId.fromRelayKeyId("relay-self-4");
  const kBuckets = new KBucketTable(selfNodeId);
  kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("seed-4"), "seed-4", makeSocket("s4"), 1000);

  const lookup = new DhtLookup(kBuckets, { alpha: 3, k: 20 });
  const targetId = DhtNodeId.fromRelayKeyId("target-4");

  const legacyRelayKeyId = "legacy-free-string-peer"; // NOT canonical format
  const legacyNodeIdHex = DhtNodeId.fromRelayKeyId(legacyRelayKeyId).hex; // honest binding

  const sendQuery = async (entry) => {
    if (entry.relayKeyId === "seed-4") {
      return {
        value: null,
        nodes: [{ nodeIdHex: legacyNodeIdHex, relayKeyId: legacyRelayKeyId }],
      };
    }
    return { value: null, nodes: [] };
  };

  const result = await lookup.findNode(targetId, sendQuery);
  const relayKeyIds = result.closestNodes.map((n) => n.relayKeyId);

  assert.ok(relayKeyIds.includes("seed-4"), "seed should still be in candidates");
  assert.equal(relayKeyIds.includes(legacyRelayKeyId), false,
    "legacy free-string relayKeyId must be rejected purely by the canonical-format gate");
});
