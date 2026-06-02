import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";
import { FileSystemDataStore } from "@rezprotocol/core";
import { DhtNode } from "../src/routing/dht/DhtNode.js";
import { DurableRecordPersistence } from "../src/routing/dht/DurableRecordPersistence.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { durableRecordTargetId } from "../src/routing/dht/DurableRecord.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRecord } from "./support/durableRecord.js";

// In-process mesh harness: each socket is a directed endpoint that knows the
// peer node it delivers to and the reverse endpoint the peer sees (so reply
// routing and the HIGH-9 same-socket guard work).
function buildMesh({ n, k, clock }) {
  // Model real transport: a socket write returns immediately and the frame is
  // dispatched on the peer in a later tick. (A synchronous dispatch would run
  // the reply handler before the query's waiter is registered — an artifact of
  // an in-process harness, not of real async sockets.)
  function deliver(socket, bytes) {
    if (!socket || socket.destroyed === true) return;
    const peer = socket._peer;
    if (!peer || !peer.alive) return;
    const obj = JSON.parse(new TextDecoder().decode(bytes));
    queueMicrotask(() => {
      if (!peer.alive || socket.destroyed === true) return;
      peer.registry.dispatch(obj._ctl, obj, socket._peerSocket).catch(() => {});
    });
  }

  const nodes = [];
  for (let i = 0; i < n; i += 1) {
    const relayKeyId = "relay-" + i;
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
    nodes.push({ relayKeyId, registry, node, nodeId: DhtNodeId.fromRelayKeyId(relayKeyId), alive: true });
  }

  for (let i = 0; i < n; i += 1) {
    for (let j = i + 1; j < n; j += 1) {
      const a = nodes[i];
      const b = nodes[j];
      const epAB = { id: a.relayKeyId + "->" + b.relayKeyId, destroyed: false };
      const epBA = { id: b.relayKeyId + "->" + a.relayKeyId, destroyed: false };
      epAB._peer = b; epAB._peerSocket = epBA;
      epBA._peer = a; epBA._peerSocket = epAB;
      a.node.addPeer(b.relayKeyId, epAB);
      b.node.addPeer(a.relayKeyId, epBA);
    }
  }
  // Sanity: chosen (n,k) must yield a fully-connected mesh (no k-bucket
  // overflow), otherwise lookups would be testing a partitioned topology.
  for (const node of nodes) {
    assert.equal(node.node.kBuckets.size, n - 1, "mesh fully connected (no bucket overflow)");
  }
  return nodes;
}

function killNode(nodes, victim) {
  victim.alive = false;
  for (const other of nodes) {
    if (other === victim) continue;
    other.node.removePeer(victim.relayKeyId);
  }
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

function byDistance(nodes, targetId) {
  return [...nodes].sort((a, b) => targetId.compareDistanceTo(a.nodeId, b.nodeId));
}

const COORDS = (publicKeyB64, recordKind, recordId) => ({ recordKind, recordId, publisherPublicKeyB64: publicKeyB64 });

describe("durable-record mesh", () => {
  it("fetches a record held only on a remote node (overlay round-trip)", async () => {
    const clock = { now: 1_000 };
    const nodes = buildMesh({ n: 4, k: 20, clock });
    const { record, publicKeyB64, localId } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "rt", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    // Place the record on exactly one node.
    nodes[2].node.recordProtocol.storeVerified(localId, record);

    // A node with no local copy resolves it over the overlay.
    assert.equal(nodes[0].node.recordStore.get(localId, clock.now), null);
    const got = await nodes[0].node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "rt"));
    assert.ok(got, "overlay GET resolves a remotely-held record");
    assert.equal(got.sigB64, record.sigB64);
  });

  it("a published record survives the publisher going offline", async () => {
    const clock = { now: 1_000 };
    const nodes = buildMesh({ n: 4, k: 20, clock });
    const { record, publicKeyB64, localId } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "off", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });

    const publisher = nodes[0];
    await publisher.node.putRecord(record);
    await flush();
    // The record propagated to the other nodes (full replication at k>=n).
    for (const n of [nodes[1], nodes[2], nodes[3]]) {
      assert.ok(n.node.recordStore.get(localId, clock.now), n.relayKeyId + " holds the published record");
    }

    killNode(nodes, publisher);
    // The record is still served by a holder with the publisher gone.
    const got = await nodes[1].node.getRecord(COORDS(publicKeyB64, "peerlink-invite", "off"));
    assert.ok(got, "record outlives the publisher");
  });

  it("re-replicates onto a newly-responsible node after holder churn", async () => {
    const clock = { now: 1_000 };
    const n = 12;
    const k = 10;
    const nodes = buildMesh({ n, k, clock });
    const { record, localId } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "churn", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    const targetId = durableRecordTargetId(localId);
    const ranked = byDistance(nodes, targetId);
    const publisher = ranked[n - 1]; // farthest — not among the k responsible holders

    await publisher.node.putRecord(record);
    await flush();

    const holders = ranked.slice(0, k);
    const newcomer = ranked[k]; // (k+1)-th closest — clean, no GET has touched it
    for (const h of holders) {
      assert.ok(h.node.recordStore.get(localId, clock.now), "holder " + h.relayKeyId + " holds the record");
    }
    assert.equal(newcomer.node.recordStore.get(localId, clock.now), null, "newcomer is not yet a holder");

    // Kill a responsible holder. The newcomer now enters the k-closest set.
    killNode(nodes, holders[0]);
    clock.now += 1;
    for (const node of nodes) {
      if (!node.alive) continue;
      node.node.republishHeldRecords(clock.now);
    }
    await flush();
    await flush();

    assert.ok(
      newcomer.node.recordStore.get(localId, clock.now),
      "re-replication seeded the record onto the newly-responsible node",
    );
  });

  it("a record stops resolving once expired", async () => {
    const clock = { now: 1_000 };
    const nodes = buildMesh({ n: 3, k: 20, clock });
    const { record, publicKeyB64 } = makeSignedRecord({
      recordKind: "ephemeral", recordId: "exp", issuedAtMs: clock.now, expiresAtMs: clock.now + 5_000,
    });
    await nodes[0].node.putRecord(record);
    await flush();

    const before = await nodes[2].node.getRecord(COORDS(publicKeyB64, "ephemeral", "exp"));
    assert.ok(before, "resolves while live");

    clock.now += 10_000; // past expiry on every node
    const after = await nodes[2].node.getRecord(COORDS(publicKeyB64, "ephemeral", "exp"));
    assert.equal(after, null, "no longer resolves once expired");
  });
});

describe("durable-record persistence", () => {
  let dir;
  before(async () => {
    dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-durable-records-"));
  });
  after(async () => {
    await fs.rm(dir, { recursive: true, force: true });
  });

  function standaloneNode(clock) {
    const registry = new ControlMessageRegistry();
    const node = new DhtNode({
      selfRelayKeyId: "relay-solo",
      controlMessageRegistry: registry,
      encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
      trySendFrame: () => {},
      nowMs: () => clock.now,
      config: { k: 3 },
    });
    node.install();
    node.setRecordPersistence(new DurableRecordPersistence({ store: new FileSystemDataStore({ basePath: dir }) }));
    return node;
  }

  it("survives a relay restart", async () => {
    const clock = { now: 1_000 };
    const { record, publicKeyB64 } = makeSignedRecord({
      recordKind: "persist", recordId: "p1", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });

    const first = standaloneNode(clock);
    await first.putRecord(record);
    // Persistence is written through fire-and-forget; let the fs write settle.
    await new Promise((r) => setTimeout(r, 50));

    const second = standaloneNode(clock);
    const loaded = await second.loadPersistedRecords();
    assert.equal(loaded, 1);
    const got = await second.getRecord({ recordKind: "persist", recordId: "p1", publisherPublicKeyB64: publicKeyB64 });
    assert.ok(got, "record reloaded from disk after restart");
    assert.equal(got.sigB64, record.sigB64);
  });
});
