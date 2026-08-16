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

  it("re-verifies on reload: a tampered persisted record is dropped, a valid one survives", async () => {
    const clock = { now: 5_000 };

    // A valid record goes through the normal persist path.
    const keep = makeSignedRecord({
      recordKind: "reverify", recordId: "keep", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    const first = standaloneNode(clock);
    await first.putRecord(keep.record);
    await new Promise((r) => setTimeout(r, 50));

    // A forged record is planted straight onto disk (bypassing the ingress
    // gate): valid envelope + correct slot, but its payload was swapped after
    // signing, so the signature no longer matches. Not expired (so the drop is
    // attributable to the signature, not TTL).
    const forged = makeSignedRecord({
      recordKind: "reverify", recordId: "tampered", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    forged.record.payloadB64 = Buffer.from("tampered-after-signing").toString("base64");
    const planter = new DurableRecordPersistence({ store: new FileSystemDataStore({ basePath: dir }) });
    await planter.put(forged.localId, { record: forged.record, storedAtMs: clock.now, ttlMs: 3_600_000 });

    const second = standaloneNode(clock);
    await second.loadPersistedRecords();

    const keptBack = await second.getRecord({ recordKind: "reverify", recordId: "keep", publisherPublicKeyB64: keep.publicKeyB64 });
    assert.ok(keptBack, "validly-signed record survives reload");
    const forgedBack = await second.getRecord({ recordKind: "reverify", recordId: "tampered", publisherPublicKeyB64: forged.publicKeyB64 });
    assert.equal(forgedBack, null, "tampered (bad-signature) record is rejected on reload, not trusted from disk");
  });
});

// ---------------------------------------------------------------------------
// P4.3/P4.4 — acknowledged, truthful replication
// ---------------------------------------------------------------------------

function buildMeshFast({ n, clock, queryTimeoutMs, recordPutDeadlineMs = null }) {
  // Same harness as buildMesh but with a short ack timeout so timeout paths
  // are testable without multi-second waits.
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
    const relayKeyId = "relay-fast-" + i;
    const registry = new ControlMessageRegistry();
    const node = new DhtNode({
      selfRelayKeyId: relayKeyId,
      controlMessageRegistry: registry,
      encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
      trySendFrame: deliver,
      nowMs: () => clock.now,
      config: {
        k: 20, alpha: 3, queryTimeoutMs, recordReplicateIntervalMs: 0,
        recordPutDeadlineMs: recordPutDeadlineMs !== null ? recordPutDeadlineMs : queryTimeoutMs * 4,
      },
    });
    node.install();
    nodes.push({ relayKeyId, registry, node, nodeId: DhtNodeId.fromRelayKeyId(relayKeyId), alive: true, sockets: new Map() });
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
      a.sockets.set(b.relayKeyId, epAB);
      b.sockets.set(a.relayKeyId, epBA);
    }
  }
  return nodes;
}

describe("acknowledged replication truth (P4.3/P4.4)", () => {
  it("putRecord counts only acknowledged holders as replicas — local storage never counts", async () => {
    const clock = { now: 1_000 };
    const nodes = buildMeshFast({ n: 4, clock, queryTimeoutMs: 250 });
    const { record } = makeSignedRecord({
      recordKind: "ackp", recordId: "all-good", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    const result = await nodes[0].node.putRecord(record);
    assert.equal(result.storedLocally, true);
    assert.ok(result.localId);
    assert.equal(result.attemptedRemote, 3);
    assert.equal(result.acknowledgedStored, 3, "all three healthy peers acked stored");
    assert.equal(result.acknowledgedRefreshed, 0);
    assert.equal(result.rejectedRemote, 0);
    assert.equal(result.timedOutRemote, 0);
    assert.equal(result.disconnectedRemote, 0);
    assert.equal(result.acknowledgedRemote, 3);
    // Local hold is reported separately from remote replicas.
    assert.ok(nodes[0].node.recordStore.get(result.localId, clock.now));
  });

  it("mixed success, timeout, and disconnect in one put are reported separately and truthfully", async () => {
    const clock = { now: 1_000 };
    const nodes = buildMeshFast({ n: 4, clock, queryTimeoutMs: 250 });
    const [publisher, healthy, unresponsive, dead] = nodes;
    // Unresponsive: connected socket, but the peer never processes frames.
    unresponsive.alive = false;
    // Disconnected: socket destroyed before the send is attempted.
    publisher.sockets.get(dead.relayKeyId).destroyed = true;

    const { record } = makeSignedRecord({
      recordKind: "ackp", recordId: "mixed", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    const result = await publisher.node.putRecord(record);
    assert.equal(result.storedLocally, true);
    assert.equal(result.targetReplicaCount, 3);
    assert.equal(result.attemptedRemote, 2, "destroyed socket is not an attempt");
    assert.equal(result.acknowledgedStored, 1, healthy.relayKeyId + " acked");
    assert.equal(result.timedOutRemote, 1, unresponsive.relayKeyId + " timed out (a send is not a store)");
    assert.equal(result.disconnectedRemote, 1);
    assert.equal(result.rejectedRemote, 0);
    assert.equal(result.acknowledgedRemote, 1, "only the acknowledged peer counts as a holder");
  });

  it("a remote rejection (older epoch/issuance conflict) is counted as rejected, not as a replica", async () => {
    const clock = { now: 10_000 };
    const nodes = buildMeshFast({ n: 2, clock, queryTimeoutMs: 250 });
    const keypairHolder = makeSignedRecord({
      recordKind: "ackp", recordId: "conflict", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    // The receiver already holds a NEWER issuance of the same slot.
    const newer = makeSignedRecord({
      keypair: keypairHolder.keypair, recordKind: "ackp", recordId: "conflict",
      issuedAtMs: clock.now + 500, expiresAtMs: clock.now + 3_600_000,
    });
    nodes[1].node.recordProtocol.storeVerified(newer.localId, newer.record);

    const result = await nodes[0].node.putRecord(keypairHolder.record);
    assert.equal(result.storedLocally, true, "publisher's own store had no newer copy");
    assert.equal(result.attemptedRemote, 1);
    assert.equal(result.rejectedRemote, 1, "stale rebroadcast is refused remotely and reported as rejected");
    assert.equal(result.acknowledgedRemote, 0);
  });

  it("re-audit R3: acks settled BEFORE the put deadline keep their true outcome when others are still pending", async () => {
    const clock = { now: 1_000 };
    // Put deadline (300ms) fires well before the unresponsive peer's ack
    // timeout (2000ms). The healthy peer's ack settles immediately — it must
    // be reported as a real holder, not discarded into a blanket timeout.
    const nodes = buildMeshFast({ n: 3, clock, queryTimeoutMs: 2000, recordPutDeadlineMs: 300 });
    const [publisher, , unresponsive] = nodes;
    unresponsive.alive = false;

    const { record } = makeSignedRecord({
      recordKind: "ackp", recordId: "partial", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    const startedAt = Date.now();
    const result = await publisher.node.putRecord(record);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(result.attemptedRemote, 2);
    assert.equal(result.acknowledgedStored, 1, "the settled ack survives the deadline snapshot");
    assert.equal(result.timedOutRemote, 1, "only the still-pending attempt is a timeout");
    assert.equal(result.acknowledgedRemote, 1);
    assert.ok(elapsedMs < 1500, "put returned at its own deadline, not the ack timeout (took " + elapsedMs + "ms)");
  });

  it("republishHeldRecords tracks attempted vs acknowledged copies separately", async () => {
    const clock = { now: 1_000 };
    const nodes = buildMeshFast({ n: 3, clock, queryTimeoutMs: 250 });
    const [holder, healthy, unresponsive] = nodes;
    unresponsive.alive = false;
    const { record, localId } = makeSignedRecord({
      recordKind: "ackp", recordId: "repub", issuedAtMs: clock.now, expiresAtMs: clock.now + 3_600_000,
    });
    holder.node.recordProtocol.storeVerified(localId, record);

    holder.node.recordProtocol.republishHeldRecords(clock.now + 1);
    await new Promise((r) => setTimeout(r, 600));

    const stats = holder.node.recordProtocol.getReplicationStats();
    assert.equal(stats.attempted, 2, "pushed to both connected peers");
    assert.equal(stats.acknowledgedStored, 1, "only the healthy peer acknowledged");
    assert.equal(stats.timedOut, 1, "the unresponsive peer is a timeout, not a copy");
    assert.equal(stats.rejected, 0);
    assert.ok(healthy.node.recordStore.get(localId, clock.now), "the acknowledged copy is really held");
  });
});
