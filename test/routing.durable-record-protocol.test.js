import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DurableRecordProtocol, durableRecordDigestHex } from "../src/routing/dht/DurableRecordProtocol.js";
import { DurableRecordStore } from "../src/routing/dht/DurableRecordStore.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { SlidingWindowRateLimiter } from "../src/util/SlidingWindowRateLimiter.js";
import {
  DHT_RECORD_STORE_PROTOCOL_VERSION,
} from "../src/contracts/wireRecords/DhtRecordStore.js";
import { makeSignedRecord } from "./support/durableRecord.js";

function harness(opts = {}) {
  const registry = new ControlMessageRegistry();
  const recordStore = new DurableRecordStore();
  const selfNodeId = DhtNodeId.fromRelayKeyId("self-relay");
  const kBuckets = new KBucketTable(selfNodeId, { k: 20 });
  const sent = [];
  const proto = new DurableRecordProtocol({
    kBuckets,
    recordStore,
    registry,
    selfNodeId,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: (socket, bytes) => sent.push({ socket, obj: JSON.parse(new TextDecoder().decode(bytes)) }),
    nowMs: opts.nowMs || (() => 1000),
    storeRateLimiter: opts.storeRateLimiter || null,
    queryTimeoutMs: opts.queryTimeoutMs || 3000,
  });
  proto.install();
  return { proto, registry, recordStore, kBuckets, sent };
}

/** Build the P4.2 acked-request frame for a record. */
function storeFrame(localId, record, requestId = "rec-s-test-" + Math.random().toString(36).slice(2)) {
  return {
    _ctl: "dht.rec_store",
    protocolVersion: DHT_RECORD_STORE_PROTOCOL_VERSION,
    requestId,
    key: localId,
    record,
  };
}

function lastAck(sent) {
  const acks = sent.filter((s) => s.obj._ctl === "dht.rec_store.ack");
  return acks.length > 0 ? acks[acks.length - 1].obj : null;
}

describe("DurableRecordProtocol handlers", () => {
  it("accepts a valid rec_store under its publisher-bound slot key and acks `stored`", async () => {
    const { registry, recordStore, sent } = harness();
    const { record, localId } = makeSignedRecord();
    const socket = { id: "p" };
    await registry.dispatch("dht.rec_store", storeFrame(localId, record, "rec-s-1"), socket);
    assert.equal(recordStore.get(localId, 1000), record);
    const ack = lastAck(sent);
    assert.ok(ack, "an acknowledgement is sent");
    assert.equal(ack.requestId, "rec-s-1");
    assert.equal(ack.key, localId);
    assert.equal(ack.status, "stored");
    assert.equal(ack.reason, null);
    assert.equal(ack.recordDigestHex, durableRecordDigestHex(record));
    assert.equal(sent[sent.length - 1].socket, socket, "ack goes back on the requesting socket");
  });

  it("acks `refreshed` when the exact record is already held", async () => {
    const { registry, sent } = harness();
    const { record, localId } = makeSignedRecord();
    await registry.dispatch("dht.rec_store", storeFrame(localId, record, "rec-s-a"), { id: "p" });
    await registry.dispatch("dht.rec_store", storeFrame(localId, record, "rec-s-b"), { id: "p" });
    const ack = lastAck(sent);
    assert.equal(ack.requestId, "rec-s-b");
    assert.equal(ack.status, "refreshed");
    assert.equal(ack.reason, null);
  });

  it("drops a legacy unacked frame (no protocolVersion/requestId) entirely", async () => {
    const { registry, recordStore, sent } = harness();
    const { record, localId } = makeSignedRecord();
    await registry.dispatch("dht.rec_store", { _ctl: "dht.rec_store", key: localId, record }, { id: "p" });
    assert.equal(recordStore.get(localId, 1000), null, "legacy shape is not stored");
    assert.equal(lastAck(sent), null, "and not acked");
  });

  it("rejects a rec_store whose announced key does not match the record (substitution) with a bounded reason", async () => {
    const { registry, recordStore, sent } = harness();
    const a = makeSignedRecord({ recordId: "a" });
    const b = makeSignedRecord({ recordId: "b" });
    // Valid record `a`, but announced under `b`'s slot key.
    await registry.dispatch("dht.rec_store", storeFrame(b.localId, a.record), { id: "p" });
    assert.equal(recordStore.get(b.localId, 1000), null);
    assert.equal(recordStore.get(a.localId, 1000), null);
    const ack = lastAck(sent);
    assert.equal(ack.status, "rejected");
    assert.equal(ack.reason, "slot-mismatch");
  });

  it("rejects a rec_store with an invalid signature (bounded reason, no attacker text echoed)", async () => {
    const { registry, recordStore, sent } = harness();
    const { record, localId } = makeSignedRecord();
    const tampered = { ...record, payloadB64: Buffer.from("evil").toString("base64") };
    await registry.dispatch("dht.rec_store", storeFrame(localId, tampered), { id: "p" });
    assert.equal(recordStore.get(localId, 1000), null);
    const ack = lastAck(sent);
    assert.equal(ack.status, "rejected");
    assert.equal(ack.reason, "invalid-record");
  });

  it("drops a rec_store over the per-peer rate limit SILENTLY (no ack — timeout is the local outcome)", async () => {
    const limiter = new SlidingWindowRateLimiter({ windowMs: 60_000, maxAttempts: 1 });
    const { registry, recordStore, sent } = harness({ storeRateLimiter: limiter });
    const a = makeSignedRecord({ recordId: "a" });
    const b = makeSignedRecord({ recordId: "b" });
    const socket = { id: "same-peer" };
    await registry.dispatch("dht.rec_store", storeFrame(a.localId, a.record), socket);
    await registry.dispatch("dht.rec_store", storeFrame(b.localId, b.record), socket);
    assert.ok(recordStore.get(a.localId, 1000), "first within budget");
    assert.equal(recordStore.get(b.localId, 1000), null, "second over budget dropped");
    const acks = sent.filter((s) => s.obj._ctl === "dht.rec_store.ack");
    assert.equal(acks.length, 1, "the rate-limited request gets no ack");
  });

  it("queryRecStore registers the wait before sending and resolves on a matching ack", async () => {
    const { proto, registry, sent } = harness();
    const { record, localId } = makeSignedRecord();
    const socket = { id: "storer" };
    const pending = proto.queryRecStore(socket, localId, record);
    const req = sent.find((s) => s.obj._ctl === "dht.rec_store");
    assert.ok(req, "request frame sent");
    assert.equal(req.obj.protocolVersion, DHT_RECORD_STORE_PROTOCOL_VERSION);
    assert.ok(req.obj.requestId.length > 10);
    await registry.dispatch("dht.rec_store.ack", {
      _ctl: "dht.rec_store.ack",
      protocolVersion: DHT_RECORD_STORE_PROTOCOL_VERSION,
      requestId: req.obj.requestId,
      key: localId,
      recordDigestHex: durableRecordDigestHex(record),
      status: "stored",
      reason: null,
    }, socket);
    const outcome = await pending;
    assert.deepEqual(outcome, { outcome: "stored", reason: null });
  });

  it("ignores mismatched-socket, wrong-digest, wrong-key, and unknown-request acks (timeout stays authoritative)", async () => {
    const { proto, registry, sent } = harness({ queryTimeoutMs: 150 });
    const { record, localId } = makeSignedRecord();
    const socket = { id: "storer" };
    const otherSocket = { id: "intruder" };
    const pending = proto.queryRecStore(socket, localId, record);
    const req = sent.find((s) => s.obj._ctl === "dht.rec_store");
    const goodDigest = durableRecordDigestHex(record);
    const base = {
      _ctl: "dht.rec_store.ack",
      protocolVersion: DHT_RECORD_STORE_PROTOCOL_VERSION,
      requestId: req.obj.requestId,
      key: localId,
      recordDigestHex: goodDigest,
      status: "stored",
      reason: null,
    };
    // Wrong socket.
    await registry.dispatch("dht.rec_store.ack", base, otherSocket);
    // Wrong digest (right socket).
    await registry.dispatch("dht.rec_store.ack", { ...base, recordDigestHex: "0".repeat(64) }, socket);
    // Wrong key (right socket).
    await registry.dispatch("dht.rec_store.ack", { ...base, key: "f".repeat(64) }, socket);
    // Unknown request id.
    await registry.dispatch("dht.rec_store.ack", { ...base, requestId: "rec-s-forged" }, socket);
    const outcome = await pending;
    assert.deepEqual(outcome, { outcome: "timeout", reason: null }, "none of the bad acks consumed the wait");
    // A LATE valid ack after timeout is also ignored (no pending state left).
    await registry.dispatch("dht.rec_store.ack", base, socket);
  });

  it("a rejected ack surfaces the bounded remote reason to the sender", async () => {
    const { proto, registry, sent } = harness();
    const { record, localId } = makeSignedRecord();
    const socket = { id: "storer" };
    const pending = proto.queryRecStore(socket, localId, record);
    const req = sent.find((s) => s.obj._ctl === "dht.rec_store");
    await registry.dispatch("dht.rec_store.ack", {
      _ctl: "dht.rec_store.ack",
      protocolVersion: DHT_RECORD_STORE_PROTOCOL_VERSION,
      requestId: req.obj.requestId,
      key: localId,
      recordDigestHex: durableRecordDigestHex(record),
      status: "rejected",
      reason: "quota",
    }, socket);
    assert.deepEqual(await pending, { outcome: "rejected", reason: "quota" });
  });

  it("serves a stored record on rec_find", async () => {
    const { registry, recordStore, sent } = harness();
    const { record, localId } = makeSignedRecord();
    recordStore.store(localId, record, 1000);
    await registry.dispatch("dht.rec_find", { _ctl: "dht.rec_find", queryId: "q1", key: localId }, { id: "asker" });
    const reply = sent.find((s) => s.obj._ctl === "dht.rec_find.reply");
    assert.ok(reply);
    assert.equal(reply.obj.queryId, "q1");
    assert.deepEqual(reply.obj.record, record);
  });

  it("returns k-closest nodes on rec_find miss", async () => {
    const { registry, kBuckets, sent } = harness();
    kBuckets.addOrUpdate(DhtNodeId.fromRelayKeyId("peer-1"), "peer-1", { id: "p1" }, 1000);
    const { localId } = makeSignedRecord();
    await registry.dispatch("dht.rec_find", { _ctl: "dht.rec_find", queryId: "q2", key: localId }, { id: "asker" });
    const reply = sent.find((s) => s.obj._ctl === "dht.rec_find.reply");
    assert.ok(reply);
    assert.equal(reply.obj.record, null);
    assert.ok(Array.isArray(reply.obj.nodes));
    assert.ok(reply.obj.nodes.some((n) => n.relayKeyId === "peer-1"));
  });
});

describe("re-audit R6: refresh persistence write-through", () => {
  it("a refresh persists the moved retention window — a refresh-kept holder survives restart", () => {
    const clock = { now: 1_000 };
    const persisted = new Map();
    const selfNodeId = DhtNodeId.fromRelayKeyId("self-r6");
    const recordStore = new DurableRecordStore({ maxRecordTtlMs: 2_000_000 });
    const proto = new DurableRecordProtocol({
      kBuckets: new KBucketTable(selfNodeId, { k: 20 }),
      recordStore,
      registry: new ControlMessageRegistry(),
      selfNodeId,
      encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
      trySendFrame: () => {},
      nowMs: () => clock.now,
      onRecordStored: (localId, entry) => persisted.set(localId, {
        localId, record: entry.record, storedAtMs: entry.storedAtMs, ttlMs: entry.ttlMs,
      }),
    });

    const { record, localId } = makeSignedRecord({
      issuedAtMs: clock.now, expiresAtMs: clock.now + 100_000_000,
    });
    assert.equal(proto.storeVerified(localId, record).reason, null, "first-time store");
    const firstWindow = { ...persisted.get(localId) };
    assert.equal(firstWindow.storedAtMs, 1_000);

    // Mid-window, an acked re-replication refreshes the identical bytes.
    clock.now = 1_500_000;
    assert.equal(proto.storeVerified(localId, record).reason, "refreshed");
    const refreshedWindow = { ...persisted.get(localId) };
    assert.equal(refreshedWindow.storedAtMs, 1_500_000,
      "the refreshed window was written through to persistence");

    // Restart AFTER the original window lapsed but within the refreshed one.
    const restartAt = 2_500_000;
    const survivor = new DurableRecordStore({ maxRecordTtlMs: 2_000_000 });
    survivor.loadFromSnapshot([refreshedWindow], restartAt);
    assert.ok(survivor.get(localId, restartAt),
      "a holder kept alive by acked refreshes still serves the record after restart");

    // The pre-fix persisted state (original window) demonstrates the gap.
    const stale = new DurableRecordStore({ maxRecordTtlMs: 2_000_000 });
    stale.loadFromSnapshot([firstWindow], restartAt);
    assert.equal(stale.get(localId, restartAt), null,
      "the stale window would have dropped the record — the exact R2 churn hazard");
  });
});
