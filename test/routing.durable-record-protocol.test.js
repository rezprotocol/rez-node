import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DurableRecordProtocol } from "../src/routing/dht/DurableRecordProtocol.js";
import { DurableRecordStore } from "../src/routing/dht/DurableRecordStore.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { SlidingWindowRateLimiter } from "../src/util/SlidingWindowRateLimiter.js";
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
  });
  proto.install();
  return { proto, registry, recordStore, kBuckets, sent };
}

describe("DurableRecordProtocol handlers", () => {
  it("accepts a valid rec_store under its publisher-bound slot key", async () => {
    const { registry, recordStore } = harness();
    const { record, localId } = makeSignedRecord();
    await registry.dispatch("dht.rec_store", { _ctl: "dht.rec_store", key: localId, record }, { id: "p" });
    assert.equal(recordStore.get(localId, 1000), record);
  });

  it("rejects a rec_store whose announced key does not match the record (substitution)", async () => {
    const { registry, recordStore } = harness();
    const a = makeSignedRecord({ recordId: "a" });
    const b = makeSignedRecord({ recordId: "b" });
    // Valid record `a`, but announced under `b`'s slot key.
    await registry.dispatch("dht.rec_store", { _ctl: "dht.rec_store", key: b.localId, record: a.record }, { id: "p" });
    assert.equal(recordStore.get(b.localId, 1000), null);
    assert.equal(recordStore.get(a.localId, 1000), null);
  });

  it("rejects a rec_store with an invalid signature", async () => {
    const { registry, recordStore } = harness();
    const { record, localId } = makeSignedRecord();
    const tampered = { ...record, payloadB64: Buffer.from("evil").toString("base64") };
    await registry.dispatch("dht.rec_store", { _ctl: "dht.rec_store", key: localId, record: tampered }, { id: "p" });
    assert.equal(recordStore.get(localId, 1000), null);
  });

  it("drops a rec_store over the per-peer rate limit", async () => {
    const limiter = new SlidingWindowRateLimiter({ windowMs: 60_000, maxAttempts: 1 });
    const { registry, recordStore } = harness({ storeRateLimiter: limiter });
    const a = makeSignedRecord({ recordId: "a" });
    const b = makeSignedRecord({ recordId: "b" });
    const socket = { id: "same-peer" };
    await registry.dispatch("dht.rec_store", { _ctl: "dht.rec_store", key: a.localId, record: a.record }, socket);
    await registry.dispatch("dht.rec_store", { _ctl: "dht.rec_store", key: b.localId, record: b.record }, socket);
    assert.ok(recordStore.get(a.localId, 1000), "first within budget");
    assert.equal(recordStore.get(b.localId, 1000), null, "second over budget dropped");
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
