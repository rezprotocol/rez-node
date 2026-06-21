import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { verifyDurableRecord, durableRecordTargetId } from "../src/routing/dht/DurableRecord.js";
import { DurableRecordStore } from "../src/routing/dht/DurableRecordStore.js";
import { makeSignedRecord } from "./support/durableRecord.js";

describe("verifyDurableRecord", () => {
  it("accepts a well-formed signed record and returns the publisher-bound localId", () => {
    const { record, localId } = makeSignedRecord({ recordKind: "peerlink-invite", recordId: "plinv_1" });
    const v = verifyDurableRecord(record, 1000);
    assert.equal(v.ok, true);
    assert.equal(v.reason, null);
    assert.equal(v.localId, localId);
    // The slot key is itself the routing target hash.
    assert.equal(durableRecordTargetId(v.localId).hex, localId);
  });

  it("rejects a tampered payload (signature breaks)", () => {
    const { record } = makeSignedRecord();
    const tampered = { ...record, payloadB64: Buffer.from("evil").toString("base64") };
    assert.equal(verifyDurableRecord(tampered, 1000).reason, "bad-signature");
  });

  it("rejects a record signed by a different key than it claims", () => {
    const a = makeSignedRecord();
    const b = makeSignedRecord();
    // Claim a's publisher but keep b's signature.
    const forged = { ...a.record, sigB64: b.record.sigB64 };
    assert.equal(verifyDurableRecord(forged, 1000).reason, "bad-signature");
  });

  it("rejects an expired record", () => {
    const { record } = makeSignedRecord({ issuedAtMs: 1000, expiresAtMs: 2000 });
    assert.equal(verifyDurableRecord(record, 5000).reason, "expired");
  });

  it("rejects a bad expiry window (expiresAtMs <= issuedAtMs)", () => {
    const { record } = makeSignedRecord({ issuedAtMs: 5000, expiresAtMs: 5000 });
    assert.equal(verifyDurableRecord(record, 1000).reason, "bad-expiry-window");
  });

  it("rejects an oversize payload", () => {
    const big = Buffer.alloc(64).toString("base64");
    const { record } = makeSignedRecord({ payloadB64: big });
    assert.equal(verifyDurableRecord(record, 1000, { maxBytes: 8 }).reason, "too-large");
  });

  it("rejects missing fields / wrong version", () => {
    const { record } = makeSignedRecord();
    assert.equal(verifyDurableRecord({ ...record, v: 99 }, 1000).reason, "bad-version");
    assert.equal(verifyDurableRecord({ ...record, sigB64: "" }, 1000).reason, "missing-fields");
  });

  it("rejects a record issued too far in the future (bounded clock skew)", () => {
    // A far-future issuedAtMs would poison the store slot (it orders by
    // issuedAtMs, so honest later updates would read as "older").
    const { record } = makeSignedRecord({ issuedAtMs: 10_000_000, expiresAtMs: 13_600_000 });
    assert.equal(verifyDurableRecord(record, 1000).reason, "future-issuance");
    // Within the skew window the same record verifies fine.
    assert.equal(verifyDurableRecord(record, 10_000_000).ok, true);
  });
});

describe("DurableRecordStore", () => {
  it("stores and retrieves a record", () => {
    const store = new DurableRecordStore();
    const { record, localId } = makeSignedRecord();
    const r = store.store(localId, record, 1000);
    assert.equal(r.stored, true);
    assert.equal(store.get(localId, 1000), record);
  });

  it("refreshes the retention window on identical re-store (idempotent)", () => {
    const store = new DurableRecordStore();
    const { record, localId } = makeSignedRecord();
    store.store(localId, record, 1000);
    const r = store.store(localId, record, 2000);
    assert.equal(r.stored, true);
    assert.equal(r.reason, "refreshed");
  });

  it("converges deterministically on equal issuedAtMs via sigB64 tie-break (no replica disagreement)", () => {
    // Two honest publishes of DIFFERENT content in the SAME millisecond to the
    // same slot. Without a deterministic tie-break, a replica that saw X first
    // and one that saw Y first would each reject the other as immutable and
    // diverge forever. The store must pick the SAME winner regardless of order.
    const keypair = makeSignedRecord().keypair;
    const x = makeSignedRecord({ keypair, recordId: "devset", issuedAtMs: 1000, expiresAtMs: 9_000_000, payloadText: "alpha" });
    const y = makeSignedRecord({ keypair, recordId: "devset", issuedAtMs: 1000, expiresAtMs: 9_000_000, payloadText: "bravo" });
    assert.equal(x.localId, y.localId, "same publisher+slot ⇒ same localId");
    assert.notEqual(x.record.sigB64, y.record.sigB64, "different content ⇒ different sig");
    const winner = x.record.sigB64 > y.record.sigB64 ? x : y;
    const loser = winner === x ? y : x;

    // Order 1: loser lands first, then the winner replaces it.
    const s1 = new DurableRecordStore();
    assert.equal(s1.store(loser.localId, loser.record, 1500).stored, true);
    const r1 = s1.store(winner.localId, winner.record, 1500);
    assert.equal(r1.stored, true);
    assert.equal(r1.reason, null, "the tie-break winner re-stores and re-replicates");
    assert.equal(s1.get(winner.localId, 1500), winner.record);

    // Order 2: winner lands first, then the loser is rejected — same end state.
    const s2 = new DurableRecordStore();
    assert.equal(s2.store(winner.localId, winner.record, 1500).stored, true);
    const r2 = s2.store(loser.localId, loser.record, 1500);
    assert.equal(r2.stored, false);
    assert.equal(r2.reason, "immutable");
    assert.equal(s2.get(winner.localId, 1500), winner.record, "both arrival orders converge on the winner");
  });

  it("treats a live slot as immutable for an unstamped (non-orderable) different record", () => {
    const store = new DurableRecordStore();
    const { record, localId } = makeSignedRecord();
    store.store(localId, record, 1000);
    // Strip issuedAtMs so the records are not comparable — keep the incumbent.
    const altered = { ...record, sigB64: "AAAAdifferent", issuedAtMs: undefined };
    const r = store.store(localId, altered, 1000);
    assert.equal(r.stored, false);
    assert.equal(r.reason, "immutable");
  });

  it("rolls a live slot strictly forward for the same publisher (monotonic by issuedAtMs)", () => {
    // A device-set add/remove re-signs the SAME slot with a later issuedAtMs.
    // The old record is still cryptographically live (30d TTL), so without
    // roll-forward the update would be silently rejected as immutable.
    const store = new DurableRecordStore();
    const keypair = makeSignedRecord().keypair;
    const v1 = makeSignedRecord({ keypair, recordId: "devset", issuedAtMs: 1000, expiresAtMs: 9_000_000 });
    const v2 = makeSignedRecord({ keypair, recordId: "devset", issuedAtMs: 2000, expiresAtMs: 9_000_000 });
    assert.equal(v1.localId, v2.localId, "same publisher+slot ⇒ same localId across revisions");
    assert.equal(store.store(v1.localId, v1.record, 1500).stored, true);
    const r = store.store(v2.localId, v2.record, 1500);
    assert.equal(r.stored, true);
    assert.equal(r.reason, null, "a newer issuance re-stores (and re-replicates)");
    assert.equal(store.get(v2.localId, 1500), v2.record, "the newer record now serves the slot");
    // Quota reflects exactly one live record, not two.
    assert.equal(store.publisherUsage(v2.record.publisherPublicKeyB64).count, 1);
  });

  it("rejects an older issuance once a newer one holds the slot (rollback / stale rebroadcast)", () => {
    const store = new DurableRecordStore();
    const keypair = makeSignedRecord().keypair;
    const older = makeSignedRecord({ keypair, recordId: "devset", issuedAtMs: 1000, expiresAtMs: 9_000_000 });
    const newer = makeSignedRecord({ keypair, recordId: "devset", issuedAtMs: 2000, expiresAtMs: 9_000_000 });
    store.store(newer.localId, newer.record, 1500);
    const r = store.store(older.localId, older.record, 1500);
    assert.equal(r.stored, false);
    assert.equal(r.reason, "older-record");
    assert.equal(store.get(newer.localId, 1500), newer.record, "the newer record is preserved");
  });

  it("enforces a per-publisher record-count quota", () => {
    const store = new DurableRecordStore({ maxRecordsPerPublisher: 2 });
    const keypair = makeSignedRecord().keypair;
    const a = makeSignedRecord({ keypair, recordId: "a" });
    const b = makeSignedRecord({ keypair, recordId: "b" });
    const c = makeSignedRecord({ keypair, recordId: "c" });
    assert.equal(store.store(a.localId, a.record, 1000).stored, true);
    assert.equal(store.store(b.localId, b.record, 1000).stored, true);
    const r = store.store(c.localId, c.record, 1000);
    assert.equal(r.stored, false);
    assert.equal(r.reason, "publisher-record-quota");
  });

  it("enforces a per-publisher byte quota", () => {
    const store = new DurableRecordStore({ maxBytesPerPublisher: 8 });
    const keypair = makeSignedRecord().keypair;
    const a = makeSignedRecord({ keypair, recordId: "a", payloadB64: Buffer.alloc(6).toString("base64") });
    const b = makeSignedRecord({ keypair, recordId: "b", payloadB64: Buffer.alloc(6).toString("base64") });
    assert.equal(store.store(a.localId, a.record, 1000).stored, true);
    const r = store.store(b.localId, b.record, 1000);
    assert.equal(r.stored, false);
    assert.equal(r.reason, "publisher-byte-quota");
  });

  it("expires records by signed expiresAtMs", () => {
    const store = new DurableRecordStore();
    const { record, localId } = makeSignedRecord({ issuedAtMs: 1000, expiresAtMs: 2000 });
    store.store(localId, record, 1000);
    assert.equal(store.get(localId, 5000), null);
    assert.equal(store.size, 0);
  });

  it("caps retention at maxRecordTtlMs even for a far-future expiry", () => {
    const store = new DurableRecordStore({ maxRecordTtlMs: 1000 });
    const { record, localId } = makeSignedRecord({ issuedAtMs: 0, expiresAtMs: 10_000_000 });
    store.store(localId, record, 0);
    assert.ok(store.get(localId, 500), "still live within cap");
    assert.equal(store.get(localId, 1500), null, "evicted past the ttl cap");
  });

  it("loadFromSnapshot rebuilds entries and recomputes quota, dropping expired", () => {
    const store = new DurableRecordStore();
    const keypair = makeSignedRecord().keypair;
    const live = makeSignedRecord({ keypair, recordId: "live", issuedAtMs: 1000, expiresAtMs: 9_000_000 });
    const dead = makeSignedRecord({ keypair, recordId: "dead", issuedAtMs: 1000, expiresAtMs: 2000 });
    store.loadFromSnapshot([
      { localId: live.localId, record: live.record, storedAtMs: 1000, ttlMs: 8_000_000 },
      { localId: dead.localId, record: dead.record, storedAtMs: 1000, ttlMs: 1000 },
    ], 5000);
    assert.equal(store.size, 1);
    assert.ok(store.get(live.localId, 5000));
    assert.equal(store.get(dead.localId, 5000), null);
    // Quota reflects only the live record.
    assert.equal(store.publisherUsage(live.publicKeyB64).count, 1);
  });

  it("keeps the per-publisher count accurate when releasing an empty-payload record", () => {
    // Regression: releasing a record must not key deletion on bytes — a
    // publisher can hold multiple 0-byte records, and dropping one would
    // otherwise wipe the count for the records still held (quota bypass).
    const store = new DurableRecordStore({ maxRecordsPerPublisher: 2 });
    const keypair = makeSignedRecord().keypair;
    const a = makeSignedRecord({ keypair, recordId: "a", payloadB64: "" });
    const b = makeSignedRecord({ keypair, recordId: "b", payloadB64: "" });
    store.store(a.localId, a.record, 1000);
    store.store(b.localId, b.record, 1000);
    assert.equal(store.publisherUsage(a.publicKeyB64).count, 2);
    store.remove(a.localId);
    assert.equal(store.publisherUsage(a.publicKeyB64).count, 1, "remaining record still counts toward quota");
  });

  it("evictExpired returns the evicted slot keys", () => {
    const store = new DurableRecordStore();
    const { record, localId } = makeSignedRecord({ issuedAtMs: 1000, expiresAtMs: 2000 });
    store.store(localId, record, 1000);
    const evicted = store.evictExpired(5000);
    assert.deepEqual(evicted, [localId]);
  });
});
