import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
import { DurableRecordStore } from "../src/routing/dht/DurableRecordStore.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { makeSignedRecord } from "./support/durableRecord.js";
import {
  buildDurableRecordV2,
  durableRecordV2SignableBytes,
  durableRecordV2Slot,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  DeviceRegistrationV1,
} from "@rezprotocol/core";

// S2.5 S8 / F2 (V7) — the overlay's version-dispatching verifier. V1 records take
// the unchanged synchronous self-authenticating path; V2 (owner/signer separated)
// routes through the rez-core dual-mode helper while the overlay keeps applying
// its anti-poison/size DoS guards. Real Ed25519 via NodeCryptoProvider.

const CRYPTO = new NodeCryptoProvider();
const NOW = 1000;
const FAR = NOW + 3_600_000;

function key() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { publicKeyB64: Buffer.from(kp.publicKey).toString("base64"), privateKey: kp.privateKey };
}

function buildCert({ account, signer, granteePub, capabilities }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePub,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePub),
    capabilities,
    maxDelegationDepth: 0,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
    signerPublicKeyB64: signer.publicKeyB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = CRYPTO.sign({ privateKey: signer.privateKey, msg: AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }) });
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig: { alg: "ed25519", sigB64: Buffer.from(sig).toString("base64") } });
}

function signV2(record, privateKey) {
  const sig = CRYPTO.sign({ privateKey, msg: durableRecordV2SignableBytes(record) });
  return { ...record, sigB64: Buffer.from(sig).toString("base64") };
}

const PAYLOAD = Buffer.from("device-set").toString("base64");

describe("verifyDurableRecordDual — version dispatch", () => {
  it("still verifies a V1 record through the dispatcher (regression, sync path)", async () => {
    const { record, localId } = makeSignedRecord({ recordKind: "peerlink-invite", recordId: "plinv_1" });
    const v = await verifyDurableRecordDual(record, NOW);
    assert.equal(v.ok, true, v.reason);
    assert.equal(v.localId, localId);
  });

  it("verifies a V2 DIRECT record (signer == owner, no chain)", async () => {
    const B = key();
    const rec = signV2(buildDurableRecordV2({
      recordKind: "rez.device-set.v1", recordId: "peer-1",
      ownerPublicKeyB64: B.publicKeyB64, payloadB64: PAYLOAD, issuedAtMs: NOW, expiresAtMs: FAR,
    }), B.privateKey);
    const v = await verifyDurableRecordDual(rec, NOW);
    assert.equal(v.ok, true, v.reason);
    assert.equal(v.mode, "direct");
    assert.equal(v.localId, durableRecordV2Slot({ ownerPublicKeyB64: B.publicKeyB64, recordKind: "rez.device-set.v1", recordId: "peer-1" }));
    assert.equal(v.signerPublicKeyB64, B.publicKeyB64);
  });

  it("verifies a V2 DELEGATED record (C signs with a B→C chain) at the OWNER slot", async () => {
    const B = key();
    const C = key();
    const leaf = buildCert({ account: B.publicKeyB64, signer: B, granteePub: C.publicKeyB64, capabilities: ["deviceSet.publish"] });
    const rec = signV2(buildDurableRecordV2({
      recordKind: "rez.device-set.v1", recordId: "peer-1",
      ownerPublicKeyB64: B.publicKeyB64, signerPublicKeyB64: C.publicKeyB64,
      certChain: [leaf], requiredCapability: "deviceSet.publish",
      payloadB64: PAYLOAD, issuedAtMs: NOW, expiresAtMs: FAR,
    }), C.privateKey);
    const v = await verifyDurableRecordDual(rec, NOW);
    assert.equal(v.ok, true, v.reason);
    assert.equal(v.mode, "delegated");
    assert.equal(v.signerPublicKeyB64, C.publicKeyB64);
    // The slot is keyed on the OWNER, so the delegated record lands at B's coordinate.
    assert.equal(v.localId, durableRecordV2Slot({ ownerPublicKeyB64: B.publicKeyB64, recordKind: "rez.device-set.v1", recordId: "peer-1" }));
  });

  it("rejects a V2 record signed by an unauthorized key (no chain, signer != owner)", async () => {
    const B = key();
    const evil = key();
    const rec = signV2(buildDurableRecordV2({
      recordKind: "rez.device-set.v1", recordId: "peer-1",
      ownerPublicKeyB64: B.publicKeyB64, signerPublicKeyB64: evil.publicKeyB64,
      payloadB64: PAYLOAD, issuedAtMs: NOW, expiresAtMs: FAR,
    }), evil.privateKey);
    const v = await verifyDurableRecordDual(rec, NOW);
    assert.equal(v.ok, false);
    assert.match(v.reason, /authority/);
  });

  it("applies the overlay DoS guards to V2: too-large, future-issuance, bad-expiry-window", async () => {
    const B = key();
    const base = (over) => signV2(buildDurableRecordV2({
      recordKind: "rez.device-set.v1", recordId: "peer-1",
      ownerPublicKeyB64: B.publicKeyB64, payloadB64: PAYLOAD, issuedAtMs: NOW, expiresAtMs: FAR, ...over,
    }), B.privateKey);
    assert.equal((await verifyDurableRecordDual(base({ payloadB64: Buffer.alloc(64).toString("base64") }), NOW, { maxBytes: 8 })).reason, "too-large");
    assert.equal((await verifyDurableRecordDual(base({ issuedAtMs: 10_000_000, expiresAtMs: 13_600_000 }), NOW)).reason, "future-issuance");
    assert.equal((await verifyDurableRecordDual(base({ issuedAtMs: 5000, expiresAtMs: 5000 }), NOW)).reason, "bad-expiry-window");
  });

  it("rejects a V2 record with a tampered payload (signature breaks)", async () => {
    const B = key();
    const rec = signV2(buildDurableRecordV2({
      recordKind: "rez.device-set.v1", recordId: "peer-1",
      ownerPublicKeyB64: B.publicKeyB64, payloadB64: PAYLOAD, issuedAtMs: NOW, expiresAtMs: FAR,
    }), B.privateKey);
    rec.payloadB64 = Buffer.from("evil").toString("base64");
    const v = await verifyDurableRecordDual(rec, NOW);
    assert.equal(v.ok, false);
    assert.match(v.reason, /signature invalid/);
  });

  it("rejects an unknown record version", async () => {
    const { record } = makeSignedRecord();
    const v = await verifyDurableRecordDual({ ...record, v: 99 }, NOW);
    assert.equal(v.ok, false);
    assert.equal(v.reason, "bad-version");
  });
});

// S2.5 S8 follow-up — the store's slot-roll + quota accounting must key V2
// records off the OWNER (the slot anchor), not the absent
// `publisherPublicKeyB64`. Before this, a same-slot V2 republish (exactly what
// a device-set refresh does) read as cross-publisher and bounced "immutable",
// and every V2 record's quota pooled under one "" bucket.
describe("DurableRecordStore — V2 owner-keyed accounting", () => {
  function v2For(owner, over = {}) {
    return signV2(buildDurableRecordV2({
      recordKind: "rez.device-set.v1", recordId: "peer-1",
      ownerPublicKeyB64: owner.publicKeyB64, payloadB64: PAYLOAD,
      issuedAtMs: NOW, expiresAtMs: FAR, ...over,
    }), over.signerPrivateKey || owner.privateKey);
  }
  function slotFor(owner, recordId = "peer-1") {
    return durableRecordV2Slot({ ownerPublicKeyB64: owner.publicKeyB64, recordKind: "rez.device-set.v1", recordId });
  }

  it("rolls a V2 slot strictly forward on a newer issuance from the same owner", () => {
    const B = key();
    const store = new DurableRecordStore();
    const slot = slotFor(B);
    assert.equal(store.store(slot, v2For(B), NOW).stored, true);
    // The refresh: same owner, same slot, different content, newer issuance.
    const r = store.store(slot, v2For(B, { issuedAtMs: NOW + 1, payloadB64: Buffer.from("device-set-v2").toString("base64") }), NOW + 1);
    assert.equal(r.stored, true, r.reason);
    assert.equal(r.reason, null);
    // Superseded record's quota was released — exactly one record charged to B.
    assert.equal(store.publisherUsage(B.publicKeyB64).count, 1);
  });

  it("still rejects an older V2 issuance (rollback defense preserved)", () => {
    const B = key();
    const store = new DurableRecordStore();
    const slot = slotFor(B);
    assert.equal(store.store(slot, v2For(B, { issuedAtMs: NOW + 10 }), NOW + 10).stored, true);
    const r = store.store(slot, v2For(B, { issuedAtMs: NOW, payloadB64: Buffer.from("stale").toString("base64") }), NOW + 10);
    assert.equal(r.stored, false);
    assert.equal(r.reason, "older-record");
  });

  it("a delegated-signed V2 record rolls the same owner slot forward", () => {
    const B = key();
    const C = key();
    const leaf = buildCert({ account: B.publicKeyB64, signer: B, granteePub: C.publicKeyB64, capabilities: ["deviceSet.publish"] });
    const store = new DurableRecordStore();
    const slot = slotFor(B);
    assert.equal(store.store(slot, v2For(B), NOW).stored, true);
    // Device C re-publishes the owner's slot: signer differs, owner (the
    // accounting key) is unchanged, so the roll-forward applies.
    const delegated = v2For(B, {
      signerPublicKeyB64: C.publicKeyB64, certChain: [leaf], requiredCapability: "deviceSet.publish",
      issuedAtMs: NOW + 1, signerPrivateKey: C.privateKey,
    });
    const r = store.store(slot, delegated, NOW + 1);
    assert.equal(r.stored, true, r.reason);
    assert.equal(store.get(slot, NOW + 1), delegated);
    assert.equal(store.publisherUsage(B.publicKeyB64).count, 1);
  });

  it("charges V2 quota to the OWNER key, separated per owner (no shared \"\" bucket)", () => {
    const B1 = key();
    const B2 = key();
    const store = new DurableRecordStore({ maxRecordsPerPublisher: 2 });
    assert.equal(store.store(slotFor(B1, "peer-1"), v2For(B1), NOW).stored, true);
    assert.equal(store.store(slotFor(B1, "peer-2"), v2For(B1, { recordId: "peer-2" }), NOW).stored, true);
    // B1 is now full; B2 must have its own untouched bucket.
    assert.equal(store.store(slotFor(B1, "peer-3"), v2For(B1, { recordId: "peer-3" }), NOW).reason, "publisher-record-quota");
    assert.equal(store.store(slotFor(B2, "peer-1"), v2For(B2), NOW).stored, true);
    assert.equal(store.publisherUsage(B1.publicKeyB64).count, 2);
    assert.equal(store.publisherUsage(B2.publicKeyB64).count, 1);
    assert.equal(store.publisherUsage("").count, 0);
    // Removal releases the owner's quota.
    assert.equal(store.remove(slotFor(B1, "peer-1")), true);
    assert.equal(store.publisherUsage(B1.publicKeyB64).count, 1);
  });

  it("a V2 record supersedes a V1 record at the same slot for the same key (identical slot math)", () => {
    const v1 = makeSignedRecord({ recordKind: "rez.device-set.v1", recordId: "peer-1", issuedAtMs: NOW, expiresAtMs: FAR });
    // The V2 slot for (owner=publisher, kind, id) IS the V1 localId — the
    // owner key occupies the publisher position in the derivation.
    const owner = { publicKeyB64: v1.publicKeyB64, privateKey: v1.privateKey };
    assert.equal(slotFor(owner), v1.localId);
    const store = new DurableRecordStore();
    assert.equal(store.store(v1.localId, v1.record, NOW).stored, true);
    const upgraded = v2For(owner, { issuedAtMs: NOW + 1 });
    const r = store.store(v1.localId, upgraded, NOW + 1);
    assert.equal(r.stored, true, r.reason);
    assert.equal(store.get(v1.localId, NOW + 1), upgraded);
    // The V1 record's quota (keyed off publisher) was released under the same key.
    assert.equal(store.publisherUsage(owner.publicKeyB64).count, 1);
  });
});
