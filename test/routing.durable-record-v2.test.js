import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
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
