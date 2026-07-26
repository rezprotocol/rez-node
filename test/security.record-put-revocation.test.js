import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  REZ_CONTRACT_TYPES,
  buildDurableRecordV2,
  durableRecordV2SignableBytes,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  DeviceRegistrationV1,
} from "@rezprotocol/core";
import { RecordHandler } from "../src/protocol/handlers/RecordHandler.js";
import { DhtNode } from "../src/routing/dht/DhtNode.js";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { makeSignedRecord } from "./support/durableRecord.js";

// AUDIT P0 follow-on (2026-07-26) — the HOME applies its own revocation state at record.put.
//
// The generic record.put deliberately does not bind a record to the session (records are
// self-authenticating). That is fine, but it handed the overlay verifier NO revocation state, so a
// device whose capability certificate had been revoked could still publish records that certificate
// no longer authorizes.
//
// The overlay is right to hold no revocation state — it is account-agnostic, and a replica that does
// not home an account cannot learn it. But the home CAN, for the accounts its cluster homes, and
// this handler is the door it controls. So: check delegated chains here, change nothing about the
// replica ingress.
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();
const NOW = 1_700_000_000_000;
const FAR = NOW + 3_600_000;
const PAYLOAD = Buffer.from("device-set").toString("base64");

function key() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { publicKeyB64: Buffer.from(kp.publicKey).toString("base64"), privateKey: kp.privateKey };
}

function buildLeaf({ account, signer, granteePub }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePub,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePub),
    capabilities: ["deviceSet.publish"],
    maxDelegationDepth: 0,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
    signerPublicKeyB64: signer.publicKeyB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = CRYPTO.sign({ privateKey: signer.privateKey, msg: AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }) });
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig: { alg: "ed25519", sigB64: Buffer.from(sig).toString("base64") } });
}

function delegatedDeviceSet({ owner, signer, leaf }) {
  const rec = buildDurableRecordV2({
    recordKind: "peerlink-device-set",
    recordId: "peer-1",
    ownerPublicKeyB64: owner.publicKeyB64,
    signerPublicKeyB64: signer.publicKeyB64,
    certChain: [leaf.toJSON()],
    requiredCapability: "deviceSet.publish",
    payloadB64: PAYLOAD,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
  });
  const sig = CRYPTO.sign({ privateKey: signer.privateKey, msg: durableRecordV2SignableBytes(rec) });
  return { ...rec, sigB64: Buffer.from(sig).toString("base64") };
}

// A DhtNode wired for local storage only: no peers, so putRecord holds a local copy and finds zero
// replicas. That is all this test needs — the question is whether the record is ACCEPTED.
function makeDht() {
  return new DhtNode({
    selfRelayKeyId: "node-dev:test",
    controlMessageRegistry: { register() {}, unregister() {} },
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: () => {},
    nowMs: () => NOW,
    config: { k: 20, alpha: 3, queryTimeoutMs: 100 },
  });
}

function makeCtx({ dht, serializer, session = true } = {}) {
  const sent = [];
  const runtime = { recordDht: dht };
  if (serializer !== undefined) runtime.accountMutationSerializer = serializer;
  const ctx = {
    runtime,
    requireSession(requestId) {
      if (!session) { this.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session required", retryable: false }); return false; }
      return true;
    },
    sendError(opts) { sent.push({ kind: "error", ...opts }); },
    sendResponse(requestId, type, body) { sent.push({ kind: "response", requestId, type, body }); },
  };
  return { ctx, sent };
}

const last = (sent) => sent.at(-1);
const authority = (revokedCertIds, minValidIssuedAtMs = 0) => ({
  async getAuthorityState() { return { epoch: 7, revokedCertIds, minValidIssuedAtMs }; },
});

describe("AUDIT P0 follow-on: record.put applies the home's revocation state", () => {
  it("REFUSES a delegated record whose certificate the owner account revoked", async () => {
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const { ctx, sent } = makeCtx({ dht, serializer: authority([leaf.certId]) });
    await new RecordHandler(ctx).handlePut("r1", { record });

    const err = last(sent);
    assert.equal(err.kind, "error");
    assert.equal(err.code, "RECORD_REJECTED");
    assert.equal(dht.recordStore.size, 0, "and nothing was stored");
  });

  it("still accepts the SAME record when the certificate is not revoked", async () => {
    // The other half of the requirement: non-revoked delegated publication must keep working, or
    // this gate would just break multi-device fan-out.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const { ctx, sent } = makeCtx({ dht, serializer: authority([]) });
    await new RecordHandler(ctx).handlePut("r1", { record });

    const res = last(sent);
    assert.equal(res.kind, "response");
    assert.equal(res.type, T.RECORD_PUT_RES);
    assert.equal(dht.recordStore.size, 1);
  });

  it("applies the issuance CUTOFF too, not just the revoked-id set", async () => {
    // minValidIssuedAtMs revokes a whole generation of certificates at once (bulk revocation). A
    // gate that only consulted revokedCertIds would sail straight past it.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const { ctx, sent } = makeCtx({ dht, serializer: authority([], NOW + 1) });
    await new RecordHandler(ctx).handlePut("r1", { record });

    assert.equal(last(sent).code, "RECORD_REJECTED");
    assert.equal(dht.recordStore.size, 0);
  });

  it("does NOT query the authority for a record that carries no delegation", async () => {
    // A direct (or V1) record has no certificate to revoke. Querying would cost a database round
    // trip on every put and widen the failure surface for nothing.
    const account = key();
    const direct = buildDurableRecordV2({
      recordKind: "peerlink-device-set",
      recordId: "peer-2",
      ownerPublicKeyB64: account.publicKeyB64,
      payloadB64: PAYLOAD,
      issuedAtMs: NOW,
      expiresAtMs: FAR,
    });
    const signed = {
      ...direct,
      sigB64: Buffer.from(CRYPTO.sign({ privateKey: account.privateKey, msg: durableRecordV2SignableBytes(direct) })).toString("base64"),
    };

    let queried = 0;
    const serializer = { async getAuthorityState() { queried += 1; return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 }; } };
    const dht = makeDht();
    const { ctx, sent } = makeCtx({ dht, serializer });
    const h = new RecordHandler(ctx);

    await h.handlePut("r1", { record: signed });
    assert.equal(last(sent).type, T.RECORD_PUT_RES);

    // ...and a V1 record likewise.
    const v1 = makeSignedRecord({ recordKind: "peerlink-invite", recordId: "plinv_1", issuedAtMs: NOW, expiresAtMs: FAR });
    await h.handlePut("r2", { record: v1.record });
    assert.equal(last(sent).type, T.RECORD_PUT_RES);

    assert.equal(queried, 0, "no delegation, no authority lookup");
  });

  it("a backend failure resolving the authority is NOT an allow", async () => {
    // The fail-open trap: if we cannot establish whether the certificate was revoked, accepting the
    // record guesses at exactly the thing being checked. It surfaces as retryable instead.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const serializer = { async getAuthorityState() { throw Object.assign(new Error("db down"), { code: "57P01" }); } };
    const { ctx, sent } = makeCtx({ dht, serializer });
    await new RecordHandler(ctx).handlePut("r1", { record });

    const err = last(sent);
    assert.equal(err.code, "SERVICE_UNAVAILABLE");
    assert.equal(err.retryable, true);
    assert.equal(dht.recordStore.size, 0);
  });

  it("a MALFORMED authority shape is not coerced into an empty (permissive) state", async () => {
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const serializer = { async getAuthorityState() { return { epoch: 3, revokedCertIds: "not-an-array", minValidIssuedAtMs: 0 }; } };
    const { ctx, sent } = makeCtx({ dht, serializer });
    await new RecordHandler(ctx).handlePut("r1", { record });

    assert.equal(last(sent).code, "INTERNAL");
    assert.equal(dht.recordStore.size, 0);
  });

  it("REPLICA behavior is preserved: a node with no account authority accepts as before", async () => {
    // fs / desktop / relay-only deployments home no accounts, so there is no revocation state to
    // withhold. This must stay byte-identical to the pre-change path, or plain relays would stop
    // accepting delegated records entirely.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const { ctx, sent } = makeCtx({ dht }); // no accountMutationSerializer wired at all
    await new RecordHandler(ctx).handlePut("r1", { record });

    assert.equal(last(sent).type, T.RECORD_PUT_RES);
    assert.equal(dht.recordStore.size, 1);
  });

  it("the OVERLAY verifier is deliberately left revocation-blind (the boundary, pinned)", async () => {
    // This asserts the ARCHITECTURE, not an oversight, and it is here so nobody "fixes" it into a
    // replication outage. verifyDurableRecordDual with no revocationState accepts a record whose
    // certificate the owner revoked — because a replica cannot know. Making the overlay fail closed
    // would mean every node refusing every delegated record for every account it does not home,
    // i.e. no replication at all, while an attacker just pushes to a node that is equally blind.
    //
    // Revocation is enforced where the answer is knowable: HERE at the home's record.put (the tests
    // above), at the home's session/dispatch guards, and at the READER, which applies the account's
    // published authority state. That published state is itself now root-signed-only and rollback-
    // floored, which is what makes the reader's check trustworthy.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const blind = await verifyDurableRecordDual(record, NOW + 1000);
    assert.equal(blind.ok, true, "an account-agnostic replica accepts it");

    // Hand the SAME verifier the revocation state and it refuses — the check works, it is the
    // knowledge that the overlay lacks.
    const informed = await verifyDurableRecordDual(record, NOW + 1000, {
      revocationState: { revokedCertIds: [leaf.certId], minValidIssuedAtMs: 0 },
    });
    assert.equal(informed.ok, false);
  });

  it("an account this cluster does not home reads as an empty state, so foreign records still land", async () => {
    // getAuthorityState answers { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 } for an
    // unknown account — the same thing null meant. A stranger's record must not be refused just
    // because we have no row for them.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account: account.publicKeyB64, signer: account, granteePub: device.publicKeyB64 });
    const record = delegatedDeviceSet({ owner: account, signer: device, leaf });

    const dht = makeDht();
    const serializer = { async getAuthorityState() { return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 }; } };
    const { ctx, sent } = makeCtx({ dht, serializer });
    await new RecordHandler(ctx).handlePut("r1", { record });

    assert.equal(last(sent).type, T.RECORD_PUT_RES);
    assert.equal(dht.recordStore.size, 1);
  });
});
