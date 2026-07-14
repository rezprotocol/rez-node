import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  DeviceRegistrationV1,
  DEVICE_REGISTRATION_PURPOSE,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  AccountDeviceMutationV1,
  ACCOUNT_DEVICE_MUTATION_PURPOSE,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
} from "@rezprotocol/core";
import { AccountMutationHandler } from "../src/protocol/handlers/AccountMutationHandler.js";
import { DeviceHandler } from "../src/protocol/handlers/DeviceHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError } from "../src/storage/DurableInbox.js";

// S2.5 S11 leaf L6: AccountMutationHandler (handleSubmit + handleGetAuthorityState).
// REAL crypto — the handler builds its own NodeCryptoProvider and verifies the
// device-signed mutation envelope; signing here with the same provider proves the
// full chain, not a mock. The serializer is the real PgAccountMutationSerializer
// against an isolated schema (the authz-reject cases never reach it).
const crypto = new NodeCryptoProvider();
const PG_URL = process.env.REZ_PG_TEST_URL || "";
const NOW = Date.now();
const ISSUED = NOW - 1000;
const EXPIRES = NOW + 3_600_000;

async function genKey() {
  const kp = await crypto.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), pub: kp.publicKey, priv: kp.privateKey };
}
async function ed(priv, msgBytes) {
  return { alg: "ed25519", sigB64: bytesToBase64(await crypto.sign({ privateKey: priv, msg: msgBytes })) };
}

// A real account→device leaf capability cert (B is the anchor + root signer),
// so a delegated session's re-validation (audit F2) runs against a genuine chain.
async function buildLeafCert({ account, grantee, capabilities }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: grantee.pubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(grantee.pubB64),
    capabilities,
    maxDelegationDepth: 0,
    issuedAtMs: ISSUED,
    expiresAtMs: EXPIRES,
    signerPublicKeyB64: account.pubB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = await ed(account.priv, AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }));
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig });
}

// A sibling device's self-certifying inbox binding (device-signed).
async function makeBinding({ inboxId }) {
  const dev = await genKey();
  const deviceId = DeviceRegistrationV1.deviceIdFor(dev.pubB64);
  const body = {
    v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64: dev.pubB64, deviceId, inboxId,
    issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const binding = { ...body, sig: await ed(dev.priv, DeviceInboxBindingV1.signableBytes(body)) };
  return { dev, deviceId, inboxId, binding };
}

// A primary device world: an account-signed registration + the device-signed
// binding for the same device (device.bind PRIMARY mode).
async function makeRegisteredDevice({ account, inboxId }) {
  const dev = await genKey();
  const deviceId = DeviceRegistrationV1.deviceIdFor(dev.pubB64);
  const regBody = {
    v: 1, purpose: DEVICE_REGISTRATION_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64, devicePublicKeyB64: dev.pubB64,
    deviceId, issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const registration = { ...regBody, sig: await ed(account.priv, DeviceRegistrationV1.signableBytes(regBody)) };
  const bindBody = {
    v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64: dev.pubB64, deviceId, inboxId,
    issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const binding = { ...bindBody, sig: await ed(dev.priv, DeviceInboxBindingV1.signableBytes(bindBody)) };
  return { dev, deviceId, inboxId, registration, binding };
}

function makeBindCtx({ durableInbox, accountDeviceRegistry, accountMutationSerializer, ownerPublicKeyB64, sessionDeviceId, inboxId }) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { durableInbox, accountDeviceRegistry, accountMutationSerializer },
    ownerPublicKeyB64,
    sessionDeviceId,
    localInboxId: inboxId,
    sessionAuthority: null,
    requireSession() { return true; },
    isInboxBound(id) { return id === inboxId; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
  };
}

// Build a signed AccountDeviceMutationV1 (signer = B primary / C delegated).
async function makeMutation({ account, signerPriv, signerPubB64, opId, expectedRevision, action, target }) {
  const body = {
    v: 1, purpose: ACCOUNT_DEVICE_MUTATION_PURPOSE,
    opId, accountIdentityPublicKeyB64: account, expectedRevision, action, target,
    signerPublicKeyB64: signerPubB64, issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const sig = await ed(signerPriv, AccountDeviceMutationV1.signableBytes(body));
  return { ...body, sig };
}

function makeCtx({ serializer = null, ownerPublicKeyB64, sessionAuthority = null } = {}) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { accountMutationSerializer: serializer },
    ownerPublicKeyB64,
    sessionAuthority,
    requireSession() { return true; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
  };
}

// ---- authz rejects (no serializer needed — must reject before touching it) ----

test("submit: SERVICE_UNAVAILABLE when the serializer is absent (fs/desktop)", async () => {
  const acct = await genKey();
  const b = await makeBinding({ inboxId: "inbox:add-1" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
    opId: "op1", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  const ctx = makeCtx({ serializer: null, ownerPublicKeyB64: acct.pubB64 });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(ctx.captured.errors[0].code, "SERVICE_UNAVAILABLE");
});

test("submit: a mutation for a DIFFERENT account than the session is forbidden", async () => {
  const acct = await genKey();
  const other = await genKey();
  const b = await makeBinding({ inboxId: "inbox:add-2" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
    opId: "op2", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  // The serializer must never be reached — a throwing stub proves that.
  const boom = { async submitMutation() { throw new Error("must not be called"); } };
  const ctx = makeCtx({ serializer: boom, ownerPublicKeyB64: other.pubB64 });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("submit (delegated): a device WITHOUT the device.add capability is forbidden", async () => {
  const acct = await genKey();
  const delegate = await genKey();
  const b = await makeBinding({ inboxId: "inbox:add-3" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: delegate.priv, signerPubB64: delegate.pubB64,
    opId: "op3", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  const boom = { async submitMutation() { throw new Error("must not be called"); } };
  const ctx = makeCtx({
    serializer: boom,
    ownerPublicKeyB64: acct.pubB64,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["deviceSet.publish"] },
  });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("submit (delegated): a mutation signed by a key OTHER than the proven session signer is forbidden", async () => {
  const acct = await genKey();
  const delegate = await genKey();
  const impostor = await genKey();
  const b = await makeBinding({ inboxId: "inbox:add-4" });
  // Signed by impostor, but the session's proven signer is `delegate`.
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: impostor.priv, signerPubB64: impostor.pubB64,
    opId: "op4", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  const boom = { async submitMutation() { throw new Error("must not be called"); } };
  const ctx = makeCtx({
    serializer: boom,
    ownerPublicKeyB64: acct.pubB64,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["device.add"] },
  });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("submit: a tampered mutation signature is rejected", async () => {
  const acct = await genKey();
  const b = await makeBinding({ inboxId: "inbox:add-5" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
    opId: "op5", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  // Flip opId AFTER signing → the sig no longer covers the body.
  const tampered = { ...mutation, opId: "op5-tampered" };
  const boom = { async submitMutation() { throw new Error("must not be called"); } };
  const ctx = makeCtx({ serializer: boom, ownerPublicKeyB64: acct.pubB64 });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation: tampered });
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

// audit R4 L2c review P3: a serializer BAD_DEVICE_ID (the registry rejected a
// non-canonical id) must surface as the wire BAD_REQUEST, not INTERNAL. The record
// guard normally catches a non-canonical target upstream, so this is a defensive
// mapping — proven with a serializer stub that raises the code after authz passes.
test("submit: a serializer BAD_DEVICE_ID surfaces as BAD_REQUEST (not INTERNAL)", async () => {
  const acct = await genKey();
  const delegate = await genKey();
  const leafCert = await buildLeafCert({ account: acct, grantee: delegate, capabilities: ["device.add"] });
  const b = await makeBinding({ inboxId: "inbox:baddev" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: delegate.priv, signerPubB64: delegate.pubB64,
    opId: "baddev-op", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  const serializer = {
    async getAuthorityState() { return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 }; },
    async submitMutation() {
      const e = new Error("device id is not a canonical rez:dev id");
      e.code = "BAD_DEVICE_ID";
      throw e;
    },
  };
  const ctx = makeCtx({
    serializer,
    ownerPublicKeyB64: acct.pubB64,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["device.add"], certChain: [leafCert.toJSON()] },
  });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(ctx.captured.errors[0].code, "BAD_REQUEST");
});

// ---- audit 2026-07-09 regressions (no Pg — must reject before the serializer) ----

// F2: a delegated session's cached capability snapshot must NOT outlive a
// revocation. Even though the connect-time grant included device.add, the handler
// re-validates the chain against the home's CURRENT authority state per op; a
// revoked leaf now fails, and the serializer is never reached.
test("submit (F2): a delegated device REVOKED mid-session can no longer mutate", async () => {
  const acct = await genKey();
  const delegate = await genKey();
  const leafCert = await buildLeafCert({ account: acct, grantee: delegate, capabilities: ["device.add", "device.revoke"] });
  const b = await makeBinding({ inboxId: "inbox:f2-add" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: delegate.priv, signerPubB64: delegate.pubB64,
    opId: "f2-op", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  let submitCalled = false;
  const serializer = {
    // The home now reports this leaf cert as revoked.
    async getAuthorityState() { return { epoch: 1, revokedCertIds: [leafCert.certId], minValidIssuedAtMs: 0 }; },
    async submitMutation() { submitCalled = true; throw new Error("must not be called after revocation"); },
  };
  const ctx = makeCtx({
    serializer,
    ownerPublicKeyB64: acct.pubB64,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["device.add", "device.revoke"], certChain: [leafCert.toJSON()] },
  });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(submitCalled, false, "the serializer must not be reached for a revoked delegated signer");
  assert.equal(ctx.captured.responses.length, 0);
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

// F2 (positive): the SAME delegated flow still succeeds when the leaf is NOT
// revoked — the re-validation does not break legitimate delegated mutations.
test("submit (F2): a delegated mutation with a valid, un-revoked chain still applies", async () => {
  const acct = await genKey();
  const delegate = await genKey();
  const leafCert = await buildLeafCert({ account: acct, grantee: delegate, capabilities: ["device.add"] });
  const b = await makeBinding({ inboxId: "inbox:f2-ok" });
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: delegate.priv, signerPubB64: delegate.pubB64,
    opId: "f2-ok-op", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: b.binding },
  });
  const serializer = {
    async getAuthorityState() { return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 }; },
    async submitMutation() { return { revision: 1, devices: [{ deviceId: b.deviceId }], authorityState: { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 }, idempotentReplay: false }; },
  };
  const ctx = makeCtx({
    serializer,
    ownerPublicKeyB64: acct.pubB64,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["device.add"], certChain: [leafCert.toJSON()] },
  });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.equal(ctx.captured.responses[0].body.revision, 1);
});

// F3: a device.add binding is a device-signed self-cert. The handler must verify
// its signature (and validity window) before enrolling deviceId/inboxId — else an
// authorized mutator could reserve/pollute an arbitrary inbox binding.
test("submit (F3): a device.add binding with a TAMPERED signature is rejected", async () => {
  const acct = await genKey();
  const b = await makeBinding({ inboxId: "inbox:f3-tamper" });
  // Flip a byte of the (well-formed) binding signature AFTER it was signed.
  const flipped = b.binding.sig.sigB64.slice(0, -2) + (b.binding.sig.sigB64.endsWith("AA") ? "BB" : "AA");
  const tamperedBinding = { ...b.binding, sig: { ...b.binding.sig, sigB64: flipped } };
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
    opId: "f3-op", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: tamperedBinding },
  });
  let submitCalled = false;
  const serializer = { async submitMutation() { submitCalled = true; throw new Error("must not be called"); } };
  const ctx = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(submitCalled, false, "the serializer must not enroll an unverified binding");
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

// F3 (window): a validly-signed but EXPIRED binding is rejected before enroll.
test("submit (F3): a device.add binding outside its validity window is rejected", async () => {
  const acct = await genKey();
  const dev = await genKey();
  const deviceId = DeviceRegistrationV1.deviceIdFor(dev.pubB64);
  // issued + expires both in the past (expires > issued, so the record is valid,
  // but nowMs is past expiresAtMs).
  const body = {
    v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64: dev.pubB64, deviceId, inboxId: "inbox:f3-expired",
    issuedAtMs: NOW - 7_200_000, expiresAtMs: NOW - 3_600_000,
  };
  const binding = { ...body, sig: await ed(dev.priv, DeviceInboxBindingV1.signableBytes(body)) };
  const mutation = await makeMutation({
    account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
    opId: "f3-exp-op", expectedRevision: 0, action: "device.add",
    target: { deviceInboxBinding: binding },
  });
  let submitCalled = false;
  const serializer = { async submitMutation() { submitCalled = true; throw new Error("must not be called"); } };
  const ctx = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });
  assert.equal(submitCalled, false);
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

// F4 (2026-07-09): the revoke fail-close is now ATOMIC inside the serializer's
// transaction (PgAccountMutationSerializer + PgDurableInbox.revokeDeviceInTx), so
// the handler no longer runs a splittable post-commit second phase. Atomicity and
// its all-or-nothing rollback are proven in
// storage.pg.account-mutation-serializer.test.js; the account-wide end-to-end
// close through the handler is proven by the real-Pg fail-close test below.

// ---- end-to-end with the real Pg serializer ----

test(
  "submit: primary add, delegated add, stale retry, idempotent replay + getAuthorityState (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_account_mutation_handler";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const acct = await genKey();
    const delegate = await genKey();

    // (1) PRIMARY device.add — B-signed, no delegated authority. epoch 0 → 1.
    const b1 = await makeBinding({ inboxId: "inbox:e2e-add-1" });
    const m1 = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "e2e-op1", expectedRevision: 0, action: "device.add",
      target: { deviceInboxBinding: b1.binding },
    });
    const ctx1 = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctx1).handleSubmit("r1", { mutation: m1 });
    assert.equal(ctx1.captured.errors.length, 0, JSON.stringify(ctx1.captured.errors));
    const res1 = ctx1.captured.responses[0];
    assert.equal(res1.type, "account.deviceMutation.submit.res");
    assert.equal(res1.body.revision, 1);
    assert.equal(res1.body.idempotentReplay, false);
    assert.ok(res1.body.devices.some((d) => d.deviceId === b1.deviceId), "the added device is in the set");
    assert.equal(res1.body.authorityState.epoch, 1);

    // (2) DELEGATED device.add — C-signed, holds device.add. epoch 1 → 2. The
    // session carries the REAL leaf cert so the handler's per-op re-validation
    // (audit F2) runs against a genuine chain + the home's (empty) revocation set.
    const leafCert = await buildLeafCert({ account: acct, grantee: delegate, capabilities: ["device.add", "device.revoke"] });
    const b2 = await makeBinding({ inboxId: "inbox:e2e-add-2" });
    const m2 = await makeMutation({
      account: acct.pubB64, signerPriv: delegate.priv, signerPubB64: delegate.pubB64,
      opId: "e2e-op2", expectedRevision: 1, action: "device.add",
      target: { deviceInboxBinding: b2.binding },
    });
    const ctx2 = makeCtx({
      serializer,
      ownerPublicKeyB64: acct.pubB64,
      sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["device.add", "device.revoke"], certChain: [leafCert.toJSON()] },
    });
    await new AccountMutationHandler(ctx2).handleSubmit("r2", { mutation: m2 });
    assert.equal(ctx2.captured.errors.length, 0, JSON.stringify(ctx2.captured.errors));
    assert.equal(ctx2.captured.responses[0].body.revision, 2);

    // (3) STALE retry — expectedRevision 0 while epoch is 2 → no clobber, latest returned.
    const b3 = await makeBinding({ inboxId: "inbox:e2e-add-3" });
    const m3 = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "e2e-op3", expectedRevision: 0, action: "device.add",
      target: { deviceInboxBinding: b3.binding },
    });
    const ctx3 = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctx3).handleSubmit("r3", { mutation: m3 });
    assert.equal(ctx3.captured.errors.length, 0, JSON.stringify(ctx3.captured.errors));
    assert.equal(ctx3.captured.responses[0].body.stale, true);
    assert.equal(ctx3.captured.responses[0].body.currentRevision, 2);
    assert.ok(!ctx3.captured.responses[0].body.devices.some((d) => d.deviceId === b3.deviceId), "stale add is NOT applied");

    // (4) IDEMPOTENT replay — resubmit op1 verbatim → same revision, replay flagged.
    const ctx4 = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctx4).handleSubmit("r4", { mutation: m1 });
    assert.equal(ctx4.captured.errors.length, 0, JSON.stringify(ctx4.captured.errors));
    assert.equal(ctx4.captured.responses[0].body.revision, 1);
    assert.equal(ctx4.captured.responses[0].body.idempotentReplay, true);

    // (5) getAuthorityState — own account, current epoch.
    const ctx5 = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctx5).handleGetAuthorityState("r5", { accountIdentityPublicKeyB64: acct.pubB64 });
    assert.equal(ctx5.captured.errors.length, 0, JSON.stringify(ctx5.captured.errors));
    assert.equal(ctx5.captured.responses[0].type, "account.authorityState.get.res");
    assert.equal(ctx5.captured.responses[0].body.epoch, 2);

    // (6) getAuthorityState for a DIFFERENT account is forbidden (blindness boundary).
    const other = await genKey();
    const ctx6 = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctx6).handleGetAuthorityState("r6", { accountIdentityPublicKeyB64: other.pubB64 });
    assert.equal(ctx6.captured.responses.length, 0);
    assert.equal(ctx6.captured.errors[0].code, "FORBIDDEN");
  },
);

test(
  "submit: device.revoke bumps the epoch and adds to the revoked-cert set (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_account_mutation_revoke";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const acct = await genKey();
    // Add a device to revoke.
    const b = await makeBinding({ inboxId: "inbox:revoke-target" });
    const add = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "rv-add", expectedRevision: 0, action: "device.add",
      target: { deviceInboxBinding: b.binding },
    });
    const ctxAdd = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctxAdd).handleSubmit("a1", { mutation: add });
    assert.equal(ctxAdd.captured.responses[0].body.revision, 1);

    // Revoke it, carrying a revoked cert id.
    const revoke = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "rv-revoke", expectedRevision: 1, action: "device.revoke",
      target: { revokedDeviceId: b.deviceId, revokedCertId: "rez:cap:revoked-leaf" },
    });
    const ctxRev = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctxRev).handleSubmit("v1", { mutation: revoke });
    assert.equal(ctxRev.captured.errors.length, 0, JSON.stringify(ctxRev.captured.errors));
    const body = ctxRev.captured.responses[0].body;
    assert.equal(body.revision, 2);
    assert.ok(!body.devices.some((d) => d.deviceId === b.deviceId), "revoked device drops out of the active set");
    assert.ok(body.authorityState.revokedCertIds.includes("rez:cap:revoked-leaf"), "revoked cert id is tracked");
  },
);

// ---- L7: device.bind enroll hook + account-wide serialized revoke fail-close ----

test(
  "device.bind enrolls the account→device→inbox linkage (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_bind_enroll";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const acct = await genKey();
    const d = await makeRegisteredDevice({ account: acct, inboxId: "inbox:bind-enroll" });
    const ctx = makeBindCtx({
      durableInbox, accountDeviceRegistry: registry, accountMutationSerializer: serializer,
      ownerPublicKeyB64: acct.pubB64, sessionDeviceId: d.deviceId, inboxId: d.inboxId,
    });
    await new DeviceHandler(ctx).handleBind("b1", { deviceRegistration: d.registration, deviceInboxBinding: d.binding });
    assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));

    const enrolled = await registry.getDevice(acct.pubB64, d.deviceId);
    assert.ok(enrolled, "the bound device is enrolled in the account registry");
    assert.equal(enrolled.inboxId, d.inboxId);
    assert.equal(enrolled.certId, null, "a primary device enrolls with no leaf cert");
    assert.equal(enrolled.status, "active");
  },
);

test(
  "a serialized device.revoke fail-closes the target's home cursor account-wide (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_serialized_revoke_failclose";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const acct = await genKey();
    const d = await makeRegisteredDevice({ account: acct, inboxId: "inbox:failclose" });

    // Bind the device: creates its durable cursor AND the registry linkage.
    const bindCtx = makeBindCtx({
      durableInbox, accountDeviceRegistry: registry, accountMutationSerializer: serializer,
      ownerPublicKeyB64: acct.pubB64, sessionDeviceId: d.deviceId, inboxId: d.inboxId,
    });
    await new DeviceHandler(bindCtx).handleBind("b1", { deviceRegistration: d.registration, deviceInboxBinding: d.binding });
    assert.equal(bindCtx.captured.errors.length, 0, JSON.stringify(bindCtx.captured.errors));

    await durableInbox.append(d.inboxId, new Uint8Array([1, 2, 3]));
    const before = await durableInbox.readAfterCursor(d.inboxId, d.deviceId, 10);
    assert.equal(before.length, 1, "a bound, non-revoked device reads its mail");

    // Serialized account-wide revoke — the submitting session is NOT bound to the
    // target inbox. The serializer resolves the device's inbox from the registry
    // row it revokes and fail-closes the home cursor ATOMICALLY in the same txn, so
    // the handler needs no post-commit close step (F4).
    const revoke = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "fc-revoke", expectedRevision: 0, action: "device.revoke",
      target: { revokedDeviceId: d.deviceId },
    });
    const revCtx = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(revCtx).handleSubmit("v1", { mutation: revoke });
    assert.equal(revCtx.captured.errors.length, 0, JSON.stringify(revCtx.captured.errors));
    assert.equal(revCtx.captured.responses[0].body.revision, 1);

    await assert.rejects(
      () => durableInbox.readAfterCursor(d.inboxId, d.deviceId, 10),
      (err) => err instanceof RevokedDeviceError,
      "the revoked device can no longer read — account-wide home fail-close",
    );
  },
);

// Audit 2026-07-09 P1 (revoke-before-bind resurrection): a device.add enrolls a
// device BEFORE it has bound (registry row, no cursor); an account-wide
// device.revoke marks it revoked (its cursor close is a no-op — no cursor yet);
// the revoked device then binds. device.bind must NOT resurrect it: the atomic
// enroll+cursor transaction (audit 2026-07-10 P2) refuses the revoked row and
// rolls back the cursor with it, so no cursor row exists at all afterward.
test(
  "device.bind cannot resurrect a device revoked before it ever bound (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_revoke_before_bind";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const acct = await genKey();
    const d = await makeRegisteredDevice({ account: acct, inboxId: "inbox:rbb" });

    // (1) device.add via the serializer — enrolls an ACTIVE registry row, no cursor.
    const add = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "rbb-add", expectedRevision: 0, action: "device.add",
      target: { deviceInboxBinding: d.binding },
    });
    const ctxAdd = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctxAdd).handleSubmit("a1", { mutation: add });
    assert.equal(ctxAdd.captured.errors.length, 0, JSON.stringify(ctxAdd.captured.errors));
    assert.equal(ctxAdd.captured.responses[0].body.revision, 1);
    const cursorsAfterAdd = await conn.query("SELECT count(*)::int AS c FROM device_cursors WHERE inbox_id = $1", [d.inboxId]);
    assert.equal(cursorsAfterAdd.rows[0].c, 0, "no delivery cursor exists before the device binds");

    // (2) account-wide device.revoke — marks the registry row revoked; the cursor
    // close is a no-op because no cursor row exists yet.
    const revoke = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "rbb-revoke", expectedRevision: 1, action: "device.revoke",
      target: { revokedDeviceId: d.deviceId },
    });
    const ctxRev = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    await new AccountMutationHandler(ctxRev).handleSubmit("v1", { mutation: revoke });
    assert.equal(ctxRev.captured.errors.length, 0, JSON.stringify(ctxRev.captured.errors));
    assert.equal((await registry.getDevice(acct.pubB64, d.deviceId)).status, "revoked");

    // (3) the revoked device now tries to bind — must be refused, and the atomic
    // transaction must roll the cursor back with the refused enroll.
    const bindCtx = makeBindCtx({
      durableInbox, accountDeviceRegistry: registry, accountMutationSerializer: serializer,
      ownerPublicKeyB64: acct.pubB64, sessionDeviceId: d.deviceId, inboxId: d.inboxId,
    });
    await new DeviceHandler(bindCtx).handleBind("b1", { deviceRegistration: d.registration, deviceInboxBinding: d.binding });
    assert.equal(bindCtx.captured.responses.length, 0, "the bind did not succeed");
    assert.equal(bindCtx.captured.errors[0].code, "FORBIDDEN", "revoked device is refused at bind");

    // No LIVE cursor survives — and with the atomic enroll+cursor transaction, no
    // cursor row exists at all (the refused enroll rolled the cursor back).
    const live = await conn.query("SELECT count(*)::int AS c FROM device_cursors WHERE inbox_id = $1 AND revoked = false", [d.inboxId]);
    assert.equal(live.rows[0].c, 0, "no live delivery cursor remains for the revoked device");
    const anyCursor = await conn.query("SELECT count(*)::int AS c FROM device_cursors WHERE inbox_id = $1", [d.inboxId]);
    assert.equal(anyCursor.rows[0].c, 0, "the refused bind left no cursor row behind");

    // The inbox has no registered device, so a wire deposit is ACCEPTED exactly like
    // a pre-bind / first-contact inbox — but it is inert: the revoked device can
    // never bind (refused above) to read it. The deposit-refuses fail-close still
    // guards the revoke-AFTER-bind case (a revoked cursor row exists there), covered
    // by the "revoked device can no longer read" test above.
    const deposit = await durableInbox.append(d.inboxId, new Uint8Array([9, 9, 9]));
    assert.ok(Number.isFinite(deposit.seq), "deposit to a device-less inbox is accepted (pre-bind semantics)");
  },
);

// Audit 2026-07-09 P2 / 2026-07-10 P2 (post-create cleanup split, now closed
// transactionally): enrollWithCursor refuses a revoke-before-bind device INSIDE
// the same transaction that would create its cursor, so there is no post-commit
// cleanup step at all. Proof: even with a session durableInbox whose
// registerDevice/revokeDevice are instrumented (revokeDevice THROWS), the bind is
// refused, neither is called, and no cursor row exists — the split cannot occur.
test(
  "revoke-before-bind is refused atomically with the cursor create, so a failing cursor-close cannot leak a live cursor (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_revoke_before_bind_preflight";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const acct = await genKey();
    const d = await makeRegisteredDevice({ account: acct, inboxId: "inbox:rbb2" });

    // device.add then account-wide device.revoke — a revoked registry row, no cursor.
    const add = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "rbb2-add", expectedRevision: 0, action: "device.add",
      target: { deviceInboxBinding: d.binding },
    });
    await new AccountMutationHandler(makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 })).handleSubmit("a1", { mutation: add });
    const revoke = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "rbb2-revoke", expectedRevision: 1, action: "device.revoke",
      target: { revokedDeviceId: d.deviceId },
    });
    await new AccountMutationHandler(makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 })).handleSubmit("v1", { mutation: revoke });
    assert.equal((await registry.getDevice(acct.pubB64, d.deviceId)).status, "revoked");

    // Bind with a session durableInbox whose registerDevice/revokeDevice we
    // instrument: revokeDevice THROWS, so any surviving post-commit cleanup path
    // would blow up here. The atomic path never touches the session durableInbox
    // for the pg persist (the registry's transaction owns the cursor create), so
    // neither is called.
    let registerCalls = 0;
    let revokeCalls = 0;
    const guardedInbox = {
      registerDevice: async (...args) => { registerCalls++; return durableInbox.registerDevice(...args); },
      revokeDevice: async () => { revokeCalls++; throw new Error("boom-cursor-close"); },
    };
    const bindCtx = makeBindCtx({
      durableInbox: guardedInbox, accountDeviceRegistry: registry, accountMutationSerializer: serializer,
      ownerPublicKeyB64: acct.pubB64, sessionDeviceId: d.deviceId, inboxId: d.inboxId,
    });
    await new DeviceHandler(bindCtx).handleBind("b1", { deviceRegistration: d.registration, deviceInboxBinding: d.binding });

    assert.equal(bindCtx.captured.responses.length, 0, "the bind did not succeed");
    assert.equal(bindCtx.captured.errors[0].code, "FORBIDDEN", "revoked device refused inside the atomic transaction");
    assert.equal(registerCalls, 0, "the session durableInbox never registered a cursor");
    assert.equal(revokeCalls, 0, "no post-commit cleanup ran (nothing to clean)");

    // And no cursor row exists at all: the split the audit flagged cannot occur.
    const rows = await conn.query("SELECT count(*)::int AS c FROM device_cursors WHERE inbox_id = $1", [d.inboxId]);
    assert.equal(rows.rows[0].c, 0, "no device_cursors row was created for the revoked device");
  },
);
