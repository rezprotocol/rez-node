import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  DeviceRegistrationV1,
  DEVICE_REGISTRATION_PURPOSE,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  DeviceRevokeV1,
  DEVICE_REVOKE_PURPOSE,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
} from "@rezprotocol/core";
import { DeviceHandler } from "../src/protocol/handlers/DeviceHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError } from "../src/storage/DurableInbox.js";

// S2.5 Slice 4 leaf C: device.bind / device.revoke. REAL crypto — the handler
// constructs its own NodeCryptoProvider and verifies the account-signed
// registration + device-signed binding + account-signed revoke; signing here
// with the same provider proves the full chain, not a mock.
const crypto = new NodeCryptoProvider();
const PG_URL = process.env.REZ_PG_TEST_URL || "";
const NOW = Date.now();
const ISSUED = NOW - 1000;
const EXPIRES = NOW + 3_600_000;

async function genKey() {
  const kp = await crypto.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}
async function ed(priv, msgBytes) {
  return { alg: "ed25519", sigB64: bytesToBase64(await crypto.sign({ privateKey: priv, msg: msgBytes })) };
}

// Build a coherent {account B, device C} world: a registration B-vouches-for-C,
// a binding C-signs for an inbox, and a revoke B-signs for C.
async function makeWorld({ inboxId = "inbox:bind-test" } = {}) {
  const acct = await genKey();
  const dev = await genKey();
  const deviceId = DeviceRegistrationV1.deviceIdFor(dev.pubB64);

  const regBody = {
    v: 1, purpose: DEVICE_REGISTRATION_PURPOSE,
    accountIdentityPublicKeyB64: acct.pubB64, devicePublicKeyB64: dev.pubB64,
    deviceId, issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const registration = { ...regBody, sig: await ed(acct.priv, DeviceRegistrationV1.signableBytes(regBody)) };

  const bindBody = {
    v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64: dev.pubB64, deviceId, inboxId,
    issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const binding = { ...bindBody, sig: await ed(dev.priv, DeviceInboxBindingV1.signableBytes(bindBody)) };

  const revBody = {
    v: 1, purpose: DEVICE_REVOKE_PURPOSE,
    accountIdentityPublicKeyB64: acct.pubB64, revokedDeviceId: deviceId, revokedDevicePublicKeyB64: dev.pubB64,
    issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const revoke = { ...revBody, sig: await ed(acct.priv, DeviceRevokeV1.signableBytes(revBody)) };

  return { acct, dev, deviceId, inboxId, registration, binding, revoke };
}

function makeCtx({ durableInbox, ownerPublicKeyB64, sessionDeviceId, boundInboxes = new Set(), localInboxId = "", sessionAuthority = null, accountMutationSerializer = null } = {}) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { durableInbox, accountMutationSerializer },
    ownerPublicKeyB64,
    sessionDeviceId,
    localInboxId,
    sessionAuthority,
    requireSession() { return true; },
    isInboxBound(id) { return boundInboxes.has(id); },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
  };
}

function recordingInbox() {
  const calls = [];
  return {
    calls,
    async registerDevice(inboxId, deviceId, opts) { calls.push({ op: "register", inboxId, deviceId, opts }); },
    async revokeDevice(inboxId, deviceId) { calls.push({ op: "revoke", inboxId, deviceId }); return true; },
  };
}

// A real account→device leaf capability cert (B is the anchor + root signer), so
// the handler's per-op delegated re-validation (audit 2026-07-10 P1) runs against
// a genuine chain — same shape as handler.account-mutation.test.js.
async function buildLeafCert({ account, granteePubB64, capabilities }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePubB64),
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

// ---- device.bind (real crypto, mock storage) ----

test("device.bind: a proven registration + binding registers the cursor on the SIGNED deviceId", async () => {
  const w = await makeWorld();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId, // session authenticated AS this device
    boundInboxes: new Set([w.inboxId]),
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });

  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.equal(durableInbox.calls.length, 1);
  assert.deepEqual(durableInbox.calls[0], {
    op: "register", inboxId: w.inboxId, deviceId: w.deviceId, opts: { devicePublicKeyB64: w.dev.pubB64 },
  });
  assert.equal(ctx.captured.responses[0].type, "device.bind.res");
  assert.deepEqual(ctx.captured.responses[0].body, { inboxId: w.inboxId, deviceId: w.deviceId });
});

test("device.bind: a registration vouched by a DIFFERENT account is rejected (trust anchor)", async () => {
  const w = await makeWorld();
  const wrong = await genKey();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: wrong.pubB64, // session is a DIFFERENT account than the registration vouches for
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });
  assert.equal(durableInbox.calls.length, 0, "must not register on an account mismatch");
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

test("device.bind: a binding for a device OTHER than the session device is forbidden", async () => {
  const w = await makeWorld();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: "rez:dev:some-other-device",
    boundInboxes: new Set([w.inboxId]),
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });
  assert.equal(durableInbox.calls.length, 0);
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("device.bind: a binding for an inbox the session has not claimed is forbidden", async () => {
  const w = await makeWorld();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set(), // session never claimed the inbox
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });
  assert.equal(durableInbox.calls.length, 0);
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("device.bind: a tampered binding signature is rejected", async () => {
  const w = await makeWorld();
  // Flip the inboxId AFTER signing → the device sig no longer covers the body.
  const tampered = { ...w.binding, inboxId: w.inboxId };
  tampered.issuedAtMs = w.binding.issuedAtMs + 1;
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: tampered });
  assert.equal(durableInbox.calls.length, 0);
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

test("device.bind: a 2nd device is surfaced as DEVICE_LIMIT while the E6 gate is closed", async () => {
  const w = await makeWorld();
  const durableInbox = {
    async registerDevice() { const e = new Error("cap"); e.code = "INBOX_CAP_EXCEEDED"; e.limitType = "devices"; throw e; },
  };
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });
  assert.equal(ctx.captured.errors[0].code, "DEVICE_LIMIT");
});

// ---- device.bind DELEGATED mode (S2.5 S8 L3 — cert chain IS the registration) ----

test("device.bind (delegated): a delegated session binds with NO registration — the cert chain is the registration", async () => {
  const w = await makeWorld({ inboxId: "inbox:delegated-bind" });
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64, // the claimed account B
    sessionDeviceId: w.deviceId, // session authenticated AS device C (self-cert of dev.pubB64)
    boundInboxes: new Set([w.inboxId]),
    // The session already proved C∈B via the cert chain at session-auth (S7).
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: w.dev.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["deviceSet.publish"] },
  });
  // No deviceRegistration in the body — a delegated device holds no B-sign key.
  await new DeviceHandler(ctx).handleBind("r1", { deviceInboxBinding: w.binding });

  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.equal(durableInbox.calls.length, 1);
  assert.deepEqual(durableInbox.calls[0], {
    op: "register", inboxId: w.inboxId, deviceId: w.deviceId, opts: { devicePublicKeyB64: w.dev.pubB64 },
  });
  assert.equal(ctx.captured.responses[0].type, "device.bind.res");
});

test("device.bind (delegated): a binding whose key differs from the session's proven device is forbidden", async () => {
  const w = await makeWorld({ inboxId: "inbox:delegated-mismatch" });
  const impostor = await genKey();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
    // The session proved a DIFFERENT device key than the binding carries.
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: impostor.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: [] },
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceInboxBinding: w.binding });
  assert.equal(durableInbox.calls.length, 0, "must not register a device the session did not authenticate as");
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

// ---- device.revoke (real crypto, mock storage) ----

test("device.revoke: an account-signed revoke for the session account revokes the device", async () => {
  const w = await makeWorld();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({ durableInbox, ownerPublicKeyB64: w.acct.pubB64, localInboxId: w.inboxId });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: w.revoke });
  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.deepEqual(durableInbox.calls[0], { op: "revoke", inboxId: w.inboxId, deviceId: w.deviceId });
  assert.equal(ctx.captured.responses[0].type, "device.revoke.res");
  assert.deepEqual(ctx.captured.responses[0].body, { inboxId: w.inboxId, revokedDeviceId: w.deviceId, revoked: true });
});

test("device.revoke: a revoke signed by a DIFFERENT account than the session is forbidden", async () => {
  const w = await makeWorld();
  const other = await genKey();
  const durableInbox = recordingInbox();
  // Session authenticated as `other`, but the revoke is for w.acct's device.
  const ctx = makeCtx({ durableInbox, ownerPublicKeyB64: other.pubB64, localInboxId: w.inboxId });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: w.revoke });
  assert.equal(durableInbox.calls.length, 0, "must not revoke another account's device");
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("device.revoke: a tampered revoke signature is rejected", async () => {
  const w = await makeWorld();
  const tampered = { ...w.revoke, issuedAtMs: w.revoke.issuedAtMs + 1 };
  const durableInbox = recordingInbox();
  const ctx = makeCtx({ durableInbox, ownerPublicKeyB64: w.acct.pubB64, localInboxId: w.inboxId });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: tampered });
  assert.equal(durableInbox.calls.length, 0);
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

test("device.revoke: with no claimed inbox is a BAD_REQUEST", async () => {
  const w = await makeWorld();
  const durableInbox = recordingInbox();
  const ctx = makeCtx({ durableInbox, ownerPublicKeyB64: w.acct.pubB64, localInboxId: "" });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: w.revoke });
  assert.equal(durableInbox.calls.length, 0);
  assert.equal(ctx.captured.errors[0].code, "BAD_REQUEST");
});

// ---- device.revoke DELEGATED mode (S2.5 S8 L4 — C-signed, gated on device.revoke) ----

// A revoke for account B but signed by a delegated device key C (the revoke body
// still NAMES account B; the home verifies the sig against the delegated signer).
async function makeDelegatedRevoke({ acct, target }) {
  const revBody = {
    v: 1, purpose: DEVICE_REVOKE_PURPOSE,
    accountIdentityPublicKeyB64: acct.pubB64, revokedDeviceId: target.deviceId, revokedDevicePublicKeyB64: target.pubB64,
    issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  return { revBody };
}

test("device.revoke (delegated): a C-signed revoke from a device holding device.revoke succeeds", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-revoke" });
  const delegate = await genKey(); // the delegated device C doing the revoking
  const target = { deviceId: w.deviceId, pubB64: w.dev.pubB64 };
  const { revBody } = await makeDelegatedRevoke({ acct: w.acct, target });
  const revoke = { ...revBody, sig: await ed(delegate.priv, DeviceRevokeV1.signableBytes(revBody)) };

  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    localInboxId: w.inboxId,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["device.revoke", "deviceSet.publish"] },
  });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: revoke });
  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.deepEqual(durableInbox.calls[0], { op: "revoke", inboxId: w.inboxId, deviceId: w.deviceId });
});

test("device.revoke (delegated): a delegated device WITHOUT device.revoke is forbidden", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-norevoke" });
  const delegate = await genKey();
  const target = { deviceId: w.deviceId, pubB64: w.dev.pubB64 };
  const { revBody } = await makeDelegatedRevoke({ acct: w.acct, target });
  const revoke = { ...revBody, sig: await ed(delegate.priv, DeviceRevokeV1.signableBytes(revBody)) };

  const durableInbox = recordingInbox();
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    localInboxId: w.inboxId,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["deviceSet.publish"] },
  });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: revoke });
  assert.equal(durableInbox.calls.length, 0, "must not revoke without the device.revoke capability");
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("device.revoke (delegated): a B-signed revoke is rejected when the session signer is C (sig must match the proven signer)", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-bsig" });
  const delegate = await genKey();
  const durableInbox = recordingInbox();
  // w.revoke is B-signed, but the delegated session's proven signer is C → sig check fails.
  const ctx = makeCtx({
    durableInbox,
    ownerPublicKeyB64: w.acct.pubB64,
    localInboxId: w.inboxId,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["device.revoke"] },
  });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: w.revoke });
  assert.equal(durableInbox.calls.length, 0);
  assert.equal(ctx.captured.errors[0].code, "INVALID_SIGNATURE");
});

// ---- audit 2026-07-10 P1: per-op delegated revalidation (stale session authority) ----
//
// The session's cert chain was proven at connect time; a leaf revoked WHILE the
// socket stays open must not keep binding/revoking. The handler re-checks the
// chain against the home's CURRENT authority state (the serializer) per op —
// mirroring AccountMutationHandler's F2 fix, via the shared revalidator.

test("device.bind (delegated): a leaf cert REVOKED mid-session can no longer bind", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-stale-bind" });
  const leafCert = await buildLeafCert({ account: w.acct, granteePubB64: w.dev.pubB64, capabilities: ["deviceSet.publish"] });
  const durableInbox = recordingInbox();
  // The home now reports this leaf cert as revoked.
  const serializer = { async getAuthorityState() { return { epoch: 2, revokedCertIds: [leafCert.certId], minValidIssuedAtMs: 0 }; } };
  const ctx = makeCtx({
    durableInbox,
    accountMutationSerializer: serializer,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: w.dev.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["deviceSet.publish"], certChain: [leafCert.toJSON()] },
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceInboxBinding: w.binding });
  assert.equal(durableInbox.calls.length, 0, "a revoked delegated device must not create a cursor");
  assert.equal(ctx.captured.responses.length, 0);
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("device.bind (delegated): a valid, un-revoked chain still binds under per-op revalidation", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-fresh-bind" });
  const leafCert = await buildLeafCert({ account: w.acct, granteePubB64: w.dev.pubB64, capabilities: ["deviceSet.publish"] });
  const durableInbox = recordingInbox();
  const serializer = { async getAuthorityState() { return { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 }; } };
  const ctx = makeCtx({
    durableInbox,
    accountMutationSerializer: serializer,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: w.dev.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["deviceSet.publish"], certChain: [leafCert.toJSON()] },
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceInboxBinding: w.binding });
  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.equal(durableInbox.calls.length, 1);
  assert.equal(ctx.captured.responses[0].type, "device.bind.res");
});

test("device.revoke (delegated): a leaf cert REVOKED mid-session can no longer revoke", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-stale-revoke" });
  const delegate = await genKey();
  const leafCert = await buildLeafCert({ account: w.acct, granteePubB64: delegate.pubB64, capabilities: ["device.revoke"] });
  const target = { deviceId: w.deviceId, pubB64: w.dev.pubB64 };
  const { revBody } = await makeDelegatedRevoke({ acct: w.acct, target });
  const revoke = { ...revBody, sig: await ed(delegate.priv, DeviceRevokeV1.signableBytes(revBody)) };

  const durableInbox = recordingInbox();
  const serializer = { async getAuthorityState() { return { epoch: 2, revokedCertIds: [leafCert.certId], minValidIssuedAtMs: 0 }; } };
  const ctx = makeCtx({
    durableInbox,
    accountMutationSerializer: serializer,
    ownerPublicKeyB64: w.acct.pubB64,
    localInboxId: w.inboxId,
    // The connect-time snapshot still grants device.revoke — the STALE state.
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["device.revoke"], certChain: [leafCert.toJSON()] },
  });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: revoke });
  assert.equal(durableInbox.calls.length, 0, "a revoked delegated device must not revoke others");
  assert.equal(ctx.captured.responses.length, 0);
  assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
});

test("device.revoke (delegated): a valid, un-revoked chain still revokes under per-op revalidation", async () => {
  const w = await makeWorld({ inboxId: "inbox:deleg-fresh-revoke" });
  const delegate = await genKey();
  const leafCert = await buildLeafCert({ account: w.acct, granteePubB64: delegate.pubB64, capabilities: ["device.revoke"] });
  const target = { deviceId: w.deviceId, pubB64: w.dev.pubB64 };
  const { revBody } = await makeDelegatedRevoke({ acct: w.acct, target });
  const revoke = { ...revBody, sig: await ed(delegate.priv, DeviceRevokeV1.signableBytes(revBody)) };

  const durableInbox = recordingInbox();
  const serializer = { async getAuthorityState() { return { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 }; } };
  const ctx = makeCtx({
    durableInbox,
    accountMutationSerializer: serializer,
    ownerPublicKeyB64: w.acct.pubB64,
    localInboxId: w.inboxId,
    sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: w.acct.pubB64, leafCertId: leafCert.certId, grantedCapabilities: ["device.revoke"], certChain: [leafCert.toJSON()] },
  });
  await new DeviceHandler(ctx).handleRevoke("r1", { deviceRevoke: revoke });
  assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
  assert.deepEqual(durableInbox.calls[0], { op: "revoke", inboxId: w.inboxId, deviceId: w.deviceId });
});

// ---- end-to-end home enforcement (real crypto + real Postgres) ----

test(
  "device.bind then device.revoke fail-closes the durable home (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_device_bind_revoke";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });

    const w = await makeWorld({ inboxId: "inbox:e2e-bind" });
    const ctx = makeCtx({
      durableInbox,
      ownerPublicKeyB64: w.acct.pubB64,
      sessionDeviceId: w.deviceId,
      boundInboxes: new Set([w.inboxId]),
      localInboxId: w.inboxId,
    });
    const handler = new DeviceHandler(ctx);

    // Bind: the home now holds the proven device key + a readable cursor.
    await handler.handleBind("b1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });
    assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
    const stored = await durableInbox.getDevice(w.inboxId, w.deviceId);
    assert.equal(stored.devicePublicKeyB64, w.dev.pubB64);

    await durableInbox.append(w.inboxId, new Uint8Array([1, 2, 3]));
    const before = await durableInbox.readAfterCursor(w.inboxId, w.deviceId, 10);
    assert.equal(before.length, 1, "a bound, non-revoked device reads its mail");

    // Revoke: the home fails closed for the device.
    await handler.handleRevoke("v1", { deviceRevoke: w.revoke });
    assert.deepEqual(ctx.captured.responses.at(-1).body, { inboxId: w.inboxId, revokedDeviceId: w.deviceId, revoked: true });

    await assert.rejects(
      () => durableInbox.readAfterCursor(w.inboxId, w.deviceId, 10),
      (err) => err instanceof RevokedDeviceError,
      "a revoked device can no longer read — home-enforced fail-closed",
    );
    await assert.rejects(
      () => durableInbox.append(w.inboxId, new Uint8Array([9]), { deviceId: w.deviceId }),
      (err) => err instanceof RevokedDeviceError,
      "a device-targeted deposit to a revoked device is refused",
    );
  },
);
