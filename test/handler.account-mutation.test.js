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
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}
async function ed(priv, msgBytes) {
  return { alg: "ed25519", sigB64: bytesToBase64(await crypto.sign({ privateKey: priv, msg: msgBytes })) };
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
    const serializer = new PgAccountMutationSerializer({ connection: conn });

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

    // (2) DELEGATED device.add — C-signed, holds device.add. epoch 1 → 2.
    const b2 = await makeBinding({ inboxId: "inbox:e2e-add-2" });
    const m2 = await makeMutation({
      account: acct.pubB64, signerPriv: delegate.priv, signerPubB64: delegate.pubB64,
      opId: "e2e-op2", expectedRevision: 1, action: "device.add",
      target: { deviceInboxBinding: b2.binding },
    });
    const ctx2 = makeCtx({
      serializer,
      ownerPublicKeyB64: acct.pubB64,
      sessionAuthority: { mode: "delegated", signerPublicKeyB64: delegate.pubB64, accountIdentityPublicKeyB64: acct.pubB64, leafCertId: "rez:cap:leaf", grantedCapabilities: ["device.add", "device.revoke"] },
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
    const serializer = new PgAccountMutationSerializer({ connection: conn });

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
    const registry = new PgAccountDeviceRegistry({ connection: conn });
    const serializer = new PgAccountMutationSerializer({ connection: conn });

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
    const registry = new PgAccountDeviceRegistry({ connection: conn });
    const serializer = new PgAccountMutationSerializer({ connection: conn });

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
    // target inbox; the handler resolves it via the registry and fail-closes.
    const revoke = await makeMutation({
      account: acct.pubB64, signerPriv: acct.priv, signerPubB64: acct.pubB64,
      opId: "fc-revoke", expectedRevision: 0, action: "device.revoke",
      target: { revokedDeviceId: d.deviceId },
    });
    const revCtx = makeCtx({ serializer, ownerPublicKeyB64: acct.pubB64 });
    revCtx.runtime.accountDeviceRegistry = registry;
    revCtx.runtime.durableInbox = durableInbox;
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
