import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  DeviceRegistrationV1,
  DEVICE_REGISTRATION_PURPOSE,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
} from "@rezprotocol/core";
import { DeviceHandler } from "../src/protocol/handlers/DeviceHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError } from "../src/storage/DurableInbox.js";

// S2.5 Slice 4 leaf C: device.bind. REAL crypto — the handler constructs its own
// NodeCryptoProvider and verifies the account-signed registration + device-signed
// binding; signing here with the same provider proves the full chain, not a mock.
// The legacy per-inbox device.revoke DIRECTIVE was retired (audit R4 L4); revoke is
// the serialized account.deviceMutation path (test/handler.account-mutation.test.js).
// The e2e below still exercises the durable home's revoke fail-close via the storage
// primitive durableInbox.revokeDevice (the same one the serializer folds under-lock).
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
// and a binding C-signs for an inbox.
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

  return { acct, dev, deviceId, inboxId, registration, binding };
}

function makeCtx({ durableInbox, ownerPublicKeyB64, sessionDeviceId, boundInboxes = new Set(), localInboxId = "", sessionAuthority = null, accountMutationSerializer = null, accountDeviceRegistry = null } = {}) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { durableInbox, accountMutationSerializer, accountDeviceRegistry },
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

// NOTE (audit R4 F3-remediation round-5 finding 2): the former DeviceHandler delegated
// per-op revalidation tests lived here, but on a Pg authority home the legacy device.revoke
// is now FAIL-CLOSED (a serializer being present ⇒ SERVICE_UNAVAILABLE — see the fail-close
// test above), because the cursor-only path is a split-brain writer. The authoritative
// delegated revoke — including the under-lock recheck of a leaf revoked mid-session and the
// happy path — now lives in the serialized account.deviceMutation path and is covered by
// test/handler.account-mutation.test.js (the L3 / round-3 finding-1 tests).

// audit R4 L2c review P3: the registry (L2c) rejects a non-canonical deviceId with
// BAD_DEVICE_ID. The bind handler must translate that to the wire BAD_REQUEST (a
// client fault), never INTERNAL. In practice the bind id is a validated self-cert, so
// this is a defensive mapping — proven here with a registry stub that raises the code.
test("device.bind: a registry BAD_DEVICE_ID surfaces as BAD_REQUEST (not INTERNAL)", async () => {
  const w = await makeWorld({ inboxId: "inbox:baddev-bind" });
  const accountDeviceRegistry = {
    async enrollWithCursor() {
      const e = new Error("device id is not a canonical rez:dev id");
      e.code = "BAD_DEVICE_ID";
      throw e;
    },
  };
  const ctx = makeCtx({
    durableInbox: recordingInbox(),
    accountDeviceRegistry,
    ownerPublicKeyB64: w.acct.pubB64,
    sessionDeviceId: w.deviceId,
    boundInboxes: new Set([w.inboxId]),
    localInboxId: w.inboxId,
  });
  await new DeviceHandler(ctx).handleBind("r1", { deviceRegistration: w.registration, deviceInboxBinding: w.binding });
  assert.equal(ctx.captured.errors[0].code, "BAD_REQUEST");
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

    // Revoke via the storage primitive (the same durableInbox.revokeDevice the serializer
    // folds under-lock on the authoritative account.deviceMutation path) — the home fails
    // closed for the device.
    const revoked = await durableInbox.revokeDevice(w.inboxId, w.deviceId);
    assert.equal(revoked, true);

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
