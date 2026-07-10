import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  DeviceRegistrationV1,
  DevicePrekeyBundleV1,
  DEVICE_PREKEY_BUNDLE_PURPOSE,
} from "@rezprotocol/core";
import { AccountDeviceBundleHandler } from "../src/protocol/handlers/AccountDeviceBundleHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceBundleStore } from "../src/storage/pg/PgAccountDeviceBundleStore.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";

// S2.5 S12 L3: the home-aggregated device bundle handler. REAL crypto — the
// handler verifies the device-signed DevicePrekeyBundleV1 with its own
// NodeCryptoProvider; real Pg for the store + registry (publish authz requires an
// ACTIVE enrolled device).
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
function rand(n) { return bytesToBase64(crypto.randomBytes(n)); }

// A device-signed DevicePrekeyBundleV1 for `account` at `inboxId`.
async function makeBundle({ account, inboxId, prekeyVersion = 1 }) {
  const dev = await genKey();
  const deviceId = DeviceRegistrationV1.deviceIdFor(dev.pubB64);
  const bundleJson = {
    receiverId: deviceId,
    identitySigningPublicKeyB64: dev.pubB64,
    identityDhPublicKeyB64: rand(32),
    identityDhSignatureB64: rand(64),
    signedPreKeyPublicB64: rand(32),
    signedPreKeySignatureB64: rand(64),
    oneTimePreKeyPublicB64: rand(32),
  };
  const body = {
    v: 1, purpose: DEVICE_PREKEY_BUNDLE_PURPOSE,
    accountIdentityPublicKeyB64: account, devicePublicKeyB64: dev.pubB64, deviceId, inboxId,
    prekeyVersion, bundleJson, issuedAtMs: ISSUED, expiresAtMs: EXPIRES,
  };
  const bundle = new DevicePrekeyBundleV1({ ...body, sig: await ed(dev.priv, DevicePrekeyBundleV1.signableBytes(body)) });
  return { dev, deviceId, inboxId, bundle };
}

function makeCtx({ bundleStore, registry, ownerPublicKeyB64, sessionDeviceId } = {}) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { accountDeviceBundleStore: bundleStore, accountDeviceRegistry: registry },
    ownerPublicKeyB64,
    sessionDeviceId,
    requireSession() { return true; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
  };
}

test("publish: SERVICE_UNAVAILABLE without a bundle store", async () => {
  const acct = await genKey();
  const w = await makeBundle({ account: acct.pubB64, inboxId: "inbox:x" });
  const ctx = makeCtx({ bundleStore: null, registry: null, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: w.deviceId });
  await new AccountDeviceBundleHandler(ctx).handlePublish("r1", { bundle: w.bundle.toJSON() });
  assert.equal(ctx.captured.errors[0].code, "SERVICE_UNAVAILABLE");
});

test(
  "publish + getDeviceSet round-trip; rejects wrong-account / wrong-session-device / inactive / tampered (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_device_bundle_handler";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    const bundleStore = new PgAccountDeviceBundleStore({ connection: conn });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox: new PgDurableInbox({ connection: conn, maxDevices: 1 }) });

    const acct = await genKey();
    const w = await makeBundle({ account: acct.pubB64, inboxId: "inbox:bundle-1" });
    // Enroll the device ACTIVE (publish requires it).
    await registry.enroll({ accountIdentityPublicKeyB64: acct.pubB64, deviceId: w.deviceId, inboxId: w.inboxId, authorityEpoch: 1 });

    // Happy path: publish, then getDeviceSet returns the bundle.
    const ctx = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: w.deviceId });
    await new AccountDeviceBundleHandler(ctx).handlePublish("p1", { bundle: w.bundle.toJSON() });
    assert.equal(ctx.captured.errors.length, 0, JSON.stringify(ctx.captured.errors));
    assert.equal(ctx.captured.responses[0].type, "account.deviceBundle.publish.res");
    assert.equal(ctx.captured.responses[0].body.deviceId, w.deviceId);

    const getCtx = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: w.deviceId });
    await new AccountDeviceBundleHandler(getCtx).handleGetDeviceSet("g1", {});
    assert.equal(getCtx.captured.errors.length, 0, JSON.stringify(getCtx.captured.errors));
    const set = getCtx.captured.responses[0].body.devices;
    assert.equal(set.length, 1);
    assert.equal(set[0].deviceId, w.deviceId);
    assert.equal(set[0].bundle.devicePublicKeyB64, w.dev.pubB64, "the full device-signed bundle is served");

    // Reject: a bundle naming a DIFFERENT account than the session.
    const other = await genKey();
    const rc1 = makeCtx({ bundleStore, registry, ownerPublicKeyB64: other.pubB64, sessionDeviceId: w.deviceId });
    await new AccountDeviceBundleHandler(rc1).handlePublish("p2", { bundle: w.bundle.toJSON() });
    assert.equal(rc1.captured.errors[0].code, "FORBIDDEN");

    // Reject: the bundle is for a device OTHER than the session device.
    const rc2 = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: "rez:dev:someone-else" });
    await new AccountDeviceBundleHandler(rc2).handlePublish("p3", { bundle: w.bundle.toJSON() });
    assert.equal(rc2.captured.errors[0].code, "FORBIDDEN");

    // Reject: a device NOT enrolled active.
    const w2 = await makeBundle({ account: acct.pubB64, inboxId: "inbox:bundle-2" });
    const rc3 = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: w2.deviceId });
    await new AccountDeviceBundleHandler(rc3).handlePublish("p4", { bundle: w2.bundle.toJSON() });
    assert.equal(rc3.captured.errors[0].code, "FORBIDDEN", "an unenrolled device cannot publish");

    // Reject: a tampered bundle (flip inboxId after signing).
    const tampered = w.bundle.toJSON();
    tampered.inboxId = "inbox:tampered";
    const rc4 = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: w.deviceId });
    await new AccountDeviceBundleHandler(rc4).handlePublish("p5", { bundle: tampered });
    // deviceId still matches the session, account matches, but the sig no longer
    // covers the body ⇒ INVALID_SIGNATURE.
    assert.equal(rc4.captured.errors[0].code, "INVALID_SIGNATURE");

    // Revoke the device → getDeviceSet no longer serves its bundle.
    await registry.setStatus({ accountIdentityPublicKeyB64: acct.pubB64, deviceId: w.deviceId, status: "revoked", authorityEpoch: 2 });
    const getCtx2 = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: w.deviceId });
    await new AccountDeviceBundleHandler(getCtx2).handleGetDeviceSet("g2", {});
    assert.equal(getCtx2.captured.responses[0].body.devices.length, 0, "a revoked device drops out of the served set");
  },
);

test(
  "getDeviceSet for a DIFFERENT account is forbidden (blindness boundary, real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_device_bundle_getset_forbid";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    const bundleStore = new PgAccountDeviceBundleStore({ connection: conn });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox: new PgDurableInbox({ connection: conn, maxDevices: 1 }) });

    const acct = await genKey();
    const other = await genKey();
    const ctx = makeCtx({ bundleStore, registry, ownerPublicKeyB64: acct.pubB64, sessionDeviceId: "rez:dev:x" });
    await new AccountDeviceBundleHandler(ctx).handleGetDeviceSet("g1", { accountIdentityPublicKeyB64: other.pubB64 });
    assert.equal(ctx.captured.responses.length, 0);
    assert.equal(ctx.captured.errors[0].code, "FORBIDDEN");
  },
);
