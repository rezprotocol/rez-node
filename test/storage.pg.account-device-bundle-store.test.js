import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceBundleStore } from "../src/storage/pg/PgAccountDeviceBundleStore.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";

// S2.5 S12 L2: the home-aggregated per-device bundle store. Real Postgres —
// monotonic prekeyVersion, and listActiveBundles JOINs the registry so a revoked
// device's stale bundle is never served.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

function bundle({ account, deviceId, inboxId, prekeyVersion }) {
  return {
    v: 1, purpose: "rez:device-prekey-bundle:v1",
    accountIdentityPublicKeyB64: account, devicePublicKeyB64: "pub:" + deviceId,
    deviceId, inboxId, prekeyVersion, bundleJson: { spk: "x", opks: [] },
    issuedAtMs: 1, expiresAtMs: 2, sig: { alg: "ed25519", sigB64: "AA" },
  };
}

test(
  "bundle store: put + list active, monotonic version, revoked device excluded (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_device_bundle_store";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    const store = new PgAccountDeviceBundleStore({ connection: conn });
    const registry = new PgAccountDeviceRegistry({ connection: conn });

    const account = "acct-B";
    const devA = "rez:dev:aaa";
    const devB = "rez:dev:bbb";

    // Enroll both devices as ACTIVE (the registry is the authoritative set).
    await registry.enroll({ accountIdentityPublicKeyB64: account, deviceId: devA, inboxId: "inbox:a", authorityEpoch: 1 });
    await registry.enroll({ accountIdentityPublicKeyB64: account, deviceId: devB, inboxId: "inbox:b", authorityEpoch: 1 });

    // Publish a bundle for each.
    await store.putBundle({ accountIdentityPublicKeyB64: account, deviceId: devA, prekeyVersion: 1, bundleJson: bundle({ account, deviceId: devA, inboxId: "inbox:a", prekeyVersion: 1 }) });
    await store.putBundle({ accountIdentityPublicKeyB64: account, deviceId: devB, prekeyVersion: 1, bundleJson: bundle({ account, deviceId: devB, inboxId: "inbox:b", prekeyVersion: 1 }) });

    let active = await store.listActiveBundles(account);
    assert.equal(active.length, 2, "both active devices' bundles are served");
    assert.deepEqual(active.map((b) => b.deviceId), [devA, devB]);

    // Monotonic: a NEWER version replaces; a stale (older) version does NOT.
    const up = await store.putBundle({ accountIdentityPublicKeyB64: account, deviceId: devA, prekeyVersion: 2, bundleJson: bundle({ account, deviceId: devA, inboxId: "inbox:a", prekeyVersion: 2 }) });
    assert.equal(up.applied, true);
    assert.equal(up.prekeyVersion, 2);
    const stale = await store.putBundle({ accountIdentityPublicKeyB64: account, deviceId: devA, prekeyVersion: 1, bundleJson: bundle({ account, deviceId: devA, inboxId: "inbox:a", prekeyVersion: 1 }) });
    assert.equal(stale.applied, false, "an older prekeyVersion does not downgrade the live bundle");
    assert.equal(stale.prekeyVersion, 2, "the live bundle stays at the higher version");

    // Revoke devB → its bundle is no longer served (join on registry status).
    await registry.setStatus({ accountIdentityPublicKeyB64: account, deviceId: devB, status: "revoked", authorityEpoch: 2 });
    active = await store.listActiveBundles(account);
    assert.equal(active.length, 1, "the revoked device's stale bundle is excluded");
    assert.equal(active[0].deviceId, devA);

    // getBundle round-trips the stored version.
    const got = await store.getBundle(account, devA);
    assert.equal(got.prekeyVersion, 2);
    assert.equal(got.bundleJson.deviceId, devA);
  },
);

test("bundle store: constructing without a connection throws", () => {
  assert.throws(() => new PgAccountDeviceBundleStore({}), /requires connection/);
});
