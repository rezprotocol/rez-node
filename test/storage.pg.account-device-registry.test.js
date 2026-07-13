import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";

// S2.5 S7 leaf A (audit F3, OPEN-A): the account→device→inbox registry — the
// explicit opt-in linkage that lets the home resolve ALL of an account's device
// inboxes (the precondition for account-wide device revocation). Real Postgres:
// enroll idempotency + conflict guards, inbox uniqueness, account-wide resolve,
// monotonic status changes.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

test(
  "PgAccountDeviceRegistry against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_account_device_registry";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE account_device_registry");
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });

    const ACCT_A = "B-SIGN-ACCOUNT-A";
    const ACCT_B = "B-SIGN-ACCOUNT-B";

    await t.test("enroll a device binding, then resolve it by account and by inbox", async () => {
      const row = await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: "rez:dev:a1",
        inboxId: "inbox-a1",
        certId: "rez:cap:leaf-a1",
        authorityEpoch: 1,
      });
      assert.equal(row.status, "active");
      assert.equal(row.inboxId, "inbox-a1");
      assert.equal(row.certId, "rez:cap:leaf-a1");
      assert.equal(row.authorityEpoch, 1);

      const byDevice = await registry.getDevice(ACCT_A, "rez:dev:a1");
      assert.equal(byDevice.inboxId, "inbox-a1");

      const byInbox = await registry.resolveInbox("inbox-a1");
      assert.equal(byInbox.accountIdentityPublicKeyB64, ACCT_A);
      assert.equal(byInbox.deviceId, "rez:dev:a1");
    });

    await t.test("a primary/direct device enrolls with a null cert", async () => {
      const row = await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: "rez:dev:a-primary",
        inboxId: "inbox-a-primary",
        certId: null,
        authorityEpoch: 1,
      });
      assert.equal(row.certId, null);
    });

    await t.test("re-enrolling the SAME binding is an idempotent no-op", async () => {
      const again = await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: "rez:dev:a1",
        inboxId: "inbox-a1",
        certId: "rez:cap:leaf-a1",
        authorityEpoch: 1,
      });
      assert.equal(again.inboxId, "inbox-a1");
    });

    await t.test("a DIFFERENT binding for an enrolled device is refused", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:a1",
          inboxId: "inbox-different",
          certId: "rez:cap:leaf-a1",
          authorityEpoch: 1,
        }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
    });

    // S2.5 S12 L4 — cert reconciliation (serializer device.add writes cert_id=NULL,
    // device.bind enroll writes the leaf certId; two writers on one column).
    const ACCT_R = "B-SIGN-ACCOUNT-RECON";
    await t.test("cert reconciliation: a NULL-cert row upgrades to a leaf cert (device.add then device.bind)", async () => {
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: "rez:dev:recon1", inboxId: "inbox-recon1", certId: null, authorityEpoch: 1 });
      const upgraded = await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: "rez:dev:recon1", inboxId: "inbox-recon1", certId: "rez:cap:leaf-recon1", authorityEpoch: 1 });
      assert.equal(upgraded.certId, "rez:cap:leaf-recon1", "a null cert upgrades to the device's leaf cert");
    });

    await t.test("cert reconciliation: a non-null cert is NOT clobbered to null (device.bind then device.add)", async () => {
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: "rez:dev:recon2", inboxId: "inbox-recon2", certId: "rez:cap:leaf-recon2", authorityEpoch: 1 });
      const kept = await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: "rez:dev:recon2", inboxId: "inbox-recon2", certId: null, authorityEpoch: 1 });
      assert.equal(kept.certId, "rez:cap:leaf-recon2", "an enroll with null cert keeps the existing leaf cert");
    });

    await t.test("cert reconciliation: two DIFFERENT non-null certs genuinely conflict", async () => {
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: "rez:dev:recon3", inboxId: "inbox-recon3", certId: "rez:cap:leaf-a", authorityEpoch: 1 });
      await assert.rejects(
        () => registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: "rez:dev:recon3", inboxId: "inbox-recon3", certId: "rez:cap:leaf-b", authorityEpoch: 1 }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
    });

    await t.test("an inbox already enrolled to another (account, device) is refused", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_B,
          deviceId: "rez:dev:b-steal",
          inboxId: "inbox-a1",
          certId: "rez:cap:leaf-b",
          authorityEpoch: 1,
        }),
        (err) => err.code === "INBOX_ALREADY_ENROLLED",
      );
    });

    await t.test("listDevices resolves all of an account's device bindings (sibling lookup)", async () => {
      await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: "rez:dev:a2",
        inboxId: "inbox-a2",
        certId: "rez:cap:leaf-a2",
        authorityEpoch: 1,
      });
      const devices = await registry.listDevices(ACCT_A);
      const ids = devices.map((d) => d.deviceId).sort();
      assert.deepEqual(ids, ["rez:dev:a-primary", "rez:dev:a1", "rez:dev:a2"]);
    });

    await t.test("revoke flips a device to revoked and stamps the authority epoch", async () => {
      const revoked = await registry.revoke({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: "rez:dev:a2",
        authorityEpoch: 2,
      });
      assert.equal(revoked.status, "revoked");
      assert.equal(revoked.authorityEpoch, 2);
      assert.equal((await registry.getDevice(ACCT_A, "rez:dev:a2")).status, "revoked");
      // A revoke of an ENROLLED device also writes the terminal tombstone.
      assert.equal(await registry.isTombstoned(ACCT_A, "rez:dev:a2"), true);
    });

    // Audit 2026-07-09 P1 (revoke-before-bind): a device.add can enroll a device
    // before it binds; an account-wide revoke marks the row revoked; a later
    // device.bind must NOT be able to re-enroll (which would leave a live cursor
    // for a revoked device). enroll fails loud on a revoked row.
    await t.test("enroll refuses a device whose registry row is revoked (no resurrection)", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:a2",
          inboxId: "inbox-a2",
          certId: "rez:cap:leaf-a2",
          authorityEpoch: 2,
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      // The row is untouched by the refused enroll.
      assert.equal((await registry.getDevice(ACCT_A, "rez:dev:a2")).status, "revoked");
    });

    // Audit R4 F1: a revoke of a NEVER-ENROLLED device writes a durable tombstone
    // (there is no registry row to flip) so a later device.add / device.bind of
    // that same deviceId can never resurrect it. Before F1 the revoke left no
    // trace and the enroll succeeded ACTIVE.
    const NE_CANON = "rez:dev:" + "f".repeat(64); // canonical, never enrolled
    await t.test("revoke of a never-enrolled device tombstones it; later enroll is refused (F1)", async () => {
      const res = await registry.revoke({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: NE_CANON,
        authorityEpoch: 5,
      });
      assert.equal(res, null, "a tombstone-only revoke returns null (no enrolled binding)");
      assert.equal(await registry.isTombstoned(ACCT_A, NE_CANON), true);
      assert.equal(await registry.getDevice(ACCT_A, NE_CANON), null, "no active/revoked registry row exists");

      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: NE_CANON,
          inboxId: "inbox-ne-canon",
          certId: null,
          authorityEpoch: 6,
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
    });

    // Audit R4 F1 enforcement on the serializer's device.add fold path: foldAddInTx
    // (the method the serializer calls under its account lock) must ALSO honor the
    // tombstone, or a signed device.add resurrects the revoked deviceId.
    await t.test("foldAddInTx refuses a tombstoned deviceId (device.add cannot resurrect)", async () => {
      await conn.withClient(async (client) => {
        await client.query("BEGIN");
        try {
          await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [ACCT_A]);
          await assert.rejects(
            () => registry.foldAddInTx(client, {
              accountIdentityPublicKeyB64: ACCT_A,
              deviceId: NE_CANON,
              inboxId: "inbox-ne-canon2",
              certId: null,
              authorityEpoch: 7,
            }),
            (err) => err.code === "DEVICE_REVOKED",
          );
        } finally {
          await client.query("ROLLBACK");
        }
      });
    });

    // Tombstone DoS syntax gate (Noah's audit-R4 warning): the AccountDeviceMutationV1
    // revoke target is a bare, forgeable string. A revoke of a NEVER-ENROLLED,
    // NON-canonical deviceId is refused so it cannot mint a permanent fake tombstone.
    await t.test("revoke of a never-enrolled non-canonical deviceId is refused (tombstone DoS gate)", async () => {
      await assert.rejects(
        () => registry.revoke({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:not-a-canonical-id",
          authorityEpoch: 8,
        }),
        (err) => err.code === "BAD_TARGET",
      );
      assert.equal(await registry.isTombstoned(ACCT_A, "rez:dev:not-a-canonical-id"), false);
    });

    await t.test("setStatus is removed (dangerous alternate writer surface)", () => {
      assert.equal(typeof registry.setStatus, "undefined");
    });

    await t.test("getDevice / resolveInbox return null for unknown keys", async () => {
      assert.equal(await registry.getDevice(ACCT_A, "rez:dev:ghost"), null);
      assert.equal(await registry.resolveInbox("inbox-ghost"), null);
      assert.equal(await registry.getDevice("", ""), null);
    });

    // ---- audit 2026-07-10 P2: atomic bind enroll+cursor ----

    await t.test("constructor fails loud without a durableInbox (atomic enroll+cursor dependency)", () => {
      assert.throws(() => new PgAccountDeviceRegistry({ connection: conn }), /durableInbox/);
    });

    await t.test("enrollWithCursor creates the registry row AND the delivery cursor in one commit", async () => {
      const ACCT = "B-SIGN-ACCOUNT-ATOMIC";
      const row = await registry.enrollWithCursor({
        accountIdentityPublicKeyB64: ACCT,
        deviceId: "rez:dev:atomic1",
        inboxId: "inbox-atomic1",
        certId: "rez:cap:leaf-atomic1",
        authorityEpoch: 1,
        devicePublicKeyB64: "DEVICE-PUB-ATOMIC1",
      });
      assert.equal(row.status, "active");
      assert.equal(row.inboxId, "inbox-atomic1");

      const cursor = await durableInbox.getDevice("inbox-atomic1", "rez:dev:atomic1");
      assert.ok(cursor, "the delivery cursor exists");
      assert.equal(cursor.revoked, false);
      assert.equal(cursor.devicePublicKeyB64, "DEVICE-PUB-ATOMIC1", "the proven device key was persisted with the cursor");
    });

    // The exact race the audit flagged: a registry row already revoked when the
    // bind's persist runs. The refused enroll must roll the cursor create back
    // with it — no cursor row may exist afterward, live OR revoked.
    await t.test("enrollWithCursor against a revoked registry row throws DEVICE_REVOKED and leaves NO cursor row", async () => {
      const ACCT = "B-SIGN-ACCOUNT-ATOMIC-RVK";
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT, deviceId: "rez:dev:atomic2", inboxId: "inbox-atomic2", certId: null, authorityEpoch: 1 });
      await registry.revoke({ accountIdentityPublicKeyB64: ACCT, deviceId: "rez:dev:atomic2", authorityEpoch: 2 });

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCT,
          deviceId: "rez:dev:atomic2",
          inboxId: "inbox-atomic2",
          certId: "rez:cap:leaf-atomic2",
          authorityEpoch: 2,
          devicePublicKeyB64: "DEVICE-PUB-ATOMIC2",
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      assert.equal(await durableInbox.getDevice("inbox-atomic2", "rez:dev:atomic2"), null, "no cursor row exists — the rollback covered the cursor create");
    });

    // Reverse direction: a cursor-create failure must roll the enroll back too.
    await t.test("a cursor-create failure (key mismatch) rolls back the registry enroll", async () => {
      const ACCT = "B-SIGN-ACCOUNT-ATOMIC-MISMATCH";
      // A cursor for this (inbox, device) already exists under a DIFFERENT key.
      await durableInbox.registerDevice("inbox-atomic3", "rez:dev:atomic3", { devicePublicKeyB64: "KEY-A" });

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCT,
          deviceId: "rez:dev:atomic3",
          inboxId: "inbox-atomic3",
          certId: null,
          authorityEpoch: 1,
          devicePublicKeyB64: "KEY-B",
        }),
        (err) => err.code === "DEVICE_KEY_MISMATCH",
      );
      assert.equal(await registry.getDevice(ACCT, "rez:dev:atomic3"), null, "the enroll rolled back with the failed cursor create");
    });
  },
);
