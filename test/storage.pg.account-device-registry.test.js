import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";

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
    const registry = new PgAccountDeviceRegistry({ connection: conn });

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

    await t.test("setStatus revokes a device and bumps the authority epoch", async () => {
      const revoked = await registry.setStatus({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: "rez:dev:a2",
        status: "revoked",
        authorityEpoch: 2,
      });
      assert.equal(revoked.status, "revoked");
      assert.equal(revoked.authorityEpoch, 2);
      assert.equal((await registry.getDevice(ACCT_A, "rez:dev:a2")).status, "revoked");
    });

    await t.test("setStatus refuses a regressing authority epoch (monotonic)", async () => {
      await assert.rejects(
        () => registry.setStatus({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:a2",
          status: "active",
          authorityEpoch: 1,
        }),
        (err) => err.code === "AUTHORITY_EPOCH_REGRESSION",
      );
    });

    await t.test("setStatus on an unenrolled device fails loud", async () => {
      await assert.rejects(
        () => registry.setStatus({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:nope",
          status: "revoked",
          authorityEpoch: 1,
        }),
        (err) => err.code === "DEVICE_NOT_ENROLLED",
      );
    });

    await t.test("setStatus rejects an unknown status value", async () => {
      await assert.rejects(
        () => registry.setStatus({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:a1",
          status: "frozen",
          authorityEpoch: 3,
        }),
        (err) => err.code === "BAD_STATUS",
      );
    });

    await t.test("getDevice / resolveInbox return null for unknown keys", async () => {
      assert.equal(await registry.getDevice(ACCT_A, "rez:dev:ghost"), null);
      assert.equal(await registry.resolveInbox("inbox-ghost"), null);
      assert.equal(await registry.getDevice("", ""), null);
    });
  },
);
