import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";

// S2.5 S11 L4 (findings F4+F5, OPEN-B): the authority-home serializer. Real
// Postgres: opId idempotency, expectedRevision CAS (stale returns latest, no
// clobber), add/revoke fold (remove-wins), monotonic epoch, revoked-cert set +
// minValidIssuedAt cutoff, concurrent submits serialize.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

test(
  "PgAccountMutationSerializer against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_account_mutation_serializer";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const s = new PgAccountMutationSerializer({ connection: conn });

    const ACCT = "B-SIGN-ACCT-1";
    const cap = (h) => "rez:cap:" + String(h).padEnd(64, "0");

    await t.test("first device.add: epoch 0 -> 1, device active, authorityState empty", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-1", expectedRevision: 0,
        action: "device.add", target: { deviceId: "rez:dev:d1", inboxId: "inbox-d1", certId: cap("leaf1") },
      });
      assert.equal(r.revision, 1);
      assert.equal(r.idempotentReplay, false);
      assert.equal(r.devices.length, 1);
      assert.equal(r.devices[0].deviceId, "rez:dev:d1");
      assert.deepEqual(r.authorityState, { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 });
    });

    await t.test("idempotent replay: the SAME opId returns the committed result, no re-apply", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-1", expectedRevision: 0,
        action: "device.add", target: { deviceId: "rez:dev:d1", inboxId: "inbox-d1" },
      });
      assert.equal(r.revision, 1, "revision unchanged");
      assert.equal(r.idempotentReplay, true);
      // Epoch did not advance.
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 1);
    });

    await t.test("stale expectedRevision returns the latest state and does NOT clobber", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-stale", expectedRevision: 0, // current is 1
        action: "device.add", target: { deviceId: "rez:dev:dX", inboxId: "inbox-dX" },
      });
      assert.equal(r.stale, true);
      assert.equal(r.currentRevision, 1);
      assert.equal(r.devices.length, 1, "dX was NOT added");
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 1, "epoch not bumped by a stale submit");
    });

    await t.test("second device.add at the right revision: epoch 1 -> 2, two active devices", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-2", expectedRevision: 1,
        action: "device.add", target: { deviceId: "rez:dev:d2", inboxId: "inbox-d2", certId: cap("leaf2") },
      });
      assert.equal(r.revision, 2);
      assert.equal(r.devices.length, 2);
    });

    await t.test("device.add rejects an inbox already held by another device", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: ACCT, opId: "op-dup-inbox", expectedRevision: 2,
          action: "device.add", target: { deviceId: "rez:dev:d3", inboxId: "inbox-d1" },
        }),
        (err) => err.code === "INBOX_ALREADY_ENROLLED",
      );
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 2, "a rejected add does not bump the epoch");
    });

    await t.test("device.revoke: remove-wins, bumps epoch, active set shrinks, cert revoked, cutoff advances", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-d2", expectedRevision: 2,
        action: "device.revoke",
        target: { revokedDeviceId: "rez:dev:d2", revokedCertId: cap("leaf2"), minValidIssuedAtMs: 5000 },
      });
      assert.equal(r.revision, 3);
      assert.equal(r.devices.length, 1, "only d1 remains active");
      assert.equal(r.devices[0].deviceId, "rez:dev:d1");
      assert.deepEqual(r.authorityState.revokedCertIds, [cap("leaf2")]);
      assert.equal(r.authorityState.minValidIssuedAtMs, 5000);
      assert.equal(r.authorityState.epoch, 3);
    });

    await t.test("revoke of an unenrolled device is idempotent (fail-close intent), still bumps + records the cert", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-ghost", expectedRevision: 3,
        action: "device.revoke", target: { revokedDeviceId: "rez:dev:ghost", revokedCertId: cap("ghostcap") },
      });
      assert.equal(r.revision, 4);
      assert.equal(r.devices.length, 1, "active set unchanged");
      assert.ok(r.authorityState.revokedCertIds.includes(cap("ghostcap")));
    });

    await t.test("minValidIssuedAtMs is monotonic (a lower cutoff does not regress it)", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-low-cutoff", expectedRevision: 4,
        action: "device.revoke", target: { revokedDeviceId: "rez:dev:d1", minValidIssuedAtMs: 100 },
      });
      assert.equal(r.authorityState.minValidIssuedAtMs, 5000, "cutoff did not regress");
    });

    await t.test("concurrent submits at the same expectedRevision serialize: exactly one commits, the other goes stale", async () => {
      const A2 = "B-SIGN-ACCT-CONCURRENT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "seed", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:s", inboxId: "inbox-s" } });
      // Both target revision 1 concurrently.
      const [r1, r2] = await Promise.all([
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c1", expectedRevision: 1, action: "device.add", target: { deviceId: "rez:dev:c1", inboxId: "inbox-c1" } }),
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c2", expectedRevision: 1, action: "device.add", target: { deviceId: "rez:dev:c2", inboxId: "inbox-c2" } }),
      ]);
      const committed = [r1, r2].filter((r) => !r.stale);
      const stale = [r1, r2].filter((r) => r.stale);
      assert.equal(committed.length, 1, "exactly one commit");
      assert.equal(stale.length, 1, "the other is stale (CAS)");
      assert.equal(committed[0].revision, 2);
      const st = await s.getAuthorityState(A2);
      assert.equal(st.epoch, 2, "epoch advanced exactly once");
    });

    // S2.5 S12 L4 — cert reconciliation on the serializer side: a device.add fold
    // (certId=null) must NOT clobber a leaf cert already written by device.bind's
    // enroll (COALESCE keep).
    await t.test("device.add does not clobber a non-null cert_id to null", async () => {
      const A3 = "B-SIGN-ACCT-COALESCE";
      const registry = new PgAccountDeviceRegistry({ connection: conn });
      // device.bind enroll writes the leaf cert first.
      await registry.enroll({ accountIdentityPublicKeyB64: A3, deviceId: "rez:dev:coal", inboxId: "inbox-coal", certId: "rez:cap:leaf-coal", authorityEpoch: 0 });
      // A serializer device.add for the SAME device (certId=null) folds the row.
      await s.submitMutation({ accountIdentityPublicKeyB64: A3, opId: "coal-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:coal", inboxId: "inbox-coal", certId: null } });
      const dev = await registry.getDevice(A3, "rez:dev:coal");
      assert.equal(dev.certId, "rez:cap:leaf-coal", "the leaf cert survives the device.add fold");
      assert.equal(dev.status, "active");
    });
  },
);
