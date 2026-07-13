import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";

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
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const s = new PgAccountMutationSerializer({ connection: conn, durableInbox });

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
      const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      // device.bind enroll writes the leaf cert first.
      await registry.enroll({ accountIdentityPublicKeyB64: A3, deviceId: "rez:dev:coal", inboxId: "inbox-coal", certId: "rez:cap:leaf-coal", authorityEpoch: 0 });
      // A serializer device.add for the SAME device (certId=null) folds the row.
      await s.submitMutation({ accountIdentityPublicKeyB64: A3, opId: "coal-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:coal", inboxId: "inbox-coal", certId: null } });
      const dev = await registry.getDevice(A3, "rez:dev:coal");
      assert.equal(dev.certId, "rez:cap:leaf-coal", "the leaf cert survives the device.add fold");
      assert.equal(dev.status, "active");
    });

    // S2.5 S11 audit F4 (2026-07-09): the revoke fail-close is ATOMIC — the target
    // device's delivery cursor (device_cursors.revoked) is closed inside the SAME
    // transaction as the authority commit, so the two can never split on a crash.
    await t.test("device.revoke closes the target device's delivery cursor in the SAME transaction", async () => {
      const A4 = "B-SIGN-ACCT-ATOMIC";
      await s.submitMutation({ accountIdentityPublicKeyB64: A4, opId: "atomic-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:atomic", inboxId: "inbox-atomic" } });
      await durableInbox.registerDevice("inbox-atomic", "rez:dev:atomic");
      const before = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", ["inbox-atomic", "rez:dev:atomic"]);
      assert.equal(before.rows[0].revoked, false, "cursor starts live");

      await s.submitMutation({ accountIdentityPublicKeyB64: A4, opId: "atomic-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: "rez:dev:atomic" } });
      const after = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", ["inbox-atomic", "rez:dev:atomic"]);
      assert.equal(after.rows[0].revoked, true, "cursor was fail-closed atomically with the authority revoke");
    });

    await t.test("a durable cursor-close failure rolls back the ENTIRE mutation (authority not bumped, device stays active)", async () => {
      // A durableInbox whose cursor-CLOSE fails. It also carries registerDeviceInTx
      // (a real durableInbox has both) so the serializer can compose its registry;
      // foldRevokeInTx never calls it — only the serializer's own cursor close does,
      // which is the failure under test.
      const failingInbox = {
        revokeDeviceInTx: async () => { throw new Error("boom-durable"); },
        registerDeviceInTx: async () => {},
      };
      const sFail = new PgAccountMutationSerializer({ connection: conn, durableInbox: failingInbox });
      const A5 = "B-SIGN-ACCT-ROLLBACK";
      await s.submitMutation({ accountIdentityPublicKeyB64: A5, opId: "rb-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:rb", inboxId: "inbox-rb" } });

      await assert.rejects(
        () => sFail.submitMutation({ accountIdentityPublicKeyB64: A5, opId: "rb-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: "rez:dev:rb" } }),
        /boom-durable/,
      );
      const st = await s.getAuthorityState(A5);
      assert.equal(st.epoch, 1, "the failed revoke rolled back — epoch not bumped");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const dev = await reg.getDevice(A5, "rez:dev:rb");
      assert.equal(dev.status, "active", "the device is still active — the revoke did not partially apply");
    });

    await t.test("constructor fails loud without a durableInbox (atomic revoke fail-close dependency)", () => {
      assert.throws(() => new PgAccountMutationSerializer({ connection: conn }), /durableInbox/);
    });

    // Audit 2026-07-10 R3 F1 (fold resurrection): the device.add fold is the second
    // writer to account_device_registry; it must honor the registry's TERMINAL
    // revocation rule. A revoked device must not be flipped back to active by a
    // re-add — otherwise a device holding only device.add undoes a sibling's revoke,
    // and a subsequent device.bind opens a fresh LIVE cursor for the "revoked" device.
    await t.test("device.add cannot resurrect a REVOKED device (terminal revocation)", async () => {
      const A6 = "B-SIGN-ACCT-RESURRECT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:res", inboxId: "inbox-res" } });
      await s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: "rez:dev:res" } });
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A6, "rez:dev:res")).status, "revoked");

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-readd", expectedRevision: 2, action: "device.add", target: { deviceId: "rez:dev:res", inboxId: "inbox-res" } }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      // The row stays revoked and the epoch did not advance on the rejected re-add.
      assert.equal((await reg.getDevice(A6, "rez:dev:res")).status, "revoked", "the revoked device was NOT resurrected");
      assert.equal((await s.getAuthorityState(A6)).epoch, 2, "a rejected re-add does not bump the epoch");
    });

    // Audit R4 F1 (NEVER-enrolled resurrection): a device.revoke can name a device
    // that was never enrolled (revoke racing ahead of its first device.add). Before
    // the durable tombstone this left NO trace, so a later device.add enrolled it
    // ACTIVE. The revoke now writes a tombstone (for a canonical id) and the fold
    // consults it — a subsequent device.add of that never-enrolled deviceId is
    // refused, exactly like an enrolled-then-revoked one.
    await t.test("device.add cannot resurrect a NEVER-ENROLLED then revoked device (F1 tombstone)", async () => {
      const A6b = "B-SIGN-ACCT-NEVER-ENROLLED";
      const NE = "rez:dev:" + "a".repeat(64); // canonical, never enrolled
      // Revoke it before it ever enrolls — succeeds (fail-close), writes a tombstone.
      const rv = await s.submitMutation({ accountIdentityPublicKeyB64: A6b, opId: "ne-revoke", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: NE } });
      assert.equal(rv.revision, 1, "the revoke committed and bumped the epoch");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal(await reg.getDevice(A6b, NE), null, "no active/revoked registry row — it was never enrolled");
      assert.equal(await reg.isTombstoned(A6b, NE), true, "but the terminal tombstone was written");

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A6b, opId: "ne-add", expectedRevision: 1, action: "device.add", target: { deviceId: NE, inboxId: "inbox-ne" } }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      assert.equal(await reg.getDevice(A6b, NE), null, "the never-enrolled revoked device was NOT resurrected");
      assert.equal((await s.getAuthorityState(A6b)).epoch, 1, "the rejected add did not bump the epoch");
    });

    // Audit 2026-07-10 R3 F2 (fold inbox re-point): a device's inbox is immutable
    // once enrolled (the registry throws ACCOUNT_DEVICE_CONFLICT). The fold must
    // not silently re-point inbox_id — that orphans the device's live cursor on the
    // old inbox, and a later revoke (resolving only the CURRENT inbox) closes the
    // wrong cursor, leaving the revoked device still draining the old inbox.
    await t.test("device.add cannot re-point an active device to a different inbox", async () => {
      const A7 = "B-SIGN-ACCT-REPOINT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A7, opId: "rp-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:rp", inboxId: "inbox-rp-old" } });

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A7, opId: "rp-move", expectedRevision: 1, action: "device.add", target: { deviceId: "rez:dev:rp", inboxId: "inbox-rp-new" } }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A7, "rez:dev:rp")).inboxId, "inbox-rp-old", "the inbox was NOT re-pointed");
      assert.equal((await s.getAuthorityState(A7)).epoch, 1, "a rejected re-point does not bump the epoch");
    });

    // Same-device, same-inbox re-add stays idempotent (the guards only fire on a
    // revoked row or a genuine inbox change) — the cert-coalesce path is unaffected.
    await t.test("device.add for the SAME device + SAME inbox still folds idempotently", async () => {
      const A8 = "B-SIGN-ACCT-SAME";
      await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "same-add", expectedRevision: 0, action: "device.add", target: { deviceId: "rez:dev:same", inboxId: "inbox-same" } });
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "same-readd", expectedRevision: 1, action: "device.add", target: { deviceId: "rez:dev:same", inboxId: "inbox-same", certId: "rez:cap:leaf-same" } });
      assert.equal(r.revision, 2, "a same-device same-inbox re-add is a normal fold (cert upgrade)");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const dev = await reg.getDevice(A8, "rez:dev:same");
      assert.equal(dev.status, "active");
      assert.equal(dev.certId, "rez:cap:leaf-same", "the leaf cert was written by the fold");
    });
  },
);
