import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { canonicalDeviceId } from "./helpers/deviceRegistryTestUtil.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { PgConnection } from "../src/storage/pg/PgConnection.js";

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
    // The registry (L2c) requires canonical device ids on every device.add fold, so
    // even the serializer's flat-target tests use real canonical ids (deterministic).
    const D = {
      d1: canonicalDeviceId("d1"), dX: canonicalDeviceId("dX"), d2: canonicalDeviceId("d2"),
      d3: canonicalDeviceId("d3"), s: canonicalDeviceId("s"), c1: canonicalDeviceId("c1"),
      c2: canonicalDeviceId("c2"), coal: canonicalDeviceId("coal"), ghost: canonicalDeviceId("ghost"),
      atomic: canonicalDeviceId("atomic"), rb: canonicalDeviceId("rb"), res: canonicalDeviceId("res"),
      rp: canonicalDeviceId("rp"), same: canonicalDeviceId("same"),
    };

    await t.test("first device.add: epoch 0 -> 1, device active, authorityState empty", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-1", expectedRevision: 0,
        action: "device.add", target: { deviceId: D.d1, inboxId: "inbox-d1", certId: cap("leaf1") },
      });
      assert.equal(r.revision, 1);
      assert.equal(r.idempotentReplay, false);
      assert.equal(r.devices.length, 1);
      assert.equal(r.devices[0].deviceId, D.d1);
      assert.deepEqual(r.authorityState, { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 });
    });

    await t.test("idempotent replay: the SAME opId returns the committed result, no re-apply", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-1", expectedRevision: 0,
        action: "device.add", target: { deviceId: D.d1, inboxId: "inbox-d1" },
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
        action: "device.add", target: { deviceId: D.dX, inboxId: "inbox-dX" },
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
        action: "device.add", target: { deviceId: D.d2, inboxId: "inbox-d2", certId: cap("leaf2") },
      });
      assert.equal(r.revision, 2);
      assert.equal(r.devices.length, 2);
    });

    await t.test("device.add rejects an inbox already held by another device", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: ACCT, opId: "op-dup-inbox", expectedRevision: 2,
          action: "device.add", target: { deviceId: D.d3, inboxId: "inbox-d1" },
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
        target: { revokedDeviceId: D.d2, revokedCertId: cap("leaf2"), minValidIssuedAtMs: 5000 },
      });
      assert.equal(r.revision, 3);
      assert.equal(r.devices.length, 1, "only d1 remains active");
      assert.equal(r.devices[0].deviceId, D.d1);
      assert.deepEqual(r.authorityState.revokedCertIds, [cap("leaf2")]);
      assert.equal(r.authorityState.minValidIssuedAtMs, 5000);
      assert.equal(r.authorityState.epoch, 3);
    });

    await t.test("revoke of an unenrolled device is idempotent (fail-close intent), still bumps + records the cert", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-ghost", expectedRevision: 3,
        action: "device.revoke", target: { revokedDeviceId: D.ghost, revokedCertId: cap("ghostcap") },
      });
      assert.equal(r.revision, 4);
      assert.equal(r.devices.length, 1, "active set unchanged");
      assert.ok(r.authorityState.revokedCertIds.includes(cap("ghostcap")));
    });

    await t.test("minValidIssuedAtMs is monotonic (a lower cutoff does not regress it)", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-low-cutoff", expectedRevision: 4,
        action: "device.revoke", target: { revokedDeviceId: D.d1, minValidIssuedAtMs: 100 },
      });
      assert.equal(r.authorityState.minValidIssuedAtMs, 5000, "cutoff did not regress");
    });

    await t.test("concurrent submits at the same expectedRevision serialize: exactly one commits, the other goes stale", async () => {
      const A2 = "B-SIGN-ACCT-CONCURRENT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "seed", expectedRevision: 0, action: "device.add", target: { deviceId: D.s, inboxId: "inbox-s" } });
      // Both target revision 1 concurrently.
      const [r1, r2] = await Promise.all([
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c1", expectedRevision: 1, action: "device.add", target: { deviceId: D.c1, inboxId: "inbox-c1" } }),
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c2", expectedRevision: 1, action: "device.add", target: { deviceId: D.c2, inboxId: "inbox-c2" } }),
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
      await registry.enroll({ accountIdentityPublicKeyB64: A3, deviceId: D.coal, inboxId: "inbox-coal", certId: "rez:cap:leaf-coal", authorityEpoch: 0 });
      // A serializer device.add for the SAME device (certId=null) folds the row.
      await s.submitMutation({ accountIdentityPublicKeyB64: A3, opId: "coal-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.coal, inboxId: "inbox-coal", certId: null } });
      const dev = await registry.getDevice(A3, D.coal);
      assert.equal(dev.certId, "rez:cap:leaf-coal", "the leaf cert survives the device.add fold");
      assert.equal(dev.status, "active");
    });

    // S2.5 S11 audit F4 (2026-07-09): the revoke fail-close is ATOMIC — the target
    // device's delivery cursor (device_cursors.revoked) is closed inside the SAME
    // transaction as the authority commit, so the two can never split on a crash.
    await t.test("device.revoke closes the target device's delivery cursor in the SAME transaction", async () => {
      const A4 = "B-SIGN-ACCT-ATOMIC";
      await s.submitMutation({ accountIdentityPublicKeyB64: A4, opId: "atomic-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.atomic, inboxId: "inbox-atomic" } });
      await durableInbox.registerDevice("inbox-atomic", D.atomic);
      const before = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", ["inbox-atomic", D.atomic]);
      assert.equal(before.rows[0].revoked, false, "cursor starts live");

      await s.submitMutation({ accountIdentityPublicKeyB64: A4, opId: "atomic-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D.atomic } });
      const after = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", ["inbox-atomic", D.atomic]);
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
      await s.submitMutation({ accountIdentityPublicKeyB64: A5, opId: "rb-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.rb, inboxId: "inbox-rb" } });

      await assert.rejects(
        () => sFail.submitMutation({ accountIdentityPublicKeyB64: A5, opId: "rb-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D.rb } }),
        /boom-durable/,
      );
      const st = await s.getAuthorityState(A5);
      assert.equal(st.epoch, 1, "the failed revoke rolled back — epoch not bumped");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const dev = await reg.getDevice(A5, D.rb);
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
      await s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.res, inboxId: "inbox-res" } });
      await s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D.res } });
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A6, D.res)).status, "revoked");

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-readd", expectedRevision: 2, action: "device.add", target: { deviceId: D.res, inboxId: "inbox-res" } }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      // The row stays revoked and the epoch did not advance on the rejected re-add.
      assert.equal((await reg.getDevice(A6, D.res)).status, "revoked", "the revoked device was NOT resurrected");
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

    // Audit R4 L2c review P2: a HISTORICAL malformed (non-canonical) row — one that
    // predates the L2c enroll guard — must still be fail-closable. foldRevokeInTx
    // flips an existing row BEFORE applying the never-enrolled canonical-shape reject,
    // so a real row is always revoked + tombstoned + its cursor closed, whatever its
    // shape. This inserts the malformed row + live cursor DIRECTLY (bypassing the now
    // strict enroll) to simulate the legacy state, then proves the full fail-close.
    await t.test("a HISTORICAL malformed (non-canonical) enrolled row is still fail-closed + tombstoned on revoke", async () => {
      const A8 = "B-SIGN-ACCT-HIST-MALFORMED";
      const malformed = "rez:dev:legacy-malformed"; // non-canonical; could never enroll under L2c
      const inbox = "inbox-hist-malformed";
      // Legacy state: an active registry row + its live delivery cursor, written raw.
      await conn.query(
        "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
          + " VALUES ($1, $2, $3, NULL, 0, 'active')",
        [A8, malformed, inbox],
      );
      await durableInbox.registerDevice(inbox, malformed, { devicePublicKeyB64: "LEGACY-DEVICE-KEY" });
      const before = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", [inbox, malformed]);
      assert.equal(before.rows[0].revoked, false, "the legacy cursor starts live");

      // The account's first authority mutation revokes the malformed device.
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "hist-revoke", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: malformed } });
      assert.equal(r.revision, 1, "the revoke committed");

      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A8, malformed)).status, "revoked", "the historical malformed row is fail-closed");
      assert.equal(await reg.isTombstoned(A8, malformed), true, "and terminally tombstoned");
      const after = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", [inbox, malformed]);
      assert.equal(after.rows[0].revoked, true, "its delivery cursor was fail-closed in the same tx");
    });

    // Audit 2026-07-10 R3 F2 (fold inbox re-point): a device's inbox is immutable
    // once enrolled (the registry throws ACCOUNT_DEVICE_CONFLICT). The fold must
    // not silently re-point inbox_id — that orphans the device's live cursor on the
    // old inbox, and a later revoke (resolving only the CURRENT inbox) closes the
    // wrong cursor, leaving the revoked device still draining the old inbox.
    await t.test("device.add cannot re-point an active device to a different inbox", async () => {
      const A7 = "B-SIGN-ACCT-REPOINT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A7, opId: "rp-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.rp, inboxId: "inbox-rp-old" } });

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A7, opId: "rp-move", expectedRevision: 1, action: "device.add", target: { deviceId: D.rp, inboxId: "inbox-rp-new" } }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A7, D.rp)).inboxId, "inbox-rp-old", "the inbox was NOT re-pointed");
      assert.equal((await s.getAuthorityState(A7)).epoch, 1, "a rejected re-point does not bump the epoch");
    });

    // Same-device, same-inbox re-add stays idempotent (the guards only fire on a
    // revoked row or a genuine inbox change) — the cert-coalesce path is unaffected.
    await t.test("device.add for the SAME device + SAME inbox still folds idempotently", async () => {
      const A8 = "B-SIGN-ACCT-SAME";
      await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "same-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.same, inboxId: "inbox-same" } });
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "same-readd", expectedRevision: 1, action: "device.add", target: { deviceId: D.same, inboxId: "inbox-same", certId: "rez:cap:leaf-same" } });
      assert.equal(r.revision, 2, "a same-device same-inbox re-add is a normal fold (cert upgrade)");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const dev = await reg.getDevice(A8, D.same);
      assert.equal(dev.status, "active");
      assert.equal(dev.certId, "rez:cap:leaf-same", "the leaf cert was written by the fold");
    });

    // ---- audit R4 L3: TOCTOU-safe delegated recheck UNDER the account lock ----

    // A delegated session's authority is re-checked, but the check must run under the
    // SAME per-account lock as the fold and against the IN-TX revocation state — else a
    // device.revoke committing between a pre-lock read and the fold is a TOCTOU. The
    // serializer accepts an optional `revalidate(inTxRevocationState) -> Promise<bool>`.
    await t.test("L3: a revalidate returning false aborts the mutation (no fold, no epoch, no journal)", async () => {
      const A = "B-SIGN-ACCT-L3-FALSE";
      const dev = canonicalDeviceId("l3-false");
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: A, opId: "l3-false-op", expectedRevision: 0,
          action: "device.add", target: { deviceId: dev, inboxId: "inbox-l3-false" },
          revalidate: async () => false,
        }),
        (err) => err.code === "DELEGATED_AUTHORITY_INVALID",
      );
      assert.equal((await s.getAuthorityState(A)).epoch, 0, "epoch not bumped on a rejected recheck");
      const enrolled = await conn.query(
        "SELECT 1 FROM account_device_registry WHERE account_identity = $1 AND device_id = $2", [A, dev],
      );
      assert.equal(enrolled.rowCount, 0, "device not enrolled");
      const journal = await conn.query(
        "SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A],
      );
      assert.equal(journal.rows[0].c, 0, "no journal row written");
    });

    await t.test("L3: a revalidate returning true proceeds normally", async () => {
      const A = "B-SIGN-ACCT-L3-TRUE";
      const dev = canonicalDeviceId("l3-true");
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: A, opId: "l3-true-op", expectedRevision: 0,
        action: "device.add", target: { deviceId: dev, inboxId: "inbox-l3-true" },
        revalidate: async () => true,
      });
      assert.equal(r.revision, 1);
      assert.equal(r.devices.length, 1);
      assert.equal(r.devices[0].deviceId, dev);
    });

    await t.test("L3: the revalidate closure sees the IN-TX revocation state (a committed-before-lock revoke is visible)", async () => {
      const A = "B-SIGN-ACCT-L3-INTX";
      const dev = canonicalDeviceId("l3-intx");
      const revokedCert = "rez:cap:" + "l3intx".padEnd(58, "0");
      // A revoke committed BEFORE this mutation takes the account lock.
      await conn.query(
        "INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch) VALUES ($1, $2, 0)",
        [A, revokedCert],
      );
      let seen = null;
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: A, opId: "l3-intx-op", expectedRevision: 0,
          action: "device.add", target: { deviceId: dev, inboxId: "inbox-l3-intx" },
          // Models the real verifier: reject iff the leaf cert is in the in-tx revoked
          // set. The closure receives the revocation state loaded UNDER the lock.
          revalidate: async (rev) => { seen = rev; return !rev.revokedCertIds.includes(revokedCert); },
        }),
        (err) => err.code === "DELEGATED_AUTHORITY_INVALID",
      );
      assert.ok(seen && seen.revokedCertIds.includes(revokedCert), "the closure saw the committed revoked cert in the in-tx state");
    });

    await t.test("L3: the recheck + fold are atomic under the account lock (a concurrent revoke cannot interleave)", async () => {
      const A = "B-SIGN-ACCT-L3-ATOMIC";
      const dA = canonicalDeviceId("l3-atomic-a");
      const dB = canonicalDeviceId("l3-atomic-b");
      // Seed dB active (epoch 1) so the concurrent revoke has a real target.
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "atomic-seed", expectedRevision: 0, action: "device.add", target: { deviceId: dB, inboxId: "inbox-l3-atomic-b" } });

      // A SECOND connection to the SAME schema — a genuine concurrent writer.
      const conn2 = new PgConnection({ connectionString: PG_URL, poolConfig: { options: `-c search_path=${SCHEMA}` } });
      const s2 = new PgAccountMutationSerializer({ connection: conn2, durableInbox: new PgDurableInbox({ connection: conn2, maxDevices: 1 }) });
      try {
        let concurrentResolved = false;
        let concurrentResult = null;
        let pendingConcurrent = null;
        const result = await s.submitMutation({
          accountIdentityPublicKeyB64: A, opId: "atomic-add", expectedRevision: 1,
          action: "device.add", target: { deviceId: dA, inboxId: "inbox-l3-atomic-a" },
          revalidate: async () => {
            // Fire a concurrent revoke on conn2 WITHOUT awaiting — it must BLOCK on the
            // per-account advisory lock this transaction already holds across the
            // recheck+fold.
            pendingConcurrent = s2.submitMutation({ accountIdentityPublicKeyB64: A, opId: "atomic-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: dB } })
              .then((r) => { concurrentResolved = true; concurrentResult = r; });
            await new Promise((resolve) => setTimeout(resolve, 200));
            assert.equal(concurrentResolved, false, "the concurrent revoke is BLOCKED on the account lock during our recheck+fold");
            return true;
          },
        });
        assert.equal(result.revision, 2, "our add committed at epoch 2");
        // The lock is released at our COMMIT; the concurrent revoke can now proceed.
        await pendingConcurrent;
        assert.equal(concurrentResolved, true, "the concurrent revoke resolved only AFTER our commit");
        assert.equal(concurrentResult.stale, true, "and it observed our committed epoch (serialized strictly after)");
      } finally {
        await conn2.close();
      }
    });

    // ---- audit R4 F3: durable admission control (input guards, caps, no-op, prune) ----

    await t.test("F3: an opId over the byte cap is rejected (BAD_REQUEST)", async () => {
      const sTiny = new PgAccountMutationSerializer({ connection: conn, durableInbox, caps: { opIdBytes: 8 } });
      await assert.rejects(
        () => sTiny.submitMutation({
          accountIdentityPublicKeyB64: "B-SIGN-F3-OPID", opId: "x".repeat(9), expectedRevision: 0,
          action: "device.add", target: { deviceId: canonicalDeviceId("f3op"), inboxId: "inbox-f3op" },
        }),
        (err) => err.code === "BAD_REQUEST",
      );
    });

    await t.test("F3: a malformed or oversized revokedCertId is rejected (BAD_TARGET)", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: "B-SIGN-F3-CERT", opId: "f3cert-op", expectedRevision: 0,
          action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("f3cert"), revokedCertId: "not-a-cap-id" },
        }),
        (err) => err.code === "BAD_TARGET",
      );
      const sTiny = new PgAccountMutationSerializer({ connection: conn, durableInbox, caps: { certIdBytes: 16 } });
      await assert.rejects(
        () => sTiny.submitMutation({
          accountIdentityPublicKeyB64: "B-SIGN-F3-CERT2", opId: "f3cert2-op", expectedRevision: 0,
          action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("f3cert2"), revokedCertId: "rez:cap:" + "z".repeat(20) },
        }),
        (err) => err.code === "BAD_TARGET",
      );
    });

    await t.test("F3: the revoked-cert set is bounded (REVOKED_CERT_QUOTA_EXCEEDED)", async () => {
      const A = "B-SIGN-F3-CERTCAP";
      const sCap = new PgAccountMutationSerializer({ connection: conn, durableInbox, caps: { revokedCerts: 2 } });
      const rc = (h) => "rez:cap:" + String(h).padEnd(58, "0");
      await sCap.submitMutation({ accountIdentityPublicKeyB64: A, opId: "cc-1", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("cc1"), revokedCertId: rc("c1") } });
      await sCap.submitMutation({ accountIdentityPublicKeyB64: A, opId: "cc-2", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("cc2"), revokedCertId: rc("c2") } });
      await assert.rejects(
        () => sCap.submitMutation({ accountIdentityPublicKeyB64: A, opId: "cc-3", expectedRevision: 2, action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("cc3"), revokedCertId: rc("c3") } }),
        (err) => err.code === "REVOKED_CERT_QUOTA_EXCEEDED",
      );
      assert.equal((await s.getAuthorityState(A)).epoch, 2, "the over-cap revoke did not bump the epoch");
    });

    await t.test("F3: a device.add that changes nothing is a no-op (no epoch bump, no journal row)", async () => {
      const A = "B-SIGN-F3-NOOPADD";
      const dev = canonicalDeviceId("f3noopadd");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "na-1", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3noopadd" } });
      const before = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "na-2", expectedRevision: 1, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3noopadd" } });
      assert.equal(r.noop, true, "the redundant add is a semantic no-op");
      assert.equal(r.revision, 1, "the epoch did NOT advance");
      assert.equal((await s.getAuthorityState(A)).epoch, 1);
      const after = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      assert.equal(after.rows[0].c, before.rows[0].c, "no journal row was appended for the no-op");
    });

    await t.test("F3: a re-revoke of an already-terminal device is a no-op (no epoch bump, no journal row)", async () => {
      const A = "B-SIGN-F3-NOOPREV";
      const dev = canonicalDeviceId("f3nooprev");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "nr-add", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3nooprev" } });
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "nr-rev", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: dev } });
      const before = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "nr-rev-again", expectedRevision: 2, action: "device.revoke", target: { revokedDeviceId: dev } });
      assert.equal(r.noop, true, "re-revoking an already-terminal device changes nothing");
      assert.equal(r.revision, 2, "the epoch did NOT advance");
      const after = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      assert.equal(after.rows[0].c, before.rows[0].c, "no journal row for the no-op revoke");
    });

    await t.test("F3: pruneExpiredReplayPayloads NULLs an old payload; a later replay returns replayExpired", async () => {
      const A = "B-SIGN-F3-PRUNE";
      const dev = canonicalDeviceId("f3prune");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "pr-op", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3prune" } });
      // A future nowMs with the default 30d TTL makes the cutoff land after this fresh
      // row's committed_at, so its replay payload is pruned (the audit row stays).
      const future = Date.now() + 40 * 24 * 60 * 60 * 1000;
      const pruned = await s.pruneExpiredReplayPayloads(future);
      assert.ok(pruned >= 1, "at least our row's payload was pruned");
      const check = await conn.query("SELECT result_json FROM account_device_mutation WHERE account_identity = $1 AND op_id = $2", [A, "pr-op"]);
      assert.equal(check.rows[0].result_json, null, "the replay payload was NULLed");
      // A replay of the pruned opId still proves it committed → replayExpired + current state.
      const replay = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "pr-op", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3prune" } });
      assert.equal(replay.idempotentReplay, true);
      assert.equal(replay.replayExpired, true);
      assert.equal(replay.revision, 1, "the current authority epoch is returned");
    });
  },
);
