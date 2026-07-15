import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { canonicalDeviceId } from "./helpers/deviceRegistryTestUtil.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { PgPropagationOutbox } from "../src/storage/pg/PgPropagationOutbox.js";

// P1#3 leaf 1 — schema + ATOMIC enqueue. A propagation obligation is enqueued IN the
// serializer's fold transaction on every REAL epoch-changing mutation, and ONLY then: a
// stale expectedRevision, a semantic no-op, and an idempotent replay must NOT enqueue. A
// failed enqueue must roll back the whole authority mutation. Rows carry no secrets / no
// peer identities. (Lease / drain / publish / ack are later leaves and absent here.)
const PG_URL = process.env.REZ_PG_TEST_URL || "";
const cap = (h) => "rez:cap:" + createHash("sha256").update(String(h)).digest("hex");
const D = (n) => canonicalDeviceId(n);

test(
  "PgPropagationOutbox leaf 1: atomic enqueue on real folds only, against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_propagation_outbox";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const outbox = new PgPropagationOutbox({ connection: conn });
    const s = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    await t.test("a real epoch-changing fold enqueues exactly one pending row at the bumped epoch", async () => {
      const A = "ACCT-real-fold";
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "add1", expectedRevision: 0, action: "device.add", target: { deviceId: D("a1"), inboxId: "inbox-a1", certId: cap("a1") } });
      const pend = await outbox.listPending(A);
      assert.deepEqual(pend.map((r) => r.epoch), [1], "one obligation at epoch 1");
      assert.equal(pend[0].kind, "authority_state");
      assert.equal(pend[0].status, "pending");
      // A second real fold → a second obligation at the next epoch.
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "add2", expectedRevision: 1, action: "device.add", target: { deviceId: D("a2"), inboxId: "inbox-a2", certId: cap("a2") } });
      assert.deepEqual((await outbox.listPending(A)).map((r) => r.epoch), [1, 2]);
    });

    await t.test("a STALE expectedRevision does NOT enqueue", async () => {
      const A = "ACCT-stale";
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "seed", expectedRevision: 0, action: "device.add", target: { deviceId: D("s1"), inboxId: "inbox-s1" } });
      const before = (await outbox.listPending(A)).length;
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "stale", expectedRevision: 0, action: "device.add", target: { deviceId: D("s2"), inboxId: "inbox-s2" } });
      assert.equal(r.stale, true);
      assert.equal((await outbox.listPending(A)).length, before, "no obligation for a stale attempt");
    });

    await t.test("a semantic NO-OP does NOT enqueue", async () => {
      const A = "ACCT-noop";
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "seed", expectedRevision: 0, action: "device.add", target: { deviceId: D("n1"), inboxId: "inbox-n1", certId: cap("n1") } });
      const before = (await outbox.listPending(A)).length;
      // Re-adding the SAME active device (same inbox + cert) is a no-op → no epoch bump.
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "noop", expectedRevision: 1, action: "device.add", target: { deviceId: D("n1"), inboxId: "inbox-n1", certId: cap("n1") } });
      assert.equal(r.noop, true);
      assert.equal((await outbox.listPending(A)).length, before, "no obligation for a semantic no-op");
    });

    await t.test("an idempotent REPLAY does NOT enqueue a second obligation", async () => {
      const A = "ACCT-replay";
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "op-x", expectedRevision: 0, action: "device.add", target: { deviceId: D("r1"), inboxId: "inbox-r1", certId: cap("r1") } });
      const after1 = (await outbox.listPending(A)).length;
      const replay = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "op-x", expectedRevision: 0, action: "device.add", target: { deviceId: D("r1"), inboxId: "inbox-r1", certId: cap("r1") } });
      assert.equal(replay.idempotentReplay, true);
      assert.equal((await outbox.listPending(A)).length, after1, "a replay adds no new obligation");
    });

    await t.test("a FAILED enqueue rolls back the whole authority mutation (atomic)", async () => {
      const A = "ACCT-rollback";
      const boom = new PgPropagationOutbox({ connection: conn });
      boom.enqueueInTx = async () => { throw new Error("outbox insert failed"); };
      const sBoom = new PgAccountMutationSerializer({ connection: conn, durableInbox, propagationOutbox: boom });
      await assert.rejects(
        () => sBoom.submitMutation({ accountIdentityPublicKeyB64: A, opId: "willroll", expectedRevision: 0, action: "device.add", target: { deviceId: D("rb1"), inboxId: "inbox-rb1", certId: cap("rb1") } }),
        /outbox insert failed/,
      );
      // The fold rolled back: no epoch bump (still 0), no device, no obligation.
      const authRow = await conn.query("SELECT epoch FROM account_authority WHERE account_identity = $1", [A]);
      assert.ok(authRow.rowCount === 0 || Number(authRow.rows[0].epoch) === 0, "authority epoch not bumped");
      const devRow = await conn.query("SELECT count(*)::int c FROM account_device_registry WHERE account_identity = $1", [A]);
      assert.equal(devRow.rows[0].c, 0, "the device was not enrolled");
      assert.equal((await outbox.listPending(A)).length, 0, "no obligation persisted");
    });

    await t.test("concurrent mutations on DIFFERENT accounts each enqueue their own obligation", async () => {
      const A1 = "ACCT-conc-1";
      const A2 = "ACCT-conc-2";
      await Promise.all([
        s.submitMutation({ accountIdentityPublicKeyB64: A1, opId: "c1", expectedRevision: 0, action: "device.add", target: { deviceId: D("cc1"), inboxId: "inbox-cc1" } }),
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c2", expectedRevision: 0, action: "device.add", target: { deviceId: D("cc2"), inboxId: "inbox-cc2" } }),
      ]);
      assert.deepEqual((await outbox.listPending(A1)).map((r) => r.epoch), [1]);
      assert.deepEqual((await outbox.listPending(A2)).map((r) => r.epoch), [1]);
    });

    await t.test("outbox rows carry NO secrets and NO peer identities — only account/epoch/kind + bookkeeping", async () => {
      const cols = await conn.query(
        "SELECT column_name FROM information_schema.columns"
          + " WHERE table_schema = current_schema() AND table_name = 'account_propagation_outbox'"
          + " ORDER BY column_name",
      );
      const names = cols.rows.map((r) => r.column_name).sort();
      assert.deepEqual(names, [
        "account_identity", "attempts", "enqueued_at", "epoch", "kind",
        "lease_expires_at", "lease_token", "status", "updated_at",
      ], "no ciphertext / key / peer-list columns exist");
    });

    await t.test("the migration is idempotent (re-running the runner is a no-op)", async () => {
      await new MigrationRunner({ connection: conn }).migrate();
      // Still queryable + intact after a re-run.
      assert.equal(typeof (await outbox.getPendingCount()), "number");
    });
  },
);
