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

    await t.test("a real device.REVOKE enqueues its bumped epoch + the cumulative revoked-cert state", async () => {
      const A = "ACCT-revoke";
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "seed", expectedRevision: 0, action: "device.add", target: { deviceId: D("rvd"), inboxId: "inbox-rvd", certId: cap("rvd") } });
      assert.deepEqual((await outbox.listPending(A)).map((r) => r.epoch), [1]);
      const rev = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "rev", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D("rvd") } });
      assert.equal(rev.revision, 2, "revoke bumped the epoch");
      assert.deepEqual((await outbox.listPending(A)).map((r) => r.epoch), [1, 2], "revoke enqueued its own obligation");
      // The obligation's epoch resolves to the CUMULATIVE authority snapshot: the device's own
      // bound cert is now revoked (Option A auto-revoke) — the state a client publishes.
      assert.ok(rev.authorityState.revokedCertIds.includes(cap("rvd")), "revoked-cert state is cumulative");
    });

    await t.test("two SAME-account mutations racing at the same expectedRevision → one fold+obligation, one stale", async () => {
      const A = "ACCT-same-race";
      const [ra, rb] = await Promise.all([
        s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "race-a", expectedRevision: 0, action: "device.add", target: { deviceId: D("ra"), inboxId: "inbox-ra" } }),
        s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "race-b", expectedRevision: 0, action: "device.add", target: { deviceId: D("rb"), inboxId: "inbox-rb" } }),
      ]);
      const staleCount = [ra, rb].filter((r) => r.stale === true).length;
      const committedCount = [ra, rb].filter((r) => r.revision === 1 && !r.stale).length;
      assert.equal(staleCount, 1, "exactly one racer is stale (per-account serialization)");
      assert.equal(committedCount, 1, "exactly one racer committed the fold");
      assert.deepEqual((await outbox.listPending(A)).map((r) => r.epoch), [1], "exactly one obligation for the race");
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
        "account_identity", "attempts", "blocked_at", "enqueued_at", "epoch", "kind",
        "last_error", "lease_expires_at", "lease_token", "next_attempt_at", "status", "updated_at",
      ], "no ciphertext / key / peer-list columns exist");
    });

    await t.test("0018 DB invariants reject bad queue state (epoch/kind/status/attempts/lease-pair/token-size)", async () => {
      const badInserts = [
        ["epoch 0", "INSERT INTO account_propagation_outbox (account_identity, epoch) VALUES ('X', 0)"],
        ["unknown kind", "INSERT INTO account_propagation_outbox (account_identity, epoch, kind) VALUES ('X', 1, 'peer_set')"],
        ["unknown status", "INSERT INTO account_propagation_outbox (account_identity, epoch, status) VALUES ('X', 1, 'weird')"],
        ["negative attempts", "INSERT INTO account_propagation_outbox (account_identity, epoch, attempts) VALUES ('X', 1, -1)"],
        ["half lease pair", "INSERT INTO account_propagation_outbox (account_identity, epoch, lease_token) VALUES ('X', 1, 'tok')"],
        ["oversized lease token", "INSERT INTO account_propagation_outbox (account_identity, epoch, lease_token, lease_expires_at) VALUES ('X', 1, repeat('a', 200), now())"],
      ];
      for (const [label, sql] of badInserts) {
        await assert.rejects(() => conn.query(sql), /violates check constraint/, "rejects " + label);
      }
    });

    await t.test("0019 lease backstop: status<->lease correlation + at most one leased row per (account, kind)", async () => {
      const future = "now() + interval '1 minute'";
      // status='leased' MUST carry a lease token+expiry.
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, status) VALUES ('L', 1, 'leased')"),
        /violates check constraint/,
        "leased with no token rejected",
      );
      // status='pending' MUST NOT carry a lease.
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, status, lease_token, lease_expires_at) VALUES ('L', 1, 'pending', 'tok', " + future + ")"),
        /violates check constraint/,
        "pending with a live token rejected",
      );
      // A single leased row is fine...
      await conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_expires_at) VALUES ('L', 5, 'authority_state', 'leased', 'tokA', " + future + ")");
      // ...but a SECOND leased row for the same (account, kind) — even at a different epoch — is
      // refused by the partial unique index (never lease N and N+1 concurrently).
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_expires_at) VALUES ('L', 6, 'authority_state', 'leased', 'tokB', " + future + ")"),
        /duplicate key value|unique constraint/,
        "a second concurrent lease on the same account+kind rejected",
      );
    });

    await t.test("MigrationRunner is idempotent (a second run applies nothing new; the store stays intact)", async () => {
      // NOTE: the runner records applied migrations, so a second migrate() is a no-op — this
      // proves RUNNER idempotency, not direct re-execution of the 0017/0018 SQL (those use
      // IF NOT EXISTS / DROP+ADD CONSTRAINT and are independently safe to re-run).
      await new MigrationRunner({ connection: conn }).migrate();
      assert.equal(typeof (await outbox.getPendingCount()), "number");
    });

    await t.test("the serializer rejects an injected outbox lacking enqueueInTx (fail loud at construction)", () => {
      assert.throws(
        () => new PgAccountMutationSerializer({ connection: conn, durableInbox, propagationOutbox: {} }),
        /propagationOutbox exposing enqueueInTx/,
      );
    });

    // ---- leaf 2: head-advancing account lease state machine ----
    const seedFold = async (A, n, rev) => s.submitMutation({
      accountIdentityPublicKeyB64: A, opId: "op-" + n, expectedRevision: rev,
      action: "device.add", target: { deviceId: D(A + n), inboxId: "inbox-" + A + n, certId: cap(A + n) },
    });

    await t.test("RACE: concurrent same-account claims — exactly one gets the lease, the other null", async () => {
      const A = "LEASE-conc";
      await seedFold(A, 1, 0);
      const [a, b] = await Promise.all([outbox.claim(A), outbox.claim(A)]);
      const won = [a, b].filter((r) => r && typeof r.token === "string");
      assert.equal(won.length, 1, "exactly one claimant leased the head");
      assert.equal(won[0].headEpoch, 1);
    });

    await t.test("RACE: N leased, then N+1 commits — prepareAck reports the ADVANCED head, anchor stays N", async () => {
      const A = "LEASE-advance";
      await seedFold(A, 1, 0);
      const lease = await outbox.claim(A);
      assert.equal(lease.headEpoch, 1);
      // A newer epoch commits UNDER the live lease (stays pending; cannot be leased — one-lease index).
      await seedFold(A, 2, 1);
      const prep = await outbox.prepareAck(A, lease.token);
      assert.equal(prep.anchorEpoch, 1, "the anchor is where the lease was taken");
      assert.equal(prep.headEpoch, 2, "the publishable head advanced under the lease");
    });

    await t.test("RACE: the newest head is backing off — claim leases NOTHING (never an older epoch)", async () => {
      const A = "LEASE-backoff";
      await seedFold(A, 1, 0); // eligible
      await seedFold(A, 2, 1); // will be forced into backoff
      await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() + interval '1 hour' WHERE account_identity = $1 AND epoch = 2", [A]);
      assert.equal(await outbox.claim(A), null, "newest is backing off ⇒ no lease");
      const pend = await outbox.listPending(A);
      assert.ok(pend.every((r) => r.status === "pending"), "no older epoch was leased");
    });

    await t.test("RECLAIM: an expired lease is returned to pending on the next claim", async () => {
      const A = "LEASE-expire";
      await seedFold(A, 1, 0);
      const lease = await outbox.claim(A);
      assert.ok(lease.token);
      // Force the lease past expiry (DB clock), then re-claim.
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      const reclaimed = await outbox.claim(A); // reclaims → pending (with backoff) → newest backing off → null
      assert.equal(reclaimed, null, "reclaimed row backs off, so no immediate re-lease");
      const row = await conn.query("SELECT status, lease_token FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.equal(row.rows[0].status, "pending", "the expired lease was reclaimed to pending");
      assert.equal(row.rows[0].lease_token, null, "the stale token was cleared");
      // Once its backoff passes, it is claimable again.
      await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 1", [A]);
      const release = await outbox.claim(A);
      assert.ok(release && release.token && release.token !== lease.token, "a fresh lease after the backoff passes");
    });

    await t.test("STALE TOKEN: release / fail / prepareAck with a wrong or expired token change NOTHING", async () => {
      const A = "LEASE-stale";
      await seedFold(A, 1, 0);
      const lease = await outbox.claim(A);
      assert.equal(await outbox.release(A, "wrong-token"), false, "release ignores a wrong token");
      assert.equal(await outbox.fail(A, "wrong-token"), null, "fail ignores a wrong token");
      assert.equal(await outbox.prepareAck(A, "wrong-token"), null, "prepareAck ignores a wrong token");
      // The real lease is still live + unchanged.
      const still = await outbox.prepareAck(A, lease.token);
      assert.equal(still.anchorEpoch, 1, "the genuine lease was untouched by the stale-token calls");
    });

    await t.test("SATURATION: repeated failures grow attempts + cap backoff, and NEVER mark the obligation done", async () => {
      const A = "LEASE-saturate";
      await seedFold(A, 1, 0);
      let lastBackoff = 0;
      for (let i = 0; i < 8; i += 1) {
        const lease = await outbox.claim(A);
        assert.ok(lease, "still claimable at iteration " + i + " (never abandoned)");
        const f = await outbox.fail(A, lease.token);
        assert.ok(f.attempts >= i + 1, "attempts grow");
        assert.ok(f.backoffMs <= 60_000, "backoff is bounded");
        lastBackoff = f.backoffMs;
        // Clear the backoff so the next iteration can re-claim immediately.
        await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 1", [A]);
      }
      assert.equal(lastBackoff, 60_000, "backoff saturated at the cap");
      const row = await conn.query("SELECT status, blocked_at FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.equal(row.rows[0].status, "pending", "still OUTSTANDING after repeated failures — never 'done'");
    });

    await t.test("P1 REGRESSION: fail on an ADVANCED head backs off M (the head), not the anchor N", async () => {
      const A = "LEASE-adv-fail";
      await seedFold(A, 1, 0);
      const lease = await outbox.claim(A);           // anchor N = 1
      await seedFold(A, 2, 1);                        // head advances to M = 2 (pending)
      const f = await outbox.fail(A, lease.token);
      assert.equal(f.anchorEpoch, 1);
      assert.equal(f.headEpoch, 2, "the failure penalty targets the advanced head");
      // The head M=2 is now backing off, so a fresh claim leases NOTHING (M is not immediately
      // re-eligible) — the bug where the advanced head bypassed backoff is closed.
      assert.equal(await outbox.claim(A), null, "advanced head is throttled after failure");
      // Once M's backoff passes, it is claimable again at the head.
      await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 2", [A]);
      const again = await outbox.claim(A);
      assert.equal(again.headEpoch, 2);
    });

    await t.test("P1 REGRESSION: expired-lease reclaim also backs off the ADVANCED head, not the anchor", async () => {
      const A = "LEASE-adv-expire";
      await seedFold(A, 1, 0);
      const lease = await outbox.claim(A);           // anchor 1
      await seedFold(A, 2, 1);                        // head 2
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      assert.equal(await outbox.claim(A), null, "reclaim backs off head 2 ⇒ nothing immediately claimable");
      const head2 = await conn.query("SELECT status, (next_attempt_at > now()) AS backing_off FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 2", [A]);
      assert.equal(head2.rows[0].status, "pending");
      assert.equal(head2.rows[0].backing_off, true, "the ADVANCED head carries the backoff");
      void lease;
    });

    await t.test("P3 EXPIRED/REPLACED tokens: release/fail/prepareAck reject a once-valid token whose lease expired", async () => {
      const A = "LEASE-expired-tok";
      await seedFold(A, 1, 0);
      const lease = await outbox.claim(A);
      // Force the lease past expiry: the token WAS valid but the lease is gone.
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      assert.equal(await outbox.release(A, lease.token), false, "release rejects an expired token");
      assert.equal(await outbox.fail(A, lease.token), null, "fail rejects an expired token");
      assert.equal(await outbox.prepareAck(A, lease.token), null, "prepareAck rejects an expired token");
    });

    await t.test("P2 attempts are DB-bounded: a direct over-limit attempts write is rejected", async () => {
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, attempts) VALUES ('OVF', 1, 2000000)"),
        /violates check constraint/,
        "attempts above the bound are rejected (no int overflow strands the obligation)",
      );
    });

    await t.test("blocked_at is stamped once attempts cross the operator threshold (still outstanding)", async () => {
      const A = "LEASE-blocked";
      await seedFold(A, 1, 0);
      // Drive attempts to the threshold via repeated claim→fail (clearing backoff each round).
      for (let i = 0; i < 20; i += 1) {
        const lease = await outbox.claim(A);
        assert.ok(lease, "always still claimable (never abandoned)");
        await outbox.fail(A, lease.token);
        await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 1", [A]);
      }
      const row = await conn.query("SELECT status, attempts, blocked_at, last_error FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.ok(Number(row.rows[0].attempts) >= 20, "attempts reached the threshold");
      assert.ok(row.rows[0].blocked_at != null, "blocked_at stamped for operator visibility");
      assert.equal(row.rows[0].last_error, "PUBLISH_FAILED");
      assert.equal(row.rows[0].status, "pending", "blocked obligation stays OUTSTANDING (never 'done')");
    });
  },
);
