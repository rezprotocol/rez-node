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

    // The lease is OWNER-bound (leaf-3 req 4): every op carries the caller's device id. A default
    // owner keeps the existing lease tests concise; owner-mismatch cases pass an explicit other id.
    const OWN = D("tester");
    // A canonical owner id for the raw-SQL lease fixtures below (the DB CHECK, leaf-3a F1, now
    // requires rez:dev:<64-hex>, so inline inserts can no longer use a short stand-in like rez:dev:o).
    const OWN_SQL = D("sqlowner");
    const doClaim = (a, o = OWN) => outbox.claim(a, o);
    const doFail = (a, tkn, o = OWN) => outbox.fail(a, tkn, o);
    const doPrepare = (a, tkn, o = OWN) => outbox.preparePublication(a, tkn, o);
    const doRelease = (a, tkn, o = OWN) => outbox.release(a, tkn, o);

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
        "last_error", "lease_expires_at", "lease_owner", "lease_token", "next_attempt_at",
        "prepared_epoch", "status", "updated_at",
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
      await conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at) VALUES ('L', 5, 'authority_state', 'leased', 'tokA', " + "'" + OWN_SQL + "'" + ", " + future + ")");
      // ...but a SECOND leased row for the same (account, kind) — even at a different epoch — is
      // refused by the partial unique index (never lease N and N+1 concurrently).
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at) VALUES ('L', 6, 'authority_state', 'leased', 'tokB', " + "'" + OWN_SQL + "'" + ", " + future + ")"),
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

    await t.test("RACE: concurrent claims by DIFFERENT owners — exactly one gets the lease, the other null", async () => {
      const A = "LEASE-conc";
      await seedFold(A, 1, 0);
      // Distinct owner devices: the lease is owner-bound, so at most one of two DIFFERENT devices
      // may hold it (a same-owner re-claim is instead idempotent — see the next test).
      const [a, b] = await Promise.all([doClaim(A, D("alice")), doClaim(A, D("bob"))]);
      const won = [a, b].filter((r) => r && typeof r.token === "string");
      assert.equal(won.length, 1, "exactly one of two distinct devices leased the head");
      assert.equal(won[0].headEpoch, 1);
    });

    await t.test("leaf-3b F3: a same-owner re-claim of a LIVE lease idempotently returns the SAME token (lost-response recovery)", async () => {
      const A = "LEASE-idempotent-claim";
      await seedFold(A, 1, 0);
      const owner = D("recoverer");
      const first = await outbox.claim(A, owner);
      assert.ok(first && first.token, "owner leased the head");
      // The response was 'lost' — the SAME owner re-claims and must get the EXISTING lease back,
      // not null, so it recovers immediately rather than waiting out the TTL.
      const again = await outbox.claim(A, owner);
      assert.ok(again, "same-owner re-claim returns the live lease (not null)");
      assert.equal(again.token, first.token, "the SAME token is returned (idempotent recovery)");
      assert.equal(again.anchorEpoch, first.anchorEpoch);
      // A DIFFERENT device still sees the account as busy (the lease is not transferable).
      assert.equal(await outbox.claim(A, D("intruder")), null, "a different device gets null (busy), never the token");
      // Exactly one leased row persists (no duplicate lease from the re-claim).
      const leasedRows = await conn.query("SELECT count(*)::int c FROM account_propagation_outbox WHERE account_identity = $1 AND status = 'leased'", [A]);
      assert.equal(leasedRows.rows[0].c, 1, "still exactly one leased row after the idempotent re-claim");
    });

    await t.test("RACE: N leased, then N+1 commits — preparePublication reports the ADVANCED head, anchor stays N", async () => {
      const A = "LEASE-advance";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      assert.equal(lease.headEpoch, 1);
      // A newer epoch commits UNDER the live lease (stays pending; cannot be leased — one-lease index).
      await seedFold(A, 2, 1);
      const prep = await doPrepare(A, lease.token);
      assert.equal(prep.anchorEpoch, 1, "the anchor is where the lease was taken");
      assert.equal(prep.headEpoch, 2, "the publishable head advanced under the lease");
    });

    await t.test("RACE: the newest head is backing off — claim leases NOTHING (never an older epoch)", async () => {
      const A = "LEASE-backoff";
      await seedFold(A, 1, 0); // eligible
      await seedFold(A, 2, 1); // will be forced into backoff
      await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() + interval '1 hour' WHERE account_identity = $1 AND epoch = 2", [A]);
      assert.equal(await doClaim(A), null, "newest is backing off ⇒ no lease");
      const pend = await outbox.listPending(A);
      assert.ok(pend.every((r) => r.status === "pending"), "no older epoch was leased");
    });

    await t.test("RECLAIM: an expired lease is returned to pending on the next claim", async () => {
      const A = "LEASE-expire";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      assert.ok(lease.token);
      // Force the lease past expiry (DB clock), then re-claim.
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      const reclaimed = await doClaim(A); // reclaims → pending (with backoff) → newest backing off → null
      assert.equal(reclaimed, null, "reclaimed row backs off, so no immediate re-lease");
      const row = await conn.query("SELECT status, lease_token FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.equal(row.rows[0].status, "pending", "the expired lease was reclaimed to pending");
      assert.equal(row.rows[0].lease_token, null, "the stale token was cleared");
      // Once its backoff passes, it is claimable again.
      await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 1", [A]);
      const release = await doClaim(A);
      assert.ok(release && release.token && release.token !== lease.token, "a fresh lease after the backoff passes");
    });

    await t.test("STALE TOKEN: release / fail / preparePublication with a wrong or expired token change NOTHING", async () => {
      const A = "LEASE-stale";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      assert.equal(await doRelease(A, "wrong-token"), false, "release ignores a wrong token");
      assert.equal(await doFail(A, "wrong-token"), null, "fail ignores a wrong token");
      assert.equal(await doPrepare(A, "wrong-token"), null, "preparePublication ignores a wrong token");
      // The real lease is still live + unchanged.
      const still = await doPrepare(A, lease.token);
      assert.equal(still.anchorEpoch, 1, "the genuine lease was untouched by the stale-token calls");
    });

    await t.test("SATURATION: repeated failures grow attempts + cap backoff, and NEVER mark the obligation done", async () => {
      const A = "LEASE-saturate";
      await seedFold(A, 1, 0);
      let lastBackoff = 0;
      for (let i = 0; i < 8; i += 1) {
        const lease = await doClaim(A);
        assert.ok(lease, "still claimable at iteration " + i + " (never abandoned)");
        const f = await doFail(A, lease.token);
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

    await t.test("leaf-2.1 P2: fail penalizes the ATTEMPTED (prepared) epoch M, NOT a later-committed K", async () => {
      const A = "LEASE-interleave";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);           // anchor N = 1
      await seedFold(A, 2, 1);                        // head advances to 2
      const prep = await doPrepare(A, lease.token); // attempted M = 2 (bound to the lease)
      assert.equal(prep.headEpoch, 2);
      await seedFold(A, 3, 2);                        // K = 3 commits AFTER preparation — never attempted
      const f = await doFail(A, lease.token);
      assert.equal(f.anchorEpoch, 1);
      assert.equal(f.attemptedEpoch, 2, "backoff targets the ATTEMPTED M=2, not the un-attempted K=3");
      const rows = await conn.query("SELECT epoch, (next_attempt_at > now()) AS backing_off FROM account_propagation_outbox WHERE account_identity = $1 AND epoch IN (2, 3)", [A]);
      const by = (e) => rows.rows.find((r) => Number(r.epoch) === e).backing_off;
      assert.equal(by(2), true, "attempted M=2 backs off");
      assert.equal(by(3), false, "un-attempted K=3 stays fresh (new authority is not throttled)");
      // So the next claim leases K=3 (newest + eligible), not the throttled M=2.
      assert.equal((await doClaim(A)).headEpoch, 3);
    });

    await t.test("leaf-2.1: expired-lease reclaim also backs off the ATTEMPTED (prepared) epoch", async () => {
      const A = "LEASE-adv-expire";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);           // anchor 1
      await seedFold(A, 2, 1);                        // head 2
      await doPrepare(A, lease.token); // attempted = 2
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      assert.equal(await doClaim(A), null, "reclaim backs off the attempted head 2 ⇒ nothing claimable");
      const head2 = await conn.query("SELECT status, (next_attempt_at > now()) AS backing_off FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 2", [A]);
      assert.equal(head2.rows[0].status, "pending");
      assert.equal(head2.rows[0].backing_off, true, "the ATTEMPTED head carries the backoff");
    });

    await t.test("REPLACED token: after reclaim installs a new lease, the OLD token is rejected by all ops", async () => {
      const A = "LEASE-replaced";
      await seedFold(A, 1, 0);
      const first = await doClaim(A);
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      await doClaim(A); // reclaim → pending (backed off) → null
      await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 1", [A]);
      const second = await doClaim(A);
      assert.ok(second && second.token !== first.token, "a NEW token replaced the old lease");
      assert.equal(await doRelease(A, first.token), false, "old token cannot release");
      assert.equal(await doFail(A, first.token), null, "old token cannot fail");
      assert.equal(await doPrepare(A, first.token), null, "old token cannot prepare");
      assert.ok(await doPrepare(A, second.token), "the current token still works");
    });

    await t.test("fail() clamps attempts at the ceiling (no overflow at MAX_PERSISTED_ATTEMPTS)", async () => {
      const A = "LEASE-ceiling";
      await seedFold(A, 1, 0);
      await conn.query("UPDATE account_propagation_outbox SET attempts = 1000000 WHERE account_identity = $1 AND epoch = 1", [A]);
      const lease = await doClaim(A);
      const f = await doFail(A, lease.token);
      assert.equal(f.attempts, 1000000, "attempts saturates at the cap (LEAST clamp) — never overflows");
      assert.equal(f.backoffMs, 60_000);
      assert.equal(f.blocked, true);
    });

    await t.test("re-review P1: repeated preparation FREEZES the first attempted epoch (idempotent, not re-pointed)", async () => {
      const A = "LEASE-freeze";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      await seedFold(A, 2, 1);
      const p1 = await doPrepare(A, lease.token); // freezes attempted = 2
      assert.equal(p1.headEpoch, 2);
      await seedFold(A, 3, 2);                                     // K = 3 commits after preparation
      const p2 = await doPrepare(A, lease.token); // duplicate/retry
      assert.equal(p2.headEpoch, 2, "repeated preparation returns the FROZEN epoch, not the newer head K=3");
      const f = await doFail(A, lease.token);
      assert.equal(f.attemptedEpoch, 2, "failure penalizes the frozen in-flight attempt, not K=3");
    });

    // leaf 3c — the VERIFIED completion (the crypto verification is the handler's; here M arrives
    // already-verified). completePublication is the ONLY writer of status='done'.
    const doComplete = (a, tkn, m, o = OWN) => outbox.completePublication(a, tkn, o, m);
    const statusOf = async (A, epoch) => {
      const r = await conn.query("SELECT status FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = $2", [A, epoch]);
      return r.rowCount === 1 ? r.rows[0].status : null;
    };

    await t.test("leaf-3c: complete(M) marks EVERY obligation <= M done (cumulative), releasing the lease", async () => {
      const A = "COMPLETE-cumulative";
      await seedFold(A, 1, 0);
      await seedFold(A, 2, 1);
      await seedFold(A, 3, 2); // three pending: 1, 2, 3
      const lease = await doClaim(A);          // leases the NEWEST head (epoch 3) as anchor
      const prep = await doPrepare(A, lease.token);
      assert.equal(prep.headEpoch, 3, "prepared/frozen epoch M = current head 3");
      const res = await doComplete(A, lease.token, 3);
      assert.deepEqual(res, { completed: true, doneThroughEpoch: 3 });
      assert.equal(await statusOf(A, 1), "done", "older obligation 1 superseded → done");
      assert.equal(await statusOf(A, 2), "done");
      assert.equal(await statusOf(A, 3), "done", "the anchor is released as done, not left leased");
      assert.deepEqual(await outbox.listPending(A), [], "nothing pending after a cumulative ack");
    });

    await t.test("leaf-3c: epochs ABOVE M (committed after prepare) stay pending", async () => {
      const A = "COMPLETE-above-M";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);              // anchor = epoch 1
      const prep = await doPrepare(A, lease.token); // freezes M = 1
      assert.equal(prep.headEpoch, 1);
      await seedFold(A, 2, 1);                       // epoch 2 commits AFTER prepare (2 > M)
      const res = await doComplete(A, lease.token, 1);
      assert.deepEqual(res, { completed: true, doneThroughEpoch: 1 });
      assert.equal(await statusOf(A, 1), "done");
      assert.deepEqual((await outbox.listPending(A)).map((r) => r.epoch), [2], "the newer epoch 2 stays pending for the next lease");
    });

    await t.test("leaf-3c: complete with M != the frozen prepared_epoch changes NOTHING", async () => {
      const A = "COMPLETE-epoch-mismatch";
      await seedFold(A, 1, 0);
      await seedFold(A, 2, 1);
      const lease = await doClaim(A);              // anchor = 2
      await doPrepare(A, lease.token);              // freezes M = 2
      const res = await doComplete(A, lease.token, 3); // ack the wrong epoch
      assert.deepEqual(res, { completed: false, expectedEpoch: 2 });
      assert.equal(await statusOf(A, 2), "leased", "the anchor is untouched — no false done");
      assert.deepEqual((await outbox.listPending(A)).map((r) => r.epoch), [1], "the pending set is unchanged");
    });

    await t.test("leaf-3c: complete BEFORE prepare (no frozen epoch) changes NOTHING", async () => {
      const A = "COMPLETE-not-prepared";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);              // anchor = 1, never prepared
      const res = await doComplete(A, lease.token, 1);
      assert.deepEqual(res, { completed: false, expectedEpoch: null }, "no frozen epoch ⇒ expectedEpoch null, nothing done");
      assert.equal(await statusOf(A, 1), "leased");
    });

    await t.test("leaf-3c: complete with a WRONG or non-owner token completes nothing (null)", async () => {
      const A = "COMPLETE-wrong-token";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      await doPrepare(A, lease.token); // M = 1
      assert.equal(await doComplete(A, "wrong-token", 1), null, "a wrong token holds no live lease");
      assert.equal(await outbox.completePublication(A, lease.token, D("intruder"), 1), null, "a different owner device holds no live lease");
      assert.equal(await statusOf(A, 1), "leased", "still leased — no completion happened");
    });

    await t.test("leaf-3c: a replayed complete after success is a benign no-op (lease already gone)", async () => {
      const A = "COMPLETE-replay";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      await doPrepare(A, lease.token);
      assert.deepEqual(await doComplete(A, lease.token, 1), { completed: true, doneThroughEpoch: 1 });
      assert.equal(await doComplete(A, lease.token, 1), null, "the lease is gone ⇒ a replay completes nothing");
      assert.equal(await statusOf(A, 1), "done", "still done, not re-toggled");
    });

    await t.test("leaf-3c: completePublication rejects a non-positive verifiedEpoch (caller contract)", async () => {
      const A = "COMPLETE-bad-epoch";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      await assert.rejects(() => outbox.completePublication(A, lease.token, OWN, 0), /positive integer verifiedEpoch/);
    });

    await t.test("re-review P2: the DB enforces the prepared binding (leased-only, >= anchor, must exist)", async () => {
      const A = "PREP-constraints";
      await seedFold(A, "x", 0); // first fold ⇒ a real obligation at epoch 1 for this account
      const future = "now() + interval '1 minute'";
      // (a) prepared_epoch on a NON-leased (pending) row is rejected.
      await assert.rejects(
        () => conn.query("UPDATE account_propagation_outbox SET prepared_epoch = 1 WHERE account_identity = $1 AND epoch = 1", [A]),
        /violates check constraint/,
        "prepared_epoch on a pending row rejected",
      );
      // (b) prepared_epoch BELOW the leased row's own epoch is rejected (leased row at epoch 5,
      //     prepared_epoch 1 references the real epoch-1 obligation but 1 < 5).
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at, prepared_epoch) VALUES ($1, 5, 'authority_state', 'leased', 'tk', " + "'" + OWN_SQL + "'" + ", " + future + ", 1)", [A]),
        /violates check constraint/,
        "prepared_epoch below the anchor rejected",
      );
      // (c) prepared_epoch that names NO obligation for this account is rejected (self-FK).
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at, prepared_epoch) VALUES ($1, 6, 'authority_state', 'leased', 'tk2', " + "'" + OWN_SQL + "'" + ", " + future + ", 99)", [A]),
        /violates foreign key constraint/,
        "prepared_epoch referencing a nonexistent epoch rejected",
      );
    });

    await t.test("lease-clock: an op that blocked on the anchor lock PAST wall-clock expiry is rejected", async () => {
      const A = "LEASE-clock";
      await seedFold(A, "x", 0);
      const lease = await doClaim(A);
      // A near wall-clock deadline.
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = clock_timestamp() + interval '400 milliseconds' WHERE account_identity = $1 AND status = 'leased'", [A]);

      // Hold the anchor lock in a SEPARATE session so fail()/preparePublication block on it.
      let release;
      let signalLocked;
      const held = new Promise((res) => { release = res; });
      const lockedP = new Promise((res) => { signalLocked = res; });
      const holder = conn.withClient(async (c2) => {
        await c2.query("BEGIN");
        await c2.query("SELECT epoch FROM account_propagation_outbox WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased' FOR UPDATE", [A]);
        signalLocked(); // the lock is now held
        await held; // hold it until told
        await c2.query("COMMIT");
      });
      await lockedP; // guarantee the holder has the lock before the racers start

      // These BEGIN while the lease is still live (their tx now() < deadline), then BLOCK on the lock.
      const failP = doFail(A, lease.token);
      const prepP = doPrepare(A, lease.token);

      // Wait PAST the wall-clock deadline, then release so they acquire the lock AFTER expiry.
      await new Promise((r) => setTimeout(r, 600));
      release();
      await holder;

      // Under the old now() (frozen at BEGIN, pre-deadline) — and even a clock_timestamp() inside the
      // FOR UPDATE target list (not re-evaluated post-lock) — these would have passed; the separate
      // post-lock clock_timestamp() statement correctly sees the expiry and rejects them.
      assert.equal(await failP, null, "fail after wall-clock expiry is rejected");
      assert.equal(await prepP, null, "preparePublication after wall-clock expiry is rejected");
    });

    await t.test("re-review: claim reclaims an expired anchor then leases the newer eligible head (no one-lease conflict)", async () => {
      const A = "LEASE-reclaim-lease";
      await seedFold(A, "1", 0);
      const lease = await doClaim(A);           // leased epoch 1 (prepared_epoch NULL)
      await seedFold(A, "2", 1);                      // epoch 2 pending (newer, fresh)
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      // One claim: lock+classify the expired anchor → reclaim it (backoff attempted=1), then lease
      // the newest ELIGIBLE head (2). Under the old expired-then-live race this could leave the
      // anchor 'leased' and the lease insert would trip the one-lease unique index.
      const next = await doClaim(A);
      assert.ok(next, "claim succeeded — no one-lease unique-index conflict");
      assert.equal(next.headEpoch, 2, "leased the newer eligible head after reclaiming the expired anchor");
      const r1 = await conn.query("SELECT status FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.equal(r1.rows[0].status, "pending", "the expired anchor was reclaimed to pending");
      void lease;
    });

    await t.test("leaf-3 req 4: a lease token is NOT transferable — a different owner device is rejected by every op", async () => {
      const A = "LEASE-owner";
      await seedFold(A, "x", 0);
      const lease = await outbox.claim(A, D("alice"));
      assert.ok(lease.token);
      // A DIFFERENT device presenting the same (leaked) token authorizes nothing.
      assert.equal(await outbox.release(A, lease.token, D("mallory")), false, "release rejects a foreign owner");
      assert.equal(await outbox.fail(A, lease.token, D("mallory")), null, "fail rejects a foreign owner");
      assert.equal(await outbox.preparePublication(A, lease.token, D("mallory")), null, "prepare rejects a foreign owner");
      // The genuine owner still holds a live, untouched lease.
      const still = await outbox.preparePublication(A, lease.token, D("alice"));
      assert.ok(still && still.anchorEpoch === 1, "the owner's lease is intact");
    });

    await t.test("leaf-3 req 5: revoking the lease-holder device ATOMICALLY releases its lease in the revoke fold", async () => {
      const A = "LEASE-revoke-holder";
      // Enroll a device D via device.add (epoch 1 obligation), then claim the lease AS D.
      const dev = D("holder");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "add", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-holder", certId: cap("holder") } });
      const lease = await outbox.claim(A, dev);
      assert.ok(lease.token, "device D holds the lease");
      // Revoking D commits its lease release in the SAME fold transaction — no 30s TTL wait.
      const rev = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "rev", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: dev } });
      assert.equal(rev.revision, 2);
      const row = await conn.query("SELECT status, lease_token, lease_owner FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.equal(row.rows[0].status, "pending", "the revoked holder's lease was released to pending");
      assert.equal(row.rows[0].lease_token, null, "the lease token was invalidated");
      assert.equal(row.rows[0].lease_owner, null, "the lease owner was cleared");
      // The old token no longer authorizes anything (the lease is gone).
      assert.equal(await outbox.preparePublication(A, lease.token, dev), null, "the revoked holder's token is dead");
    });

    await t.test("P3 EXPIRED/REPLACED tokens: release/fail/preparePublication reject a once-valid token whose lease expired", async () => {
      const A = "LEASE-expired-tok";
      await seedFold(A, 1, 0);
      const lease = await doClaim(A);
      // Force the lease past expiry: the token WAS valid but the lease is gone.
      await conn.query("UPDATE account_propagation_outbox SET lease_expires_at = now() - interval '1 second' WHERE account_identity = $1 AND status = 'leased'", [A]);
      assert.equal(await doRelease(A, lease.token), false, "release rejects an expired token");
      assert.equal(await doFail(A, lease.token), null, "fail rejects an expired token");
      assert.equal(await doPrepare(A, lease.token), null, "preparePublication rejects an expired token");
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
        const lease = await doClaim(A);
        assert.ok(lease, "always still claimable (never abandoned)");
        await doFail(A, lease.token);
        await conn.query("UPDATE account_propagation_outbox SET next_attempt_at = now() - interval '1 second' WHERE account_identity = $1 AND epoch = 1", [A]);
      }
      const row = await conn.query("SELECT status, attempts, blocked_at, last_error FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.ok(Number(row.rows[0].attempts) >= 20, "attempts reached the threshold");
      assert.ok(row.rows[0].blocked_at != null, "blocked_at stamped for operator visibility");
      assert.equal(row.rows[0].last_error, "PUBLISH_FAILED");
      assert.equal(row.rows[0].status, "pending", "blocked obligation stays OUTSTANDING (never 'done')");
      // blocked_at is stamped ONCE: a further failure must not move it.
      const stampedAt = row.rows[0].blocked_at;
      const l2 = await doClaim(A);
      await doFail(A, l2.token);
      const row2 = await conn.query("SELECT blocked_at FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.deepEqual(row2.rows[0].blocked_at, stampedAt, "blocked_at is stamped only once (unchanged by later failures)");
    });

    await t.test("leaf-3a F1: a NON-canonical owner is rejected at the JS boundary AND by the DB CHECK", async () => {
      const A = "LEASE-owner-shape";
      await seedFold(A, "x", 0);
      // JS boundary: the owner-ASSERTING ops (claim + release / fail / prepare) reject any owner that
      // is not rez:dev:<64-hex>. (releaseOwnedInTx is revoke-side cleanup and is intentionally lenient.)
      for (const bad of ["rez:dev:alice", "rez:dev:" + "a".repeat(63), "not-a-device", "", "  "]) {
        await assert.rejects(() => outbox.claim(A, bad), /canonical rez:dev:<64-hex>/, "claim rejects owner " + JSON.stringify(bad));
        await assert.rejects(() => outbox.fail(A, "tok", bad), /canonical rez:dev:<64-hex>/, "fail rejects owner " + JSON.stringify(bad));
        await assert.rejects(() => outbox.preparePublication(A, "tok", bad), /canonical rez:dev:<64-hex>/, "prepare rejects owner " + JSON.stringify(bad));
        await assert.rejects(() => outbox.release(A, "tok", bad), /canonical rez:dev:<64-hex>/, "release rejects owner " + JSON.stringify(bad));
      }
      // DB backstop: a raw INSERT of a non-canonical lease_owner is rejected by the shape CHECK.
      const future = "now() + interval '1 minute'";
      await assert.rejects(
        () => conn.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at) VALUES ('OWNSHAPE', 1, 'authority_state', 'leased', 'tk', 'rez:dev:short', " + future + ")"),
        /violates check constraint/,
        "the DB rejects a non-canonical lease_owner even on a raw write",
      );
    });

    await t.test("leaf-3a F2: the 23→24 runner path swaps to the canonical CHECK, reclaiming ONLY non-canonical leases", async () => {
      // A FORWARD migration (0024), not an in-place edit of 0023, is what reaches a database already
      // at version 23 (MigrationRunner applies only versions above its recorded max). Prove the actual
      // runner path on an ISOLATED schema: migrate fully, REWIND to a faithful "recorded version 23"
      // world (length CHECK, 0024 un-applied), plant one MALFORMED-owner lease + one VALID canonical
      // owner-bound lease, then run the runner again and assert it advances to 24 — reclaiming only the
      // malformed lease, PRESERVING the valid one, and installing the canonical shape CHECK.
      const SCHEMA2 = "test_pg_outbox_2324";
      const conn2 = await createIsolatedPgConnection(PG_URL, SCHEMA2);
      try {
        await new MigrationRunner({ connection: conn2 }).migrate();
        // Rewind to the v23 world: forget 0024 AND EVERYTHING AFTER IT, then restore the
        // length-only CHECK 0024 replaced. Deleting only version 24 is not a rewind — the runner
        // applies versions above its recorded MAX, so any later migration left recorded would keep
        // max > 24 and 0024 would never re-run. (Later migrations are re-applied here too; they are
        // idempotent by construction.)
        await conn2.query("DELETE FROM schema_migrations WHERE version >= 24");
        await conn2.query("ALTER TABLE account_propagation_outbox DROP CONSTRAINT account_propagation_outbox_lease_owner_shape");
        await conn2.query("ALTER TABLE account_propagation_outbox ADD CONSTRAINT account_propagation_outbox_lease_owner_len CHECK (lease_owner IS NULL OR (octet_length(lease_owner) BETWEEN 1 AND 128))");
        const future = "now() + interval '1 minute'";
        // A length-legal but NON-canonical owner (only possible under the v23 length CHECK)...
        await conn2.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at) VALUES ('MAL', 1, 'authority_state', 'leased', 'tokM', 'rez:dev:legacy-noncanonical', " + future + ")");
        // ...and a VALID canonical owner-bound lease on a DIFFERENT account (the one-lease index is per account+kind).
        const goodOwner = D("valid-holder");
        await conn2.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at) VALUES ('GOOD', 1, 'authority_state', 'leased', 'tokG', '" + goodOwner + "', " + future + ")");

        // Re-run the runner: it applies 0024 FORWARD (version 24 > the recorded 23) rather than
        // editing 0023 in place. Asserted as ">= 24", not "== 24": the subject here is that the
        // 23→24 step ran forward, and pinning the exact max version would make every future
        // migration fail this test for an unrelated reason.
        await new MigrationRunner({ connection: conn2 }).migrate();
        const v = await conn2.query("SELECT max(version)::int AS v FROM schema_migrations");
        assert.ok(v.rows[0].v >= 24, "the runner advanced the database past version 24 (forward migration, not an in-place edit)");
        const applied24 = await conn2.query("SELECT 1 FROM schema_migrations WHERE version = 24");
        assert.equal(applied24.rowCount, 1, "0024 itself was recorded as applied");

        // The malformed lease was reclaimed to pending; the VALID canonical lease is untouched.
        const mal = await conn2.query("SELECT status, lease_token, lease_owner FROM account_propagation_outbox WHERE account_identity = 'MAL' AND epoch = 1");
        assert.equal(mal.rows[0].status, "pending", "the non-canonical lease was reclaimed");
        assert.equal(mal.rows[0].lease_token, null, "its token was cleared");
        assert.equal(mal.rows[0].lease_owner, null, "its malformed owner was cleared");
        const good = await conn2.query("SELECT status, lease_token, lease_owner FROM account_propagation_outbox WHERE account_identity = 'GOOD' AND epoch = 1");
        assert.equal(good.rows[0].status, "leased", "the VALID canonical lease was PRESERVED (not released)");
        assert.equal(good.rows[0].lease_token, "tokG", "its token survived the upgrade");
        assert.equal(good.rows[0].lease_owner, goodOwner, "its canonical owner survived the upgrade");

        // 0024 installed the canonical shape CHECK — a raw non-canonical write is now rejected.
        await assert.rejects(
          () => conn2.query("INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status, lease_token, lease_owner, lease_expires_at) VALUES ('POST', 1, 'authority_state', 'leased', 'tk', 'rez:dev:short', " + future + ")"),
          /violates check constraint/,
          "0024 installed the canonical shape CHECK",
        );
      } finally {
        await conn2.close();
        await dropSchema(PG_URL, SCHEMA2);
      }
    });

    await t.test("leaf-3a F3: a fold that fails AFTER releaseOwnedInTx changed the lease rolls back the release too", async () => {
      const A = "LEASE-revoke-atomic-rollback";
      const dev = D("atomic-holder");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "add", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-ah", certId: cap("ah") } });
      const lease = await outbox.claim(A, dev);
      assert.ok(lease.token, "the holder device leased the head");
      // Inject an outbox whose releaseOwnedInTx performs the REAL in-tx lease release, then whose
      // enqueueInTx (called later in the SAME fold) throws — so the release is committed to the tx
      // but the tx then rolls back. Everything, including the lease change, must be undone.
      const real = new PgPropagationOutbox({ connection: conn });
      const faulting = new PgPropagationOutbox({ connection: conn });
      faulting.releaseOwnedInTx = (client, acct, owner) => real.releaseOwnedInTx(client, acct, owner);
      faulting.enqueueInTx = async () => { throw new Error("post-release enqueue boom"); };
      const sFault = new PgAccountMutationSerializer({ connection: conn, durableInbox, propagationOutbox: faulting });
      await assert.rejects(
        () => sFault.submitMutation({ accountIdentityPublicKeyB64: A, opId: "rev-atomic", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: dev } }),
        /post-release enqueue boom/,
      );
      // The whole revoke rolled back: the lease is STILL leased with its original token + owner...
      const leaseRow = await conn.query("SELECT status, lease_token, lease_owner FROM account_propagation_outbox WHERE account_identity = $1 AND epoch = 1", [A]);
      assert.equal(leaseRow.rows[0].status, "leased", "the lease release was rolled back (still leased)");
      assert.equal(leaseRow.rows[0].lease_token, lease.token, "the original lease token survived the rollback");
      assert.equal(leaseRow.rows[0].lease_owner, dev, "the original owner survived the rollback");
      // ...the device is STILL active, the epoch is unchanged, and NO revoke journal row was written.
      const devRow = await conn.query("SELECT status FROM account_device_registry WHERE account_identity = $1 AND device_id = $2", [A, dev]);
      assert.equal(devRow.rows[0].status, "active", "the device was not revoked");
      const authRow = await conn.query("SELECT epoch FROM account_authority WHERE account_identity = $1", [A]);
      assert.equal(Number(authRow.rows[0].epoch), 1, "the authority epoch did not advance");
      const jrnl = await conn.query("SELECT count(*)::int c FROM account_device_mutation WHERE account_identity = $1 AND op_id = 'rev-atomic'", [A]);
      assert.equal(jrnl.rows[0].c, 0, "no journal row for the rolled-back revoke");
      // And the genuine lease still works end-to-end.
      assert.ok(await outbox.preparePublication(A, lease.token, dev), "the surviving lease is still usable");
    });
  },
);
