import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountRateBudget } from "../src/storage/pg/PgAccountRateBudget.js";
import { PgPropagationOutbox } from "../src/storage/pg/PgPropagationOutbox.js";

// F3 (audit leaf-3c) — the CLUSTER-WIDE per-account request budget.
//
// The per-node limiter bounds one node. Behind a non-sticky load balancer a device spreads its
// requests over every node and multiplies its ceiling by the node count. The durable resource is
// already safe (the one-leased unique index means no volume produces a second lease), so what this
// bounds is work amplification. The defining property is that N "nodes" sharing one database see
// ONE budget.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

test(
  "PgAccountRateBudget against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_account_rate_budget";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();

    const budget = new PgAccountRateBudget({ connection: conn });
    const WINDOW = 60_000;
    const BUCKET = "outbox_lease";

    await t.test("counts up to the ceiling, then refuses within the same window", async () => {
      const account = "acct-basic";
      const now = 1_000_000;
      for (let i = 1; i <= 3; i += 1) {
        const v = await budget.consume({ accountIdentityPublicKeyB64: account, bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 3, nowMs: now });
        assert.equal(v.allowed, true, "request " + i + " is within the ceiling");
        assert.equal(v.count, i, "the count is the POST-increment value");
      }
      const over = await budget.consume({ accountIdentityPublicKeyB64: account, bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 3, nowMs: now });
      assert.equal(over.allowed, false);
      assert.equal(over.count, 4);
      assert.ok(over.retryAfterMs > 0 && over.retryAfterMs <= WINDOW, "tells the caller when the window rolls");
    });

    await t.test("SEPARATE nodes sharing one database share ONE budget — this is the point of F3", async () => {
      // Two independent instances stand in for two nodes behind the load balancer. With per-node
      // limiters only, each would grant a full ceiling; here the second sees the first's spend.
      const nodeA = new PgAccountRateBudget({ connection: conn });
      const nodeB = new PgAccountRateBudget({ connection: conn });
      const account = "acct-two-nodes";
      const now = 2_000_000;
      const args = { accountIdentityPublicKeyB64: account, bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 2, nowMs: now };

      assert.equal((await nodeA.consume(args)).allowed, true);
      assert.equal((await nodeB.consume(args)).allowed, true, "node B sees node A's spend and is still under");
      const third = await nodeB.consume(args);
      assert.equal(third.allowed, false, "the ceiling did NOT scale with the node count");
      assert.equal(third.count, 3);
    });

    await t.test("a new window starts fresh", async () => {
      const account = "acct-window-roll";
      const args = { accountIdentityPublicKeyB64: account, bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 1 };
      const first = await budget.consume({ ...args, nowMs: 3_000_000 });
      assert.equal(first.allowed, true);
      assert.equal((await budget.consume({ ...args, nowMs: 3_000_500 })).allowed, false, "same window, over");
      const nextWindow = await budget.consume({ ...args, nowMs: 3_000_000 + WINDOW });
      assert.equal(nextWindow.allowed, true, "the next window is a clean slate");
      assert.equal(nextWindow.count, 1);
    });

    await t.test("buckets and accounts do not rob each other", async () => {
      const now = 4_000_000;
      const base = { windowMs: WINDOW, maxPerWindow: 1, nowMs: now };
      assert.equal((await budget.consume({ ...base, accountIdentityPublicKeyB64: "acct-x", bucket: "outbox_lease" })).allowed, true);
      // Same account, DIFFERENT bucket: unaffected.
      assert.equal((await budget.consume({ ...base, accountIdentityPublicKeyB64: "acct-x", bucket: "something_else" })).allowed, true);
      // Different account, same bucket: unaffected.
      assert.equal((await budget.consume({ ...base, accountIdentityPublicKeyB64: "acct-y", bucket: "outbox_lease" })).allowed, true);
      // ...and the first pair is still individually exhausted.
      assert.equal((await budget.consume({ ...base, accountIdentityPublicKeyB64: "acct-x", bucket: "outbox_lease" })).allowed, false);
    });

    await t.test("concurrent consumption from many callers cannot overshoot via a stale read", async () => {
      // The upsert returns the POST-increment count, so two racing callers cannot both read the
      // same value and both conclude they are under the limit.
      const account = "acct-concurrent";
      const now = 5_000_000;
      const args = { accountIdentityPublicKeyB64: account, bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 5, nowMs: now };
      const verdicts = await Promise.all(Array.from({ length: 20 }, () => budget.consume(args)));
      const allowed = verdicts.filter((v) => v.allowed).length;
      assert.equal(allowed, 5, "exactly the ceiling was granted, no more");
      const counts = verdicts.map((v) => v.count).sort((a, b) => a - b);
      assert.deepEqual(counts, Array.from({ length: 20 }, (_, i) => i + 1), "every request got a distinct count");
    });

    await t.test("sweep removes only CLOSED windows", async () => {
      const account = "acct-sweep";
      const args = { accountIdentityPublicKeyB64: account, bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 10 };
      await budget.consume({ ...args, nowMs: 6_000_000 });
      await budget.consume({ ...args, nowMs: 6_000_000 + WINDOW * 5 });

      const removed = await budget.sweep({ olderThanMs: 6_000_000 + WINDOW });
      assert.ok(removed >= 1, "the old window was collected");
      const left = await conn.query(
        "SELECT window_start_ms FROM account_rate_budget WHERE account_identity = $1",
        [account],
      );
      assert.equal(left.rowCount, 1, "the recent window survived");
      assert.equal(Number(left.rows[0].window_start_ms), 6_000_000 + WINDOW * 5);
    });

    await t.test("the budget is DERIVED from the outbox's own connection (no split-brain wiring)", async () => {
      // Same reasoning as the runtime deriving propagationOutbox from the serializer: an embedder
      // must not be able to point the budget at a different database than the one it bounds.
      const outbox = new PgPropagationOutbox({ connection: conn });
      assert.ok(outbox.accountRateBudget instanceof PgAccountRateBudget);
      const v = await outbox.accountRateBudget.consume({
        accountIdentityPublicKeyB64: "acct-derived",
        bucket: BUCKET,
        windowMs: WINDOW,
        maxPerWindow: 1,
        nowMs: 7_000_000,
      });
      assert.equal(v.allowed, true);
      // It writes to the same table the standalone instance reads.
      const seen = await budget.consume({
        accountIdentityPublicKeyB64: "acct-derived",
        bucket: BUCKET,
        windowMs: WINDOW,
        maxPerWindow: 1,
        nowMs: 7_000_000,
      });
      assert.equal(seen.allowed, false, "one shared counter, not two");
    });

    await t.test("invalid arguments fail loudly rather than silently allowing", async () => {
      const base = { accountIdentityPublicKeyB64: "acct-args", bucket: BUCKET, windowMs: WINDOW, maxPerWindow: 1, nowMs: 8_000_000 };
      await assert.rejects(() => budget.consume({ ...base, accountIdentityPublicKeyB64: "  " }), /requires accountIdentityPublicKeyB64/);
      await assert.rejects(() => budget.consume({ ...base, bucket: "" }), /requires bucket/);
      await assert.rejects(() => budget.consume({ ...base, windowMs: 0 }), /positive integer windowMs/);
      await assert.rejects(() => budget.consume({ ...base, maxPerWindow: 0 }), /positive integer maxPerWindow/);
      await assert.rejects(() => budget.consume({ ...base, nowMs: Number.NaN }), /finite nowMs/);
      assert.throws(() => new PgAccountRateBudget({}), /requires connection/);
    });
  },
);
