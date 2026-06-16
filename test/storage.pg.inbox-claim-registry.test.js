import test from "node:test";
import assert from "node:assert/strict";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgInboxClaimRegistry } from "../src/storage/pg/PgInboxClaimRegistry.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";

test(
  "PgInboxClaimRegistry atomic claim against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_inbox_claims";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE inbox_claims");
    const reg = new PgInboxClaimRegistry({ connection: conn });
    await reg.hydrate();

    await t.test("claim then lookup", async () => {
      await reg.claim({ inboxId: "ibx-1", claimantPublicKeyB64: "pkA", claimedAtMs: 1000 });
      assert.equal(await reg.getClaimantPublicKey("ibx-1"), "pkA");
      assert.equal(await reg.hasInbox("ibx-1"), true);
      assert.equal(await reg.hasInbox("nope"), false);
    });

    await t.test("re-claim of a taken inbox throws INBOX_ALREADY_CLAIMED", async () => {
      await reg.claim({ inboxId: "ibx-2", claimantPublicKeyB64: "pkB", claimedAtMs: 1 });
      await assert.rejects(
        () => reg.claim({ inboxId: "ibx-2", claimantPublicKeyB64: "pkC", claimedAtMs: 2 }),
        (e) => e && e.code === "INBOX_ALREADY_CLAIMED",
      );
      assert.equal(await reg.getClaimantPublicKey("ibx-2"), "pkB", "original claimant preserved");
    });

    await t.test("concurrent race: exactly one of N claimants wins", async () => {
      const id = "ibx-race";
      const N = 12;
      const results = await Promise.allSettled(
        Array.from({ length: N }, (_u, i) =>
          reg.claim({ inboxId: id, claimantPublicKeyB64: `pk${i}`, claimedAtMs: 100 + i })),
      );
      const won = results.filter((r) => r.status === "fulfilled");
      const lost = results.filter((r) => r.status === "rejected");
      assert.equal(won.length, 1, "exactly one claim succeeds");
      assert.equal(lost.length, N - 1, "the rest lose");
      assert.ok(
        lost.every((r) => r.reason && r.reason.code === "INBOX_ALREADY_CLAIMED"),
        "losers all get INBOX_ALREADY_CLAIMED",
      );
      const winner = won[0].value.claimantPublicKeyB64;
      assert.equal(await reg.getClaimantPublicKey(id), winner, "registry reflects the winner");
    });
  },
);
