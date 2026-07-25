import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { InboxClaimRegistry, DEFAULT_MAX_INBOXES_PER_CLAIMANT } from "../src/inbox/InboxClaimRegistry.js";
import { PgInboxClaimRegistry } from "../src/storage/pg/PgInboxClaimRegistry.js";

// Track 2 — abuse quotas on OPEN registration.
//
// Registration is deliberately open: any caller with a valid signature may claim an inbox, with no
// allowlist. That is a design property, not an oversight — but it means one keypair could otherwise
// mint inboxes without bound, and since each inbox carries its own retention budget, multiply the
// node's storage by the claim count. The per-inbox item/byte caps bound each inbox; this ceiling
// bounds how many a single claimant gets, which is what makes total storage per claimant finite.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

test("InboxClaimRegistry (fs): a claimant cannot exceed its inbox ceiling", async () => {
  const registry = new InboxClaimRegistry({
    storageProvider: new MemoryStorageProvider(),
    maxInboxesPerClaimant: 3,
  });
  await registry.hydrate();

  for (let i = 0; i < 3; i += 1) {
    const row = await registry.claim({ inboxId: "inbox-" + i, claimantPublicKeyB64: "KEY-A", claimedAtMs: 1000 + i });
    assert.equal(row.inboxId, "inbox-" + i);
  }
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox-over", claimantPublicKeyB64: "KEY-A", claimedAtMs: 2000 }),
    (err) => err.code === "INBOX_CLAIM_QUOTA_EXCEEDED",
  );

  // The ceiling is PER CLAIMANT — a different key is unaffected.
  const other = await registry.claim({ inboxId: "inbox-other", claimantPublicKeyB64: "KEY-B", claimedAtMs: 2001 });
  assert.equal(other.claimantPublicKeyB64, "KEY-B");
});

test("InboxClaimRegistry (fs): concurrent claims by one key cannot both pass the ceiling", async () => {
  // The count is taken inside the write mutex; if it were read before entering, two claims could
  // each see "one under the limit" and both insert.
  const registry = new InboxClaimRegistry({
    storageProvider: new MemoryStorageProvider(),
    maxInboxesPerClaimant: 2,
  });
  await registry.hydrate();
  await registry.claim({ inboxId: "c-0", claimantPublicKeyB64: "KEY-C", claimedAtMs: 1000 });

  const results = await Promise.allSettled([
    registry.claim({ inboxId: "c-1", claimantPublicKeyB64: "KEY-C", claimedAtMs: 1001 }),
    registry.claim({ inboxId: "c-2", claimantPublicKeyB64: "KEY-C", claimedAtMs: 1002 }),
  ]);
  const ok = results.filter((r) => r.status === "fulfilled");
  const rejected = results.filter((r) => r.status === "rejected");
  assert.equal(ok.length, 1, "exactly one of the racing claims was granted");
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0].reason.code, "INBOX_CLAIM_QUOTA_EXCEEDED");
});

test("the fs registry rejects a nonsensical ceiling rather than defaulting quietly", () => {
  const storageProvider = new MemoryStorageProvider();
  assert.throws(() => new InboxClaimRegistry({ storageProvider, maxInboxesPerClaimant: 0 }), /positive integer/);
  assert.throws(() => new InboxClaimRegistry({ storageProvider, maxInboxesPerClaimant: 1.5 }), /positive integer/);
  assert.ok(DEFAULT_MAX_INBOXES_PER_CLAIMANT > 0, "there is a default ceiling — open does not mean unbounded");
});

test(
  "PgInboxClaimRegistry: the ceiling holds across the cluster, in one transaction",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_inbox_claim_quota";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();

    const registry = new PgInboxClaimRegistry({ connection: conn, maxInboxesPerClaimant: 3 });

    for (let i = 0; i < 3; i += 1) {
      await registry.claim({ inboxId: "pg-inbox-" + i, claimantPublicKeyB64: "PG-KEY-A", claimedAtMs: 1000 + i });
    }
    await assert.rejects(
      () => registry.claim({ inboxId: "pg-inbox-over", claimantPublicKeyB64: "PG-KEY-A", claimedAtMs: 2000 }),
      (err) => err.code === "INBOX_CLAIM_QUOTA_EXCEEDED",
    );
    // The rejected claim left NO row — the ceiling check and the insert share one transaction.
    const rows = await conn.query("SELECT count(*)::int AS c FROM inbox_claims WHERE claimant_pubkey = $1", ["PG-KEY-A"]);
    assert.equal(rows.rows[0].c, 3);

    await t.test("a SEPARATE registry instance (another node) sees the same ceiling", async () => {
      const otherNode = new PgInboxClaimRegistry({ connection: conn, maxInboxesPerClaimant: 3 });
      await assert.rejects(
        () => otherNode.claim({ inboxId: "pg-inbox-other-node", claimantPublicKeyB64: "PG-KEY-A", claimedAtMs: 3000 }),
        (err) => err.code === "INBOX_CLAIM_QUOTA_EXCEEDED",
      );
    });

    await t.test("concurrent claims by one key across instances cannot overshoot", async () => {
      // Per-claimant advisory lock: the racing transactions serialize, so the count one reads
      // already includes what the other inserted.
      const a = new PgInboxClaimRegistry({ connection: conn, maxInboxesPerClaimant: 2 });
      const b = new PgInboxClaimRegistry({ connection: conn, maxInboxesPerClaimant: 2 });
      const results = await Promise.allSettled([
        a.claim({ inboxId: "race-1", claimantPublicKeyB64: "PG-KEY-RACE", claimedAtMs: 4000 }),
        b.claim({ inboxId: "race-2", claimantPublicKeyB64: "PG-KEY-RACE", claimedAtMs: 4001 }),
        a.claim({ inboxId: "race-3", claimantPublicKeyB64: "PG-KEY-RACE", claimedAtMs: 4002 }),
      ]);
      assert.equal(results.filter((r) => r.status === "fulfilled").length, 2, "exactly the ceiling was granted");
      const held = await conn.query("SELECT count(*)::int AS c FROM inbox_claims WHERE claimant_pubkey = $1", ["PG-KEY-RACE"]);
      assert.equal(held.rows[0].c, 2);
    });

    await t.test("a different claimant is unaffected", async () => {
      const row = await registry.claim({ inboxId: "pg-inbox-b", claimantPublicKeyB64: "PG-KEY-B", claimedAtMs: 5000 });
      assert.equal(row.claimantPublicKeyB64, "PG-KEY-B");
    });
  },
);
