import test from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync, sign as edSign } from "node:crypto";
import { PgConnection } from "../src/storage/pg/PgConnection.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { ReceiptSigner } from "../src/settlement/ReceiptSigner.js";
import { PgSettlementProvider } from "../src/settlement/PgSettlementProvider.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";

function makeSigner() {
  const { privateKey } = generateKeyPairSync("ed25519");
  return new ReceiptSigner({
    relayKeyId: "relay-test",
    signFn: async (bytes) => new Uint8Array(edSign(null, Buffer.from(bytes), privateKey)),
  });
}

const SVC = { serviceId: "mailbox.deposit", serviceRef: "mailbox:ibx-1" };

test(
  "PgSettlementProvider atomic debit against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const conn = new PgConnection({ connectionString: PG_URL });
    t.after(async () => {
      await conn.close();
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE settlement_balances, settlement_journal");
    const provider = new PgSettlementProvider({ connection: conn, receiptSigner: makeSigner(), networkId: "rez:testnet:v1" });

    await t.test("credit funds the wallet; balance reflects it", async () => {
      const r = await provider.credit("acctA", 100);
      assert.equal(r.available, 100);
      assert.deepEqual(await provider.balance("acctA"), { available: 100, escrowed: 0, total: 100 });
    });

    await t.test("debit produces a signed networkId-bound receipt", async () => {
      const receipt = await provider.debit("acctA", 10, SVC);
      assert.equal(receipt.amount, 10);
      assert.equal(receipt.accountId, "acctA");
      assert.equal(receipt.serviceRef, "mailbox:ibx-1");
      assert.ok(receipt.sig, "receipt is signed");
      assert.equal((await provider.balance("acctA")).available, 90);
      const journal = await provider.listJournal("acctA");
      assert.ok(journal.some((e) => e.kind === "debit" && e.networkId === "rez:testnet:v1"));
    });

    await t.test("underfunded debit throws INSUFFICIENT_FUNDS, balance untouched", async () => {
      await assert.rejects(
        () => provider.debit("acctA", 1000, SVC),
        (e) => e && e.code === "INSUFFICIENT_FUNDS",
      );
      assert.equal((await provider.balance("acctA")).available, 90);
    });

    await t.test("REGRESSION: exact-boundary debit succeeds; the next is INSUFFICIENT_FUNDS (not a raw CHECK error)", async () => {
      await provider.credit("acctBoundary", 50);
      const r = await provider.debit("acctBoundary", 50, SVC); // available >= amt with equality
      assert.equal(r.amount, 50);
      assert.equal((await provider.balance("acctBoundary")).available, 0);
      await assert.rejects(
        () => provider.debit("acctBoundary", 1, SVC),
        (e) => e && e.code === "INSUFFICIENT_FUNDS",
        "drained wallet rejects with INSUFFICIENT_FUNDS, never a raw 23514",
      );
    });

    await t.test("CONCURRENT two-device debit cannot overdraft", async () => {
      await provider.credit("acctRace", 100);
      const results = await Promise.allSettled([
        provider.debit("acctRace", 60, SVC),
        provider.debit("acctRace", 60, SVC),
      ]);
      const ok = results.filter((r) => r.status === "fulfilled");
      const fail = results.filter((r) => r.status === "rejected");
      assert.equal(ok.length, 1, "exactly one 60-debit succeeds");
      assert.equal(fail.length, 1, "the other is rejected");
      assert.equal(fail[0].reason.code, "INSUFFICIENT_FUNDS");
      assert.equal((await provider.balance("acctRace")).available, 40, "no overdraft");
    });

    await t.test("idempotent debit (sequential) charges once", async () => {
      await provider.credit("acctIdem", 100);
      const first = await provider.debit("acctIdem", 30, { ...SVC, idempotencyKey: "req-1" });
      const replay = await provider.debit("acctIdem", 30, { ...SVC, idempotencyKey: "req-1" });
      assert.equal(replay.receiptId, first.receiptId, "same receipt returned");
      assert.equal((await provider.balance("acctIdem")).available, 70, "charged once");
    });

    await t.test("idempotent debit (concurrent, same key) charges once", async () => {
      await provider.credit("acctIdem2", 100);
      const [a, b] = await Promise.all([
        provider.debit("acctIdem2", 30, { ...SVC, idempotencyKey: "req-2" }),
        provider.debit("acctIdem2", 30, { ...SVC, idempotencyKey: "req-2" }),
      ]);
      assert.equal(a.receiptId, b.receiptId, "both callers get the same receipt");
      assert.equal((await provider.balance("acctIdem2")).available, 70, "charged once despite the race");
    });
  },
);
