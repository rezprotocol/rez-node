import test from "node:test";
import assert from "node:assert/strict";
import { PgConnection } from "../src/storage/pg/PgConnection.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError } from "../src/storage/DurableInbox.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";
const bytes = (...n) => new Uint8Array(n);

test(
  "PgDurableInbox against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const conn = new PgConnection({ connectionString: PG_URL });
    t.after(async () => {
      await conn.close();
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors");
    const inbox = new PgDurableInbox({ connection: conn });

    await t.test("append is gap-free; readAfterCursor returns in order", async () => {
      const id = "ib-order";
      assert.deepEqual(await inbox.append(id, bytes(1)), { seq: 1, deduped: false });
      assert.deepEqual(await inbox.append(id, bytes(2)), { seq: 2, deduped: false });
      assert.deepEqual(await inbox.append(id, bytes(3)), { seq: 3, deduped: false });
      const all = await inbox.readAfterCursor(id, "devA", 50);
      assert.deepEqual(all.map((e) => e.seq), [1, 2, 3]);
      assert.deepEqual(all[0].body, bytes(1));
      assert.deepEqual(all[2].body, bytes(3));
    });

    await t.test("dedupe by (inbox, dedupeKey) is idempotent", async () => {
      const id = "ib-dedupe";
      const first = await inbox.append(id, bytes(9), { dedupeKey: "h1" });
      const again = await inbox.append(id, bytes(9), { dedupeKey: "h1" });
      assert.equal(first.deduped, false);
      assert.equal(again.deduped, true);
      assert.equal(again.seq, first.seq);
      const all = await inbox.readAfterCursor(id, "d", 50);
      assert.equal(all.length, 1, "no double-append");
    });

    await t.test("per-device cursors are independent; ack advances only its own", async () => {
      const id = "ib-multidev";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.append(id, bytes(3));
      // devA consumes through 2, devB nothing
      await inbox.cursorAck(id, "devA", 2);
      const aRest = await inbox.readAfterCursor(id, "devA", 50);
      assert.deepEqual(aRest.map((e) => e.seq), [3], "devA sees only seq>2");
      const bRest = await inbox.readAfterCursor(id, "devB", 50);
      assert.deepEqual(bRest.map((e) => e.seq), [1, 2, 3], "devB cursor untouched");
    });

    await t.test("cursorAck is monotonic and bounded", async () => {
      const id = "ib-ack";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      const a = await inbox.cursorAck(id, "d", 2);
      assert.equal(a.lastSeq, 2);
      const regress = await inbox.cursorAck(id, "d", 1); // cannot regress
      assert.equal(regress.lastSeq, 2);
      const overshoot = await inbox.cursorAck(id, "d", 999); // clamped to max seq
      assert.equal(overshoot.lastSeq, 2);
    });

    await t.test("revocation fails closed for read + ack", async () => {
      const id = "ib-revoke";
      await inbox.append(id, bytes(1));
      await inbox.registerDevice(id, "devR");
      assert.equal(await inbox.revokeDevice(id, "devR"), true);
      await assert.rejects(() => inbox.readAfterCursor(id, "devR", 50), RevokedDeviceError);
      await assert.rejects(() => inbox.cursorAck(id, "devR", 1), RevokedDeviceError);
    });

    await t.test("prune deletes below slowest live cursor; stale device excluded", async () => {
      const id = "ib-prune";
      for (let i = 1; i <= 5; i += 1) {
        await inbox.append(id, bytes(i));
      }
      // devFast consumed all 5; devSlow only 2 → watermark = 2
      await inbox.cursorAck(id, "devFast", 5);
      await inbox.cursorAck(id, "devSlow", 2);
      const pruned = await inbox.prune(id, {});
      assert.equal(pruned.watermark, 2);
      assert.equal(pruned.deleted, 2, "seq 1,2 removed");
      assert.deepEqual((await inbox.readAfterCursor(id, "fresh", 50)).map((e) => e.seq), [3, 4, 5]);

      // Now exclude devSlow as stale → watermark rises to devFast (5)
      await conn.query(
        "UPDATE device_cursors SET updated_at = now() - interval '1 hour' WHERE inbox_id = $1 AND device_id = $2",
        [id, "devSlow"],
      );
      const pruned2 = await inbox.prune(id, { staleGraceMs: 60_000 });
      assert.equal(pruned2.watermark, 5, "stale devSlow excluded from watermark");
      assert.deepEqual((await inbox.readAfterCursor(id, "fresh", 50)).map((e) => e.seq), []);
    });

    await t.test("concurrent appends to one inbox stay gap-free", async () => {
      const id = "ib-concurrent";
      const N = 25;
      const results = await Promise.all(
        Array.from({ length: N }, (_unused, i) => inbox.append(id, bytes(i))),
      );
      const seqs = results.map((r) => r.seq).sort((a, b) => a - b);
      assert.deepEqual(seqs, Array.from({ length: N }, (_u, i) => i + 1), "1..N, no gaps, no dups");
    });
  },
);
