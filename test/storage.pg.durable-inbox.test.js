import test from "node:test";
import assert from "node:assert/strict";
import { PgConnection } from "../src/storage/pg/PgConnection.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError, InboxCapExceededError } from "../src/storage/DurableInbox.js";

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
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");
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
      assert.equal((await inbox.readAfterCursor(id, "d", 50)).length, 1);
    });

    await t.test("per-device cursors are independent; ack advances only its own", async () => {
      const id = "ib-multidev";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.append(id, bytes(3));
      await inbox.cursorAck(id, "devA", 2);
      assert.deepEqual((await inbox.readAfterCursor(id, "devA", 50)).map((e) => e.seq), [3]);
      assert.deepEqual((await inbox.readAfterCursor(id, "devB", 50)).map((e) => e.seq), [1, 2, 3]);
    });

    await t.test("cursorAck is monotonic and bounded; returns the STORED value", async () => {
      const id = "ib-ack";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      assert.equal((await inbox.cursorAck(id, "d", 2)).lastSeq, 2);
      assert.equal((await inbox.cursorAck(id, "d", 1)).lastSeq, 2, "cannot regress");
      assert.equal((await inbox.cursorAck(id, "d", 999)).lastSeq, 2, "clamped to high-water");
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
      await inbox.cursorAck(id, "devFast", 5);
      await inbox.cursorAck(id, "devSlow", 2);
      const pruned = await inbox.prune(id, {});
      assert.equal(pruned.watermark, 2);
      assert.equal(pruned.deleted, 2);
      assert.deepEqual((await inbox.readAfterCursor(id, "fresh", 50)).map((e) => e.seq), [3, 4, 5]);

      await conn.query(
        "UPDATE device_cursors SET updated_at = now() - interval '1 hour' WHERE inbox_id = $1 AND device_id = $2",
        [id, "devSlow"],
      );
      const pruned2 = await inbox.prune(id, { staleGraceMs: 60_000 });
      assert.equal(pruned2.watermark, 5, "stale devSlow excluded");
      assert.deepEqual((await inbox.readAfterCursor(id, "fresh", 50)).map((e) => e.seq), []);
    });

    await t.test("concurrent appends to one inbox stay gap-free", async () => {
      const id = "ib-concurrent";
      const N = 25;
      const results = await Promise.all(
        Array.from({ length: N }, (_u, i) => inbox.append(id, bytes(i))),
      );
      const seqs = results.map((r) => r.seq).sort((a, b) => a - b);
      assert.deepEqual(seqs, Array.from({ length: N }, (_u, i) => i + 1));
    });

    // --- AUDIT REGRESSIONS ---

    await t.test("REGRESSION (CRITICAL): seq is NOT reused after prune-to-empty", async () => {
      const id = "ib-reuse";
      await inbox.append(id, bytes(1)); // seq 1
      await inbox.append(id, bytes(2)); // seq 2
      await inbox.append(id, bytes(3)); // seq 3
      // The only/slowest device consumes all 3, then we prune to empty.
      await inbox.cursorAck(id, "devD", 3);
      const pruned = await inbox.prune(id, {});
      assert.equal(pruned.deleted, 3, "table emptied");
      // New deposit: seq MUST continue at 4, not reset to 1.
      const next = await inbox.append(id, bytes(4));
      assert.equal(next.seq, 4, "seq continues from durable high-water, not reused");
      // devD's cursor is at 3 → it MUST see the new event (no silent loss).
      const got = await inbox.readAfterCursor(id, "devD", 50);
      assert.deepEqual(got.map((e) => e.seq), [4], "device with old cursor still receives new mail");
    });

    await t.test("REGRESSION: cursorAck after prune returns the stored value, never regresses", async () => {
      const id = "ib-ack-prune";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.cursorAck(id, "d", 2);
      await inbox.prune(id, {}); // empties table; high-water stays 2
      const reAck = await inbox.cursorAck(id, "d", 2); // idempotent re-ack
      assert.equal(reAck.lastSeq, 2, "stored cursor unchanged, return matches stored");
    });

    await t.test("REGRESSION: TTL prune does NOT delete below a live device's cursor", async () => {
      const id = "ib-ttl-live";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      // A live device exists but has consumed nothing (cursor 0).
      await inbox.registerDevice(id, "liveDev");
      // Age the events well past the TTL.
      await conn.query("UPDATE mailbox_events SET created_at = now() - interval '1 day' WHERE inbox_id = $1", [id]);
      const pruned = await inbox.prune(id, { ttlMs: 1000, staleGraceMs: 3_600_000 });
      assert.equal(pruned.deleted, 0, "live device's unconsumed mail is NOT TTL-deleted");
      assert.deepEqual((await inbox.readAfterCursor(id, "liveDev", 50)).map((e) => e.seq), [1, 2]);
    });

    await t.test("REGRESSION: TTL prune reclaims an abandoned inbox (no live devices)", async () => {
      const id = "ib-ttl-abandoned";
      await inbox.append(id, bytes(1));
      await conn.query("UPDATE mailbox_events SET created_at = now() - interval '1 day' WHERE inbox_id = $1", [id]);
      const pruned = await inbox.prune(id, { ttlMs: 1000 }); // no devices registered
      assert.equal(pruned.deleted, 1, "abandoned old events reclaimed");
    });

    await t.test("event cap rejects over-limit append", async () => {
      const capped = new PgDurableInbox({ connection: conn, maxEvents: 2 });
      const id = "ib-cap-events";
      await capped.append(id, bytes(1));
      await capped.append(id, bytes(2));
      await assert.rejects(() => capped.append(id, bytes(3)), InboxCapExceededError);
    });

    await t.test("device cap rejects over-limit registration", async () => {
      const capped = new PgDurableInbox({ connection: conn, maxDevices: 2 });
      const id = "ib-cap-devices";
      await capped.registerDevice(id, "d1");
      await capped.registerDevice(id, "d2");
      await capped.registerDevice(id, "d1"); // idempotent, no new row
      await assert.rejects(() => capped.registerDevice(id, "d3"), InboxCapExceededError);
    });
  },
);
