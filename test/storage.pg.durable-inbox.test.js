import test from "node:test";
import assert from "node:assert/strict";
import { PgConnection } from "../src/storage/pg/PgConnection.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError, InboxCapExceededError, DeviceNotRegisteredError } from "../src/storage/DurableInbox.js";

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

    // Helper: register a device then read.
    const readAs = async (id, dev, limit = 50) => {
      await inbox.registerDevice(id, dev);
      return inbox.readAfterCursor(id, dev, limit);
    };

    await t.test("append is gap-free; readAfterCursor returns in order", async () => {
      const id = "ib-order";
      assert.deepEqual(await inbox.append(id, bytes(1)), { seq: 1, deduped: false });
      assert.deepEqual(await inbox.append(id, bytes(2)), { seq: 2, deduped: false });
      assert.deepEqual(await inbox.append(id, bytes(3)), { seq: 3, deduped: false });
      const all = await readAs(id, "devA");
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
      assert.equal((await readAs(id, "d")).length, 1);
    });

    await t.test("read/ack require a registered device (no implicit creation)", async () => {
      const id = "ib-unreg";
      await inbox.append(id, bytes(1));
      await assert.rejects(() => inbox.readAfterCursor(id, "ghost", 50), DeviceNotRegisteredError);
      await assert.rejects(() => inbox.cursorAck(id, "ghost", 1), DeviceNotRegisteredError);
      // No row was implicitly created.
      const rows = await conn.query("SELECT count(*)::int c FROM device_cursors WHERE inbox_id=$1 AND device_id=$2", [id, "ghost"]);
      assert.equal(rows.rows[0].c, 0);
    });

    await t.test("per-device cursors are independent; ack advances only its own", async () => {
      const id = "ib-multidev";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.append(id, bytes(3));
      await readAs(id, "devA"); // delivers 1..3
      await inbox.cursorAck(id, "devA", 2);
      assert.deepEqual((await inbox.readAfterCursor(id, "devA", 50)).map((e) => e.seq), [3]);
      assert.deepEqual((await readAs(id, "devB")).map((e) => e.seq), [1, 2, 3]);
    });

    await t.test("cursorAck is DELIVERED-bounded (not global max), monotonic, returns stored value", async () => {
      const id = "ib-ack";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.append(id, bytes(3)); // high-water = 3
      await inbox.registerDevice(id, "d");
      await inbox.readAfterCursor(id, "d", 2); // delivered only seq 1,2 (limit 2)
      assert.equal((await inbox.cursorAck(id, "d", 3)).lastSeq, 2, "cannot ack past what was delivered (2), even though max is 3");
      assert.equal((await inbox.cursorAck(id, "d", 1)).lastSeq, 2, "cannot regress");
      await inbox.readAfterCursor(id, "d", 50); // now delivers seq 3
      assert.equal((await inbox.cursorAck(id, "d", 3)).lastSeq, 3, "now allowed up to 3");
    });

    await t.test("revocation fails closed for read + ack", async () => {
      const id = "ib-revoke";
      await inbox.append(id, bytes(1));
      await inbox.registerDevice(id, "devR");
      assert.equal(await inbox.revokeDevice(id, "devR"), true);
      await assert.rejects(() => inbox.readAfterCursor(id, "devR", 50), RevokedDeviceError);
      await assert.rejects(() => inbox.cursorAck(id, "devR", 1), RevokedDeviceError);
    });

    await t.test("device-targeted append to a revoked device is rejected", async () => {
      const id = "ib-append-revoke";
      await inbox.registerDevice(id, "devX");
      await inbox.revokeDevice(id, "devX");
      await assert.rejects(() => inbox.append(id, bytes(1), { deviceId: "devX" }), RevokedDeviceError);
      // Inbox-level append (no deviceId) is unaffected.
      assert.equal((await inbox.append(id, bytes(1))).seq, 1);
    });

    await t.test("prune deletes below slowest live cursor; stale device excluded", async () => {
      const id = "ib-prune";
      for (let i = 1; i <= 5; i += 1) {
        await inbox.append(id, bytes(i));
      }
      await readAs(id, "devFast"); // delivered 5
      await readAs(id, "devSlow"); // delivered 5
      await inbox.cursorAck(id, "devFast", 5);
      await inbox.cursorAck(id, "devSlow", 2);
      const pruned = await inbox.prune(id, {});
      assert.equal(pruned.watermark, 2);
      assert.equal(pruned.deleted, 2);
      assert.deepEqual((await readAs(id, "checker")).map((e) => e.seq), [3, 4, 5]);

      await conn.query(
        "UPDATE device_cursors SET updated_at = now() - interval '1 hour' WHERE inbox_id = $1 AND device_id IN ('devSlow','checker')",
        [id],
      );
      const pruned2 = await inbox.prune(id, { staleGraceMs: 60_000 });
      assert.equal(pruned2.watermark, 5, "stale devSlow/checker excluded; devFast=5 is the watermark");
    });

    await t.test("concurrent appends to one inbox stay gap-free", async () => {
      const id = "ib-concurrent";
      const N = 25;
      const results = await Promise.all(Array.from({ length: N }, (_u, i) => inbox.append(id, bytes(i))));
      const seqs = results.map((r) => r.seq).sort((a, b) => a - b);
      assert.deepEqual(seqs, Array.from({ length: N }, (_u, i) => i + 1));
    });

    // --- AUDIT REGRESSIONS ---

    await t.test("REGRESSION (CRITICAL): seq is NOT reused after prune-to-empty", async () => {
      const id = "ib-reuse";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.append(id, bytes(3));
      await readAs(id, "devD"); // delivered 3
      await inbox.cursorAck(id, "devD", 3);
      assert.equal((await inbox.prune(id, {})).deleted, 3);
      const next = await inbox.append(id, bytes(4));
      assert.equal(next.seq, 4, "seq continues from durable high-water");
      assert.deepEqual((await inbox.readAfterCursor(id, "devD", 50)).map((e) => e.seq), [4], "old-cursor device still receives");
    });

    await t.test("REGRESSION: cursorAck after prune returns stored value, never regresses", async () => {
      const id = "ib-ack-prune";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await readAs(id, "d"); // delivered 2
      await inbox.cursorAck(id, "d", 2);
      await inbox.prune(id, {});
      assert.equal((await inbox.cursorAck(id, "d", 2)).lastSeq, 2);
    });

    await t.test("REGRESSION: TTL prune does NOT delete below a live device's cursor", async () => {
      const id = "ib-ttl-live";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.registerDevice(id, "liveDev"); // live, cursor 0
      await conn.query("UPDATE mailbox_events SET created_at = now() - interval '1 day' WHERE inbox_id = $1", [id]);
      const pruned = await inbox.prune(id, { ttlMs: 1000, staleGraceMs: 3_600_000 });
      assert.equal(pruned.deleted, 0, "live device's unconsumed mail is NOT TTL-deleted");
      assert.deepEqual((await inbox.readAfterCursor(id, "liveDev", 50)).map((e) => e.seq), [1, 2]);
    });

    await t.test("REGRESSION: TTL prune reclaims an abandoned inbox (no live devices)", async () => {
      const id = "ib-ttl-abandoned";
      await inbox.append(id, bytes(1));
      await conn.query("UPDATE mailbox_events SET created_at = now() - interval '1 day' WHERE inbox_id = $1", [id]);
      assert.equal((await inbox.prune(id, { ttlMs: 1000 })).deleted, 1);
    });

    await t.test("event / byte / body-size / device caps reject over-limit", async () => {
      const evCap = new PgDurableInbox({ connection: conn, maxEvents: 2 });
      await evCap.append("ib-cap-ev", bytes(1));
      await evCap.append("ib-cap-ev", bytes(2));
      await assert.rejects(() => evCap.append("ib-cap-ev", bytes(3)), InboxCapExceededError);

      const byteCap = new PgDurableInbox({ connection: conn, maxBytes: 10 });
      await byteCap.append("ib-cap-by", bytes(1, 2, 3, 4, 5, 6)); // 6 bytes
      await assert.rejects(() => byteCap.append("ib-cap-by", bytes(1, 2, 3, 4, 5, 6)), InboxCapExceededError); // 12 > 10

      const bodyCap = new PgDurableInbox({ connection: conn, maxBodyBytes: 4 });
      await assert.rejects(() => bodyCap.append("ib-cap-body", bytes(1, 2, 3, 4, 5)), InboxCapExceededError);

      const devCap = new PgDurableInbox({ connection: conn, maxDevices: 2 });
      await devCap.registerDevice("ib-cap-dev", "d1");
      await devCap.registerDevice("ib-cap-dev", "d2");
      await devCap.registerDevice("ib-cap-dev", "d1"); // idempotent
      await assert.rejects(() => devCap.registerDevice("ib-cap-dev", "d3"), InboxCapExceededError);
    });
  },
);
