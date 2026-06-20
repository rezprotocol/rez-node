import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { RevokedDeviceError, InboxCapExceededError, DeviceNotRegisteredError } from "../src/storage/DurableInbox.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";
const MIGRATIONS_DIR = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..", "src", "storage", "pg", "migrations",
);
const bytes = (...n) => new Uint8Array(n);

test(
  "PgDurableInbox against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_durable_inbox";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
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
    });

    await t.test("wire-path append (no deviceId) fails closed when the inbox has no live device (Audit P1)", async () => {
      // The production deposit path (DurableHomeInboxStore.depositFromWire) names
      // no device. With per-device inbox addressing an inbox maps 1:1 to its
      // device, so once that device is revoked the home must reject deposits — a
      // lagging sender cannot keep filling a revoked device's inbox.
      const revoked = "ib-wire-revoked";
      await inbox.registerDevice(revoked, "only");
      await inbox.revokeDevice(revoked, "only");
      await assert.rejects(() => inbox.append(revoked, bytes(1)), RevokedDeviceError);

      // A live device on the inbox (even alongside a revoked one) still accepts.
      const mixed = "ib-wire-mixed";
      await inbox.registerDevice(mixed, "live");
      await inbox.registerDevice(mixed, "dead");
      await inbox.revokeDevice(mixed, "dead");
      assert.equal((await inbox.append(mixed, bytes(2))).seq, 1, "a live device keeps the inbox open");

      // No device registered yet (pre-bind / first contact) still accepts, so
      // legit mail for a not-yet-connected device is never dropped.
      assert.equal((await inbox.append("ib-wire-unbound", bytes(3))).seq, 1);
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

    await t.test("REGRESSION: an actively-reading (unacked) device refreshes freshness; prune keeps its unread", async () => {
      const id = "ib-read-fresh";
      await inbox.append(id, bytes(1));
      await inbox.append(id, bytes(2));
      await inbox.append(id, bytes(3));
      await inbox.registerDevice(id, "reader");
      await inbox.registerDevice(id, "acker");
      await inbox.readAfterCursor(id, "reader", 50); // gets 1..3 but cannot ack yet (cursor stays 0)
      await readAs(id, "acker"); // delivered 3
      await inbox.cursorAck(id, "acker", 3); // acker fully caught up
      // Time passes; both device rows age out of the freshness window.
      await conn.query("UPDATE device_cursors SET updated_at = now() - interval '1 hour' WHERE inbox_id = $1", [id]);
      // The reader keeps reading (still cannot ack). This read must refresh freshness.
      await inbox.readAfterCursor(id, "reader", 50);
      const pruned = await inbox.prune(id, { staleGraceMs: 60_000 });
      // reader is live again (cursor 0) → watermark 0 → nothing pruned; its unread is safe.
      assert.equal(pruned.watermark, 0, "the still-reading device is counted live, pinning the watermark at its cursor");
      assert.equal(pruned.deleted, 0);
      assert.deepEqual((await inbox.readAfterCursor(id, "reader", 50)).map((e) => e.seq), [1, 2, 3]);
    });

    await t.test("REGRESSION: an empty read still refreshes freshness", async () => {
      const id = "ib-read-fresh-empty";
      await inbox.append(id, bytes(1));
      await inbox.registerDevice(id, "d");
      await readAs(id, "d"); // delivers 1
      await inbox.cursorAck(id, "d", 1); // caught up
      await conn.query("UPDATE device_cursors SET updated_at = now() - interval '1 hour' WHERE inbox_id = $1", [id]);
      assert.equal((await inbox.readAfterCursor(id, "d", 50)).length, 0, "nothing new to read");
      const row = await conn.query(
        "SELECT updated_at > now() - interval '1 minute' AS fresh FROM device_cursors WHERE inbox_id = $1 AND device_id = 'd'",
        [id],
      );
      assert.equal(row.rows[0].fresh, true, "an empty read still bumps the freshness clock");
    });

    await t.test("REGRESSION (upgrade path): 0006 backfills mailbox_seq from pre-existing events", async () => {
      const id = "ib-upgrade-v6";
      // Simulate pre-v6 durable state: events already exist, but the high-water
      // counter row does NOT — exactly the v5→v6 upgrade scenario.
      await conn.query("DELETE FROM mailbox_seq WHERE inbox_id = $1", [id]);
      await conn.query("DELETE FROM mailbox_events WHERE inbox_id = $1", [id]);
      await conn.query(
        "INSERT INTO mailbox_events (inbox_id, seq, body) VALUES ($1, 1, $2), ($1, 2, $2), ($1, 3, $2)",
        [id, Buffer.from([9])],
      );
      // A device whose cursor is already past the would-be-reused low seqs.
      await conn.query(
        "INSERT INTO device_cursors (inbox_id, device_id, last_seq, last_delivered) VALUES ($1, 'old', 3, 3)",
        [id],
      );
      // Apply the REAL 0006 migration SQL (idempotent) — this is the backfill.
      const sql = readFileSync(path.join(MIGRATIONS_DIR, "0006_mailbox_seq.sql"), "utf8");
      await conn.query(sql);
      const seeded = await conn.query("SELECT last_seq FROM mailbox_seq WHERE inbox_id = $1", [id]);
      assert.equal(Number(seeded.rows[0].last_seq), 3, "counter seeded from max(seq) of existing events");
      // The next append continues monotonically (4) — never reusing 1..3, so the
      // device at cursor 3 receives it instead of losing it to a reset.
      assert.equal((await inbox.append(id, bytes(4))).seq, 4);
      assert.deepEqual((await inbox.readAfterCursor(id, "old", 50)).map((e) => e.seq), [4]);
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

      // maxDevices=2 is gate-OPEN (>1), so only PROVEN (key-bound) cursors count
      // toward the cap (Audit P1) — register with device keys.
      const devCap = new PgDurableInbox({ connection: conn, maxDevices: 2 });
      await devCap.registerDevice("ib-cap-dev", "d1", { devicePublicKeyB64: "k1" });
      await devCap.registerDevice("ib-cap-dev", "d2", { devicePublicKeyB64: "k2" });
      await devCap.registerDevice("ib-cap-dev", "d1", { devicePublicKeyB64: "k1" }); // idempotent
      await assert.rejects(() => devCap.registerDevice("ib-cap-dev", "d3", { devicePublicKeyB64: "k3" }), InboxCapExceededError);
    });

    await t.test("setOnDeposit fires once per FRESH append, post-commit; a dedupe hit does NOT re-notify", async () => {
      const id = "ib-notify";
      const fired = [];
      inbox.setOnDeposit((inboxId, seq) => fired.push([inboxId, seq]));
      const a = await inbox.append(id, bytes(1), { dedupeKey: "k1" });
      const dup = await inbox.append(id, bytes(1), { dedupeKey: "k1" }); // dedupe → no notify
      const c = await inbox.append(id, bytes(2));
      assert.equal(dup.deduped, true);
      assert.deepEqual(fired, [[id, a.seq], [id, c.seq]], "only fresh appends notify");
      inbox.setOnDeposit(null);
    });

    await t.test("a throwing onDeposit hook does NOT fail append; the row stays durable and dedupe still suppresses re-notify", async () => {
      const id = "ib-notify-throws";
      let calls = 0;
      inbox.setOnDeposit(() => { calls += 1; throw new Error("hook boom"); });
      // The row is committed BEFORE the hook; a throwing hook must not reject.
      const a = await inbox.append(id, bytes(7), { dedupeKey: "boom" });
      assert.equal(a.deduped, false);
      assert.equal(calls, 1, "hook ran once");
      // The message is genuinely stored and readable.
      assert.deepEqual((await readAs(id, "dthrow")).map((e) => e.seq), [a.seq]);
      // A retry with the same dedupe key is suppressed AND does not re-notify
      // (so a stored-but-notify-failed message can't be duplicated by retry).
      const again = await inbox.append(id, bytes(7), { dedupeKey: "boom" });
      assert.equal(again.deduped, true);
      assert.equal(again.seq, a.seq);
      assert.equal(calls, 1, "dedupe hit must not re-run the hook");
      inbox.setOnDeposit(null);
    });

    await t.test("gate OPEN (maxDevices>1): an UNPROVEN claim-path cursor is refused; only device.bind creates one (Audit P1)", async () => {
      const open = new PgDurableInbox({ connection: conn, maxDevices: 8 });
      const id = "ib-unproven-gate-open";
      // Claim path: registerDevice with NO key (unproven). Gate open ⇒ no-op.
      await open.registerDevice(id, "rez:dev:claimonly");
      assert.equal(await open.getDevice(id, "rez:dev:claimonly"), null, "unproven cursor NOT created when the gate is open");
      // device.bind path: proven key ⇒ cursor created and counts.
      await open.registerDevice(id, "rez:dev:proven", { devicePublicKeyB64: "provenkey" });
      const proven = await open.getDevice(id, "rez:dev:proven");
      assert.equal(proven.devicePublicKeyB64, "provenkey", "proven cursor created with its key");
    });

    await t.test("gate CLOSED (maxDevices=1): the legacy unproven claim cursor is still created (unchanged)", async () => {
      const closed = new PgDurableInbox({ connection: conn, maxDevices: 1 });
      const id = "ib-unproven-gate-closed";
      await closed.registerDevice(id, "rez:dev:legacy"); // no key
      const dev = await closed.getDevice(id, "rez:dev:legacy");
      assert.ok(dev, "legacy single-device claim cursor created");
      assert.equal(dev.devicePublicKeyB64, null, "and it is unproven (no key) — the shipped path");
    });

    await t.test("pruneAll sweeps every inbox holding events and reclaims below the live cursor", async () => {
      const sweepInbox = new PgDurableInbox({ connection: conn });
      // Two inboxes with consumed events + one with no events (only a device).
      for (const id of ["sweep-a", "sweep-b"]) {
        await sweepInbox.append(id, bytes(1));
        await sweepInbox.append(id, bytes(2));
        await sweepInbox.append(id, bytes(3));
        await sweepInbox.registerDevice(id, "d");
        await sweepInbox.readAfterCursor(id, "d", 50);
        await sweepInbox.cursorAck(id, "d", 3); // fully consumed
      }
      await sweepInbox.registerDevice("sweep-empty", "d"); // no events → not enumerated

      // pruneAll is GLOBAL (it sweeps every inbox holding events in the shared
      // schema), so assert inbox-locally rather than on the cluster-wide totals.
      const res = await sweepInbox.pruneAll({});
      assert.ok(res.inboxesSwept >= 2, "swept at least both event-holding inboxes");
      assert.ok(res.deleted >= 6, "reclaimed at least the 6 consumed events of sweep-a/sweep-b");
      assert.equal((await sweepInbox.readAfterCursor("sweep-a", "d", 50)).length, 0, "sweep-a fully reclaimed");
      assert.equal((await sweepInbox.readAfterCursor("sweep-b", "d", 50)).length, 0, "sweep-b fully reclaimed");
      // sweep-empty held no events → pruneAll does not enumerate it (nothing to do).
      assert.equal((await sweepInbox.readAfterCursor("sweep-empty", "d", 50)).length, 0);
    });

    await t.test("pruneAll un-wedges a capped inbox: prune consumed events → append succeeds again", async () => {
      const capped = new PgDurableInbox({ connection: conn, maxEvents: 3 });
      const id = "sweep-wedge";
      const s1 = await capped.append(id, bytes(1));
      await capped.append(id, bytes(2));
      await capped.append(id, bytes(3));
      // Cap reached — append wedges until consumed events are reclaimed.
      await assert.rejects(() => capped.append(id, bytes(4)), InboxCapExceededError);
      // Consume the first two and sweep.
      await capped.registerDevice(id, "d");
      await capped.readAfterCursor(id, "d", 50);
      await capped.cursorAck(id, "d", 2);
      const res = await capped.pruneAll({});
      assert.ok(res.deleted >= 2, "reclaimed at least the two consumed events of sweep-wedge");
      // Only the unconsumed seq 3 remains for this inbox.
      assert.deepEqual((await capped.readAfterCursor(id, "d", 50)).map((e) => e.seq), [3]);
      // The wedge is cleared (count 1 < cap 3); new mail flows, seq NOT reused.
      const s4 = await capped.append(id, bytes(4));
      assert.ok(s4.seq > s1.seq + 2, "seq stays monotonic across prune");
    });
  },
);
