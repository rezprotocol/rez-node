import test from "node:test";
import assert from "node:assert/strict";

import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { pgTestUrl } from "./support/integrationBackends.js";

const PG_URL = pgTestUrl();
const bytes = (...n) => new Uint8Array(n);

test(
  "readUndelivered pushes each event ONCE (live path): no re-drain, un-acked poison can't amplify (P1)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_read_undelivered";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    const inbox = new PgDurableInbox({ connection: conn });
    const id = "ib";
    const dev = "devA";
    await inbox.registerDevice(id, dev);
    await inbox.append(id, bytes(1));
    await inbox.append(id, bytes(2));

    // First ping: delivers 1,2 and advances last_delivered past them.
    const first = await inbox.readUndelivered(id, dev, 100);
    assert.deepEqual(first.map((e) => e.seq), [1, 2]);

    // Second ping with NO new deposits + NO cursorAck (events un-consumed):
    // must return NOTHING. The old readAfterCursor (consumed cursor) would here
    // re-return 1,2 forever — the amplification / poison-pin bug.
    const second = await inbox.readUndelivered(id, dev, 100);
    assert.deepEqual(second.map((e) => e.seq), [], "no re-drain from the consumed cursor");

    // A new deposit pings only the NEW event.
    await inbox.append(id, bytes(3));
    const third = await inbox.readUndelivered(id, dev, 100);
    assert.deepEqual(third.map((e) => e.seq), [3]);

    // The consumed cursor never moved (nothing acked), yet cursorAck is still
    // bound to last_delivered (=3) — a live-pushed event stays ackable.
    const acked = await inbox.cursorAck(id, dev, 3);
    assert.equal(acked.lastSeq, 3, "cursorAck reaches the live-delivered watermark");

    // After acking through 3, reconnect catch-up (readAfterCursor from the
    // consumed cursor) returns nothing — consumed == delivered.
    const rest = await inbox.readAfterCursor(id, dev, 100);
    assert.deepEqual(rest.map((e) => e.seq), []);
  },
);

test(
  "readUndelivered fails closed for an unregistered / revoked device",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_read_undelivered_guard";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    const inbox = new PgDurableInbox({ connection: conn });
    await inbox.append("ib", bytes(1));
    await assert.rejects(() => inbox.readUndelivered("ib", "ghost", 10), /not registered/);

    await inbox.registerDevice("ib", "devA");
    await inbox.revokeDevice("ib", "devA");
    await assert.rejects(() => inbox.readUndelivered("ib", "devA", 10), /revoked/);
  },
);
