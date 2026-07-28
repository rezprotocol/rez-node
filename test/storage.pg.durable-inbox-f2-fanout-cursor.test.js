import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { UnprovenLegacyCursorError, RevokedDeviceError, DeviceNotRegisteredError } from "../src/storage/DurableInbox.js";
import { pgTestUrl } from "./support/integrationBackends.js";

// Audit R4 F2 — legacy-cursor fail-close. A cursor registered by the single-device
// CLAIM path carries device_public_key = NULL (no DeviceInboxBindingV1 proof). Once the
// per-device fan-out gate is OPEN (maxDevices > 1) an account can hold N devices, so a
// null-key cursor is no longer attributable to a proven key: read / drain / ack MUST fail
// closed (UnprovenLegacyCursorError) until a device.bind backfills the key. Gate CLOSED
// (maxDevices == 1) leaves the legacy single-device path byte-identical. This is the read
// side of the migration; the backfill (registerDevice with a key) already existed.
const PG_URL = pgTestUrl();
const bytes = (...n) => new Uint8Array(n);

test(
  "PgDurableInbox F2 legacy-cursor fail-close under an open fan-out gate",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_durable_inbox_f2";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    // Two views of the SAME store: the gate CLOSED one seeds a legacy null-key cursor (as a
    // pre-flip node would), the gate OPEN one is the node AFTER an operator enables fan-out.
    const closed = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const open = new PgDurableInbox({ connection: conn, maxDevices: 8 });

    await t.test("gate CLOSED: a legacy null-key cursor reads / drains / acks (byte-identical legacy path)", async () => {
      const id = "ib-legacy-closed";
      await closed.registerDevice(id, "rez:dev:legacy"); // legacy claim: null key
      assert.equal((await closed.getDevice(id, "rez:dev:legacy")).devicePublicKeyB64, null);
      await closed.append(id, bytes(1));
      await closed.append(id, bytes(2));
      assert.deepEqual((await closed.readAfterCursor(id, "rez:dev:legacy", 50)).map((e) => e.seq), [1, 2]);
      assert.deepEqual((await closed.cursorAck(id, "rez:dev:legacy", 2)), { lastSeq: 2 });
    });

    await t.test("gate OPEN: read / drain / ack on a legacy null-key cursor all fail closed (device.bind required)", async () => {
      const id = "ib-legacy-open";
      // Seed the legacy row + mail via the CLOSED view (a pre-flip cursor); registerDevice on
      // the OPEN view would refuse to CREATE an unproven cursor, so the row must predate the flip.
      await closed.registerDevice(id, "rez:dev:legacy");
      await open.append(id, bytes(1));
      await open.append(id, bytes(2));

      const isUnproven = (err) => err instanceof UnprovenLegacyCursorError && err.code === "DEVICE_UNPROVEN";
      await assert.rejects(() => open.readAfterCursor(id, "rez:dev:legacy", 50), isUnproven);
      await assert.rejects(() => open.readUndelivered(id, "rez:dev:legacy", 50), isUnproven);
      await assert.rejects(() => open.cursorAck(id, "rez:dev:legacy", 2), isUnproven);

      // Fail-close leaves NO state advance: cursor + delivered watermark stay at 0.
      const dev = await open.getDevice(id, "rez:dev:legacy");
      assert.equal(dev.lastSeq, 0);
      assert.equal(dev.lastDelivered, 0);
    });

    await t.test("gate OPEN: device.bind backfills the key, then reads / acks succeed", async () => {
      const id = "ib-backfill-open";
      await closed.registerDevice(id, "rez:dev:bf"); // legacy null-key row
      await open.append(id, bytes(7)); // seq 1 (seq is per-inbox 1-based, not the payload)
      await open.append(id, bytes(8)); // seq 2
      await assert.rejects(() => open.readAfterCursor(id, "rez:dev:bf", 50), UnprovenLegacyCursorError);

      // device.bind: backfill the proven key (registerDevice on the open view backfills a
      // pre-existing null-key row — the exact unification the migration relies on).
      await open.registerDevice(id, "rez:dev:bf", { devicePublicKeyB64: "PUBKEY-BF" });
      assert.equal((await open.getDevice(id, "rez:dev:bf")).devicePublicKeyB64, "PUBKEY-BF");

      // Now proven → reads / drains / acks flow normally even with the gate open.
      assert.deepEqual((await open.readAfterCursor(id, "rez:dev:bf", 50)).map((e) => e.seq), [1, 2]);
      assert.deepEqual(await open.cursorAck(id, "rez:dev:bf", 2), { lastSeq: 2 });
    });

    await t.test("gate OPEN: a PROVEN cursor (bound key from the start) is unaffected", async () => {
      const id = "ib-proven-open";
      await open.registerDevice(id, "rez:dev:proven", { devicePublicKeyB64: "PUBKEY-P" });
      await open.append(id, bytes(3)); // seq 1
      assert.deepEqual((await open.readAfterCursor(id, "rez:dev:proven", 50)).map((e) => e.seq), [1]);
      assert.deepEqual((await open.readUndelivered(id, "rez:dev:proven", 50)).map((e) => e.seq), []); // already delivered
      assert.deepEqual(await open.cursorAck(id, "rez:dev:proven", 1), { lastSeq: 1 });
    });

    await t.test("assertReadable (the mailbox.fetch gate, No-Go P1#1) mirrors the read-path gate exactly", async () => {
      // assertReadable is the SSOT gate for the random-access fetch surface, which does not
      // otherwise touch the device cursor. It must throw the SAME typed errors as list/drain/ack.
      const id = "ib-assert-readable";
      // Unregistered → DeviceNotRegisteredError (even under the open gate).
      await assert.rejects(() => open.assertReadable(id, "rez:dev:none"), DeviceNotRegisteredError);
      // Legacy null-key row (seeded gate-closed): readable when closed, refused when open.
      await closed.registerDevice(id, "rez:dev:legacy");
      await closed.assertReadable(id, "rez:dev:legacy"); // resolves (gate closed)
      await assert.rejects(() => open.assertReadable(id, "rez:dev:legacy"), UnprovenLegacyCursorError);
      // Proven key → readable under the open gate.
      await open.registerDevice(id, "rez:dev:legacy", { devicePublicKeyB64: "PUBKEY-AR" });
      await open.assertReadable(id, "rez:dev:legacy"); // resolves (proven)
      // Revoked → RevokedDeviceError (terminal), even though it is now proven.
      await open.revokeDevice(id, "rez:dev:legacy");
      await assert.rejects(() => open.assertReadable(id, "rez:dev:legacy"), RevokedDeviceError);
    });

    await t.test("gate OPEN: a REVOKED null-key cursor still reports revoked (terminal wins over unproven)", async () => {
      const id = "ib-revoked-open";
      await closed.registerDevice(id, "rez:dev:rv"); // legacy null-key
      await open.revokeDevice(id, "rez:dev:rv");
      // Revocation is terminal and checked first — a revoked device is DEVICE_REVOKED, not
      // the remediable DEVICE_UNPROVEN (binding a revoked device must not "un-revoke" it).
      await assert.rejects(() => open.readAfterCursor(id, "rez:dev:rv", 50), RevokedDeviceError);
      await assert.rejects(() => open.cursorAck(id, "rez:dev:rv", 1), RevokedDeviceError);
    });
  },
);
