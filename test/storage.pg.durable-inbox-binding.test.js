import test from "node:test";
import assert from "node:assert/strict";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { DeviceKeyMismatchError } from "../src/storage/DurableInbox.js";

// S2.5 Slice 4 leaf A: the home persists the PROVEN device key behind a device
// cursor (its copy of the verified DeviceInboxBindingV1). Real Postgres — the
// device_public_key column + registerDevice binding semantics + getDevice read.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

test(
  "PgDurableInbox device-inbox binding (device_public_key) against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_durable_inbox_binding";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");
    const inbox = new PgDurableInbox({ connection: conn });

    await t.test("legacy claim path (no device key) registers a cursor with a null bound key", async () => {
      const id = "ib-legacy";
      await inbox.registerDevice(id, "rez:dev:legacy");
      const dev = await inbox.getDevice(id, "rez:dev:legacy");
      assert.equal(dev.devicePublicKeyB64, null);
      assert.equal(dev.revoked, false);
      assert.equal(dev.lastSeq, 0);
    });

    await t.test("proven device.bind path stores the device public key", async () => {
      const id = "ib-bound";
      await inbox.registerDevice(id, "rez:dev:bound", { devicePublicKeyB64: "PUBKEY-AAA" });
      const dev = await inbox.getDevice(id, "rez:dev:bound");
      assert.equal(dev.devicePublicKeyB64, "PUBKEY-AAA");
    });

    await t.test("re-register with the SAME key is an idempotent no-op", async () => {
      const id = "ib-idem";
      await inbox.registerDevice(id, "rez:dev:x", { devicePublicKeyB64: "PUBKEY-X" });
      await inbox.registerDevice(id, "rez:dev:x", { devicePublicKeyB64: "PUBKEY-X" });
      const dev = await inbox.getDevice(id, "rez:dev:x");
      assert.equal(dev.devicePublicKeyB64, "PUBKEY-X");
    });

    await t.test("backfill: a legacy null-key row gains its proven key on bind (unification)", async () => {
      const id = "ib-backfill";
      await inbox.registerDevice(id, "rez:dev:bf"); // legacy: null key
      assert.equal((await inbox.getDevice(id, "rez:dev:bf")).devicePublicKeyB64, null);
      await inbox.registerDevice(id, "rez:dev:bf", { devicePublicKeyB64: "PUBKEY-BF" });
      assert.equal((await inbox.getDevice(id, "rez:dev:bf")).devicePublicKeyB64, "PUBKEY-BF");
    });

    await t.test("a differing key for the same deviceId is refused (substitution guard)", async () => {
      const id = "ib-mismatch";
      await inbox.registerDevice(id, "rez:dev:m", { devicePublicKeyB64: "PUBKEY-ONE" });
      await assert.rejects(
        () => inbox.registerDevice(id, "rez:dev:m", { devicePublicKeyB64: "PUBKEY-TWO" }),
        (err) => err instanceof DeviceKeyMismatchError && err.code === "DEVICE_KEY_MISMATCH",
      );
      // The stored binding is unchanged.
      assert.equal((await inbox.getDevice(id, "rez:dev:m")).devicePublicKeyB64, "PUBKEY-ONE");
    });

    await t.test("getDevice returns null for an unregistered device", async () => {
      assert.equal(await inbox.getDevice("ib-none", "rez:dev:none"), null);
    });
  },
);
