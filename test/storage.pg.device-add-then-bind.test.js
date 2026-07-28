import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { canonicalDeviceId } from "./helpers/deviceRegistryTestUtil.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { pgTestUrl } from "./support/integrationBackends.js";

// L5 (P1#2) — device.add THEN device.bind, against real Postgres.
//
// Registration-before-release means the home now has a registry row for a device BEFORE that
// device ever connects: device.add writes it (carrying the leaf certId) during the link ceremony,
// and the device runs device.bind later, on its first boot. So device.bind must land on an
// EXISTING row and be idempotent about it — a second writer for the same device, not a conflict —
// while still rejecting a bind that disagrees with what was registered.
//
// The device.add side is exercised through the registry's fold entry point (foldAddInTx), which is
// what PgAccountMutationSerializer calls inside its account transaction.
const PG_URL = pgTestUrl();
const cap = (h) => "rez:cap:" + createHash("sha256").update(String(h)).digest("hex");

test(
  "L5: device.bind after device.add is idempotent, and disagreement is still a conflict",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_device_add_then_bind";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();

    // maxDevices > 1: the fan-out gate OPEN is the configuration this whole slice targets, and it
    // is also the one where an unproven cursor is refused — so a bind that does NOT carry the
    // device key would leave no cursor at all.
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 4 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const ACCOUNT = "B-SIGN-ACCOUNT-L5";

    // device.add as the serializer performs it: inside a transaction, under the account lock,
    // stamping the new authority epoch.
    async function deviceAdd({ deviceId, inboxId, certId, epoch }) {
      return conn.withClient(async (client) => {
        await client.query("BEGIN");
        try {
          await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [ACCOUNT]);
          const row = await registry.foldAddInTx(client, {
            accountIdentityPublicKeyB64: ACCOUNT,
            deviceId,
            inboxId,
            certId,
            authorityEpoch: epoch,
          });
          await client.query("COMMIT");
          return row;
        } catch (err) {
          await client.query("ROLLBACK");
          throw err;
        }
      });
    }

    await t.test("device.add binds the certId; the later bind is a NO-OP enroll that proves the cursor", async () => {
      const dev = canonicalDeviceId("l5-happy");
      const inbox = "inbox-l5-happy";
      const cert = cap("l5-happy-leaf");

      const added = await deviceAdd({ deviceId: dev, inboxId: inbox, certId: cert, epoch: 1 });
      assert.equal(added.certId, cert, "the leaf certId is bound at ADD time — before any release");
      assert.equal(added.status, "active");
      // Registration alone creates no cursor: the device has not proven its key yet.
      const beforeBind = await conn.query(
        "SELECT device_public_key FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
        [inbox, dev],
      );
      assert.equal(beforeBind.rowCount, 0, "no cursor until the device binds");

      // device.bind, carrying the SAME (deviceId, inboxId, certId) plus the proven device key.
      const bound = await registry.enrollWithCursor({
        accountIdentityPublicKeyB64: ACCOUNT,
        deviceId: dev,
        inboxId: inbox,
        certId: cert,
        authorityEpoch: 1,
        devicePublicKeyB64: "device-pub-l5-happy",
      });
      assert.equal(bound.deviceId, dev);
      assert.equal(bound.certId, cert);

      // ONE registry row, and a PROVEN cursor.
      const rows = await conn.query(
        "SELECT device_id, inbox_id, cert_id, status FROM account_device_registry WHERE account_identity = $1 AND device_id = $2",
        [ACCOUNT, dev],
      );
      assert.equal(rows.rowCount, 1, "the bind reused the device.add row rather than making a second");
      assert.equal(rows.rows[0].cert_id, cert);
      const cursor = await conn.query(
        "SELECT device_public_key FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
        [inbox, dev],
      );
      assert.equal(cursor.rowCount, 1, "the bind created the cursor");
      assert.equal(cursor.rows[0].device_public_key, "device-pub-l5-happy", "and proved it with the device key");
    });

    await t.test("re-binding the same device again is idempotent (a retry must converge)", async () => {
      const dev = canonicalDeviceId("l5-retry");
      const inbox = "inbox-l5-retry";
      const cert = cap("l5-retry-leaf");
      await deviceAdd({ deviceId: dev, inboxId: inbox, certId: cert, epoch: 2 });

      const args = {
        accountIdentityPublicKeyB64: ACCOUNT,
        deviceId: dev,
        inboxId: inbox,
        certId: cert,
        authorityEpoch: 2,
        devicePublicKeyB64: "device-pub-l5-retry",
      };
      await registry.enrollWithCursor(args);
      await registry.enrollWithCursor(args);
      const again = await registry.enrollWithCursor(args);
      assert.equal(again.certId, cert);

      const rows = await conn.query(
        "SELECT 1 FROM account_device_registry WHERE account_identity = $1 AND device_id = $2",
        [ACCOUNT, dev],
      );
      assert.equal(rows.rowCount, 1);
      const cursors = await conn.query(
        "SELECT 1 FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
        [inbox, dev],
      );
      assert.equal(cursors.rowCount, 1, "still exactly one cursor");
    });

    await t.test("a bind naming a DIFFERENT inbox than the one registered is a conflict", async () => {
      // The registered inbox is the one the ceremony assigned and the one device.add committed;
      // re-pointing it at bind time would strand deposits already addressed to it.
      const dev = canonicalDeviceId("l5-inbox-mismatch");
      const cert = cap("l5-inbox-mismatch-leaf");
      await deviceAdd({ deviceId: dev, inboxId: "inbox-l5-registered", certId: cert, epoch: 3 });

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCOUNT,
          deviceId: dev,
          inboxId: "inbox-l5-somewhere-else",
          certId: cert,
          authorityEpoch: 3,
          devicePublicKeyB64: "device-pub-l5-inbox-mismatch",
        }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
    });

    await t.test("a bind carrying a DIFFERENT certId than the one registered is a conflict", async () => {
      // The bound certId is what a revoke names. A bind that silently replaced it would leave the
      // published authority state revoking a cert the device no longer presents.
      const dev = canonicalDeviceId("l5-cert-mismatch");
      const inbox = "inbox-l5-cert-mismatch";
      await deviceAdd({ deviceId: dev, inboxId: inbox, certId: cap("l5-registered-leaf"), epoch: 4 });

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCOUNT,
          deviceId: dev,
          inboxId: inbox,
          certId: cap("l5-some-other-leaf"),
          authorityEpoch: 4,
          devicePublicKeyB64: "device-pub-l5-cert-mismatch",
        }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );

      // The registered cert is untouched by the rejected attempt.
      const row = await registry.getDevice(ACCOUNT, dev);
      assert.equal(row.certId, cap("l5-registered-leaf"));
    });

    await t.test("a device REVOKED between add and bind cannot bind (fail closed)", async () => {
      // The pre-online window is exactly when an abandoned ceremony gets revoked. The device may
      // still hold a released leaf, so its bind must be refused rather than resurrecting it.
      const dev = canonicalDeviceId("l5-revoked-before-bind");
      const inbox = "inbox-l5-revoked-before-bind";
      const cert = cap("l5-revoked-leaf");
      await deviceAdd({ deviceId: dev, inboxId: inbox, certId: cert, epoch: 5 });
      await conn.query(
        "UPDATE account_device_registry SET status = 'revoked' WHERE account_identity = $1 AND device_id = $2",
        [ACCOUNT, dev],
      );

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCOUNT,
          deviceId: dev,
          inboxId: inbox,
          certId: cert,
          authorityEpoch: 5,
          devicePublicKeyB64: "device-pub-l5-revoked",
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
    });

    await t.test("a bind whose device key disagrees with an existing proven cursor is refused", async () => {
      const dev = canonicalDeviceId("l5-key-mismatch");
      const inbox = "inbox-l5-key-mismatch";
      const cert = cap("l5-key-mismatch-leaf");
      await deviceAdd({ deviceId: dev, inboxId: inbox, certId: cert, epoch: 6 });
      const base = {
        accountIdentityPublicKeyB64: ACCOUNT,
        deviceId: dev,
        inboxId: inbox,
        certId: cert,
        authorityEpoch: 6,
      };
      await registry.enrollWithCursor({ ...base, devicePublicKeyB64: "device-pub-l5-original" });
      await assert.rejects(
        () => registry.enrollWithCursor({ ...base, devicePublicKeyB64: "device-pub-l5-impostor" }),
        (err) => err.name === "DeviceKeyMismatchError" || /device key/i.test(String(err && err.message)),
      );
      const cursor = await conn.query(
        "SELECT device_public_key FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
        [inbox, dev],
      );
      assert.equal(cursor.rows[0].device_public_key, "device-pub-l5-original", "the proven key is unchanged");
    });
  },
);
