import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";

// Audit R4 F3-remediation finding 3 (+ round-4 findings 2/3): migration 0014 is the
// explicit upgrade path that quarantines pre-remediation NON-canonical durable cert ids so
// they can never poison the authority path. It cleans (1) the live account_revoked_cert +
// account_device_registry tables, (2) malformed journal replay snapshots (forcing the
// replayExpired/current-state path), and (3) re-fences with DB CHECK constraints so an old
// node cannot re-poison the columns. The migration is idempotent, so we run the full chain
// (which applies 0014 once against empty tables + adds the constraints), DROP the
// constraints to seed pre-guard malformed rows written RAW, then re-apply the migration SQL
// and assert exactly the malformed values are cleaned and the constraints re-enforce.

const PG_URL = process.env.REZ_PG_TEST_URL || "";
const MIG_0014 = readFileSync(
  fileURLToPath(new URL("../src/storage/pg/migrations/0014_canonical_cert_ids.sql", import.meta.url)),
  "utf8",
);

test(
  "migration 0014: quarantines non-canonical cert ids in live tables + journal snapshots, and re-fences with CHECK constraints",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_mig_0014_canonical";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();

    const A = "B-SIGN-MIG-0014";
    const canonicalCert = "rez:cap:" + "a".repeat(64);
    const malformedCert = "rez:cap:leaf-legacy"; // pre-remediation, non-canonical
    const devCanon = "rez:dev:" + "b".repeat(64);
    const devMalformed = "rez:dev:" + "c".repeat(64);

    // 0014 already added the CHECK constraints at migrate() time. Drop them so we can seed
    // pre-guard malformed rows exactly as an old deploy would have left them.
    await conn.query("ALTER TABLE account_revoked_cert DROP CONSTRAINT IF EXISTS account_revoked_cert_cert_id_canonical");
    await conn.query("ALTER TABLE account_device_registry DROP CONSTRAINT IF EXISTS account_device_registry_cert_id_canonical");

    // Live tables: a canonical row and a legacy malformed row in each.
    await conn.query(
      "INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch) VALUES ($1,$2,1),($1,$3,1)",
      [A, canonicalCert, malformedCert],
    );
    await conn.query(
      "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
        + " VALUES ($1,$2,'inbox-canon',$3,1,'active'),($1,$4,'inbox-mal',$5,1,'active')",
      [A, devCanon, canonicalCert, devMalformed, malformedCert],
    );

    // Journal: a malformed replay snapshot (round-4 finding 2) + a clean one that must survive.
    await conn.query("INSERT INTO account_authority (account_identity, epoch, min_valid_issued_at_ms) VALUES ($1,5,0)", [A]);
    const malformedSnapshot = JSON.stringify({ revision: 5, devices: [], authorityState: { epoch: 5, revokedCertIds: [malformedCert], minValidIssuedAtMs: 0 } });
    const cleanSnapshot = JSON.stringify({ revision: 4, devices: [], authorityState: { epoch: 4, revokedCertIds: [canonicalCert], minValidIssuedAtMs: 0 } });
    // Round-5 finding 5: a JSON null element in the array (which jsonb_array_elements_text
    // would collapse to SQL NULL and slip past a text regex), and a non-array revokedCertIds.
    const nullElemSnapshot = JSON.stringify({ revision: 6, devices: [], authorityState: { epoch: 6, revokedCertIds: [canonicalCert, null], minValidIssuedAtMs: 0 } });
    const nonArraySnapshot = JSON.stringify({ revision: 7, devices: [], authorityState: { epoch: 7, revokedCertIds: "not-an-array", minValidIssuedAtMs: 0 } });
    await conn.query(
      "INSERT INTO account_device_mutation (account_identity, op_id, epoch, action, target_device_id, target_cert_id, result_json)"
        + " VALUES ($1,'legacy-op',5,'device.revoke',$2,NULL,$3::jsonb),($1,'clean-op',4,'device.revoke',$4,$5,$6::jsonb),"
        + " ($1,'null-elem-op',6,'device.revoke',$2,NULL,$7::jsonb),($1,'nonarray-op',7,'device.revoke',$2,NULL,$8::jsonb)",
      [A, devMalformed, malformedSnapshot, devCanon, canonicalCert, cleanSnapshot, nullElemSnapshot, nonArraySnapshot],
    );

    // Re-apply the (idempotent) migration → cleans malformed live rows + journal snapshot, re-adds constraints.
    await conn.query(MIG_0014);

    // (1) live tables cleaned.
    const revoked = await conn.query("SELECT cert_id FROM account_revoked_cert WHERE account_identity=$1 ORDER BY cert_id", [A]);
    assert.deepEqual(revoked.rows.map((r) => r.cert_id), [canonicalCert], "the malformed revoked cert was deleted; the canonical one kept");
    const regRows = await conn.query("SELECT device_id, cert_id FROM account_device_registry WHERE account_identity=$1 ORDER BY device_id", [A]);
    const byDev = Object.fromEntries(regRows.rows.map((r) => [r.device_id, r.cert_id]));
    assert.equal(regRows.rowCount, 2, "both device rows are KEPT (only the malformed cert is dropped)");
    assert.equal(byDev[devCanon], canonicalCert, "the canonical registry cert is untouched");
    assert.equal(byDev[devMalformed], null, "the malformed registry cert was NULLed");

    // (2) journal: malformed snapshot NULLed, clean snapshot survives.
    const jm = await conn.query("SELECT result_json FROM account_device_mutation WHERE account_identity=$1 AND op_id='legacy-op'", [A]);
    assert.equal(jm.rows[0].result_json, null, "the malformed-string journal replay snapshot was NULLed");
    const jc = await conn.query("SELECT result_json FROM account_device_mutation WHERE account_identity=$1 AND op_id='clean-op'", [A]);
    assert.ok(jc.rows[0].result_json, "the clean journal snapshot survived");
    // Round-5 finding 5: the null-element and non-array snapshots are also NULLed.
    const jn = await conn.query("SELECT result_json FROM account_device_mutation WHERE account_identity=$1 AND op_id='null-elem-op'", [A]);
    assert.equal(jn.rows[0].result_json, null, "a JSON null element in revokedCertIds is caught (NULLed)");
    const jna = await conn.query("SELECT result_json FROM account_device_mutation WHERE account_identity=$1 AND op_id='nonarray-op'", [A]);
    assert.equal(jna.rows[0].result_json, null, "a non-array revokedCertIds is caught (NULLed)");

    // (2b) replaying the pruned opId falls through to the clean current-state path.
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });
    const replay = await serializer.submitMutation({
      accountIdentityPublicKeyB64: A, opId: "legacy-op", expectedRevision: 5,
      action: "device.revoke", target: { revokedDeviceId: devMalformed },
    });
    assert.equal(replay.idempotentReplay, true);
    assert.equal(replay.replayExpired, true, "the pruned snapshot forces the replayExpired path");
    assert.deepEqual(replay.authorityState.revokedCertIds, [canonicalCert], "replay returns the CLEAN current revoked set, not the malformed snapshot");

    // (3) the CHECK constraints re-enforce: a malformed insert now fails at the DB.
    await assert.rejects(
      () => conn.query("INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch) VALUES ($1,'rez:cap:not-canonical',1)", [A]),
      /check constraint/i,
      "a non-canonical revoked-cert insert is rejected by the DB CHECK",
    );
    await assert.rejects(
      () => conn.query("INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status) VALUES ($1,$2,'inbox-x','rez:cap:bad',1,'active')", [A, "rez:dev:" + "d".repeat(64)]),
      /check constraint/i,
      "a non-canonical registry cert insert is rejected by the DB CHECK",
    );
  },
);
