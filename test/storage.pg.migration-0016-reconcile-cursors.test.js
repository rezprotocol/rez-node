import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { pgTestUrl } from "./support/integrationBackends.js";

// Audit R4 F3-remediation round-6 finding 2: the legacy device.revoke flipped ONLY
// device_cursors.revoked, leaving a registry row `{status:'active', cursor.revoked:true,
// tombstoned:false, cert_revoked:false}` — which passes both new reconnect guards and retains
// authority. Migration 0015 backfilled only status='revoked' rows, so it misses this exact
// state. Migration 0016 reconciles a revoked cursor into the full terminal state. We seed the
// exploit state RAW (as a pre-guard deploy left it), apply 0016, and assert full terminalization.

const PG_URL = pgTestUrl();
const MIG_0016 = readFileSync(
  fileURLToPath(new URL("../src/storage/pg/migrations/0016_reconcile_legacy_cursor_revokes.sql", import.meta.url)),
  "utf8",
);

test(
  "migration 0016: reconciles a legacy cursor-only revoke into full terminal authority state",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_mig_0016_reconcile";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();

    const A = "B-SIGN-MIG-0016";
    const canonicalCert = "rez:cap:" + "a".repeat(64);
    const dev = "rez:dev:" + "b".repeat(64);
    const inbox = "inbox-legacy-split";

    // The historical split-brain state a legacy device.revoke left behind.
    await conn.query(
      "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
        + " VALUES ($1,$2,$3,$4,3,'active')",
      [A, dev, inbox, canonicalCert],
    );
    await conn.query("INSERT INTO device_cursors (inbox_id, device_id, revoked) VALUES ($1,$2,true)", [inbox, dev]);
    await conn.query("INSERT INTO account_authority (account_identity, epoch, min_valid_issued_at_ms) VALUES ($1,3,0)", [A]);

    // Round-8 finding 2: a legacy BIND-ONLY account can have a registry/cursor row but NO
    // account_authority row (device.bind's enrollWithCursor never creates one). 0016 must
    // CREATE that row (upsert), not do a zero-row UPDATE that leaves the epoch at 0.
    const C = "B-SIGN-MIG-0016-BINDONLY";
    const certC = "rez:cap:" + "c".repeat(64);
    const devC = "rez:dev:" + "d".repeat(64);
    const inboxC = "inbox-bindonly";
    await conn.query(
      "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
        + " VALUES ($1,$2,$3,$4,0,'active')",
      [C, devC, inboxC, certC],
    );
    await conn.query("INSERT INTO device_cursors (inbox_id, device_id, revoked) VALUES ($1,$2,true)", [inboxC, devC]);
    // NOTE: no account_authority row for C.

    // Pre-reconciliation: this is exactly the auditor's reproduced exploit state.
    const before = await conn.query(
      "SELECT r.status, c.revoked,"
        + " EXISTS(SELECT 1 FROM account_revoked_device t WHERE t.account_identity=r.account_identity AND t.device_id=r.device_id) AS tombstoned,"
        + " EXISTS(SELECT 1 FROM account_revoked_cert rc WHERE rc.account_identity=r.account_identity AND rc.cert_id=r.cert_id) AS cert_revoked"
        + " FROM account_device_registry r JOIN device_cursors c ON c.inbox_id=r.inbox_id AND c.device_id=r.device_id"
        + " WHERE r.account_identity=$1 AND r.device_id=$2",
      [A, dev],
    );
    assert.deepEqual(
      { status: before.rows[0].status, revoked: before.rows[0].revoked, tombstoned: before.rows[0].tombstoned, cert_revoked: before.rows[0].cert_revoked },
      { status: "active", revoked: true, tombstoned: false, cert_revoked: false },
      "seeded the exact split-brain exploit state",
    );

    await conn.query(MIG_0016);

    const reg = await conn.query("SELECT status FROM account_device_registry WHERE account_identity=$1 AND device_id=$2", [A, dev]);
    assert.equal(reg.rows[0].status, "revoked", "registry row is now terminal");
    const tomb = await conn.query("SELECT 1 FROM account_revoked_device WHERE account_identity=$1 AND device_id=$2", [A, dev]);
    assert.equal(tomb.rowCount, 1, "the device is now tombstoned");
    const rc = await conn.query("SELECT 1 FROM account_revoked_cert WHERE account_identity=$1 AND cert_id=$2", [A, canonicalCert]);
    assert.equal(rc.rowCount, 1, "the device's bound cert is now revoked");
    const auth = await conn.query("SELECT epoch FROM account_authority WHERE account_identity=$1", [A]);
    assert.equal(Number(auth.rows[0].epoch), 4, "the authority epoch was bumped 3 -> 4");

    // Round-8 finding 2: ONE consistent epoch stamped across EVERY reconciled row for A (not
    // registry@3 while authority@4).
    const aReg = await conn.query("SELECT authority_epoch FROM account_device_registry WHERE account_identity=$1 AND device_id=$2", [A, dev]);
    const aTomb = await conn.query("SELECT revoked_at_epoch FROM account_revoked_device WHERE account_identity=$1 AND device_id=$2", [A, dev]);
    const aCert = await conn.query("SELECT revoked_at_epoch FROM account_revoked_cert WHERE account_identity=$1 AND cert_id=$2", [A, canonicalCert]);
    assert.equal(Number(aReg.rows[0].authority_epoch), 4, "registry stamped at the new epoch 4");
    assert.equal(Number(aTomb.rows[0].revoked_at_epoch), 4, "tombstone stamped at the new epoch 4");
    assert.equal(Number(aCert.rows[0].revoked_at_epoch), 4, "cert-revoke stamped at the new epoch 4");

    // Round-8 finding 2: the bind-only account C had its authority row CREATED at epoch 1, and
    // every reconciled row is terminal + stamped at 1.
    const cAuth = await conn.query("SELECT epoch FROM account_authority WHERE account_identity=$1", [C]);
    assert.equal(cAuth.rowCount, 1, "the missing authority row was CREATED (not a zero-row UPDATE)");
    assert.equal(Number(cAuth.rows[0].epoch), 1, "the created authority row is at epoch 1");
    const cReg = await conn.query("SELECT status, authority_epoch FROM account_device_registry WHERE account_identity=$1 AND device_id=$2", [C, devC]);
    assert.equal(cReg.rows[0].status, "revoked", "C's registry row is terminal");
    assert.equal(Number(cReg.rows[0].authority_epoch), 1, "C's registry stamped at epoch 1");
    const cTomb = await conn.query("SELECT revoked_at_epoch FROM account_revoked_device WHERE account_identity=$1 AND device_id=$2", [C, devC]);
    assert.equal(Number(cTomb.rows[0].revoked_at_epoch), 1, "C is tombstoned at epoch 1");
    const cCert = await conn.query("SELECT revoked_at_epoch FROM account_revoked_cert WHERE account_identity=$1 AND cert_id=$2", [C, certC]);
    assert.equal(Number(cCert.rows[0].revoked_at_epoch), 1, "C's cert revoked at epoch 1");

    // Round-7 finding 4: IDEMPOTENT — re-running reconciles nothing new (the row is already
    // 'revoked'), so the epoch is NOT bumped again.
    await conn.query(MIG_0016);
    const authAgain = await conn.query("SELECT epoch FROM account_authority WHERE account_identity=$1", [A]);
    assert.equal(Number(authAgain.rows[0].epoch), 4, "re-running 0016 does not re-bump the epoch");

    // Round-7 finding 4: an ALREADY-correctly-revoked device (serializer output: status='revoked'
    // with a revoked cursor) must NOT trigger an epoch bump for its account.
    const B = "B-SIGN-MIG-0016-CLEAN";
    const devB = "rez:dev:" + "e".repeat(64);
    const inboxB = "inbox-clean";
    await conn.query(
      "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
        + " VALUES ($1,$2,$3,NULL,7,'revoked')",
      [B, devB, inboxB],
    );
    await conn.query("INSERT INTO device_cursors (inbox_id, device_id, revoked) VALUES ($1,$2,true)", [inboxB, devB]);
    await conn.query("INSERT INTO account_authority (account_identity, epoch, min_valid_issued_at_ms) VALUES ($1,7,0)", [B]);
    await conn.query(MIG_0016);
    const authB = await conn.query("SELECT epoch FROM account_authority WHERE account_identity=$1", [B]);
    assert.equal(Number(authB.rows[0].epoch), 7, "an already-revoked device does not trigger an epoch bump");
  },
);
