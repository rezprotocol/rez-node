-- 0016_reconcile_legacy_cursor_revokes — audit R4 F3-remediation round-6 finding 2. The
-- legacy DeviceHandler.device.revoke path (now fail-closed on Pg) flipped ONLY
-- device_cursors.revoked=true; it never touched account_device_registry.status, the
-- tombstone set, the bound cert, or the authority epoch. Migration 0015 backfilled tombstones
-- only for rows ALREADY marked status='revoked', so it MISSES these historical split-brain
-- rows — a device left `{status:'active', cursor.revoked:true, tombstoned:false,
-- cert_revoked:false}` passes both new reconnect guards and retains account authority.
--
-- Reconcile every registry row whose delivery cursor is revoked into the FULL terminal state
-- the serializer would have produced: status='revoked' + tombstone + its bound cert revoked +
-- an authority-epoch bump. Joined on (inbox_id, device_id) — a bound device always has both a
-- registry row and a cursor.
--
-- Round-7 finding 4 + round-8 finding 2: this MUST be idempotent, limited to the rows it
-- genuinely reconciles, AND consistent — one NEXT epoch per affected account, stamped on EVERY
-- reconciled row (registry, tombstone, cert) and on the authority row itself. A single statement:
--
--   reconciled — the still-'active' rows whose cursor is revoked (read on the shared snapshot).
--   bumped     — UPSERT one authority row per affected account and derive its NEXT epoch (1 for a
--                legacy bind-only account with no authority row yet — round-8 finding 2; else
--                epoch+1). This creates the missing row instead of a zero-row UPDATE.
--   flipped    — flip each reconciled registry row to 'revoked' stamping the SAME derived epoch,
--                returning it so the tombstone + cert writes use that one epoch too.
--
-- Idempotent: on a re-run `reconciled` is empty (all already 'revoked') ⇒ `accts` empty ⇒ `bumped`
-- writes nothing ⇒ no epoch bump; already-correctly-revoked devices are never touched.
WITH reconciled AS (
  SELECT r.account_identity, r.device_id, r.inbox_id, r.cert_id
  FROM account_device_registry r
  JOIN device_cursors c ON c.inbox_id = r.inbox_id AND c.device_id = r.device_id
  WHERE c.revoked = true AND r.status <> 'revoked'
),
accts AS (
  SELECT DISTINCT account_identity FROM reconciled
),
bumped AS (
  INSERT INTO account_authority (account_identity, epoch)
    SELECT account_identity, 1 FROM accts
    ON CONFLICT (account_identity) DO UPDATE SET epoch = account_authority.epoch + 1, updated_at = now()
    RETURNING account_identity, epoch
),
flipped AS (
  UPDATE account_device_registry r
    SET status = 'revoked', authority_epoch = b.epoch, updated_at = now()
    FROM reconciled rec, bumped b
    WHERE r.account_identity = rec.account_identity
      AND r.device_id = rec.device_id
      AND b.account_identity = rec.account_identity
    RETURNING r.account_identity, r.device_id, r.cert_id, b.epoch AS new_epoch
),
tombstoned AS (
  INSERT INTO account_revoked_device (account_identity, device_id, revoked_at_epoch)
    SELECT account_identity, device_id, new_epoch FROM flipped
    ON CONFLICT (account_identity, device_id) DO NOTHING
)
-- After migration 0014 every stored cert_id is canonical or NULL, so this satisfies the CHECK.
INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch)
  SELECT account_identity, cert_id, new_epoch FROM flipped WHERE cert_id IS NOT NULL
  ON CONFLICT (account_identity, cert_id) DO NOTHING;
