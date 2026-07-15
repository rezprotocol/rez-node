-- 0014_canonical_cert_ids — audit R4 F3-remediation finding 3 (+ round-4 findings 2/3).
-- rez-core now enforces the EXACT canonical account-capability cert-id shape (rez:cap: +
-- 64 lowercase hex) on every record that carries one (mutation, capability-revoke,
-- authority-state, parent-cert), and PgAccountDeviceRegistry enforces it on every stored
-- device cert. A row written BEFORE that guard could still hold a non-canonical cert id,
-- which now poisons the authority path: AccountAuthorityStateV1's revokedCertIds validator
-- rejects it, so a home holding a malformed value could fail to publish/consume authority
-- state indefinitely, and device.revoke's Option A auto-revoke would trust a malformed
-- stored registry cert.
--
-- (1) Quarantine the two LIVE durable homes of a cert id.
--   * account_revoked_cert: DELETE any non-canonical row (never a real revoked cap cert).
--   * account_device_registry.cert_id: NULL any non-canonical binding — the device row is
--     KEPT (only its malformed cert is dropped); a later canonical device.bind can backfill
--     it, and revoking a NULL-cert device simply revokes no cert.
DELETE FROM account_revoked_cert
  WHERE cert_id !~ '^rez:cap:[0-9a-f]{64}$';

UPDATE account_device_registry
  SET cert_id = NULL, updated_at = now()
  WHERE cert_id IS NOT NULL AND cert_id !~ '^rez:cap:[0-9a-f]{64}$';

-- (2) Round-4 finding 2 — the mutation journal's replay payload (result_json) is a verbatim
-- snapshot returned on an idempotent replay. A pre-guard snapshot can carry a malformed
-- authorityState.revokedCertIds. NULL those payloads so a replay falls through to the
-- replayExpired path (which rebuilds the CURRENT, now-clean authority state). The audit row
-- (account, op_id, epoch, action, targets, committed_at) is untouched — this is the same
-- prunable-payload split migration 0013 established.
-- Round-5 finding 5: the predicate must catch a revokedCertIds that is present but NOT an
-- array, AND an array containing a JSON null / non-string element. Using
-- jsonb_array_elements_text would collapse a JSON null element to SQL NULL, and `NULL !~
-- regex` is NULL (not true) — so a null element would slip through. Iterate with
-- jsonb_array_elements (jsonb, not text) and reject any element whose jsonb_typeof is not
-- 'string' or whose text is non-canonical.
UPDATE account_device_mutation
  SET result_json = NULL
  WHERE result_json IS NOT NULL
    AND (result_json -> 'authorityState') ? 'revokedCertIds'
    AND (
      jsonb_typeof(result_json -> 'authorityState' -> 'revokedCertIds') <> 'array'
      OR EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE WHEN jsonb_typeof(result_json -> 'authorityState' -> 'revokedCertIds') = 'array'
               THEN result_json -> 'authorityState' -> 'revokedCertIds'
               ELSE '[]'::jsonb END
        ) AS c(elem)
        WHERE jsonb_typeof(c.elem) <> 'string'
           OR (c.elem #>> '{}') !~ '^rez:cap:[0-9a-f]{64}$'
      )
    );

-- (3) Round-4 finding 3 — a DB-level shape fence so an already-running OLD cluster node
-- cannot re-poison these columns AFTER the one-shot cleanup above. DROP-then-ADD makes the
-- statement idempotent (re-runnable). The constraints are added AFTER the cleanup, so no
-- surviving row violates them.
--
-- SCOPE NOTE: a shape constraint fences SYNTAX only. The SEMANTIC Option A change — a
-- device.revoke now revokes ONLY the target's own bound cert (not an arbitrary caller cert)
-- — cannot be enforced by a column CHECK (an old node could still revoke an unrelated but
-- syntactically-canonical cert). That transition therefore requires an OPERATIONAL fence:
-- drain/upgrade all writer nodes to the Option A serializer before/at this migration (a
-- single-writer-version deploy), not merely running this SQL against a live mixed cluster.
ALTER TABLE account_revoked_cert
  DROP CONSTRAINT IF EXISTS account_revoked_cert_cert_id_canonical;
ALTER TABLE account_revoked_cert
  ADD CONSTRAINT account_revoked_cert_cert_id_canonical
  CHECK (cert_id ~ '^rez:cap:[0-9a-f]{64}$');

ALTER TABLE account_device_registry
  DROP CONSTRAINT IF EXISTS account_device_registry_cert_id_canonical;
ALTER TABLE account_device_registry
  ADD CONSTRAINT account_device_registry_cert_id_canonical
  CHECK (cert_id IS NULL OR cert_id ~ '^rez:cap:[0-9a-f]{64}$');
