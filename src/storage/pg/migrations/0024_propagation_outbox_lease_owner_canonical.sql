-- 0024_propagation_outbox_lease_owner_canonical — enforce the CANONICAL device-id shape on
-- lease_owner (audit leaf-3a F1), as a FORWARD migration. MigrationRunner applies only versions
-- above its recorded max, so a database already at version 23 (which carries only the length bound
-- from 0023) would never receive the canonical constraint if it were edited into 0023 in place; a
-- new version is the only mechanism that reaches such a database.
--
-- Idempotent + non-destructive to healthy state:
--   1. Reclaim ONLY ownerless / non-canonical leases (a length-bounded but malformed owner was
--      possible under the 0023 length CHECK) back to 'pending'; a valid canonical owner-bound lease
--      is PRESERVED. next_attempt_at is left as-is so its existing backoff still applies.
--   2. Replace the length CHECK with the canonical rez:dev:<64-lc-hex> shape CHECK (which also bounds
--      size). The reclaim above clears every owner that would violate the new CHECK, so ADD succeeds;
--      the DROP ... IF EXISTS pair makes the swap safe to re-run.
UPDATE account_propagation_outbox
  SET status = 'pending', lease_token = NULL, lease_owner = NULL,
      lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()
  WHERE kind = 'authority_state' AND status = 'leased'
    AND (lease_owner IS NULL OR lease_owner !~ '^rez:dev:[0-9a-f]{64}$');

ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_owner_len;
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_owner_shape;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_lease_owner_shape
    CHECK (lease_owner IS NULL OR lease_owner ~ '^rez:dev:[0-9a-f]{64}$');
