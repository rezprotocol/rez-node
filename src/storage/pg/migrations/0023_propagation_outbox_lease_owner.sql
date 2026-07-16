-- 0023_propagation_outbox_lease_owner — bind a lease to its OWNER device (audit leaf-3 req 4/5).
-- A lease token alone must NOT be transferable between devices/sessions: every token-bound op must
-- also match the owner that claimed it. And a revoked device must ATOMICALLY lose its lease (req 5,
-- enforced in PgAccountMutationSerializer's device.revoke fold), not merely wait out the TTL.
--
-- lease_owner is the claimant's device identity. Present iff the row is leased. This migration
-- size-bounds it; the CANONICAL rez:dev:<64-lc-hex> shape CHECK is added by the FORWARD migration
-- 0024 (the runner only applies versions above its recorded max, so a database already at version 23
-- receives the canonical repair via 0024 rather than a silent in-place edit of this file).
ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS lease_owner text;

-- UPGRADE SAFETY (audit leaf-3a F2): a lease that predates this column has lease_owner = NULL but
-- still carries a lease_token — which the owner_pair CHECK below would reject, failing the migration
-- on a database that ran leaf 2. Such a lease cannot be attributed to a device, so reclaim it: return
-- it to 'pending' and clear the lease + frozen prepared epoch. The predicate is restricted to
-- ownerless (or otherwise non-canonical) owners so RE-EXECUTION can never release a healthy
-- owner-bound lease. next_attempt_at is left as-is (existing backoff still applies) so a legacy
-- in-flight publication is simply re-driven by a fresh owner-bound claim. Runs BEFORE owner_pair.
UPDATE account_propagation_outbox
  SET status = 'pending', lease_token = NULL, lease_owner = NULL,
      lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()
  WHERE kind = 'authority_state' AND status = 'leased'
    AND (lease_owner IS NULL OR lease_owner !~ '^rez:dev:[0-9a-f]{64}$');

-- lease_owner and lease_token appear/disappear together (leased ⇔ owner present ⇔ token present).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_owner_pair;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_owner_pair
    CHECK ((lease_owner IS NULL) = (lease_token IS NULL));

-- Size-bound the owner id (the canonical shape — which also bounds size — arrives in 0024).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_owner_len;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_lease_owner_len
    CHECK (lease_owner IS NULL OR (octet_length(lease_owner) BETWEEN 1 AND 128));
