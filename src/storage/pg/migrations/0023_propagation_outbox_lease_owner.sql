-- 0023_propagation_outbox_lease_owner — bind a lease to its OWNER device (audit leaf-3 req 4/5).
-- A lease token alone must NOT be transferable between devices/sessions: every token-bound op must
-- also match the owner that claimed it. And a revoked device must ATOMICALLY lose its lease (req 5,
-- enforced in PgAccountMutationSerializer's device.revoke fold), not merely wait out the TTL.
--
-- lease_owner is the claimant's device identity (canonical rez:dev:<64-lc-hex>). Present iff leased.
ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS lease_owner text;

-- UPGRADE SAFETY (audit leaf-3a F2): any lease that predates this column has lease_owner = NULL but
-- still carries a lease_token — which the owner_pair CHECK below would reject, failing the migration
-- on a database that ran leaf 2. A pre-owner lease cannot be attributed to a device, so reclaim it:
-- return it to 'pending' and clear the lease + frozen prepared epoch. next_attempt_at is left as-is
-- (its existing backoff, if any, still applies), so a legacy in-flight publication is simply re-driven
-- by a fresh owner-bound claim. This runs BEFORE the owner_pair constraint so the constraint can hold.
UPDATE account_propagation_outbox
  SET status = 'pending', lease_token = NULL, lease_owner = NULL,
      lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()
  WHERE kind = 'authority_state' AND status = 'leased';

-- lease_owner and lease_token appear/disappear together (leased ⇔ owner present ⇔ token present).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_owner_pair;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_owner_pair
    CHECK ((lease_owner IS NULL) = (lease_token IS NULL));

-- lease_owner is a CANONICAL device id (rez:dev:<64 lowercase hex>) — the same SSOT shape rez-core's
-- isCanonicalDeviceId enforces at every JS entry point. The DB backstops it so a raw write can never
-- persist a non-device owner; the fixed 72-char shape also bounds its size (no separate length CHECK).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_owner_len;
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_owner_shape;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_lease_owner_shape
    CHECK (lease_owner IS NULL OR lease_owner ~ '^rez:dev:[0-9a-f]{64}$');
