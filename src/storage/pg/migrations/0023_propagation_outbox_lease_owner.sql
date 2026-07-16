-- 0023_propagation_outbox_lease_owner — bind a lease to its OWNER device (audit leaf-3 req 4/5).
-- A lease token alone must NOT be transferable between devices/sessions: every token-bound op must
-- also match the owner that claimed it. And a revoked device must ATOMICALLY lose its lease (req 5,
-- enforced in PgAccountMutationSerializer's device.revoke fold), not merely wait out the TTL.
--
-- lease_owner is the claimant's device identity (rez:dev:...). Present iff the row is leased.
ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS lease_owner text;

-- lease_owner and lease_token appear/disappear together (leased ⇔ owner present ⇔ token present).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_owner_pair;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_owner_pair
    CHECK ((lease_owner IS NULL) = (lease_token IS NULL));

-- Bound the (untrusted, session-derived) owner id size.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_owner_len;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_lease_owner_len
    CHECK (lease_owner IS NULL OR (octet_length(lease_owner) BETWEEN 1 AND 128));
