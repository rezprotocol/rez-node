-- 0018_propagation_outbox_constraints — harden account_propagation_outbox BEFORE its
-- status/lease columns become remotely-driven state (audit P1#3 leaf-1.1). 0017 shipped the
-- columns permissively; these invariants land as a FOLLOW-UP migration because 0017 may already
-- be recorded as applied locally and in test databases.
--
-- Also adds next_attempt_at (retry-backoff availability): a leased/failed row is not re-eligible
-- until now() >= next_attempt_at. Enqueue leaves it at now() (immediately available).

ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();

-- epoch is a real, positive authority epoch.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_epoch_positive;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_epoch_positive CHECK (epoch > 0);

-- One exact kind for now — the single account-signed authority-state record.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_kind_known;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_kind_known CHECK (kind = 'authority_state');

-- Bounded status lifecycle: pending (enqueued) → leased (a client holds it) → done (verified).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_status_known;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_status_known CHECK (status IN ('pending', 'leased', 'done'));

ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_attempts_nonneg;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_attempts_nonneg CHECK (attempts >= 0);

-- The lease is a (token, expiry) PAIR — both null (unleased) or both non-null (leased).
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_pair;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_lease_pair
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL));

-- Bound the (untrusted, client-echoed) lease-token size.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_lease_token_len;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_lease_token_len
    CHECK (lease_token IS NULL OR (octet_length(lease_token) BETWEEN 1 AND 128));

-- The queue is drained NEWEST-epoch-first per account (authority_state is CUMULATIVE — a verified
-- publication of epoch N satisfies every pending obligation <= N), so index by (account, epoch),
-- not the oldest-first enqueue order 0017 created.
DROP INDEX IF EXISTS account_propagation_outbox_pending;
CREATE INDEX IF NOT EXISTS account_propagation_outbox_pending
  ON account_propagation_outbox (account_identity, epoch)
  WHERE status = 'pending';
