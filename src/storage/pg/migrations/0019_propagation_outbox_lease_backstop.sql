-- 0019_propagation_outbox_lease_backstop — DB backstop for the head-advancing account lease
-- (P1#3 leaf 2). The lease is ACCOUNT-scoped: one token covers (account, authority_state); at
-- most one leased row per (account, kind); a verified publication of the current epoch M
-- completes every pending obligation <= M. These invariants are enforced at the DB layer so no
-- application bug (or a stale/racing drainer) can lease two heads or desync status from the lease.

-- (a) status <-> lease correlation. Combined with 0018's lease-pair check
--     (lease_token IS NULL) = (lease_expires_at IS NULL), this ties status/token/expiry:
--       'leased'        => lease_token + lease_expires_at present
--       'pending'/'done' => lease_token + lease_expires_at absent
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_status_lease;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_status_lease
    CHECK ((status = 'leased') = (lease_token IS NOT NULL));

-- (b) at most ONE leased row per (account, kind) — the DB backstop against N and N+1 being
--     leased concurrently. The head-advancing account lease is singular by construction.
DROP INDEX IF EXISTS account_propagation_outbox_one_lease;
CREATE UNIQUE INDEX IF NOT EXISTS account_propagation_outbox_one_lease
  ON account_propagation_outbox (account_identity, kind)
  WHERE status = 'leased';
