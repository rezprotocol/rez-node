-- 0017_account_propagation_outbox — P1#2/P1#3 propagation outbox (leaf 1: schema only).
--
-- A durable, NODE-OWNED queue of authority-state publication obligations. A row is enqueued
-- ATOMICALLY inside PgAccountMutationSerializer's fold transaction on every REAL epoch-changing
-- device mutation (never on a semantic no-op, a stale expectedRevision, or an idempotent replay —
-- those commit and return before the fold). A client (rez-chat) later LEASES a row, signs +
-- publishes the account-signed AccountAuthorityStateV1 for that epoch, verifies the durable
-- record, and acknowledges it (leaves 2–4).
--
-- OWNERSHIP / SCOPE (audit P1#3a):
--   * This queue covers ONLY the single account-signed authority-state record — the node CAN
--     verify its publication by coordinate/hash. It does NOT cover peer-specific device-set
--     fan-out: the node does not know the account's peer list, so one row can never prove global
--     peer convergence. Peer fan-out is a SEPARATE client-owned per-peer queue, never this table.
--   * Contains NO secrets and NO peer identities — only the account's OWN identity (already stored
--     across the authority tables) + the epoch to publish + queue bookkeeping.
--   * Pending rows are NEVER pruned — a pending row is a durable publish obligation. Rows leave
--     'pending' only via an acknowledged publication (leaf 4) or a compensating terminal action.
--
-- Identity is (account_identity, epoch, kind): each real fold bumps to a unique epoch, so enqueue
-- is naturally one-per-fold; the PK makes it idempotent under any retry.
CREATE TABLE IF NOT EXISTS account_propagation_outbox (
  account_identity  text        NOT NULL,
  epoch             bigint      NOT NULL,
  kind              text        NOT NULL DEFAULT 'authority_state',
  status            text        NOT NULL DEFAULT 'pending',   -- 'pending' | 'done' (drainer, leaf 2+)
  attempts          integer     NOT NULL DEFAULT 0,           -- publish attempts (leaf 2+)
  lease_token       text,                                     -- null until claimed (leaf 2)
  lease_expires_at  timestamptz,                              -- null until claimed (leaf 2)
  enqueued_at       timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, epoch, kind)
);

-- Drainer scan (leaf 2): oldest pending first, cheap under a partial index.
CREATE INDEX IF NOT EXISTS account_propagation_outbox_pending
  ON account_propagation_outbox (enqueued_at)
  WHERE status = 'pending';
