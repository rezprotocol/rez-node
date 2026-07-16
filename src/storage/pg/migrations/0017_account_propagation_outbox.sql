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
--   * Outstanding rows are NEVER pruned — each is a durable publish obligation. A row goes
--     'pending' → 'leased' when a client claims the account head, and reaches 'done' ONLY via a
--     VERIFIED publication that completes every obligation <= the published epoch (leaf 4). A
--     failed/expired lease returns the row to 'pending' (never abandoned).
--   * NOTE (superseded by 0018/0019): the CUMULATIVE drain contract leases the NEWEST account
--     head, not the oldest — see PgPropagationOutbox's docstring + the (account_identity, epoch)
--     index migration 0018 installs (this file's original oldest-first index is replaced there).
--
-- Identity is (account_identity, epoch, kind): each real fold bumps to a unique epoch, so enqueue
-- is naturally one-per-fold; the PK makes it idempotent under any retry.
CREATE TABLE IF NOT EXISTS account_propagation_outbox (
  account_identity  text        NOT NULL,
  epoch             bigint      NOT NULL,
  kind              text        NOT NULL DEFAULT 'authority_state',
  status            text        NOT NULL DEFAULT 'pending',   -- 'pending' | 'leased' | 'done'
  attempts          integer     NOT NULL DEFAULT 0,           -- saturating publish attempts (leaf 2)
  lease_token       text,                                     -- null unless 'leased' (leaf 2)
  lease_expires_at  timestamptz,                              -- null unless 'leased' (leaf 2)
  enqueued_at       timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, epoch, kind)
);

-- Original oldest-first index — REPLACED by migration 0018's (account_identity, epoch) index for
-- the newest-account-head drain. Kept here only as the schema's historical state.
CREATE INDEX IF NOT EXISTS account_propagation_outbox_pending
  ON account_propagation_outbox (enqueued_at)
  WHERE status = 'pending';
