-- 0004_registries — per-row migration of the two whole-blob KV registries so N
-- nodes don't clobber a single shared key, and inbox-claim uniqueness becomes a
-- DB constraint (atomic first-claim-wins) instead of an in-process mutex.

-- InboxClaimRegistry → one row per inbox; the PRIMARY KEY enforces
-- INBOX_ALREADY_CLAIMED atomically across the cluster (INSERT … ON CONFLICT).
CREATE TABLE IF NOT EXISTS inbox_claims (
  inbox_id        text        PRIMARY KEY,
  claimant_pubkey text        NOT NULL,
  claimed_at_ms   bigint      NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- HostedInboxRegistry → one row per claimant pubkey (was a single blob map).
CREATE TABLE IF NOT EXISTS hosted_inboxes (
  claimant_pubkey text        PRIMARY KEY,
  delegation      jsonb       NOT NULL,
  updated_at      timestamptz NOT NULL DEFAULT now()
);
