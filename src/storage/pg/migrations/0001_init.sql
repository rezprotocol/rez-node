-- 0001_init — shared key/value store with owner partition + version for CAS.
--
-- `owner` is the storage-partition handle the StorageProvider already passes
-- (claimant pubkey for hosted rows; '' = the root/cross-owner namespace used by
-- node-level orchestration). It is a storage partition, NOT a node-visible
-- account correlation (the node stays account-blind — see CAPABILITY_MODEL §8).
--
-- `version` backs optimistic concurrency (CAS): every write bumps it, and
-- PgKeyValueStore.setVersioned() updates only when the caller's expected version
-- matches. This replaces the FsStorageProvider's single-process assumptions so N
-- cluster nodes can write the same shared row safely.

CREATE TABLE IF NOT EXISTS kv (
  owner       text        NOT NULL DEFAULT '',
  key         text        NOT NULL,
  value       jsonb       NOT NULL,
  version     bigint      NOT NULL DEFAULT 1,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (owner, key)
);

-- Supports keys(prefix) via `key LIKE 'prefix%'` within an owner partition.
CREATE INDEX IF NOT EXISTS kv_owner_key_prefix
  ON kv (owner, key text_pattern_ops);
