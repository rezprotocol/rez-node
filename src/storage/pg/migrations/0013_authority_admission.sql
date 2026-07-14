-- 0013_authority_admission — durable admission control for the account authority
-- home (S2.5 audit R4 F3, a fan-out release blocker). The per-account caps (active /
-- lifetime devices, revoked-cert set, tombstones) and the input-shape guards (opId /
-- cert-id byte + format bounds) are enforced IN CODE under the per-account lock —
-- they need no schema, only counts. This migration only supports the JOURNAL
-- RETENTION split: the mutation journal is BOTH an idempotency replay store AND a
-- permanent audit log, but the replay payload (result_json) only needs to answer a
-- retry for a bounded window. Keeping every result_json forever grows unbounded.
--
-- Split: the audit row (account, op_id, epoch, action, targets, committed_at) stays
-- forever; the replay payload becomes prunable. A periodic sweep NULLs result_json
-- older than the retention window (DurableInboxPruner). A replay whose payload has
-- been pruned still proves the op committed (the row exists) — the serializer returns
-- the CURRENT authority state with replayExpired:true rather than the exact snapshot.

-- The replay payload is now optional (an old, pruned row keeps its audit columns but
-- drops result_json). A fresh commit always writes it.
ALTER TABLE account_device_mutation ALTER COLUMN result_json DROP NOT NULL;

-- Index the retention sweep's scan key (prune WHERE committed_at < cutoff).
CREATE INDEX IF NOT EXISTS account_device_mutation_committed_at
  ON account_device_mutation (committed_at);
