-- 0006_mailbox_seq — per-inbox monotonic high-water sequence counter.
--
-- AUDIT FIX (CRITICAL): seq must be a DURABLE high-water mark, not
-- `max(seq)` of the prunable `mailbox_events` table. Deriving seq from a table
-- that prune() deletes from meant pruning an inbox to empty reset seq to 1, so a
-- device whose cursor was at an older seq silently lost every subsequent message
-- (its cursor was permanently ahead of the reused low seqs). This counter only
-- ever increments and is never touched by prune.

CREATE TABLE IF NOT EXISTS mailbox_seq (
  inbox_id  text   PRIMARY KEY,
  last_seq  bigint NOT NULL DEFAULT 0
);
