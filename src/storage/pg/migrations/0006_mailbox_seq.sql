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

-- UPGRADE-PATH BACKFILL (CRITICAL): on a v5→v6 upgrade the inbox already holds
-- `mailbox_events` rows. Without seeding the high-water counter, the first
-- post-upgrade append would start `last_seq` at 1 and either collide with the
-- existing (inbox_id, seq) rows or — after a prune — reuse low seqs that live
-- device cursors are already past, silently losing mail. Seed the counter from
-- the current max(seq) per inbox so it continues monotonically. Idempotent: a
-- fresh deploy (empty `mailbox_events`) inserts nothing, and a re-run only ever
-- raises the counter (GREATEST), never lowers it.
INSERT INTO mailbox_seq (inbox_id, last_seq)
  SELECT inbox_id, max(seq) FROM mailbox_events GROUP BY inbox_id
  ON CONFLICT (inbox_id) DO UPDATE
    SET last_seq = GREATEST(mailbox_seq.last_seq, EXCLUDED.last_seq);
