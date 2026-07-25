-- 0026_rate_budget_subject — generalize the rate budget's key from "account" to "subject".
--
-- 0025 introduced the table for ONE caller (the outbox lease ops), whose subject happened to be an
-- account identity. Open-registration abuse quotas need the same mechanism keyed on a CLAIMANT KEY
-- and on a CLIENT IP, neither of which is an account — and on a node that is deliberately
-- account-blind, calling an IP an "account_identity" is exactly the kind of name that misleads a
-- reader later. The bucket column already distinguishes which budget a row belongs to, so the key
-- only ever needed to be an opaque subject string.
--
-- Renamed rather than re-created so any database already carrying 0025 keeps its rows.
--
-- IDEMPOTENT. A bare `ALTER TABLE ... RENAME` fails on a second run, and re-running migrations is
-- not hypothetical: the 23→24 runner test rewinds `schema_migrations` and replays everything above
-- it, and an operator recovering a half-applied migration will do the same. Every step below is
-- therefore guarded on the state it is about to change.

DO $$
BEGIN
  IF to_regclass('account_rate_budget') IS NOT NULL AND to_regclass('rate_budget') IS NULL THEN
    EXECUTE 'ALTER TABLE account_rate_budget RENAME TO rate_budget';
  END IF;

  IF to_regclass('rate_budget') IS NOT NULL
     AND EXISTS (
       SELECT 1 FROM information_schema.columns
       WHERE table_name = 'rate_budget' AND column_name = 'account_identity'
     )
  THEN
    EXECUTE 'ALTER TABLE rate_budget RENAME COLUMN account_identity TO subject';
  END IF;

  IF to_regclass('account_rate_budget_window_idx') IS NOT NULL
     AND to_regclass('rate_budget_window_idx') IS NULL
  THEN
    EXECUTE 'ALTER INDEX account_rate_budget_window_idx RENAME TO rate_budget_window_idx';
  END IF;
END
$$;

-- Keep constraint names in step with the table so a reader grepping either finds both. DROP ... IF
-- EXISTS + ADD is already replay-safe.
ALTER TABLE rate_budget
  DROP CONSTRAINT IF EXISTS account_rate_budget_count_bounded;
ALTER TABLE rate_budget
  DROP CONSTRAINT IF EXISTS rate_budget_count_bounded;
ALTER TABLE rate_budget
  ADD CONSTRAINT rate_budget_count_bounded CHECK (count >= 0 AND count <= 1000000000);

ALTER TABLE rate_budget
  DROP CONSTRAINT IF EXISTS account_rate_budget_window_positive;
ALTER TABLE rate_budget
  DROP CONSTRAINT IF EXISTS rate_budget_window_positive;
ALTER TABLE rate_budget
  ADD CONSTRAINT rate_budget_window_positive CHECK (window_start_ms >= 0);
