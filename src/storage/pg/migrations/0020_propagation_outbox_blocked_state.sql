-- 0020_propagation_outbox_blocked_state — bound the attempt counter + add operator-visible
-- blocked state (audit leaf-2 P2). Without an upper bound, attempts = attempts + 1 grows
-- unbounded and eventually overflows the int column, stranding the obligation. And a repeatedly
-- failing obligation had no operator-visible signal. It must stay OUTSTANDING and recoverable —
-- never 'done'.

ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS blocked_at timestamptz;   -- first time attempts crossed the blocked threshold
ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS last_error text;          -- bounded error code from the last failure

-- Replace 0018's attempts >= 0 check with a BOUNDED one (the code also LEAST-clamps the writes),
-- so the persisted counter can never overflow.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_attempts_nonneg;
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_attempts_bounded;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_attempts_bounded CHECK (attempts >= 0 AND attempts <= 1000000);

-- Bound the (node-written) error-code size.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_last_error_len;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_last_error_len
    CHECK (last_error IS NULL OR octet_length(last_error) <= 200);
