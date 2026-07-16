-- 0021_propagation_outbox_prepared_epoch — bind the ATTEMPTED epoch to the lease (audit leaf-2.1).
--
-- Without this, failure accounting used the newest outstanding epoch AT FAILURE TIME, not the
-- epoch the holder actually prepared + attempted to publish. Interleaving
--   lease N → preparePublication M → a newer epoch K commits → publish M fails → fail(token)
-- would penalize K (never attempted) and leave M un-throttled. A client-supplied epoch is
-- forgeable (a malicious holder could redirect backoff), so the attempted epoch is recorded
-- SERVER-SIDE on the leased anchor: set at claim (= the leased head) and at preparePublication
-- (= the current head M), and consumed by fail() / expiry-reclaim / (leaf 4) completion.
ALTER TABLE account_propagation_outbox
  ADD COLUMN IF NOT EXISTS prepared_epoch bigint;

-- Meaningful only while leased; when set it is a real epoch.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_prepared_epoch_valid;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_prepared_epoch_valid
    CHECK (prepared_epoch IS NULL OR prepared_epoch > 0);
