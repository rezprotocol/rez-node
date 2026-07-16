-- 0022_propagation_outbox_prepared_binding — enforce the attempted-epoch binding at the DB layer
-- (audit leaf-2.1 re-review P2). 0021 only required prepared_epoch > 0; it still permitted a
-- prepared_epoch on a non-leased row, one below the anchor, or one that names no obligation.

-- (a) prepared_epoch is meaningful ONLY while leased.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_prepared_only_leased;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_prepared_only_leased
    CHECK (prepared_epoch IS NULL OR status = 'leased');

-- (b) the attempted epoch is at or after the leased anchor (the row's own epoch) — the head only
--     advances forward, never behind the claim point.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_prepared_ge_anchor;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_prepared_ge_anchor
    CHECK (prepared_epoch IS NULL OR prepared_epoch >= epoch);

-- (c) the attempted epoch must be a REAL obligation for this account (self-FK to the outbox PK).
--     Enforced only when set (nullable FK, MATCH SIMPLE); referenced rows are never pruned.
ALTER TABLE account_propagation_outbox
  DROP CONSTRAINT IF EXISTS account_propagation_outbox_prepared_fk;
ALTER TABLE account_propagation_outbox
  ADD CONSTRAINT account_propagation_outbox_prepared_fk
    FOREIGN KEY (account_identity, prepared_epoch, kind)
    REFERENCES account_propagation_outbox (account_identity, epoch, kind);
