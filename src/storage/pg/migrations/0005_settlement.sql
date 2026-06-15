-- 0005_settlement — atomic Pg settlement (S4). Replaces the read-modify-write
-- LocalSettlementProvider.debit (which can overdraft under concurrent device
-- spend) with a row-locked debit + append-only journal + idempotency keys.

CREATE TABLE IF NOT EXISTS settlement_balances (
  account_id  text        PRIMARY KEY,           -- rez:acct:* payer wallet (account identity)
  available   numeric     NOT NULL DEFAULT 0,
  escrowed    numeric     NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT settlement_balances_available_nonneg CHECK (available >= 0),
  CONSTRAINT settlement_balances_escrowed_nonneg  CHECK (escrowed  >= 0)
);

-- Append-only economic audit log (the designed SettlementJournal). Every entry
-- carries the immutable networkId so only official-network activity counts.
CREATE TABLE IF NOT EXISTS settlement_journal (
  entry_id         text        PRIMARY KEY,
  account_id       text        NOT NULL,
  kind             text        NOT NULL,          -- 'debit' | 'credit'
  amount           numeric     NOT NULL,
  service_id       text,
  service_ref      text,                          -- e.g. mailbox:<inboxId> (paid-path linkage, documented)
  network_id       text        NOT NULL,
  idempotency_key  text,
  receipt          jsonb,                         -- signed receipt (debit) for idempotent replay
  created_at_ms    bigint      NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now()
);

-- One settled debit per (account, idempotency_key): a retried paid request never
-- double-charges.
CREATE UNIQUE INDEX IF NOT EXISTS settlement_journal_idem
  ON settlement_journal (account_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS settlement_journal_account
  ON settlement_journal (account_id, created_at);
