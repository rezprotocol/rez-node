-- 0003_durable_inbox — the durable home inbox: append-only ciphertext log with
-- per-device cursors. This is the cluster's system of record (NOT the transient
-- relay buffer / RMailbox, which stays delete-after-delivery for WAN egress).
--
-- Shape A: `body` is ciphertext only; the node never holds plaintext or keys.
--
-- `seq` is a per-inbox, gap-free monotonic counter assigned under a per-inbox
-- advisory lock at append time (see PgDurableInbox.append), so a reader at
-- cursor C reading `seq > C` never skips an event committed out of order — the
-- sequence-gap-under-concurrent-commit hazard the plan calls out.

CREATE TABLE IF NOT EXISTS mailbox_events (
  inbox_id    text        NOT NULL,
  seq         bigint      NOT NULL,
  body        bytea       NOT NULL,
  dedupe_key  text,                          -- SHA-256(decoded outer body), pre-decrypt; home-independent
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (inbox_id, seq)
);

-- Home-independent idempotency: a re-delivery (e.g. after re-home) with the same
-- ciphertext hash does not double-append.
CREATE UNIQUE INDEX IF NOT EXISTS mailbox_events_dedupe
  ON mailbox_events (inbox_id, dedupe_key)
  WHERE dedupe_key IS NOT NULL;

-- Per-device cursor: ack = advance THIS device's cursor (never a destructive
-- delete). Pruning deletes below min(last_seq) across non-revoked, non-stale
-- devices. `revoked` is home-enforced: a revoked device cannot read or ack.
CREATE TABLE IF NOT EXISTS device_cursors (
  inbox_id       text        NOT NULL,
  device_id      text        NOT NULL,
  last_seq       bigint      NOT NULL DEFAULT 0,
  revoked        boolean     NOT NULL DEFAULT false,
  registered_at  timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (inbox_id, device_id)
);

CREATE INDEX IF NOT EXISTS device_cursors_inbox
  ON device_cursors (inbox_id);
