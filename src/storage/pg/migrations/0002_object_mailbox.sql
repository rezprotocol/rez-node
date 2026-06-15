-- 0002_object_mailbox — node-global object + mailbox-index stores.
--
-- These mirror FsObjectStore / FsMailboxStore, which are node-global (the
-- StorageProvider object/mailbox accessors take no owner argument). `data` holds
-- the exact serialized string the store wrote — plaintext envelope/array JSON, or
-- the sealed {encrypted,...} blob the Encrypted* wrappers produce — so the
-- Pg backend is a drop-in behind the same Encrypted* decorators (which round-trip
-- via _writeSealed/_readRaw).

CREATE TABLE IF NOT EXISTS objects (
  id          text        PRIMARY KEY,
  data        text        NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mailbox_index (
  mailbox_id  text        PRIMARY KEY,
  data        text        NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);
