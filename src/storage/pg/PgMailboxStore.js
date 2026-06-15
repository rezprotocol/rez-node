import { MailboxStore } from "@rezprotocol/core";

/**
 * Postgres-backed MailboxStore (node-global ordered index of objectIds per
 * mailbox), mirroring FsMailboxStore. `data` holds the serialized index — a
 * plaintext JSON array, or the sealed blob the EncryptedMailboxStore decorator
 * writes via `_writeSealed`/`_readRaw`.
 *
 * Note: append is read-modify-write to match Fs semantics exactly (this simple
 * index is NOT the cluster delivery hot path — the durable home inbox, an
 * append-only BIGSERIAL log with per-device cursors, is a separate contract).
 */
export class PgMailboxStore extends MailboxStore {
  #conn;

  constructor({ connection } = {}) {
    super();
    if (!connection) {
      throw new Error("PgMailboxStore requires connection");
    }
    this.#conn = connection;
  }

  async append(mailboxId, objectId) {
    const items = await this.list(mailboxId);
    items.push(objectId);
    await this._writeSealed(mailboxId, JSON.stringify(items));
  }

  async list(mailboxId) {
    const raw = await this._readRaw(mailboxId);
    if (raw === null) {
      return [];
    }
    const json = JSON.parse(raw);
    return Array.isArray(json) ? json : [];
  }

  async deleteMailbox(mailboxId) {
    const res = await this.#conn.query(
      "DELETE FROM mailbox_index WHERE mailbox_id = $1",
      [String(mailboxId)],
    );
    return res.rowCount > 0;
  }

  // --- raw accessors used by EncryptedMailboxStore ---

  async _writeSealed(mailboxId, data) {
    await this.#conn.query(
      `INSERT INTO mailbox_index (mailbox_id, data, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (mailbox_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
      [String(mailboxId), String(data)],
    );
  }

  async _readRaw(mailboxId) {
    const res = await this.#conn.query(
      "SELECT data FROM mailbox_index WHERE mailbox_id = $1",
      [String(mailboxId)],
    );
    if (res.rowCount === 0) {
      return null;
    }
    return res.rows[0].data;
  }
}
