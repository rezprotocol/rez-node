import { Envelope, canonicalize, ObjectStore } from "@rezprotocol/core";

/**
 * Postgres-backed ObjectStore (node-global, matching FsObjectStore — the
 * StorageProvider object accessor takes no owner argument).
 *
 * `data` holds the exact serialized string the caller wrote: the plaintext
 * canonicalized envelope JSON, or the sealed blob the EncryptedObjectStore
 * decorator produces via `_writeSealed`/`_readRaw`. That keeps this a drop-in
 * behind the same Encrypted* wrappers the Fs provider uses.
 */
export class PgObjectStore extends ObjectStore {
  #conn;

  constructor({ connection } = {}) {
    super();
    if (!connection) {
      throw new Error("PgObjectStore requires connection");
    }
    this.#conn = connection;
  }

  async put(envelope) {
    super.put(envelope); // validates Envelope + header.id
    const id = envelope.header.id;
    const data = JSON.stringify(canonicalize(envelope.toJSON()));
    await this._writeSealed(id, data);
  }

  async get(id) {
    const raw = await this._readRaw(id);
    if (raw === null) {
      return null;
    }
    return Envelope.fromJSON(JSON.parse(raw));
  }

  async has(id) {
    const res = await this.#conn.query("SELECT 1 FROM objects WHERE id = $1", [String(id)]);
    return res.rowCount > 0;
  }

  async delete(id) {
    const res = await this.#conn.query("DELETE FROM objects WHERE id = $1", [String(id)]);
    return res.rowCount > 0;
  }

  async listIds() {
    const res = await this.#conn.query("SELECT id FROM objects ORDER BY id");
    return res.rows.map((r) => r.id);
  }

  // --- raw accessors used by EncryptedObjectStore ---

  async _writeSealed(id, data) {
    await this.#conn.query(
      `INSERT INTO objects (id, data, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
      [String(id), String(data)],
    );
  }

  async _readRaw(id) {
    const res = await this.#conn.query("SELECT data FROM objects WHERE id = $1", [String(id)]);
    if (res.rowCount === 0) {
      return null;
    }
    return res.rows[0].data;
  }
}
