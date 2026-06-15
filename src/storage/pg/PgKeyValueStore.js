import { KeyValueStore } from "@rezprotocol/core";

/**
 * Escape LIKE wildcards in a user-supplied prefix so `keys(prefix)` matches the
 * literal prefix (default backslash escape).
 */
function escapeLikePrefix(prefix) {
  return String(prefix).replace(/([%_\\])/g, "\\$1");
}

/**
 * Postgres-backed KeyValueStore, partitioned by storage owner.
 *
 * Honors the `ownerAccountId` partition that StorageProvider already passes and
 * that FsStorageProvider silently drops — each instance is bound to one owner
 * (`''` = root/cross-owner namespace). Rows carry a `version` so a cluster of N
 * nodes can write the same shared key safely via setVersioned() (CAS).
 *
 * The owner is a STORAGE partition handle (claimant pubkey for hosted rows), not
 * a node-visible account correlation — the node stays account-blind.
 */
export class PgKeyValueStore extends KeyValueStore {
  #conn;
  #owner;

  /**
   * @param {{ connection: import("./PgConnection.js").PgConnection, ownerAccountId?: string|null }} opts
   */
  constructor({ connection, ownerAccountId = null } = {}) {
    super();
    if (!connection) {
      throw new Error("PgKeyValueStore requires connection");
    }
    this.#conn = connection;
    this.#owner = typeof ownerAccountId === "string" && ownerAccountId.length > 0 ? ownerAccountId : "";
  }

  get owner() {
    return this.#owner;
  }

  async set(key, value) {
    await this.#conn.query(
      `INSERT INTO kv (owner, key, value, version, updated_at)
       VALUES ($1, $2, $3::jsonb, 1, now())
       ON CONFLICT (owner, key) DO UPDATE
         SET value = EXCLUDED.value,
             version = kv.version + 1,
             updated_at = now()`,
      [this.#owner, String(key), JSON.stringify(value)],
    );
  }

  async get(key) {
    const res = await this.#conn.query(
      "SELECT value FROM kv WHERE owner = $1 AND key = $2",
      [this.#owner, String(key)],
    );
    if (res.rowCount === 0) {
      return undefined;
    }
    return res.rows[0].value;
  }

  async delete(key) {
    const res = await this.#conn.query(
      "DELETE FROM kv WHERE owner = $1 AND key = $2",
      [this.#owner, String(key)],
    );
    return res.rowCount > 0;
  }

  async keys(prefix = "") {
    const p = String(prefix);
    if (p.length === 0) {
      const res = await this.#conn.query(
        "SELECT key FROM kv WHERE owner = $1 ORDER BY key",
        [this.#owner],
      );
      return res.rows.map((r) => r.key);
    }
    const res = await this.#conn.query(
      "SELECT key FROM kv WHERE owner = $1 AND key LIKE $2 ORDER BY key",
      [this.#owner, `${escapeLikePrefix(p)}%`],
    );
    return res.rows.map((r) => r.key);
  }

  /**
   * Read the current version alongside the value, for callers doing CAS.
   * @returns {Promise<{ value: unknown, version: number } | undefined>}
   */
  async getVersioned(key) {
    const res = await this.#conn.query(
      "SELECT value, version FROM kv WHERE owner = $1 AND key = $2",
      [this.#owner, String(key)],
    );
    if (res.rowCount === 0) {
      return undefined;
    }
    return { value: res.rows[0].value, version: Number(res.rows[0].version) };
  }

  /**
   * Compare-and-set. `expectedVersion` null/0 means "must not already exist".
   * Returns `{ ok: false }` on a version conflict (no write happened) so callers
   * can retry — the real cross-node CAS that FsStorageProvider can't provide.
   * @returns {Promise<{ ok: boolean, version: number|null }>}
   */
  async setVersioned(key, value, expectedVersion) {
    const k = String(key);
    const json = JSON.stringify(value);
    if (expectedVersion == null || expectedVersion === 0) {
      const res = await this.#conn.query(
        `INSERT INTO kv (owner, key, value, version, updated_at)
         VALUES ($1, $2, $3::jsonb, 1, now())
         ON CONFLICT (owner, key) DO NOTHING
         RETURNING version`,
        [this.#owner, k, json],
      );
      if (res.rowCount === 0) {
        return { ok: false, version: null };
      }
      return { ok: true, version: Number(res.rows[0].version) };
    }
    const res = await this.#conn.query(
      `UPDATE kv
         SET value = $3::jsonb, version = version + 1, updated_at = now()
       WHERE owner = $1 AND key = $2 AND version = $4
       RETURNING version`,
      [this.#owner, k, json, expectedVersion],
    );
    if (res.rowCount === 0) {
      return { ok: false, version: null };
    }
    return { ok: true, version: Number(res.rows[0].version) };
  }
}
