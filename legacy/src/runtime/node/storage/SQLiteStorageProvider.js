import StorageProvider from "../../../core/storage/StorageProvider.js";

/**
 * SQLiteStorageProvider
 *
 * Uses better-sqlite3 (already in repo deps) for durable storage.
 * Stores JSON values in a single table keyed by (namespace, key).
 */
export default class SQLiteStorageProvider extends StorageProvider {
  /**
   * @param {{ filename: string }} options
   */
  constructor(options) {
    super();
    if (!options?.filename) throw new Error("SQLiteStorageProvider requires options.filename");
    this._filename = options.filename;
    this._db = null;
  }

  async init() {
    // Dynamic import keeps the module load light and explicit.
    const mod = await import("better-sqlite3");
    const Database = mod.default ?? mod;
    this._db = new Database(this._filename);
    this._db.pragma("journal_mode = WAL");
    this._db.exec(`
      CREATE TABLE IF NOT EXISTS rez_kv (
        namespace TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(namespace, key)
      );
      CREATE INDEX IF NOT EXISTS rez_kv_ns_key ON rez_kv(namespace, key);
    `);
    this._initialized = true;
  }

  async close() {
    if (this._db) {
      this._db.close();
      this._db = null;
    }
    this._initialized = false;
  }

  _ensureInit() {
    if (!this._initialized || !this._db) throw new Error("SQLiteStorageProvider not initialized");
  }

  async get(namespace, key) {
    this._ensureInit();
    const row = this._db
      .prepare("SELECT value FROM rez_kv WHERE namespace = ? AND key = ?")
      .get(namespace, key);
    if (!row) return null;
    return JSON.parse(row.value);
  }

  async put(namespace, key, value) {
    this._ensureInit();
    const raw = JSON.stringify(value);
    const now = Date.now();
    this._db
      .prepare(
        "INSERT INTO rez_kv(namespace, key, value, updated_at) VALUES(?,?,?,?) ON CONFLICT(namespace, key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at"
      )
      .run(namespace, key, raw, now);
  }

  async delete(namespace, key) {
    this._ensureInit();
    this._db.prepare("DELETE FROM rez_kv WHERE namespace = ? AND key = ?").run(namespace, key);
  }

  async list(namespace, options = {}) {
    this._ensureInit();
    const prefix = options.prefix ?? "";
    const limit = options.limit ?? 1000;
    const cursor = options.cursor ?? null;

    // Cursor is the last key returned. We do key > cursor.
    let rows;
    if (prefix) {
      const like = `${prefix}%`;
      if (cursor) {
        rows = this._db
          .prepare(
            "SELECT key FROM rez_kv WHERE namespace = ? AND key LIKE ? AND key > ? ORDER BY key ASC LIMIT ?"
          )
          .all(namespace, like, cursor, limit);
      } else {
        rows = this._db
          .prepare("SELECT key FROM rez_kv WHERE namespace = ? AND key LIKE ? ORDER BY key ASC LIMIT ?")
          .all(namespace, like, limit);
      }
    } else {
      if (cursor) {
        rows = this._db
          .prepare("SELECT key FROM rez_kv WHERE namespace = ? AND key > ? ORDER BY key ASC LIMIT ?")
          .all(namespace, cursor, limit);
      } else {
        rows = this._db
          .prepare("SELECT key FROM rez_kv WHERE namespace = ? ORDER BY key ASC LIMIT ?")
          .all(namespace, limit);
      }
    }

    const keys = rows.map((r) => r.key);

    // Determine if there are more rows by probing one beyond the last key.
    let nextCursor = null;
    if (keys.length === limit) {
      nextCursor = keys[keys.length - 1];
      // We don't guarantee there are more; cursor means "resume after this".
    }

    return { keys, cursor: nextCursor };
  }
}
