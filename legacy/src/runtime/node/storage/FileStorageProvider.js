import StorageProvider from "../../../core/storage/StorageProvider.js";
import fs from "node:fs/promises";
import path from "node:path";

/**
 * FileStorageProvider
 *
 * Durable storage using the filesystem. Each key is stored as a JSON file:
 *   baseDir/<encodedNamespace>/<encodedKey>.json
 *
 * Notes:
 * - This is designed for small-to-medium datasets.
 * - list() is implemented by reading the namespace directory and decoding keys.
 */
export default class FileStorageProvider extends StorageProvider {
  /**
   * @param {{ baseDir: string }} options
   */
  constructor(options) {
    super();
    if (!options?.baseDir) throw new Error("FileStorageProvider requires options.baseDir");
    this._baseDir = options.baseDir;
  }

  async init() {
    await fs.mkdir(this._baseDir, { recursive: true });
    this._initialized = true;
  }

  async close() {
    this._initialized = false;
  }

  _ensureInit() {
    if (!this._initialized) throw new Error("FileStorageProvider not initialized");
  }

  _enc(s) {
    // Safe for filenames; reversible.
    return encodeURIComponent(s);
  }

  _dec(s) {
    return decodeURIComponent(s);
  }

  _nsDir(namespace) {
    return path.join(this._baseDir, this._enc(namespace));
  }

  _keyPath(namespace, key) {
    return path.join(this._nsDir(namespace), `${this._enc(key)}.json`);
  }

  async get(namespace, key) {
    this._ensureInit();
    const p = this._keyPath(namespace, key);
    try {
      const raw = await fs.readFile(p, "utf8");
      return JSON.parse(raw);
    } catch (err) {
      if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
      throw err;
    }
  }

  async put(namespace, key, value) {
    this._ensureInit();
    const dir = this._nsDir(namespace);
    await fs.mkdir(dir, { recursive: true });
    const p = this._keyPath(namespace, key);
    const raw = JSON.stringify(value);
    await fs.writeFile(p, raw, "utf8");
  }

  async delete(namespace, key) {
    this._ensureInit();
    const p = this._keyPath(namespace, key);
    try {
      await fs.unlink(p);
    } catch (err) {
      if (err && err.code === "ENOENT") return;
      throw err;
    }
  }

  async list(namespace, options = {}) {
    this._ensureInit();
    const prefix = options.prefix ?? "";
    const limit = options.limit ?? 1000;
    const cursor = options.cursor ?? null;

    const dir = this._nsDir(namespace);
    let entries;
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch (err) {
      if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) {
        return { keys: [], cursor: null };
      }
      throw err;
    }

    const keysAll = entries
      .filter((e) => e.isFile() && e.name.endsWith(".json"))
      .map((e) => this._dec(e.name.slice(0, -".json".length)))
      .sort();

    const filtered = prefix ? keysAll.filter((k) => k.startsWith(prefix)) : keysAll;

    let start = 0;
    if (cursor) {
      const idx = filtered.indexOf(cursor);
      start = idx >= 0 ? idx + 1 : 0;
    }

    const slice = filtered.slice(start, start + limit);
    const nextCursor = start + limit < filtered.length ? slice[slice.length - 1] : null;
    return { keys: slice, cursor: nextCursor };
  }
}
