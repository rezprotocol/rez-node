import RezObject from "../../../core/RezObject.js";
import fs from "node:fs/promises";
import path from "node:path";

/**
 * AccountIndexStore
 *
 * Manages the plaintext account index file (index.json) with crash-safe atomic writes,
 * recovery fallback, and single-writer queue for async mutation safety.
 *
 * Single Responsibility: Index.json file persistence and recovery
 */
export default class AccountIndexStore extends RezObject {
  /**
   * @param {string} dataDir - Base data directory
   */
  constructor(dataDir) {
    super();
    this._dataDir = dataDir;
    this._indexPath = path.join(dataDir, "accounts", "index.json");
    this._bakPath = path.join(dataDir, "accounts", "index.json.bak");
    this._cache = null; // In-memory canonical copy of the index
    this._writeQueue = []; // Promise queue for async mutation locking
    this._isWriting = false;
  }

  /**
   * Initialize and load index from disk.
   * Must be called before using the store.
   * @returns {Promise<void>}
   */
  async init() {
    const accountsDir = path.join(this._dataDir, "accounts");
    await fs.mkdir(accountsDir, { recursive: true });

    // #region agent log
    let indexExists = false;
    try {
      await fs.access(this._indexPath);
      indexExists = true;
    } catch (_) {}
    try {
      const u = typeof globalThis !== "undefined" && globalThis.fetch ? globalThis.fetch : typeof fetch !== "undefined" ? fetch : null;
      if (u) u("http://127.0.0.1:7242/ingest/319bef52-3862-4a8b-a355-437325400389", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ location: "AccountIndexStore.js:init", message: "before loadIndex", data: { dataDir: this._dataDir, indexPath: this._indexPath, indexExists }, timestamp: Date.now(), sessionId: "debug-session", hypothesisId: "H1" }) }).catch(() => {});
    } catch (_) {}
    // #endregion

    const loaded = await this.loadIndex();

    // #region agent log
    const accountCount = loaded && loaded.accounts ? Object.keys(loaded.accounts).length : 0;
    try {
      const u = typeof globalThis !== "undefined" && globalThis.fetch ? globalThis.fetch : typeof fetch !== "undefined" ? fetch : null;
      if (u) u("http://127.0.0.1:7242/ingest/319bef52-3862-4a8b-a355-437325400389", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ location: "AccountIndexStore.js:init", message: "after loadIndex", data: { loadedNull: loaded === null, accountCount }, timestamp: Date.now(), sessionId: "debug-session", hypothesisId: "H1" }) }).catch(() => {});
    } catch (_) {}
    // #endregion

    if (loaded) {
      this._cache = loaded;
    } else {
      // No index exists: set empty structure in memory only (do not write yet).
      // Migration runs after init() and may add accounts; ensurePersisted() writes afterward.
      this._cache = {
        formatVersion: "rez-account-index/1",
        updatedAt: Date.now(),
        accounts: {},
      };
    }
  }

  /**
   * Ensure index is persisted to disk if it exists in memory but file is missing.
   * Call after migration so that first run with no old accounts still gets an index file.
   * @returns {Promise<void>}
   */
  async ensurePersisted() {
    if (!this._cache) return;
    let fileExists = false;
    try {
      await fs.access(this._indexPath);
      fileExists = true;
    } catch (_) {}
    // #region agent log
    try {
      const u = typeof globalThis !== "undefined" && globalThis.fetch ? globalThis.fetch : typeof fetch !== "undefined" ? fetch : null;
      if (u) u("http://127.0.0.1:7242/ingest/319bef52-3862-4a8b-a355-437325400389", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ location: "AccountIndexStore.js:ensurePersisted", message: "before write check", data: { fileExists, willWrite: !fileExists, cacheAccountCount: this._cache?.accounts ? Object.keys(this._cache.accounts).length : 0 }, timestamp: Date.now(), sessionId: "debug-session", hypothesisId: "H3" }) }).catch(() => {});
    } catch (_) {}
    // #endregion
    if (fileExists) return;
    await this.atomicSaveIndex(this._cache);
  }

  /**
   * Internal: Load index from disk with fallback to backup.
   * @returns {Promise<object|null>} Parsed index data or null if both files corrupted/missing
   */
  async loadIndex() {
    // Try primary index
    try {
      const data = await fs.readFile(this._indexPath, "utf8");
      const parsed = JSON.parse(data);
      // Validate format version
      if (parsed.formatVersion !== "rez-account-index/1") {
        throw new Error("Invalid format version");
      }
      return parsed;
    } catch (err) {
      // #region agent log
      try {
        const u = typeof globalThis !== "undefined" && globalThis.fetch ? globalThis.fetch : typeof fetch !== "undefined" ? fetch : null;
        if (u) u("http://127.0.0.1:7242/ingest/319bef52-3862-4a8b-a355-437325400389", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ location: "AccountIndexStore.js:loadIndex", message: "primary index failed", data: { reason: err?.message || String(err) }, timestamp: Date.now(), sessionId: "debug-session", hypothesisId: "H2" }) }).catch(() => {});
      } catch (_) {}
      // #endregion
      // Try backup
      try {
        const data = await fs.readFile(this._bakPath, "utf8");
        const parsed = JSON.parse(data);
        if (parsed.formatVersion !== "rez-account-index/1") {
          throw new Error("Invalid format version in backup");
        }
        return parsed;
      } catch (err2) {
        // #region agent log
        try {
          const u = typeof globalThis !== "undefined" && globalThis.fetch ? globalThis.fetch : typeof fetch !== "undefined" ? fetch : null;
          if (u) u("http://127.0.0.1:7242/ingest/319bef52-3862-4a8b-a355-437325400389", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ location: "AccountIndexStore.js:loadIndex", message: "backup also failed", data: { reason: err2?.message || String(err2) }, timestamp: Date.now(), sessionId: "debug-session", hypothesisId: "H2" }) }).catch(() => {});
        } catch (_) {}
        // #endregion
        // Both failed, return null to trigger recovery scan
        // NEVER auto-rebuild - require user confirmation
        return null;
      }
    }
  }

  /**
   * Internal: Acquire write lock (waits for queue).
   * @returns {Promise<void>}
   */
  async _acquireWriteLock() {
    return new Promise((resolve) => {
      this._writeQueue.push(resolve);
      if (!this._isWriting) {
        this._processWriteQueue();
      }
    });
  }

  /**
   * Internal: Process write queue.
   * @returns {Promise<void>}
   */
  async _processWriteQueue() {
    if (this._writeQueue.length === 0) {
      this._isWriting = false;
      return;
    }
    this._isWriting = true;
    const next = this._writeQueue.shift();
    next();
  }

  /**
   * Internal: Release write lock.
   * @returns {Promise<void>}
   */
  async _releaseWriteLock() {
    await this._processWriteQueue();
  }

  /**
   * Internal: Atomic write with crash safety.
   * @param {object} indexData - Index data to save
   * @returns {Promise<void>}
   */
  async atomicSaveIndex(indexData) {
    const accountsDir = path.join(this._dataDir, "accounts");
    const tmpPath = path.join(accountsDir, "index.json.tmp");
    const indexPath = path.join(accountsDir, "index.json");
    const bakPath = path.join(accountsDir, "index.json.bak");

    // 1. Write new file → index.json.tmp
    await fs.writeFile(tmpPath, JSON.stringify(indexData, null, 2), "utf8");

    // 2. fsync(tmp) - CRITICAL for crash safety
    const fd = await fs.open(tmpPath, "r+");
    await fd.sync();
    await fd.close();

    // 3. Rename current to backup (if exists) - BEFORE replacing
    try {
      await fs.rename(indexPath, bakPath);
    } catch (err) {
      // No existing index, skip backup
    }

    // 4. Rename tmp → index.json (atomic replace)
    await fs.rename(tmpPath, indexPath);

    // 5. fsync(directory) - CRITICAL for crash safety
    const dirFd = await fs.open(accountsDir, "r");
    await dirFd.sync();
    await dirFd.close();

    // 6. Clean up stale tmp files (best-effort)
    try {
      const entries = await fs.readdir(accountsDir);
      for (const entry of entries) {
        if (entry.startsWith("index.json.tmp.")) {
          await fs.unlink(path.join(accountsDir, entry)).catch(() => {});
        }
      }
    } catch {
      // Ignore cleanup errors
    }
  }

  /**
   * Scan account directories and return candidates for user confirmation.
   * NEVER auto-imports orphan DBs.
   * @returns {Promise<Array<{accountId: string, accountFolderId: string, accountRelDir: string, label: string|null}>>}
   */
  async rebuildIndexFromScan() {
    // Scan accounts/*/rez.db and accounts/*/account.json
    // Build candidate list with accountId from account.json
    // Return candidates - UI must prompt user for confirmation
    // NEVER auto-import orphan DBs
    const accountsDir = path.join(this._dataDir, "accounts");
    const accountsDirResolved = path.resolve(accountsDir);
    const candidates = [];

    try {
      const entries = await fs.readdir(accountsDir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory()) {
          const accountDir = path.join(accountsDir, entry.name);
          const accountJsonPath = path.join(accountDir, "account.json");
          const dbPath = path.join(accountDir, "rez.db");

          // CRITICAL: Path jail enforcement - accountRelDir must stay inside accounts/
          const accountRelDir = path.join("accounts", entry.name);
          const resolvedRelDir = path.resolve(this._dataDir, accountRelDir);
          if (!resolvedRelDir.startsWith(accountsDirResolved + path.sep)) {
            continue; // Skip - path escape attempt
          }

          try {
            // Check for account.json (contains accountId for orphan recovery)
            const accountJsonData = await fs.readFile(accountJsonPath, "utf8");
            const accountJson = JSON.parse(accountJsonData);

            // Validate format version
            if (accountJson.formatVersion !== "rez-account/1") {
              continue; // Skip invalid format
            }

            // Check DB exists and is not symlink
            const dbStats = await fs.lstat(dbPath);
            if (dbStats.isFile() && !dbStats.isSymbolicLink()) {
              // Verify account directory is not a symlink
              const dirStats = await fs.lstat(accountDir);
              if (!dirStats.isSymbolicLink()) {
                candidates.push({
                  accountId: accountJson.accountId,
                  accountFolderId: entry.name,
                  accountRelDir: accountRelDir,
                  label: accountJson.label || null,
                });
              }
            }
          } catch {
            // Skip if account.json missing, DB missing, or is symlink
          }
        }
      }
    } catch {
      // Directory doesn't exist yet
    }

    return candidates; // Return candidates for user confirmation
  }

  /**
   * Add account to index (mutates cache + disk atomically).
   * @param {string} accountId - Account ID
   * @param {object} metadata - Account metadata { label, createdAt, lastUsed, accountRelDir }
   * @returns {Promise<void>}
   */
  async addAccount(accountId, metadata) {
    await this._acquireWriteLock();
    try {
      // Mutate cache
      this._cache.accounts[accountId] = {
        label: metadata.label || null,
        createdAt: metadata.createdAt || Date.now(),
        lastUsed: metadata.lastUsed || Date.now(),
        accountRelDir: metadata.accountRelDir,
      };
      this._cache.updatedAt = Date.now();

      // Persist to disk
      await this.atomicSaveIndex(this._cache);
    } finally {
      await this._releaseWriteLock();
    }
  }

  /**
   * Update account metadata (mutates cache + disk atomically).
   * @param {string} accountId - Account ID
   * @param {object} updates - Partial metadata to update
   * @returns {Promise<void>}
   */
  async updateAccount(accountId, updates) {
    await this._acquireWriteLock();
    try {
      if (!this._cache.accounts[accountId]) {
        throw new Error(`Account ${accountId} not found in index`);
      }

      // Mutate cache
      Object.assign(this._cache.accounts[accountId], updates);
      this._cache.updatedAt = Date.now();

      // Persist to disk
      await this.atomicSaveIndex(this._cache);
    } finally {
      await this._releaseWriteLock();
    }
  }

  /**
   * Remove account from index (mutates cache + disk atomically).
   * @param {string} accountId - Account ID
   * @returns {Promise<void>}
   */
  async removeAccount(accountId) {
    await this._acquireWriteLock();
    try {
      // Mutate cache
      delete this._cache.accounts[accountId];
      this._cache.updatedAt = Date.now();

      // Persist to disk
      await this.atomicSaveIndex(this._cache);
    } finally {
      await this._releaseWriteLock();
    }
  }

  /**
   * Get account metadata (synchronous, reads from cache).
   * @param {string} accountId - Account ID
   * @returns {object|null} Account metadata or null if not found
   */
  getAccount(accountId) {
    if (!this._cache) {
      throw new Error("AccountIndexStore not initialized. Call init() first.");
    }
    return this._cache.accounts[accountId] || null;
  }

  /**
   * List all accounts (synchronous, reads from cache).
   * @returns {Array<{accountId: string, label: string|null, createdAt: number, lastUsed: number, accountRelDir: string}>}
   */
  listAccounts() {
    if (!this._cache) {
      throw new Error("AccountIndexStore not initialized. Call init() first.");
    }
    return Object.entries(this._cache.accounts).map(([accountId, metadata]) => ({
      accountId,
      ...metadata,
    }));
  }
}
