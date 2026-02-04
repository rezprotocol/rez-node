import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";

const ELECTRON_APP_NAME = "Rez Chat";

/**
 * Returns the platform-specific default userData path that Electron would use (Rez Chat app).
 * Used only for migration; does not call Electron.
 * @returns {string}
 */
function getElectronDefaultUserData() {
  const home = os.homedir();
  switch (process.platform) {
    case "darwin":
      return path.join(home, "Library", "Application Support", ELECTRON_APP_NAME);
    case "win32":
      return path.join(process.env.APPDATA || home, ELECTRON_APP_NAME);
    default:
      return path.join(process.env.XDG_CONFIG_HOME || path.join(home, ".config"), ELECTRON_APP_NAME);
  }
}

/**
 * RezDataRoot
 *
 * Single source of truth for the persistent data directory used by Rez (web-chat and Electron).
 * Default: ~/.rez. Overridable via REZ_DATA_DIR (env) or an explicit override (e.g. --dataDir, REZ_ELECTRON_USER_DATA).
 */
export default class RezDataRoot {
  /**
   * Resolve the absolute path to the Rez data directory.
   * @param {string} [override] - Explicit path override (e.g. CLI --dataDir or REZ_ELECTRON_USER_DATA)
   * @returns {string} Absolute path to the data directory
   */
  getRoot(override) {
    if (override != null && String(override).trim() !== "") {
      return path.resolve(override);
    }
    const envDir = process.env.REZ_DATA_DIR;
    if (envDir != null && String(envDir).trim() !== "") {
      return path.resolve(envDir);
    }
    return path.join(os.homedir(), ".rez");
  }

  /**
   * Legacy paths to try as migration sources (absolute paths).
   * If REZ_MIGRATE_FROM is set, that path is tried first (e.g. backup or old install).
   * @param {'web'|'electron'} context - 'web' or 'electron'
   * @returns {string[]}
   */
  getLegacyPaths(context) {
    const userPath = process.env.REZ_MIGRATE_FROM;
    const extra = userPath && String(userPath).trim() ? [path.resolve(userPath.trim())] : [];
    const webLegacy = path.join(os.homedir(), ".rez-web-chat");
    if (context === "web") {
      return [...extra, webLegacy];
    }
    if (context === "electron") {
      return [...extra, getElectronDefaultUserData(), webLegacy];
    }
    return extra;
  }

  /**
   * @param {string} dirPath
   * @returns {Promise<boolean>}
   * @private
   */
  async _hasData(dirPath) {
    try {
      const stat = await fs.stat(dirPath);
      if (!stat.isDirectory()) return false;
      const entries = await fs.readdir(dirPath);
      return entries.length > 0;
    } catch {
      return false;
    }
  }

  /**
   * True only when root has accounts/index.json with at least one account.
   * Used so we do not skip migration when root exists but has an empty index (e.g. after a prior run wrote empty index).
   * @param {string} rootPath
   * @returns {Promise<boolean>}
   * @private
   */
  async _hasAccountData(rootPath) {
    try {
      const indexPath = path.join(rootPath, "accounts", "index.json");
      const data = await fs.readFile(indexPath, "utf8");
      const parsed = JSON.parse(data);
      if (parsed.formatVersion !== "rez-account-index/1") return false;
      const count = parsed.accounts && typeof parsed.accounts === "object" ? Object.keys(parsed.accounts).length : 0;
      return count > 0;
    } catch {
      return false;
    }
  }

  /**
   * Ensure the data directory exists (mkdir -p). Call once at startup.
   * @param {string} [override] - Same as getRoot(override); uses this to resolve root first
   * @returns {Promise<string>} The resolved root path after ensuring it exists
   */
  async ensureExists(override) {
    const root = this.getRoot(override);
    await fs.mkdir(root, { recursive: true });
    return root;
  }

  /**
   * Ensure the data directory exists and, if it does not or is empty, migrate from a legacy path.
   * @param {string} [override] - Same as getRoot(override)
   * @param {'web'|'electron'} context - Which legacy paths to try
   * @param {{ legacyPaths?: string[] }} [options] - If legacyPaths is provided (e.g. for tests), use it instead of getLegacyPaths(context)
   * @returns {Promise<string>} The resolved root path
   */
  async ensureExistsAndMigrate(override, context, options = {}) {
    const root = path.resolve(this.getRoot(override));
    const legacyPaths = options.legacyPaths ?? this.getLegacyPaths(context);

    console.log(`[RezDataRoot] Data directory: ${root}`);

    if (await this._hasAccountData(root)) {
      console.log(`[RezDataRoot] Data directory already has account data, skipping migration.`);
      await fs.mkdir(root, { recursive: true });
      return root;
    }

    // Never overwrite an existing root directory: copying from legacy would replace
    // the user's data (accounts, contacts, groups). Only migrate when root does not exist.
    let rootExists = false;
    try {
      const stat = await fs.stat(root);
      rootExists = stat.isDirectory();
    } catch {
      // root doesn't exist
    }
    if (rootExists) {
      console.log(`[RezDataRoot] Data directory already exists, skipping migration to avoid overwriting data.`);
      await fs.mkdir(root, { recursive: true });
      return root;
    }

    // Root does not exist: safe to copy from legacy
    // Prefer a legacy that has actual account data (index with >= 1 account)
    for (const legacy of legacyPaths) {
      const legacyResolved = path.resolve(legacy);
      if (legacyResolved === root) continue;
      if (!(await this._hasAccountData(legacyResolved))) continue;

      await fs.mkdir(root, { recursive: true });
      await fs.cp(legacyResolved, root, { recursive: true });
      console.log(`[RezDataRoot] Migrated data from ${legacyResolved} to ${root}`);
      return root;
    }

    // Fallback: any legacy with content
    for (const legacy of legacyPaths) {
      const legacyResolved = path.resolve(legacy);
      if (legacyResolved === root) continue;
      if (!(await this._hasData(legacyResolved))) continue;

      await fs.mkdir(root, { recursive: true });
      await fs.cp(legacyResolved, root, { recursive: true });
      console.log(`[RezDataRoot] Migrated data from ${legacyResolved} to ${root}`);
      return root;
    }

    console.log(`[RezDataRoot] No legacy data found; tried: [${legacyPaths.map((p) => path.resolve(p)).join(", ")}]. Creating empty data directory.`);
    await fs.mkdir(root, { recursive: true });
    return root;
  }
}
