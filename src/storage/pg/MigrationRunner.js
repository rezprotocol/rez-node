import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Fixed Postgres advisory-lock key for rez schema migrations. N nodes booting
 * against one shared DB serialize on this so exactly one applies the DDL; the
 * others wait, then observe the migrations already applied and no-op.
 */
const ADVISORY_LOCK_KEY = 4747110001;

function parseVersion(filename) {
  const m = /^(\d+)_/.exec(filename);
  if (!m) return null;
  return Number(m[1]);
}

/**
 * Dependency-free, forward-only SQL migration runner (no migration library —
 * stays within the pg/ioredis-only dep budget).
 *
 * - Migrations live in `migrations/NNNN_*.sql`, applied in numeric order, each
 *   in its own transaction, recorded in `schema_migrations`.
 * - A Postgres advisory lock guards the whole run so concurrent node boots can
 *   never race the DDL.
 * - A schema-version GATE: a node refuses to start against a DB whose applied
 *   version is newer than the migrations it ships (prevents an old node
 *   corrupting a forward-migrated cluster).
 */
export class MigrationRunner {
  #conn;
  #dir;

  /**
   * @param {{ connection: import("./PgConnection.js").PgConnection, migrationsDir?: string }} opts
   */
  constructor({ connection, migrationsDir = null } = {}) {
    if (!connection) {
      throw new Error("MigrationRunner requires connection");
    }
    this.#conn = connection;
    this.#dir = migrationsDir
      ? migrationsDir
      : path.join(path.dirname(fileURLToPath(import.meta.url)), "migrations");
  }

  async #loadMigrations() {
    let entries;
    try {
      entries = await fs.readdir(this.#dir);
    } catch (err) {
      if (err && err.code === "ENOENT") {
        return [];
      }
      throw err;
    }
    const migrations = [];
    for (const name of entries) {
      if (!name.endsWith(".sql")) {
        continue;
      }
      const version = parseVersion(name);
      if (version == null) {
        throw new Error(`Migration file has no NNNN_ prefix: ${name}`);
      }
      migrations.push({ version, name });
    }
    migrations.sort((a, b) => a.version - b.version);
    for (let i = 1; i < migrations.length; i += 1) {
      if (migrations[i].version === migrations[i - 1].version) {
        throw new Error(`Duplicate migration version ${migrations[i].version}`);
      }
    }
    return migrations;
  }

  /**
   * Apply all pending migrations. Returns the version span and which versions
   * were applied this run.
   * @returns {Promise<{ appliedBefore: number, shipped: number, appliedNow: number[] }>}
   */
  async migrate() {
    const migrations = await this.#loadMigrations();
    // A node ships migrations; finding ZERO means the migrations directory could
    // not be resolved (e.g. an SEA-bundled binary without the .sql files beside
    // it). Fail loudly rather than silently reporting "up to date at version 0".
    if (migrations.length === 0) {
      throw new Error(
        `MigrationRunner found no migrations at ${this.#dir} — packaging/resolution error (refusing to silently no-op)`,
      );
    }
    const shipped = migrations[migrations.length - 1].version;

    return this.#conn.withClient(async (client) => {
      // Don't wedge the whole cluster on a stuck migrator: fail fast on lock/DDL.
      await client.query("SET lock_timeout = '30s'");
      await client.query("SET statement_timeout = '300s'");
      await client.query("SELECT pg_advisory_lock($1)", [ADVISORY_LOCK_KEY]);
      try {
        await client.query(
          `CREATE TABLE IF NOT EXISTS schema_migrations (
             version    bigint PRIMARY KEY,
             name       text NOT NULL,
             applied_at timestamptz NOT NULL DEFAULT now()
           )`,
        );

        const appliedRes = await client.query(
          "SELECT coalesce(max(version), 0) AS v FROM schema_migrations",
        );
        const appliedBefore = Number(appliedRes.rows[0].v);

        if (appliedBefore > shipped) {
          throw new Error(
            `schema-version gate: database is at migration ${appliedBefore} but this node ships only `
              + `${shipped}; refusing to start (an old node must not run against a forward-migrated cluster)`,
          );
        }

        const pending = migrations.filter((m) => m.version > appliedBefore);
        const appliedNow = [];
        for (const m of pending) {
          const sql = await fs.readFile(path.join(this.#dir, m.name), "utf8");
          await client.query("BEGIN");
          try {
            await client.query(sql);
            await client.query(
              "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
              [m.version, m.name],
            );
            await client.query("COMMIT");
          } catch (err) {
            await client.query("ROLLBACK");
            throw new Error(
              `Migration ${m.name} failed: ${err && err.message ? err.message : String(err)}`,
            );
          }
          appliedNow.push(m.version);
        }

        return { appliedBefore, shipped, appliedNow };
      } finally {
        await client.query("SELECT pg_advisory_unlock($1)", [ADVISORY_LOCK_KEY]);
      }
    });
  }
}
