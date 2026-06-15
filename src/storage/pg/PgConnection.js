import pg from "pg";

const { Pool } = pg;

/**
 * Thin wrapper around a single shared `pg.Pool` for the cluster's Postgres
 * backend. Owns the pool lifecycle and exposes a pooled `query` plus a
 * `withClient` helper for operations that must run on ONE session (advisory
 * locks, transactions).
 *
 * SSOT: every Pg store/registry/migration in this process shares one
 * `PgConnection`, so connection config + pool sizing live in exactly one place.
 */
export class PgConnection {
  #pool;
  #ownsPool;

  /**
   * @param {{ connectionString?: string, pool?: import("pg").Pool, poolConfig?: object }} opts
   */
  constructor({ connectionString = null, pool = null, poolConfig = null } = {}) {
    if (pool) {
      this.#pool = pool;
      this.#ownsPool = false;
      return;
    }
    if (!connectionString) {
      throw new Error("PgConnection requires connectionString or pool");
    }
    const config = poolConfig && typeof poolConfig === "object" ? poolConfig : {};
    this.#pool = new Pool({ connectionString, ...config });
    this.#ownsPool = true;
  }

  /**
   * Pooled query — a fresh connection per call. Use for single statements.
   * @param {string} text
   * @param {Array<unknown>} params
   */
  async query(text, params = []) {
    return this.#pool.query(text, params);
  }

  /**
   * Run `fn` against ONE checked-out client (same session for every query
   * inside). Required for advisory locks and multi-statement transactions.
   * The client is always released, even on throw.
   * @param {(client: import("pg").PoolClient) => Promise<unknown>} fn
   */
  async withClient(fn) {
    const client = await this.#pool.connect();
    try {
      return await fn(client);
    } finally {
      client.release();
    }
  }

  async close() {
    if (this.#ownsPool) {
      await this.#pool.end();
    }
  }

  get pool() {
    return this.#pool;
  }
}
