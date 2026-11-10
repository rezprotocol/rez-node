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
  #closed;
  #poolErrorHandler;

  /**
   * @param {{ connectionString?: string, pool?: import("pg").Pool, poolConfig?: object }} opts
   */
  constructor({ connectionString = null, pool = null, poolConfig = null } = {}) {
    this.#closed = false;
    this.#poolErrorHandler = (err) => {
      // node-postgres emits `error` for a failed idle client. Without a
      // listener EventEmitter terminates the process, turning a database
      // outage into a crash loop instead of an honest /ready failure. The pool
      // discards that client and creates another on a later query.
      console.error("[PgConnection] idle backend connection failed");
    };
    if (pool) {
      this.#pool = pool;
      this.#ownsPool = false;
      this.#pool.on("error", this.#poolErrorHandler);
      return;
    }
    if (!connectionString) {
      throw new Error("PgConnection requires connectionString or pool");
    }
    const config = poolConfig && typeof poolConfig === "object" ? poolConfig : {};
    this.#pool = new Pool({ connectionString, connectionTimeoutMillis: 3000,
      statement_timeout: 5000, query_timeout: 6000, ...config });
    this.#ownsPool = true;
    this.#pool.on("error", this.#poolErrorHandler);
  }

  /**
   * Pooled query — a fresh connection per call. Use for single statements.
   * @param {string} text
   * @param {Array<unknown>} params
   */
  async query(text, params = []) {
    try {
      return await this.#pool.query(text, params);
    } catch (err) {
      throw this.#normalizeTimeout(err);
    }
  }

  /**
   * Run `fn` against ONE checked-out client (same session for every query
   * inside). Required for advisory locks and multi-statement transactions.
   * The client is always released, even on throw.
   * @param {(client: import("pg").PoolClient) => Promise<unknown>} fn
   */
  async withClient(fn) {
    let client;
    let failed = false;
    try {
      client = await this.#pool.connect();
      return await fn(client);
    } catch (err) {
      failed = true;
      throw this.#normalizeTimeout(err);
    } finally {
      // Never return an ambiguous transaction/session to the pool. The server
      // rolls back any open transaction when the failed connection closes.
      if (client) client.release(failed);
    }
  }

  #normalizeTimeout(err) {
    if (err && !err.code && ["Query read timeout", "timeout exceeded when trying to connect", "Connection terminated due to connection timeout"].includes(err.message)) {
      err.code = "ETIMEDOUT";
    }
    return err;
  }

  async close() {
    // Idempotent: pg.Pool.end() throws if called twice, and shutdown paths can
    // double-close (e.g. SIGINT then SIGTERM both calling stop()).
    if (this.#closed) return;
    this.#closed = true;
    try {
      if (this.#ownsPool) await this.#pool.end();
    } finally {
      this.#pool.removeListener("error", this.#poolErrorHandler);
    }
  }

  get pool() {
    return this.#pool;
  }
}
