/**
 * PgRateBudget — a CLUSTER-WIDE per-subject request budget (audit leaf-3c F3).
 *
 * The SUBJECT is an opaque string chosen by the caller together with a BUCKET: an account identity
 * for the outbox lease ops, a claimant key or a client IP for open-registration abuse quotas. It is
 * deliberately not typed as an account — this node is account-blind, and naming an IP an "account"
 * would mislead every later reader.
 *
 * The per-node sliding-window limiter bounds how often ONE node will serve an account. Behind a
 * non-sticky load balancer that is not a ceiling at all: a device spreads its requests across N
 * nodes and gets N times the budget. The durable resource is already safe (Postgres' one-leased
 * partial unique index means no request volume can produce a second lease), so what is at stake is
 * work amplification — node CPU and Pg round-trips spent on ops that will all lose the lease race.
 * This makes the ceiling independent of node count.
 *
 * FIXED window, one upsert per request. A burst straddling a boundary can reach up to 2x the
 * ceiling within one window length; that is fine for an amplification bound and wrong for exact
 * quota accounting, so do not reuse this for the latter.
 *
 * FAILURE POLICY: consume() propagates backend errors rather than allowing the request. This is
 * not a new outage mode — every op this guards already requires the same database — and the
 * caller maps it to a retryable SERVICE_UNAVAILABLE like any other backend fault. There is
 * deliberately no "allow on error" path: a limiter that opens under load is not a limiter.
 */
export class PgRateBudget {
  #conn;

  constructor({ connection } = {}) {
    if (!connection || typeof connection.query !== "function") {
      throw new Error("PgRateBudget requires connection");
    }
    this.#conn = connection;
  }

  #normalize(value, field) {
    const v = typeof value === "string" ? value.trim() : "";
    if (v.length === 0) throw new Error("PgRateBudget requires " + field);
    return v;
  }

  /**
   * Count one request against (account, bucket) and report whether it is within the ceiling.
   *
   * The upsert is atomic and returns the post-increment count, so concurrent requests on different
   * nodes cannot both read a stale value and both decide they are under the limit.
   *
   * @param {object} args
   * @param {string} args.subject — opaque key (account identity, claimant pubkey, client IP, ...)
   * @param {string} args.bucket — which budget, so two op families cannot rob each other
   * @param {number} args.windowMs
   * @param {number} args.maxPerWindow
   * @param {number} args.nowMs
   * @returns {Promise<{ allowed: boolean, count: number, windowStartMs: number, retryAfterMs: number }>}
   */
  async consume({ subject, bucket, windowMs, maxPerWindow, nowMs } = {}) {
    const subj = this.#normalize(subject, "subject");
    const b = this.#normalize(bucket, "bucket");
    if (!Number.isInteger(windowMs) || windowMs <= 0) {
      throw new Error("PgRateBudget.consume requires a positive integer windowMs");
    }
    if (!Number.isInteger(maxPerWindow) || maxPerWindow <= 0) {
      throw new Error("PgRateBudget.consume requires a positive integer maxPerWindow");
    }
    if (!Number.isFinite(nowMs) || nowMs < 0) {
      throw new Error("PgRateBudget.consume requires a non-negative finite nowMs");
    }
    const windowStartMs = Math.floor(nowMs / windowMs) * windowMs;

    // LEAST clamps the stored counter so a sustained flood cannot overflow the column. Once the
    // ceiling is passed the exact count stops mattering — only "over" does.
    const res = await this.#conn.query(
      "INSERT INTO rate_budget (subject, bucket, window_start_ms, count)"
        + " VALUES ($1, $2, $3, 1)"
        + " ON CONFLICT (subject, bucket, window_start_ms)"
        + " DO UPDATE SET count = LEAST(rate_budget.count + 1, 1000000000), updated_at = now()"
        + " RETURNING count",
      [subj, b, windowStartMs],
    );
    const count = Number(res.rows[0].count);
    return {
      allowed: count <= maxPerWindow,
      count,
      windowStartMs,
      // How long until this window rolls over — what a caller should tell the client to wait.
      retryAfterMs: windowStartMs + windowMs - nowMs,
    };
  }

  /**
   * Delete windows that closed before `olderThanMs`. Counters are only meaningful inside their own
   * window, so this is pure garbage collection — it can never affect a live decision.
   * @returns {Promise<number>} rows removed
   */
  async sweep({ olderThanMs } = {}) {
    if (!Number.isFinite(olderThanMs) || olderThanMs < 0) {
      throw new Error("PgRateBudget.sweep requires a non-negative finite olderThanMs");
    }
    const res = await this.#conn.query(
      "DELETE FROM rate_budget WHERE window_start_ms < $1",
      [Math.floor(olderThanMs)],
    );
    return res.rowCount;
  }
}
