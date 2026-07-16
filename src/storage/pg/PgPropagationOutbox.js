/**
 * PgPropagationOutbox (P1#2/P1#3 — leaf 1: schema + atomic enqueue only).
 *
 * The node-owned durable queue of authority-state publication obligations
 * (account_propagation_outbox, migrations 0017 + 0018). Leaf 1 exposes ONLY:
 *   - enqueueInTx(client, ...) — the SSOT enqueue SQL, called by
 *     PgAccountMutationSerializer WITHIN its fold transaction so the queue row and the
 *     authority commit succeed or roll back together (a queue failure rolls back the
 *     mutation). Enqueue is reached only on a REAL epoch-changing fold — the serializer
 *     returns before it for a semantic no-op, a stale expectedRevision, or an idempotent
 *     replay.
 *   - listPending / getPendingCount — read helpers for tests / observability.
 *
 * DRAIN SEMANTICS — HEAD-ADVANCING ACCOUNT LEASE (the contract leaf 2 implements; NOT
 * oldest-first): `authority_state` is CUMULATIVE — the published AccountAuthorityStateV1 is the
 * LATEST full snapshot, and a client can only reconstruct the CURRENT authority, never an exact
 * superseded epoch (whose journal replay payload may have expired). The lease is therefore
 * ACCOUNT-scoped, not row-scoped:
 *   - ONE lease token covers (account, authority_state), anchored at the epoch that was head when
 *     it was taken. At most one leased row per (account, kind) — the DB backstops this with a
 *     partial unique index (migration 0019).
 *   - While a lease is held, newer epochs may still COMMIT (they stay pending) but CANNOT receive
 *     a second lease — so N and N+1 are never leased concurrently (no out-of-order publish).
 *   - The holder may publish any pending CURRENT epoch M >= its anchor N (the head can advance
 *     under it between claim and publish — that is expected, not a conflict).
 *   - A VERIFIED ack for M completes EVERY pending obligation <= M; epochs above M stay pending
 *     for the same or the next lease.
 * The lease / claim / reclaim / publish / ack drainer itself is a LATER leaf and deliberately
 * absent here.
 *
 * The row carries NO secrets and NO peer identities — only the account's own id + the epoch.
 * Peer-specific device-set fan-out is a SEPARATE client-owned per-peer queue, never this table.
 */
export class PgPropagationOutbox {
  #conn;

  /** @param {{ connection?: object }} opts connection is only needed for the standalone read helpers. */
  constructor({ connection = null } = {}) {
    this.#conn = connection;
  }

  /**
   * Enqueue a propagation obligation WITHIN a caller-owned transaction (the serializer's
   * per-account fold, under its advisory lock). Idempotent on (account, epoch, kind): a real
   * fold produces a unique epoch, so this inserts exactly one row per epoch-changing mutation;
   * ON CONFLICT DO NOTHING makes any retry a no-op. Throws propagate to the caller, which owns
   * ROLLBACK — so a failed enqueue rolls back the authority mutation (no committed fold without
   * its publication obligation).
   *
   * @param {object} client the in-transaction pg client the serializer already holds
   * @param {{ accountIdentityPublicKeyB64: string, epoch: number, kind?: string }} row
   */
  async enqueueInTx(client, { accountIdentityPublicKeyB64, epoch, kind = "authority_state" } = {}) {
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    if (!account) {
      throw new Error("PgPropagationOutbox.enqueueInTx requires accountIdentityPublicKeyB64");
    }
    if (!Number.isSafeInteger(epoch) || epoch < 1) {
      throw new Error("PgPropagationOutbox.enqueueInTx requires a positive integer epoch");
    }
    const k = typeof kind === "string" && kind.trim().length > 0 ? kind.trim() : "authority_state";
    await client.query(
      "INSERT INTO account_propagation_outbox (account_identity, epoch, kind, status)"
        + " VALUES ($1, $2, $3, 'pending')"
        + " ON CONFLICT (account_identity, epoch, kind) DO NOTHING",
      [account, epoch, k],
    );
  }

  /**
   * The pending obligations for an account, ordered by epoch ascending. This ordering is
   * for OBSERVABILITY / tests ONLY — it is NOT lease priority. The drainer leases the NEWEST
   * pending epoch per account (see the class docstring's cumulative drain contract), never the
   * oldest. This helper takes no lease.
   * @returns {Promise<Array<{ epoch: number, kind: string, status: string, attempts: number }>>}
   */
  async listPending(accountIdentityPublicKeyB64) {
    if (!this.#conn) throw new Error("PgPropagationOutbox.listPending requires a connection");
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    const res = await this.#conn.query(
      "SELECT epoch, kind, status, attempts FROM account_propagation_outbox"
        + " WHERE account_identity = $1 AND status = 'pending' ORDER BY epoch",
      [account],
    );
    return res.rows.map((r) => ({
      epoch: Number(r.epoch),
      kind: String(r.kind),
      status: String(r.status),
      attempts: Number(r.attempts),
    }));
  }

  /** Total pending obligations across all accounts (test/observability). */
  async getPendingCount() {
    if (!this.#conn) throw new Error("PgPropagationOutbox.getPendingCount requires a connection");
    const res = await this.#conn.query(
      "SELECT count(*)::bigint AS c FROM account_propagation_outbox WHERE status = 'pending'",
    );
    return Number(res.rows[0].c);
  }
}
