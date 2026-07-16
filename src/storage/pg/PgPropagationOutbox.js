/**
 * PgPropagationOutbox (P1#2/P1#3 — leaves 1-2: schema, atomic enqueue, and the crypto-free
 * head-advancing account-lease drain state machine).
 *
 * The node-owned durable queue of authority-state publication obligations
 * (account_propagation_outbox, migrations 0017-0021). Surface:
 *   - enqueueInTx(client, ...) — the SSOT enqueue SQL, called by PgAccountMutationSerializer
 *     WITHIN its fold transaction so the queue row and the authority commit succeed or roll back
 *     together. Reached only on a REAL epoch-changing fold (no-op / stale / replay return before).
 *   - claim / preparePublication / fail / release — the lease state machine (below).
 *   - listPending / getPendingCount — read helpers for tests / observability.
 *
 * The crypto (sign/verify/publish) and the VERIFIED-completion op that marks obligations done live
 * in later leaves (3-5); this class is node-side + crypto-free.
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
import { randomBytes } from "node:crypto";

// SERVER-OWNED lease policy (audit crit 4: clients choose NONE of these). Lease duration, retry
// backoff, and the attempt cap are node constants; all times are computed by Postgres (now()).
const LEASE_TTL_MS = 30_000;        // max time a claimant holds the account head before it expires.
const BACKOFF_BASE_MS = 1_000;      // first retry delay after a failed/expired lease.
const BACKOFF_MAX_MS = 60_000;      // backoff caps here (never grows unbounded).
const ATTEMPT_BACKOFF_CAP = 16;     // exponent saturates here so a hot obligation still retries.
// The PERSISTED attempt counter is LEAST-clamped to this (also a DB CHECK, migration 0020) so it
// can never overflow the int column and strand the obligation.
const MAX_PERSISTED_ATTEMPTS = 1_000_000;
// Operator-visible blocked threshold: at/above this many failures a `blocked_at` timestamp is
// stamped (once). The obligation stays OUTSTANDING + eligible after backoff — never 'done'.
const BLOCKED_ATTEMPT_THRESHOLD = 20;

// Bounded exponential backoff from the row's saturating attempt count. Never returns "give up":
// exhaustion just holds at BACKOFF_MAX_MS, keeping the obligation outstanding + eligible (crit 7).
function backoffMsFor(attempts) {
  const n = Number.isSafeInteger(attempts) && attempts > 0 ? Math.min(attempts, ATTEMPT_BACKOFF_CAP) : 1;
  return Math.min(BACKOFF_BASE_MS * Math.pow(2, n - 1), BACKOFF_MAX_MS);
}

function mintLeaseToken() {
  return randomBytes(24).toString("hex"); // 48 hex chars, well under the 128-byte DB cap.
}

export class PgPropagationOutbox {
  #conn;

  /** @param {{ connection?: object }} opts connection is only needed for the standalone read helpers. */
  constructor({ connection = null } = {}) {
    this.#conn = connection;
  }

  /**
   * Claim the account's NEWEST pending authority-state head under a fresh server-minted lease.
   * Head-advancing account lease (audit leaf 2):
   *   - crit 1: serialize on the account row (FOR UPDATE SKIP LOCKED) BEFORE selecting the head,
   *     so two claimants can never pick different pending rows for one account.
   *   - reclaim: an EXPIRED prior lease is returned to 'pending' (with backoff) before we choose;
   *     a LIVE lease means the account is busy → null.
   *   - crit 2: pick the ABSOLUTE newest pending epoch, THEN test its backoff — if the newest is
   *     backing off, return null (never lease an older epoch while a newer one waits).
   *   - crit 3/4: lease that head as the anchor with a server token + server TTL + DB time.
   * @returns {Promise<null | { token: string, anchorEpoch: number, headEpoch: number, leaseExpiresAtMs: number, attempts: number }>}
   *   null when the account is busy, has nothing outstanding, or its head is backing off.
   */
  async claim(accountIdentityPublicKeyB64) {
    if (!this.#conn) throw new Error("PgPropagationOutbox.claim requires a connection");
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    if (!account) throw new Error("PgPropagationOutbox.claim requires accountIdentityPublicKeyB64");
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        // crit 1 — per-account serialization + SKIP LOCKED. No authority row ⇒ no obligations
        // could have been enqueued; a locked row ⇒ another claimant holds this account.
        const lock = await client.query(
          "SELECT epoch FROM account_authority WHERE account_identity = $1 FOR UPDATE SKIP LOCKED",
          [account],
        );
        if (lock.rowCount === 0) {
          await client.query("COMMIT");
          return null;
        }
        // Reclaim an EXPIRED lease: release its anchor and apply the failure penalty to the epoch
        // that lease actually ATTEMPTED (its prepared_epoch), NOT the newest-at-reclaim-time head
        // — a still-newer epoch may have committed and was never attempted (audit leaf-2.1 P2).
        const expired = await client.query(
          "SELECT epoch, prepared_epoch FROM account_propagation_outbox"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased'"
            + " AND lease_expires_at <= now() FOR UPDATE",
          [account],
        );
        if (expired.rowCount > 0) {
          const e = expired.rows[0];
          const attempted = e.prepared_epoch == null ? Number(e.epoch) : Number(e.prepared_epoch);
          await this.#releaseAnchorAndBackoffEpoch(client, account, Number(e.epoch), attempted, "LEASE_EXPIRED");
        }
        // A still-LIVE lease ⇒ the account is busy (the one-leased index also backstops this).
        const live = await client.query(
          "SELECT 1 FROM account_propagation_outbox WHERE account_identity = $1 AND kind = 'authority_state'"
            + " AND status = 'leased' AND lease_expires_at > now() LIMIT 1",
          [account],
        );
        if (live.rowCount > 0) {
          await client.query("COMMIT");
          return null;
        }
        // crit 2 — the ABSOLUTE newest pending epoch, then its backoff.
        const head = await client.query(
          "SELECT epoch, attempts, (next_attempt_at <= now()) AS eligible FROM account_propagation_outbox"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'pending'"
            + " ORDER BY epoch DESC LIMIT 1",
          [account],
        );
        if (head.rowCount === 0 || head.rows[0].eligible !== true) {
          await client.query("COMMIT");
          return null; // nothing outstanding, OR the newest head is backing off.
        }
        const headEpoch = Number(head.rows[0].epoch);
        const token = mintLeaseToken();
        // The lease itself does NOT touch `attempts` — that counts FAILURES (bumped only by
        // fail() / expired-reclaim), so it never double-counts a successful claim. `prepared_epoch`
        // is seeded to the leased head so failure accounting has an attempted epoch even if the
        // holder fails before calling preparePublication (which advances it to the current head).
        const up = await client.query(
          "UPDATE account_propagation_outbox SET status = 'leased', lease_token = $3,"
            + " lease_expires_at = now() + ($4::bigint * interval '1 millisecond'), prepared_epoch = $2,"
            + " updated_at = now()"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2 AND status = 'pending'"
            + " RETURNING attempts, (extract(epoch from lease_expires_at) * 1000)::bigint AS lease_ms",
          [account, headEpoch, token, LEASE_TTL_MS],
        );
        await client.query("COMMIT");
        return {
          token,
          anchorEpoch: headEpoch,
          headEpoch,
          leaseExpiresAtMs: Number(up.rows[0].lease_ms),
          attempts: Number(up.rows[0].attempts),
        };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Voluntarily RELEASE a held lease back to 'pending' WITHOUT a failure penalty (immediately
   * re-eligible) — e.g. the client found nothing new to publish. Token-bound (crit 5): a wrong,
   * expired, or replaced token changes nothing.
   * @returns {Promise<boolean>} true iff this exact live lease was released.
   */
  async release(accountIdentityPublicKeyB64, token) {
    const { account, tok } = this.#requireTokenArgs("release", accountIdentityPublicKeyB64, token);
    const res = await this.#conn.query(
      "UPDATE account_propagation_outbox SET status = 'pending', lease_token = NULL,"
        + " lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased'"
        + " AND lease_token = $2 AND lease_expires_at > now()",
      [account, tok],
    );
    return res.rowCount > 0;
  }

  /**
   * Record a FAILED publish: release the anchor and apply bounded backoff + a SATURATING attempt
   * count to the epoch the holder actually ATTEMPTED (the lease's prepared_epoch) — never abandoned,
   * never 'done'. Token-bound (crit 5).
   * @returns {Promise<null | { attemptedEpoch: number, anchorEpoch: number, attempts: number, backoffMs: number, blocked: boolean }>}
   *   null iff the token does not hold a live lease.
   */
  async fail(accountIdentityPublicKeyB64, token) {
    const { account, tok } = this.#requireTokenArgs("fail", accountIdentityPublicKeyB64, token);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        // Token-bound: only the live lease holder may record a failure. FOR UPDATE locks the anchor.
        const anchor = await client.query(
          "SELECT epoch, prepared_epoch FROM account_propagation_outbox"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased'"
            + " AND lease_token = $2 AND lease_expires_at > now() FOR UPDATE",
          [account, tok],
        );
        if (anchor.rowCount === 0) {
          await client.query("COMMIT");
          return null;
        }
        const a = anchor.rows[0];
        const attempted = a.prepared_epoch == null ? Number(a.epoch) : Number(a.prepared_epoch);
        const result = await this.#releaseAnchorAndBackoffEpoch(client, account, Number(a.epoch), attempted, "PUBLISH_FAILED");
        await client.query("COMMIT");
        return result;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Shared failure/expiry accounting (audit leaf-2.1 P2): release the ANCHOR back to 'pending'
   * (clear its lease + prepared_epoch) and apply the backoff + SATURATING attempt count to the epoch
   * that lease actually ATTEMPTED — its bound prepared_epoch, NOT the newest-at-failure-time head.
   * A still-newer epoch that committed after preparation was never attempted, so it stays fresh and
   * eligible (publishing it later completes the older obligation cumulatively). The counter is
   * LEAST-clamped (also a DB CHECK) so it can never overflow; crossing the blocked threshold stamps
   * `blocked_at` once. The obligation stays OUTSTANDING — never 'done'. Runs in a caller-owned
   * transaction that already locked the anchor.
   * @returns {Promise<{ attemptedEpoch: number, anchorEpoch: number, attempts: number, backoffMs: number, blocked: boolean }>}
   */
  async #releaseAnchorAndBackoffEpoch(client, account, anchorEpoch, attemptedEpoch, errCode) {
    // Lock the attempted-epoch row + read its attempts. (If it == the anchor, the anchor is already
    // locked by the caller; re-locking in the same tx is a no-op.)
    const row = await client.query(
      "SELECT attempts FROM account_propagation_outbox"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2 FOR UPDATE",
      [account, attemptedEpoch],
    );
    // Release the anchor (clears the lease + prepared_epoch; if attempted == anchor the backoff
    // below re-touches the now-pending row).
    await client.query(
      "UPDATE account_propagation_outbox SET status = 'pending', lease_token = NULL,"
        + " lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2",
      [account, anchorEpoch],
    );
    // Defensive: the attempted row should always exist (obligations are never deleted), but if it
    // somehow does not, there is nothing to back off — return without inventing state.
    const priorAttempts = row.rowCount > 0 ? Number(row.rows[0].attempts) : 0;
    const attempts = Math.min(priorAttempts + 1, MAX_PERSISTED_ATTEMPTS);
    const backoffMs = backoffMsFor(attempts);
    const blocked = attempts >= BLOCKED_ATTEMPT_THRESHOLD;
    await client.query(
      "UPDATE account_propagation_outbox SET attempts = $3,"
        + " next_attempt_at = now() + ($4::bigint * interval '1 millisecond'),"
        + " blocked_at = CASE WHEN $5 AND blocked_at IS NULL THEN now() ELSE blocked_at END,"
        + " last_error = $6, updated_at = now()"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2",
      [account, attemptedEpoch, attempts, backoffMs, blocked, errCode],
    );
    return { attemptedEpoch, anchorEpoch, attempts, backoffMs, blocked };
  }

  /**
   * CRYPTO-FREE publication preparation: under a live token, resolve the CURRENT head M (the max
   * outstanding epoch — the head may have advanced under the lease), RECORD it as the lease's
   * attempted epoch (prepared_epoch), and report { anchorEpoch, headEpoch: M } for the client to
   * publish. Named "prepare*Publication*", NOT ack: an ACK is exclusively the later VERIFIED-
   * completion op (leaf 4), which alone may mark obligations <= M 'done' and MUST lock this same
   * anchor + re-check the token AFTER verification. Binding the attempted epoch here is what lets
   * fail()/expiry back off the epoch actually attempted, not a later un-attempted head.
   * @returns {Promise<null | { anchorEpoch: number, headEpoch: number }>} null iff the token does
   *   not hold a live lease.
   */
  async preparePublication(accountIdentityPublicKeyB64, token) {
    const { account, tok } = this.#requireTokenArgs("preparePublication", accountIdentityPublicKeyB64, token);
    // Under READ COMMITTED each statement takes a fresh snapshot; the SAFETY comes from FOR UPDATE
    // holding the leased anchor across the transaction — the lease cannot expire / be reclaimed /
    // be completed while we hold it. A newer head committing between statements is INTENTIONALLY
    // visible (that is the head we want to record + publish). Leaf-4 completion must likewise lock
    // this anchor before validating the token.
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        const anchor = await client.query(
          "SELECT epoch FROM account_propagation_outbox WHERE account_identity = $1 AND kind = 'authority_state'"
            + " AND status = 'leased' AND lease_token = $2 AND lease_expires_at > now() FOR UPDATE",
          [account, tok],
        );
        if (anchor.rowCount === 0) {
          await client.query("COMMIT");
          return null;
        }
        const anchorEpoch = Number(anchor.rows[0].epoch);
        const head = await client.query(
          "SELECT max(epoch) AS m FROM account_propagation_outbox"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND status IN ('pending', 'leased')",
          [account],
        );
        const headEpoch = Number(head.rows[0].m); // anchor is a live leased row ⇒ head >= anchor, never 0.
        // Bind the attempted epoch to the lease (on the anchor row) so failure accounting targets it.
        await client.query(
          "UPDATE account_propagation_outbox SET prepared_epoch = $3, updated_at = now()"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2",
          [account, anchorEpoch, headEpoch],
        );
        await client.query("COMMIT");
        return { anchorEpoch, headEpoch };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  #requireTokenArgs(fn, accountIdentityPublicKeyB64, token) {
    if (!this.#conn) throw new Error("PgPropagationOutbox." + fn + " requires a connection");
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    const tok = typeof token === "string" ? token.trim() : "";
    if (!account) throw new Error("PgPropagationOutbox." + fn + " requires accountIdentityPublicKeyB64");
    if (!tok) throw new Error("PgPropagationOutbox." + fn + " requires a lease token");
    return { account, tok };
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
