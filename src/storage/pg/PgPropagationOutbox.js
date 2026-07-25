import { PgAccountRateBudget } from "./PgAccountRateBudget.js";
/**
 * PgPropagationOutbox (P1#2/P1#3 — leaves 1-2: schema, atomic enqueue, and the crypto-free
 * head-advancing account-lease drain state machine).
 *
 * The node-owned durable queue of authority-state publication obligations
 * (account_propagation_outbox, migrations 0017-0024). Surface:
 *   - enqueueInTx(client, ...) — the SSOT enqueue SQL, called by PgAccountMutationSerializer
 *     WITHIN its fold transaction so the queue row and the authority commit succeed or roll back
 *     together. Reached only on a REAL epoch-changing fold (no-op / stale / replay return before).
 *   - claim / preparePublication / fail / release — the lease state machine (below).
 *   - completePublication — the VERIFIED-completion (leaf 3c) that marks obligations 'done'.
 *   - listPending / getPendingCount — read helpers for tests / observability.
 *
 * This class stays node-side + CRYPTO-FREE. completePublication takes an ALREADY-VERIFIED epoch M:
 * the signature/cert-chain verification of the publication happens in the HANDLER
 * (PropagationOutboxHandler.handleComplete) BEFORE it calls this method, which only re-checks the
 * lease token and writes the 'done' watermark under the anchor lock.
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
 * completePublication implements that verified ack (its crypto verification is the handler's).
 *
 * The row carries NO secrets and NO peer identities — only the account's own id + the epoch.
 * Peer-specific device-set fan-out is a SEPARATE client-owned per-peer queue, never this table.
 */
import { randomBytes } from "node:crypto";
import { isCanonicalDeviceId } from "@rezprotocol/core";

// SERVER-OWNED lease policy (audit crit 4: clients choose NONE of these). Lease duration, retry
// backoff, and the attempt cap are node constants; all times are computed by Postgres. Expiry /
// backoff / lease predicates use clock_timestamp() (the WALL clock, re-evaluated at statement
// execution) — NOT now(), which freezes at BEGIN and would let a request that blocked on a row
// lock past the real deadline still act on an expired lease (audit lease-clock fix).
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

// The lease owner is a CANONICAL device id (rez:dev:<64 lc-hex>) — the rez-core SSOT shape (audit
// leaf-3a F1). Enforced at the owner-ASSERTING JS entry points (claim + release / fail /
// preparePublication) so a non-device owner is rejected before it reaches SQL; the migration 0024
// CHECK is the DB backstop. NOT revoke-release: releaseOwnedInTx is revoke-side cleanup that must
// tolerate a historical non-canonical device the fold fail-closes (see the note there).
function requireCanonicalOwner(fn, ownerDeviceId) {
  const owner = typeof ownerDeviceId === "string" ? ownerDeviceId.trim() : "";
  if (!isCanonicalDeviceId(owner)) {
    throw new Error("PgPropagationOutbox." + fn + " requires ownerDeviceId to be a canonical rez:dev:<64-hex> id");
  }
  return owner;
}

export class PgPropagationOutbox {
  #conn;
  #accountRateBudget;

  /** @param {{ connection?: object }} opts connection is only needed for the standalone read helpers. */
  constructor({ connection = null } = {}) {
    this.#conn = connection;
    // F3 (audit leaf-3c): the cluster-wide per-account request budget, built over the SAME
    // connection. Exposed here for the same reason the runtime derives the outbox from the
    // serializer — so the budget can never be pointed at a different database than the resource it
    // bounds. Null without a connection (the in-fold-only construction used by the serializer).
    this.#accountRateBudget = connection ? new PgAccountRateBudget({ connection }) : null;
  }

  /** The cluster-wide per-account rate budget over this outbox's own connection (F3). */
  get accountRateBudget() {
    return this.#accountRateBudget;
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
  async claim(accountIdentityPublicKeyB64, ownerDeviceId) {
    if (!this.#conn) throw new Error("PgPropagationOutbox.claim requires a connection");
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    if (!account) throw new Error("PgPropagationOutbox.claim requires accountIdentityPublicKeyB64");
    // req 4: the lease is bound to its OWNER device — a token alone is not transferable. The owner
    // is the caller's authenticated device principal, derived at the wire boundary (never the request
    // body), and must be a canonical device id (leaf-3a F1).
    const owner = requireCanonicalOwner("claim", ownerDeviceId);
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
        // Classify the account's leased anchor (if any) in ONE post-lock decision — under the
        // account lock the one-leased index guarantees at most one. LOCK it first (no clock
        // predicate in the FOR UPDATE), then evaluate liveness in a SEPARATE clock_timestamp()
        // statement (the post-lock rule). This replaces the old expired-then-live pair, which had
        // a boundary race: the deadline could fall BETWEEN the two clock evaluations, so neither
        // reclaimed nor saw-live the anchor, and the later lease insert then tripped the one-lease
        // unique index instead of reclaiming cleanly.
        const leased = await client.query(
          "SELECT epoch, prepared_epoch, lease_owner, lease_token FROM account_propagation_outbox"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased' FOR UPDATE",
          [account],
        );
        if (leased.rowCount > 0) {
          const lrow = leased.rows[0];
          const lepoch = Number(lrow.epoch);
          const cls = await client.query(
            "SELECT (lease_expires_at > clock_timestamp()) AS live,"
              + " (extract(epoch from lease_expires_at) * 1000)::bigint AS lease_ms, attempts"
              + " FROM account_propagation_outbox"
              + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2 AND status = 'leased'",
            [account, lepoch],
          );
          if (cls.rowCount === 1 && cls.rows[0].live === true) {
            // IDEMPOTENT CLAIM RECOVERY (audit leaf-3b F3): if the SAME owner re-claims a still-live
            // lease — e.g. its original claim RESPONSE was lost in flight — return the EXISTING
            // lease + token rather than null, so the authorized device recovers AT ONCE instead of
            // waiting out the ~30s TTL. A DIFFERENT device still sees the account as busy (null):
            // the lease is not transferable, and this returns the token ONLY to the owner that
            // already holds it (the same owner that would have received it originally).
            if (lrow.lease_owner === owner) {
              await client.query("COMMIT");
              return {
                token: lrow.lease_token,
                anchorEpoch: lepoch,
                headEpoch: lepoch,
                leaseExpiresAtMs: Number(cls.rows[0].lease_ms),
                attempts: Number(cls.rows[0].attempts),
              };
            }
            await client.query("COMMIT");
            return null; // a still-LIVE lease held by ANOTHER device ⇒ the account is busy.
          }
          // Expired ⇒ reclaim: release the anchor + apply the failure penalty to the epoch that
          // lease actually ATTEMPTED (its prepared_epoch), not a still-newer un-attempted head.
          const attempted = lrow.prepared_epoch == null ? lepoch : Number(lrow.prepared_epoch);
          await this.#releaseAnchorAndBackoffEpoch(client, account, lepoch, attempted, "LEASE_EXPIRED");
        }
        // crit 2 — the ABSOLUTE newest pending epoch, then its backoff.
        const head = await client.query(
          "SELECT epoch, attempts, (next_attempt_at <= clock_timestamp()) AS eligible FROM account_propagation_outbox"
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
        // The lease itself does NOT touch `attempts` (that counts FAILURES, bumped only by
        // fail() / expired-reclaim) and leaves `prepared_epoch` NULL — the attempted epoch is
        // FROZEN by the first preparePublication (idempotently). A holder that fails/expires
        // BEFORE preparing has no attempted epoch, so failure accounting conservatively targets
        // the anchor.
        const up = await client.query(
          "UPDATE account_propagation_outbox SET status = 'leased', lease_token = $3, lease_owner = $5,"
            + " lease_expires_at = clock_timestamp() + ($4::bigint * interval '1 millisecond'), updated_at = now()"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2 AND status = 'pending'"
            + " RETURNING attempts, (extract(epoch from lease_expires_at) * 1000)::bigint AS lease_ms",
          [account, headEpoch, token, LEASE_TTL_MS, owner],
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
  async release(accountIdentityPublicKeyB64, token, ownerDeviceId) {
    const { account, tok, owner } = this.#requireTokenArgs("release", accountIdentityPublicKeyB64, token, ownerDeviceId);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        const anchor = await this.#lockAndCheckLive(client, account, tok, owner);
        if (!anchor.live) {
          await client.query("COMMIT");
          return false;
        }
        await client.query(
          "UPDATE account_propagation_outbox SET status = 'pending', lease_token = NULL,"
            + " lease_owner = NULL, lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2",
          [account, anchor.epoch],
        );
        await client.query("COMMIT");
        return true;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Lock the token-bearing leased anchor (statement 1: FOR UPDATE, by identity — NO clock in the
   * WHERE), then evaluate its wall-clock liveness in a SEPARATE statement (statement 2:
   * clock_timestamp(), which is only correctly evaluated post-lock as its OWN statement, not inside
   * the FOR UPDATE target list under EvalPlanQual). SSOT for the "is this token holding a live
   * lease" gate used by fail / preparePublication / release.
   * @returns {Promise<{ live: boolean, epoch: number|null, prepared_epoch: (number|string|null) }>}
   */
  async #lockAndCheckLive(client, account, tok, owner) {
    // req 4: match the token AND the OWNER device. The binding is to the device PRINCIPAL, not a
    // socket session — a token presented by a DIFFERENT device does not match the leased row, so it
    // can neither authorize nor mutate the lease. Two authenticated sessions of the SAME device
    // legitimately share the lease (intended, for reconnect recovery).
    const locked = await client.query(
      "SELECT epoch, prepared_epoch FROM account_propagation_outbox"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased'"
        + " AND lease_token = $2 AND lease_owner = $3 FOR UPDATE",
      [account, tok, owner],
    );
    if (locked.rowCount === 0) {
      return { live: false, epoch: null, prepared_epoch: null };
    }
    const epoch = Number(locked.rows[0].epoch);
    const liveRes = await client.query(
      "SELECT (lease_expires_at > clock_timestamp()) AS live FROM account_propagation_outbox"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2"
        + " AND status = 'leased' AND lease_token = $3",
      [account, epoch, tok],
    );
    return {
      live: liveRes.rowCount === 1 && liveRes.rows[0].live === true,
      epoch,
      prepared_epoch: locked.rows[0].prepared_epoch,
    };
  }

  /**
   * Record a FAILED publish: release the anchor and apply bounded backoff + a SATURATING attempt
   * count to the epoch the holder actually ATTEMPTED (the lease's prepared_epoch) — never abandoned,
   * never 'done'. Token-bound (crit 5).
   * @returns {Promise<null | { attemptedEpoch: number, anchorEpoch: number, attempts: number, backoffMs: number, blocked: boolean }>}
   *   null iff the token does not hold a live lease.
   */
  async fail(accountIdentityPublicKeyB64, token, ownerDeviceId) {
    const { account, tok, owner } = this.#requireTokenArgs("fail", accountIdentityPublicKeyB64, token, ownerDeviceId);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        // Token-bound: only the LIVE lease holder may record a failure. Lock the anchor by
        // account/token/status FIRST (statement 1), then compare expiry against clock_timestamp()
        // in a SEPARATE statement (statement 2). The clock check MUST be its own statement: a
        // volatile clock_timestamp() in the FOR UPDATE statement's target list is not re-evaluated
        // post-lock (EvalPlanQual), so a request that blocked on the row lock past the real
        // deadline would still see it live. now() is worse (frozen at BEGIN).
        const anchor = await this.#lockAndCheckLive(client, account, tok, owner);
        if (!anchor.live) {
          await client.query("COMMIT");
          return null;
        }
        const a = anchor;
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
    // locked by the caller; re-locking in the same tx is a no-op.) The attempted row MUST exist
    // (obligations are never deleted + a self-FK guards the binding) — a missing row is invariant
    // drift, so throw and roll back rather than release the anchor and silently discard the retry
    // accounting (fail-loud rule).
    const row = await client.query(
      "SELECT attempts FROM account_propagation_outbox"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2 FOR UPDATE",
      [account, attemptedEpoch],
    );
    if (row.rowCount !== 1) {
      throw new Error(
        "PgPropagationOutbox: attempted epoch " + attemptedEpoch + " has no obligation for account "
          + account + " (invariant drift) — refusing to release the lease",
      );
    }
    // Release the anchor (clears the lease + prepared_epoch; if attempted == anchor the backoff
    // below re-touches the now-pending row).
    await client.query(
      "UPDATE account_propagation_outbox SET status = 'pending', lease_token = NULL,"
        + " lease_owner = NULL, lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2",
      [account, anchorEpoch],
    );
    const attempts = Math.min(Number(row.rows[0].attempts) + 1, MAX_PERSISTED_ATTEMPTS);
    const backoffMs = backoffMsFor(attempts);
    const blocked = attempts >= BLOCKED_ATTEMPT_THRESHOLD;
    await client.query(
      "UPDATE account_propagation_outbox SET attempts = $3,"
        + " next_attempt_at = clock_timestamp() + ($4::bigint * interval '1 millisecond'),"
        + " blocked_at = CASE WHEN $5 AND blocked_at IS NULL THEN clock_timestamp() ELSE blocked_at END,"
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
  async preparePublication(accountIdentityPublicKeyB64, token, ownerDeviceId) {
    const { account, tok, owner } = this.#requireTokenArgs("preparePublication", accountIdentityPublicKeyB64, token, ownerDeviceId);
    // Under READ COMMITTED each statement takes a fresh snapshot; the SAFETY comes from FOR UPDATE
    // holding the leased anchor across the transaction — the lease cannot expire / be reclaimed /
    // be completed while we hold it. A newer head committing between statements is INTENTIONALLY
    // visible (that is the head we want to record + publish). Leaf-4 completion must likewise lock
    // this anchor before validating the token.
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        // Lock the anchor + a SEPARATE post-lock wall-clock liveness check (see #lockAndCheckLive)
        // so a request that blocked on the row lock past the real deadline is rejected.
        const anchor = await this.#lockAndCheckLive(client, account, tok, owner);
        if (!anchor.live) {
          await client.query("COMMIT");
          return null;
        }
        const anchorEpoch = anchor.epoch;
        const head = await client.query(
          "SELECT max(epoch) AS m FROM account_propagation_outbox"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND status IN ('pending', 'leased')",
          [account],
        );
        const currentHead = Number(head.rows[0].m); // anchor is a live leased row ⇒ head >= anchor.
        // IDEMPOTENT FREEZE (audit re-review P1): the FIRST preparation of this lease binds the
        // attempted epoch; a repeated/duplicate preparation (a retry, or two sessions sharing the
        // bearer token) returns the ALREADY-FROZEN epoch and never re-points it to a newer head —
        // so the in-flight publication's epoch cannot be changed under it. COALESCE keeps the
        // existing prepared_epoch when set, else freezes the current head.
        const frozen = await client.query(
          // Authorization was linearized at #lockAndCheckLive (post-lock, wall clock) and we STILL
          // hold that FOR UPDATE lock — so the row cannot change under us. NO second expiry
          // predicate here: re-checking clock_timestamp() could yield zero RETURNING rows if the
          // deadline fell between the statements, and then rows[0] would throw. Guard by the locked
          // identity (epoch + token + leased) only.
          "UPDATE account_propagation_outbox SET prepared_epoch = COALESCE(prepared_epoch, $3), updated_at = now()"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch = $2"
            + " AND status = 'leased' AND lease_token = $4"
            + " RETURNING prepared_epoch",
          [account, anchorEpoch, currentHead, tok],
        );
        await client.query("COMMIT");
        // headEpoch = the FROZEN attempted epoch (currentHead on first prepare, unchanged on repeats).
        return { anchorEpoch, headEpoch: Number(frozen.rows[0].prepared_epoch) };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * VERIFIED completion (leaf 3c) — the FIRST and ONLY writer of status='done'. Under the same
   * anchor lock + post-lock wall-clock liveness re-check the crypto-free ops use, and ONLY after the
   * caller has cryptographically verified the publication for epoch M, mark EVERY outstanding
   * obligation (epoch <= M, status pending|leased) 'done' and clear the anchor's lease + prepared_
   * epoch. This is the CUMULATIVE drain: a verified ack of M satisfies all obligations <= M (older
   * un-drained epochs are superseded by the newer published snapshot); epochs above M stay pending.
   *
   * Re-checking the token HERE — after the caller's verification — is the "token re-checked AFTER
   * verification" invariant: a lease that lapsed during verification completes nothing. M is bound to
   * the lease's FROZEN prepared_epoch (you may only complete the epoch you prepared); a mismatch or a
   * not-yet-prepared lease completes nothing and reports the expected epoch so the handler answers a
   * protocol error rather than silently acking the wrong head. This method is crypto-FREE — M is
   * already authenticated by the handler.
   *
   * @param {number} verifiedEpoch the epoch M the handler extracted from the VERIFIED inner
   *   AccountAuthorityStateV1 (a positive integer — the handler guarantees it).
   * @returns {Promise<null | { completed: boolean, doneThroughEpoch?: number, expectedEpoch?: number|null }>}
   *   null — the token does not hold a live lease (benign lease-lost race).
   *   { completed:false, expectedEpoch } — live lease, but M != its frozen prepared_epoch
   *     (expectedEpoch is the frozen epoch, or null if the lease was never prepared).
   *   { completed:true, doneThroughEpoch: M } — obligations <= M marked done; the lease is released.
   */
  async completePublication(accountIdentityPublicKeyB64, token, ownerDeviceId, verifiedEpoch) {
    const { account, tok, owner } = this.#requireTokenArgs("completePublication", accountIdentityPublicKeyB64, token, ownerDeviceId);
    // Defensive caller contract (the handler validates the inner epoch before calling): M must be a
    // positive integer. An obligation is only ever enqueued for epoch >= 1, so a non-positive M
    // could never match a frozen prepared_epoch — fail loud rather than run a meaningless UPDATE.
    if (!Number.isSafeInteger(verifiedEpoch) || verifiedEpoch < 1) {
      throw new Error("PgPropagationOutbox.completePublication requires a positive integer verifiedEpoch");
    }
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        // Lock the anchor + a SEPARATE post-lock wall-clock liveness check (see #lockAndCheckLive):
        // a request that blocked on the row lock past the real deadline is rejected. This is the
        // token re-check AFTER the caller's verification.
        const anchor = await this.#lockAndCheckLive(client, account, tok, owner);
        if (!anchor.live) {
          await client.query("COMMIT");
          return null;
        }
        // The epoch acked MUST equal the lease's FROZEN attempted epoch. A lease that never prepared
        // has nothing frozen (prepared_epoch NULL) — reject; do not guess a head. No coercion: a raw
        // NULL stays null, and only an exact integer match proceeds.
        const prepared = anchor.prepared_epoch == null ? null : Number(anchor.prepared_epoch);
        if (prepared === null || prepared !== verifiedEpoch) {
          await client.query("COMMIT");
          return { completed: false, expectedEpoch: prepared };
        }
        // Cumulative drain: mark EVERY outstanding obligation <= M done and clear lease state on
        // those rows. prepared_epoch >= anchor epoch (0022 CHECK) ⇒ the leased anchor is <= M, so
        // this releases it too. Already-'done' rows are excluded ⇒ a replay is a harmless no-op.
        await client.query(
          "UPDATE account_propagation_outbox SET status = 'done', lease_token = NULL, lease_owner = NULL,"
            + " lease_expires_at = NULL, prepared_epoch = NULL, updated_at = now()"
            + " WHERE account_identity = $1 AND kind = 'authority_state' AND epoch <= $2"
            + " AND status IN ('pending', 'leased')",
          [account, verifiedEpoch],
        );
        await client.query("COMMIT");
        return { completed: true, doneThroughEpoch: verifiedEpoch };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  #requireTokenArgs(fn, accountIdentityPublicKeyB64, token, ownerDeviceId) {
    if (!this.#conn) throw new Error("PgPropagationOutbox." + fn + " requires a connection");
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    const tok = typeof token === "string" ? token.trim() : "";
    if (!account) throw new Error("PgPropagationOutbox." + fn + " requires accountIdentityPublicKeyB64");
    if (!tok) throw new Error("PgPropagationOutbox." + fn + " requires a lease token");
    const owner = requireCanonicalOwner(fn, ownerDeviceId);
    return { account, tok, owner };
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
   * ATOMICALLY invalidate any lease held by `ownerDeviceId` on this account, WITHIN the caller's
   * transaction (req 5). Called from the serializer's device.revoke fold so a revoked device loses
   * its lease AT ONCE — committed with the revocation — rather than waiting out the ~30s TTL. The
   * obligation returns to 'pending', IMMEDIATELY re-eligible (next_attempt_at = now): a revocation is
   * NOT a publish failure, so no backoff — a surviving device should take over promptly. Clears the
   * lease + prepared_epoch. Throws propagate to the caller's ROLLBACK.
   * @returns {Promise<number>} number of leases released (0 or 1).
   */
  async releaseOwnedInTx(client, accountIdentityPublicKeyB64, ownerDeviceId) {
    const account = typeof accountIdentityPublicKeyB64 === "string" ? accountIdentityPublicKeyB64.trim() : "";
    const owner = typeof ownerDeviceId === "string" ? ownerDeviceId.trim() : "";
    if (!account) throw new Error("PgPropagationOutbox.releaseOwnedInTx requires accountIdentityPublicKeyB64");
    if (!owner) throw new Error("PgPropagationOutbox.releaseOwnedInTx requires ownerDeviceId");
    // NOTE: owner is NOT required to be canonical here (unlike claim / token ops). This is the
    // revoke-side cleanup called from the device.revoke fold with whatever id is being revoked —
    // including a HISTORICAL non-canonical device the fold deliberately fail-closes. A non-canonical
    // device can never have CLAIMED a lease (claim enforces canonical + the DB CHECK), so the
    // WHERE lease_owner = $2 below harmlessly matches nothing (releases 0) rather than throwing and
    // rolling back the legitimate revoke.
    const res = await client.query(
      "UPDATE account_propagation_outbox SET status = 'pending', lease_token = NULL, lease_owner = NULL,"
        + " lease_expires_at = NULL, prepared_epoch = NULL, next_attempt_at = clock_timestamp(), updated_at = now()"
        + " WHERE account_identity = $1 AND kind = 'authority_state' AND status = 'leased' AND lease_owner = $2",
      [account, owner],
    );
    return res.rowCount;
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
