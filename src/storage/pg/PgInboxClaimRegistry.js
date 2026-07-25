import { DEFAULT_MAX_INBOXES_PER_CLAIMANT } from "../../inbox/InboxClaimRegistry.js";
/**
 * Postgres-backed inbox-claim registry — the cluster-correct replacement for the
 * single-process InboxClaimRegistry (whole-blob KV + promise mutex).
 *
 * First-claim-wins is enforced by the `inbox_claims` PRIMARY KEY via
 * `INSERT … ON CONFLICT DO NOTHING`: an atomic claim across N nodes, no mutex.
 *
 * Reads are ASYNC and authoritative (hit Postgres) — unlike the in-memory
 * registry's synchronous cache, which would go stale in a cluster. Callers that
 * make authz decisions must await these. Stays account-blind: rows key on the
 * claimant pubkey, never an account.
 */
export class PgInboxClaimRegistry {
  #conn;
  #maxInboxesPerClaimant;

  constructor({ connection, maxInboxesPerClaimant = DEFAULT_MAX_INBOXES_PER_CLAIMANT } = {}) {
    if (!connection) {
      throw new Error("PgInboxClaimRegistry requires connection");
    }
    if (!Number.isInteger(maxInboxesPerClaimant) || maxInboxesPerClaimant < 1) {
      throw new Error("PgInboxClaimRegistry requires a positive integer maxInboxesPerClaimant");
    }
    this.#conn = connection;
    this.#maxInboxesPerClaimant = maxInboxesPerClaimant;
  }

  #normalize(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  /** No-op: Postgres is the source of truth (kept for API parity with hydrate-first callers). */
  async hydrate() {
    // intentionally empty — no in-memory cache to hydrate
  }

  /**
   * Atomic first-claim-wins. Throws INBOX_ALREADY_CLAIMED if taken.
   * @returns {Promise<{ inboxId: string, claimantPublicKeyB64: string, claimedAtMs: number }>}
   */
  async claim({ inboxId, claimantPublicKeyB64, claimedAtMs } = {}) {
    const id = this.#normalize(inboxId);
    const pubkey = this.#normalize(claimantPublicKeyB64);
    const at = Number(claimedAtMs);
    if (!id) throw new Error("PgInboxClaimRegistry.claim requires inboxId");
    if (!pubkey) throw new Error("PgInboxClaimRegistry.claim requires claimantPublicKeyB64");
    if (!Number.isFinite(at) || at <= 0) {
      throw new Error("PgInboxClaimRegistry.claim requires positive claimedAtMs");
    }

    // Track 2 abuse quota: the ceiling is evaluated and the row inserted in ONE transaction, under
    // a per-claimant advisory lock, so two concurrent claims by the same key cannot both count
    // below the limit and both insert. Open registration means anyone with a keypair can claim;
    // each inbox carries its own retention budget, so an unbounded claim count multiplies this
    // node's storage. The per-inbox item/byte caps bound each inbox — this bounds the count.
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [pubkey]);
        const held = await client.query(
          "SELECT count(*)::int AS c FROM inbox_claims WHERE claimant_pubkey = $1",
          [pubkey],
        );
        if (held.rows[0].c >= this.#maxInboxesPerClaimant) {
          const err = new Error(
            "claimant already holds " + held.rows[0].c + " inboxes (max " + this.#maxInboxesPerClaimant + ")",
          );
          err.code = "INBOX_CLAIM_QUOTA_EXCEEDED";
          throw err;
        }
        const res = await client.query(
          `INSERT INTO inbox_claims (inbox_id, claimant_pubkey, claimed_at_ms)
           VALUES ($1, $2, $3)
           ON CONFLICT (inbox_id) DO NOTHING
           RETURNING inbox_id`,
          [id, pubkey, at],
        );
        if (res.rowCount === 0) {
          const err = new Error("inbox already claimed");
          err.code = "INBOX_ALREADY_CLAIMED";
          throw err;
        }
        await client.query("COMMIT");
        return { inboxId: id, claimantPublicKeyB64: pubkey, claimedAtMs: at };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /** @returns {Promise<string|null>} claimant pubkey or null */
  async getClaimantPublicKey(inboxId) {
    const id = this.#normalize(inboxId);
    if (!id) return null;
    const res = await this.#conn.query(
      "SELECT claimant_pubkey FROM inbox_claims WHERE inbox_id = $1",
      [id],
    );
    if (res.rowCount === 0) return null;
    return res.rows[0].claimant_pubkey;
  }

  /** @returns {Promise<boolean>} */
  async hasInbox(inboxId) {
    const id = this.#normalize(inboxId);
    if (!id) return false;
    const res = await this.#conn.query("SELECT 1 FROM inbox_claims WHERE inbox_id = $1", [id]);
    return res.rowCount > 0;
  }

  /** @returns {Promise<string[]>} */
  async listInboxIds() {
    const res = await this.#conn.query("SELECT inbox_id FROM inbox_claims ORDER BY inbox_id");
    return res.rows.map((r) => r.inbox_id);
  }

  /** @returns {Promise<number>} */
  async size() {
    const res = await this.#conn.query("SELECT count(*)::int AS c FROM inbox_claims");
    return res.rows[0].c;
  }
}
