import { SettlementProvider, DebitReceiptV1 } from "@rezprotocol/core";
import { generateSettlementId } from "./settlementUtil.js";

/**
 * Postgres settlement provider — atomic, cluster-safe replacement for
 * LocalSettlementProvider's read-modify-write debit.
 *
 * `debit` runs in a transaction with `SELECT … FOR UPDATE` on the balance row +
 * an idempotency key, so concurrent debits against one shared wallet (the same
 * account across N devices) can never overdraft or double-charge. Every movement
 * appends to the SettlementJournal carrying the immutable `networkId`.
 *
 * Debit is the paid-service path ServiceGate calls; balances are funded via
 * credit. escrow/release/slash are not yet implemented here (explicit throw —
 * never a silent no-op).
 */
export class PgSettlementProvider extends SettlementProvider {
  static type = "PgSettlementProvider";

  #conn;
  #signer;
  #networkId;

  /**
   * @param {{ connection: object, receiptSigner: object, networkId: string }} opts
   */
  constructor({ connection, receiptSigner, networkId } = {}) {
    super();
    if (!connection) {
      throw new Error("PgSettlementProvider requires connection");
    }
    if (!receiptSigner || typeof receiptSigner.sign !== "function") {
      throw new Error("PgSettlementProvider requires receiptSigner");
    }
    if (!networkId || typeof networkId !== "string") {
      throw new Error("PgSettlementProvider requires networkId (immutable economic binding)");
    }
    this.#conn = connection;
    this.#signer = receiptSigner;
    this.#networkId = networkId;
  }

  async balance(accountId) {
    const res = await this.#conn.query(
      "SELECT available, escrowed FROM settlement_balances WHERE account_id = $1",
      [String(accountId)],
    );
    if (res.rowCount === 0) {
      return { available: 0, escrowed: 0, total: 0 };
    }
    const available = Number(res.rows[0].available);
    const escrowed = Number(res.rows[0].escrowed);
    return { available, escrowed, total: available + escrowed };
  }

  /**
   * Fund a wallet. Returns the new available balance.
   * (Full CreditReceiptV1 parity is deferred — credit is the admin/funding path,
   * not the ServiceGate hot path.)
   */
  async credit(accountId, amount, serviceInfo = {}) {
    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      throw new Error("PgSettlementProvider.credit requires positive amount");
    }
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        const res = await client.query(
          `INSERT INTO settlement_balances (account_id, available)
           VALUES ($1, $2)
           ON CONFLICT (account_id)
             DO UPDATE SET available = settlement_balances.available + EXCLUDED.available, updated_at = now()
           RETURNING available`,
          [String(accountId), amt],
        );
        const entryId = generateSettlementId();
        await client.query(
          `INSERT INTO settlement_journal
             (entry_id, account_id, kind, amount, service_id, network_id, created_at_ms)
           VALUES ($1, $2, 'credit', $3, $4, $5, $6)`,
          [entryId, String(accountId), amt, serviceInfo.serviceId || null, this.#networkId, Date.now()],
        );
        await client.query("COMMIT");
        return { accountId: String(accountId), amount: amt, available: Number(res.rows[0].available) };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Atomic debit. Throws INSUFFICIENT_FUNDS if underfunded. Idempotent on
   * `serviceInfo.idempotencyKey`: a retry returns the original signed receipt and
   * never double-charges (race-safe via the unique journal index).
   * @returns {Promise<DebitReceiptV1>}
   */
  async debit(accountId, amount, serviceInfo = {}) {
    const acct = String(accountId);
    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      throw new Error("PgSettlementProvider.debit requires positive amount");
    }
    if (!serviceInfo || !serviceInfo.serviceId || !serviceInfo.serviceRef) {
      throw new Error("PgSettlementProvider.debit requires serviceInfo with serviceId and serviceRef");
    }
    const idem = serviceInfo.idempotencyKey ? String(serviceInfo.idempotencyKey) : null;

    const existing = idem ? await this.#findReceiptByIdem(acct, idem) : null;
    if (existing) {
      this.#assertIdemMatch(existing, amt, serviceInfo);
      return existing;
    }

    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        // Single atomic guarded decrement — the comparison happens in SQL on the
        // exact `numeric`, so there is no FOR-UPDATE-then-JS-compare window and no
        // Number() rounding can let an overdraft slip. rowCount 0 = no row OR
        // insufficient → INSUFFICIENT_FUNDS (never a raw CHECK 23514).
        const upd = await client.query(
          `UPDATE settlement_balances
             SET available = available - $2, updated_at = now()
           WHERE account_id = $1 AND available >= $2
           RETURNING available`,
          [acct, amt],
        );
        if (upd.rowCount === 0) {
          await client.query("ROLLBACK");
          const err = new Error(`Insufficient balance for ${acct}: required=${amt}`);
          err.code = "INSUFFICIENT_FUNDS";
          throw err;
        }

        const receiptId = generateSettlementId();
        const createdAtMs = Date.now();
        const body = {
          v: 1,
          receiptId,
          accountId: acct,
          amount: amt,
          serviceId: serviceInfo.serviceId,
          serviceRef: serviceInfo.serviceRef,
          relayKeyId: this.#signer.relayKeyId,
          createdAtMs,
        };
        const sig = await this.#signer.sign(body);
        const receipt = new DebitReceiptV1({ ...body, sig });
        const entryId = generateSettlementId();

        try {
          await client.query(
            `INSERT INTO settlement_journal
               (entry_id, account_id, kind, amount, service_id, service_ref, network_id, idempotency_key, receipt, created_at_ms)
             VALUES ($1, $2, 'debit', $3, $4, $5, $6, $7, $8::jsonb, $9)`,
            [
              entryId, acct, amt, serviceInfo.serviceId, serviceInfo.serviceRef,
              this.#networkId, idem, JSON.stringify(receipt.toJSON()), createdAtMs,
            ],
          );
        } catch (err) {
          // Idempotency race: another txn settled the SAME key first (unique
          // index on (account_id, idempotency_key)). Roll back our balance change
          // and return the winner's receipt. Match the specific constraint so an
          // unrelated 23505 (e.g. entry_id PK) is not mistaken for an idem race.
          if (idem && err && err.code === "23505" && err.constraint === "settlement_journal_idem") {
            await client.query("ROLLBACK");
            const winner = await this.#findReceiptByIdem(acct, idem);
            if (winner) {
              this.#assertIdemMatch(winner, amt, serviceInfo);
              return winner;
            }
          }
          throw err;
        }

        await client.query("COMMIT");
        return receipt;
      } catch (err) {
        // Ensure no dangling txn on unexpected failure.
        try {
          await client.query("ROLLBACK");
        } catch (rollbackErr) {
          void rollbackErr; // already rolled back / connection gone
        }
        throw err;
      }
    });
  }

  /**
   * An idempotency key is bound to ONE request. A replay with the same key but a
   * different amount/serviceId/serviceRef is a client error, not a silent return
   * of the original receipt.
   */
  #assertIdemMatch(receipt, amt, serviceInfo) {
    if (receipt.amount !== amt
        || receipt.serviceId !== serviceInfo.serviceId
        || receipt.serviceRef !== serviceInfo.serviceRef) {
      const err = new Error(
        "idempotency key reused with a different request (amount/serviceId/serviceRef mismatch)",
      );
      err.code = "IDEMPOTENCY_KEY_REUSED";
      throw err;
    }
  }

  async #findReceiptByIdem(accountId, idem) {
    const res = await this.#conn.query(
      "SELECT receipt FROM settlement_journal WHERE account_id = $1 AND idempotency_key = $2",
      [accountId, idem],
    );
    if (res.rowCount === 0 || !res.rows[0].receipt) {
      return null;
    }
    return DebitReceiptV1.fromJSON(res.rows[0].receipt);
  }

  /** Read the append-only journal for an account (newest last). */
  async listJournal(accountId) {
    const res = await this.#conn.query(
      `SELECT entry_id, kind, amount, service_id, service_ref, network_id, idempotency_key, created_at_ms
         FROM settlement_journal WHERE account_id = $1 ORDER BY created_at, entry_id`,
      [String(accountId)],
    );
    return res.rows.map((r) => ({
      entryId: r.entry_id,
      kind: r.kind,
      amount: Number(r.amount),
      serviceId: r.service_id,
      serviceRef: r.service_ref,
      networkId: r.network_id,
      idempotencyKey: r.idempotency_key,
      createdAtMs: Number(r.created_at_ms),
    }));
  }

  escrow() {
    throw new Error("PgSettlementProvider.escrow not implemented yet");
  }

  releaseEscrow() {
    throw new Error("PgSettlementProvider.releaseEscrow not implemented yet");
  }

  slash() {
    throw new Error("PgSettlementProvider.slash not implemented yet");
  }
}
