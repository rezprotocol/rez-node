import { SettlementProvider } from "@rezprotocol/core";
import {
  DebitReceiptV1,
  CreditReceiptV1,
  EscrowReceiptV1,
  ReleaseReceiptV1,
  SlashReceiptV1,
} from "@rezprotocol/core";
import { generateSettlementId } from "./settlementUtil.js";

const KEY_PREFIX_BALANCE = "settlement:balance:";
const KEY_PREFIX_ESCROW = "settlement:escrow:";

/**
 * KV-backed settlement provider for local relay accounting.
 *
 * This is a permanent, production-ready implementation for relays
 * that operate without on-chain settlement (personal relays, small networks).
 *
 * Balances and escrows are persisted in the relay's KeyValueStore.
 * All receipts are Ed25519-signed and verifiable via verifySettlementReceipt.
 */
export class LocalSettlementProvider extends SettlementProvider {
  static type = "LocalSettlementProvider";

  #kvStore;
  #receiptSigner;
  #networkId;

  /**
   * @param {object} opts
   * @param {KeyValueStore} opts.kvStore — persistent KV store for balances and escrows
   * @param {ReceiptSigner} opts.receiptSigner — signs receipt bodies with Ed25519
   * @param {string} opts.networkId — immutable settlement-network binding stamped
   *   into every debit receipt (so a receipt can't be replayed cross-network)
   */
  constructor({ kvStore, receiptSigner, networkId }) {
    super();
    if (!kvStore) throw new Error("LocalSettlementProvider requires kvStore");
    if (!receiptSigner) throw new Error("LocalSettlementProvider requires receiptSigner");
    if (!networkId || typeof networkId !== "string") {
      throw new Error("LocalSettlementProvider requires networkId (immutable economic binding)");
    }
    this.#kvStore = kvStore;
    this.#receiptSigner = receiptSigner;
    this.#networkId = networkId;
  }

  async balance(accountId) {
    const record = await this.#kvStore.get(KEY_PREFIX_BALANCE + accountId);
    if (!record) return { available: 0, escrowed: 0, total: 0 };
    const available = record.available || 0;
    const escrowed = record.escrowed || 0;
    return { available, escrowed, total: available + escrowed };
  }

  async debit(accountId, amount, serviceInfo) {
    if (!accountId || typeof accountId !== "string") throw new Error("debit requires accountId");
    if (typeof amount !== "number" || amount <= 0) throw new Error("debit requires positive amount");
    if (!serviceInfo || !serviceInfo.serviceId || !serviceInfo.serviceRef) {
      throw new Error("debit requires serviceInfo with serviceId and serviceRef");
    }

    const bal = await this.#getOrCreateBalance(accountId);
    if (bal.available < amount) {
      throw new Error(`Insufficient balance: available=${bal.available}, required=${amount}`);
    }

    bal.available -= amount;
    await this.#kvStore.set(KEY_PREFIX_BALANCE + accountId, bal);

    const receiptId = generateSettlementId();
    const createdAtMs = Date.now();
    const body = {
      v: 1,
      receiptId,
      accountId,
      amount,
      serviceId: serviceInfo.serviceId,
      serviceRef: serviceInfo.serviceRef,
      networkId: this.#networkId,
      relayKeyId: this.#receiptSigner.relayKeyId,
      createdAtMs,
    };
    const sig = await this.#receiptSigner.sign(body);

    return new DebitReceiptV1({ ...body, sig });
  }

  async credit(accountId, amount, reason) {
    if (!accountId || typeof accountId !== "string") throw new Error("credit requires accountId");
    if (typeof amount !== "number" || amount <= 0) throw new Error("credit requires positive amount");
    if (!reason || typeof reason !== "string") throw new Error("credit requires reason");

    const bal = await this.#getOrCreateBalance(accountId);
    bal.available += amount;
    await this.#kvStore.set(KEY_PREFIX_BALANCE + accountId, bal);

    const receiptId = generateSettlementId();
    const createdAtMs = Date.now();
    const body = {
      v: 1,
      receiptId,
      accountId,
      amount,
      reason,
      relayKeyId: this.#receiptSigner.relayKeyId,
      createdAtMs,
    };
    const sig = await this.#receiptSigner.sign(body);

    return new CreditReceiptV1({ ...body, sig });
  }

  async escrow(accountId, amount, commitmentInfo) {
    if (!accountId || typeof accountId !== "string") throw new Error("escrow requires accountId");
    if (typeof amount !== "number" || amount <= 0) throw new Error("escrow requires positive amount");
    if (!commitmentInfo || !commitmentInfo.commitment || !commitmentInfo.expiresAtMs) {
      throw new Error("escrow requires commitmentInfo with commitment and expiresAtMs");
    }

    const bal = await this.#getOrCreateBalance(accountId);
    if (bal.available < amount) {
      throw new Error(`Insufficient balance: available=${bal.available}, required=${amount}`);
    }

    bal.available -= amount;
    bal.escrowed += amount;
    await this.#kvStore.set(KEY_PREFIX_BALANCE + accountId, bal);

    const escrowId = generateSettlementId();
    const createdAtMs = Date.now();

    const escrowRecord = {
      escrowId,
      accountId,
      amount,
      commitment: commitmentInfo.commitment,
      expiresAtMs: commitmentInfo.expiresAtMs,
      createdAtMs,
      status: "active",
    };
    await this.#kvStore.set(KEY_PREFIX_ESCROW + escrowId, escrowRecord);

    const body = {
      v: 1,
      escrowId,
      accountId,
      amount,
      commitment: commitmentInfo.commitment,
      expiresAtMs: commitmentInfo.expiresAtMs,
      relayKeyId: this.#receiptSigner.relayKeyId,
      createdAtMs,
    };
    const sig = await this.#receiptSigner.sign(body);

    return new EscrowReceiptV1({ ...body, sig });
  }

  async releaseEscrow(escrowId, recipientId) {
    if (!escrowId || typeof escrowId !== "string") throw new Error("releaseEscrow requires escrowId");
    if (!recipientId || typeof recipientId !== "string") throw new Error("releaseEscrow requires recipientId");

    const escrowRecord = await this.#kvStore.get(KEY_PREFIX_ESCROW + escrowId);
    if (!escrowRecord) throw new Error(`Escrow not found: ${escrowId}`);
    if (escrowRecord.status !== "active") throw new Error(`Escrow not active: ${escrowId} (status=${escrowRecord.status})`);

    const bal = await this.#getOrCreateBalance(escrowRecord.accountId);
    bal.escrowed -= escrowRecord.amount;
    await this.#kvStore.set(KEY_PREFIX_BALANCE + escrowRecord.accountId, bal);

    escrowRecord.status = "released";
    await this.#kvStore.set(KEY_PREFIX_ESCROW + escrowId, escrowRecord);

    const receiptId = generateSettlementId();
    const createdAtMs = Date.now();
    const body = {
      v: 1,
      receiptId,
      escrowId,
      recipientId,
      amount: escrowRecord.amount,
      relayKeyId: this.#receiptSigner.relayKeyId,
      createdAtMs,
    };
    const sig = await this.#receiptSigner.sign(body);

    return new ReleaseReceiptV1({ ...body, sig });
  }

  async slash(escrowId, reason) {
    if (!escrowId || typeof escrowId !== "string") throw new Error("slash requires escrowId");
    if (!reason || typeof reason !== "string") throw new Error("slash requires reason");

    const escrowRecord = await this.#kvStore.get(KEY_PREFIX_ESCROW + escrowId);
    if (!escrowRecord) throw new Error(`Escrow not found: ${escrowId}`);
    if (escrowRecord.status !== "active") throw new Error(`Escrow not active: ${escrowId} (status=${escrowRecord.status})`);

    const bal = await this.#getOrCreateBalance(escrowRecord.accountId);
    bal.escrowed -= escrowRecord.amount;
    await this.#kvStore.set(KEY_PREFIX_BALANCE + escrowRecord.accountId, bal);

    escrowRecord.status = "slashed";
    await this.#kvStore.set(KEY_PREFIX_ESCROW + escrowId, escrowRecord);

    const receiptId = generateSettlementId();
    const createdAtMs = Date.now();
    const body = {
      v: 1,
      receiptId,
      escrowId,
      amount: escrowRecord.amount,
      reason,
      relayKeyId: this.#receiptSigner.relayKeyId,
      createdAtMs,
    };
    const sig = await this.#receiptSigner.sign(body);

    return new SlashReceiptV1({ ...body, sig });
  }

  async #getOrCreateBalance(accountId) {
    const record = await this.#kvStore.get(KEY_PREFIX_BALANCE + accountId);
    if (record) return record;
    const fresh = { available: 0, escrowed: 0 };
    await this.#kvStore.set(KEY_PREFIX_BALANCE + accountId, fresh);
    return fresh;
  }

}
