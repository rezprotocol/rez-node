import { canonicalJSONStringify } from "../util/canonicalize.js";

/**
 * Signs settlement receipt bodies with Ed25519.
 *
 * Shared by all SettlementProvider implementations (LocalSettlementProvider,
 * future ChainSettlementProvider). Produces signatures compatible with
 * verifySettlementReceipt().
 *
 * Signing process:
 * 1. Canonicalize the body (deterministic JSON key ordering)
 * 2. Encode to UTF-8 bytes
 * 3. Sign with Ed25519
 * 4. Return { alg: "ed25519", relayKeyId, sig: Uint8Array }
 */
export class ReceiptSigner {
  #relayKeyId;
  #signFn;

  /**
   * @param {object} opts
   * @param {string} opts.relayKeyId — this relay's key ID (appears in receipts)
   * @param {function(Uint8Array): Promise<Uint8Array>} opts.signFn — Ed25519 signing function
   */
  constructor({ relayKeyId, signFn }) {
    if (!relayKeyId || typeof relayKeyId !== "string") {
      throw new Error("ReceiptSigner requires relayKeyId");
    }
    if (typeof signFn !== "function") {
      throw new Error("ReceiptSigner requires signFn");
    }
    this.#relayKeyId = relayKeyId;
    this.#signFn = signFn;
  }

  get relayKeyId() {
    return this.#relayKeyId;
  }

  /**
   * Sign a receipt body. The body should be a plain object with all receipt
   * fields EXCEPT sig. The returned sig object is suitable for inclusion
   * in any settlement receipt or attestation record.
   *
   * @param {object} body — receipt fields (no sig)
   * @returns {Promise<{alg: string, relayKeyId: string, sig: Uint8Array}>}
   */
  async sign(body) {
    const bytes = new TextEncoder().encode(canonicalJSONStringify(body));
    const sigBytes = await this.#signFn(bytes);
    return { alg: "ed25519", relayKeyId: this.#relayKeyId, sig: sigBytes };
  }
}
