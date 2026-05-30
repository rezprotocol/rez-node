import { createHash } from "node:crypto";
import { nonEmpty } from "@rezprotocol/core";

function hashAccountId(accountId) {
  return createHash("sha256").update(String(accountId)).digest("hex").slice(0, 32);
}

/**
 * Server-side encrypted keystore blob store.
 *
 * Stores and retrieves the encrypted keystore envelope (ciphertext only) for an account.
 * The server is a blind ciphertext store — it never has access to the user's password
 * or plaintext private key. Decryption always happens client-side.
 *
 * Key format: app:keystore/{accountHash}/envelope
 */
export class KeystoreBlobStore {
  constructor({ storageProvider, ownerAccountId } = {}) {
    if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
      throw new Error("KeystoreBlobStore requires storageProvider");
    }
    const owner = nonEmpty(ownerAccountId);
    if (!owner) throw new Error("KeystoreBlobStore requires ownerAccountId");

    this._kv = storageProvider.getKeyValueStore(owner);
    if (!this._kv || typeof this._kv.get !== "function" || typeof this._kv.set !== "function") {
      throw new Error("KeystoreBlobStore requires key-value store with get/set");
    }

    this._ownerAccountId = owner;
    this._ownerHash = hashAccountId(owner);
  }

  ownerHash() {
    return this._ownerHash;
  }

  /**
   * Store the encrypted keystore envelope for this account.
   * @param {object} envelope - a validated keystore envelope (from assertKeystoreEnvelope)
   */
  async putEnvelope(envelope) {
    if (!envelope || typeof envelope !== "object") {
      throw new Error("KeystoreBlobStore.putEnvelope requires an envelope object");
    }
    await this._kv.set(this._envelopeKey(), envelope);
  }

  /**
   * Retrieve the stored keystore envelope, or null if none exists.
   * @returns {object|null}
   */
  async getEnvelope() {
    const value = await Promise.resolve(this._kv.get(this._envelopeKey()));
    if (!value || typeof value !== "object") return null;
    return value;
  }

  /**
   * Delete the stored keystore envelope.
   */
  async deleteEnvelope() {
    await this._kv.delete(this._envelopeKey());
  }

  _envelopeKey() {
    return `app:keystore/${this._ownerHash}/envelope`;
  }
}
