import { MailboxStore, isBytes } from "@rezprotocol/core";

const ENVELOPE_VERSION = 1;

function toBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function fromBase64(str) {
  return new Uint8Array(Buffer.from(str, "base64"));
}

/**
 * EncryptedMailboxStore — decorator that transparently encrypts mailbox
 * index data using AES-256-GCM before writing to the inner MailboxStore.
 *
 * The mailboxId is used as AAD to bind ciphertext to the mailbox identity.
 *
 * Legacy plaintext indices (pre-encryption) are returned as-is on read
 * and will be encrypted on next write (progressive migration).
 */
export class EncryptedMailboxStore extends MailboxStore {
  #inner;
  #crypto;
  #key;

  constructor({ inner, crypto, key } = {}) {
    super();
    if (!inner || typeof inner.append !== "function") {
      throw new Error("EncryptedMailboxStore requires inner MailboxStore");
    }
    if (!crypto || typeof crypto.aeadEncrypt !== "function") {
      throw new Error("EncryptedMailboxStore requires crypto provider");
    }
    if (!isBytes(key) || key.length !== 32) {
      throw new Error("EncryptedMailboxStore requires 32-byte key");
    }
    this.#inner = inner;
    this.#crypto = crypto;
    this.#key = key;
  }

  async append(mailboxId, objectId) {
    // Read existing items (decrypting if needed), add new ID, re-encrypt entire list
    const items = await this.list(mailboxId);
    items.push(objectId);
    await this.#writeEncrypted(mailboxId, items);
  }

  async list(mailboxId) {
    const raw = await this.#inner._readRaw(mailboxId);
    if (raw === null) return [];

    const json = JSON.parse(raw);

    // Legacy plaintext: plain array of strings
    if (Array.isArray(json)) {
      return json;
    }

    // Encrypted envelope
    if (json && json.encrypted === ENVELOPE_VERSION) {
      const nonce = fromBase64(json.n);
      const ciphertext = fromBase64(json.c);
      const aad = new TextEncoder().encode(String(mailboxId));
      const plaintext = this.#crypto.aeadDecrypt({
        key: this.#key,
        nonce,
        ciphertext,
        aad,
      });
      const items = JSON.parse(new TextDecoder().decode(plaintext));
      return Array.isArray(items) ? items : [];
    }

    return [];
  }

  async deleteMailbox(mailboxId) {
    return this.#inner.deleteMailbox(mailboxId);
  }

  async #writeEncrypted(mailboxId, items) {
    const plaintext = new TextEncoder().encode(JSON.stringify(items));
    const nonce = this.#crypto.randomBytes(12);
    const aad = new TextEncoder().encode(String(mailboxId));
    const ciphertext = this.#crypto.aeadEncrypt({
      key: this.#key,
      nonce,
      plaintext,
      aad,
    });
    const sealed = JSON.stringify({
      encrypted: ENVELOPE_VERSION,
      n: toBase64(nonce),
      c: toBase64(ciphertext),
    });
    await this.#inner._writeSealed(mailboxId, sealed);
  }
}
