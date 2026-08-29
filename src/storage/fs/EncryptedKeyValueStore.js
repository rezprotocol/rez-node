import { KeyValueStore, KeyValueUnreadableError, isBytes } from "@rezprotocol/core";

const ENVELOPE_VERSION = 1;

function toBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function fromBase64(str) {
  return new Uint8Array(Buffer.from(str, "base64"));
}

/**
 * Detect whether a stored value is an encrypted envelope.
 * Envelopes are objects with shape { v: 1, n: <base64>, c: <base64> }.
 * Everything else is legacy plaintext.
 */
function isEncryptedEnvelope(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  return value.v === ENVELOPE_VERSION
    && typeof value.n === "string"
    && typeof value.c === "string";
}

/**
 * EncryptedKeyValueStore — decorator that transparently encrypts values
 * using AES-256-GCM before writing to the inner KeyValueStore and
 * decrypts on read.
 *
 * Keys are NOT encrypted (needed for prefix-based queries).
 * The storage key string is used as AAD to bind ciphertext to its key.
 *
 * Legacy plaintext values (pre-encryption) are returned as-is on read
 * and will be encrypted on next write (progressive migration).
 */
export class EncryptedKeyValueStore extends KeyValueStore {
  #inner;
  #crypto;
  #key;

  constructor({ inner, crypto, key } = {}) {
    super();
    if (!inner || typeof inner.get !== "function") {
      throw new Error("EncryptedKeyValueStore requires inner KeyValueStore");
    }
    if (!crypto || typeof crypto.aeadEncrypt !== "function") {
      throw new Error("EncryptedKeyValueStore requires crypto provider");
    }
    if (!isBytes(key) || key.length !== 32) {
      throw new Error("EncryptedKeyValueStore requires 32-byte key");
    }
    this.#inner = inner;
    this.#crypto = crypto;
    this.#key = key;
  }

  async set(key, value) {
    const plaintext = new TextEncoder().encode(JSON.stringify(value));
    const nonce = this.#crypto.randomBytes(12);
    const aad = new TextEncoder().encode(String(key));
    const ciphertext = this.#crypto.aeadEncrypt({
      key: this.#key,
      nonce,
      plaintext,
      aad,
    });
    const envelope = {
      v: ENVELOPE_VERSION,
      n: toBase64(nonce),
      c: toBase64(ciphertext),
    };
    return this.#inner.set(key, envelope);
  }

  async get(key) {
    return this.#readValue(key, false);
  }

  async getStrict(key) {
    return this.#readValue(key, true);
  }

  async #readValue(key, strict) {
    let stored;
    try {
      stored = strict ? await this.#inner.getStrict(key) : await this.#inner.get(key);
    } catch (err) {
      if (err instanceof KeyValueUnreadableError) throw err;
      if (strict) throw new KeyValueUnreadableError({ key, cause: err });
      throw err;
    }
    if (stored === undefined) return undefined;

    // Legacy plaintext: return as-is (progressive migration)
    if (!isEncryptedEnvelope(stored)) {
      return stored;
    }

    try {
      const nonce = fromBase64(stored.n);
      const ciphertext = fromBase64(stored.c);
      const aad = new TextEncoder().encode(String(key));
      const plaintext = this.#crypto.aeadDecrypt({
        key: this.#key,
        nonce,
        ciphertext,
        aad,
      });
      return JSON.parse(new TextDecoder().decode(plaintext));
    } catch (err) {
      if (strict) throw new KeyValueUnreadableError({ key, cause: err });
      throw err;
    }
  }

  async delete(key) {
    return this.#inner.delete(key);
  }

  async keys(prefix = "") {
    return this.#inner.keys(prefix);
  }
}
