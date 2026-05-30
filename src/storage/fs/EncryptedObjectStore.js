import { ObjectStore, Envelope, canonicalize, isBytes } from "@rezprotocol/core";

const ENVELOPE_VERSION = 1;

function toBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function fromBase64(str) {
  return new Uint8Array(Buffer.from(str, "base64"));
}

/**
 * EncryptedObjectStore — decorator that transparently encrypts Envelope
 * objects using AES-256-GCM before writing to the inner ObjectStore and
 * decrypts on read.
 *
 * The envelope's header.id is used as AAD to bind ciphertext to its identity.
 * The ID itself is NOT encrypted (needed for lookups/filenames).
 *
 * Legacy plaintext envelopes (pre-encryption) are returned as-is on read
 * and will be encrypted on next write (progressive migration).
 */
export class EncryptedObjectStore extends ObjectStore {
  #inner;
  #crypto;
  #key;

  constructor({ inner, crypto, key } = {}) {
    super();
    if (!inner || typeof inner.put !== "function") {
      throw new Error("EncryptedObjectStore requires inner ObjectStore");
    }
    if (!crypto || typeof crypto.aeadEncrypt !== "function") {
      throw new Error("EncryptedObjectStore requires crypto provider");
    }
    if (!isBytes(key) || key.length !== 32) {
      throw new Error("EncryptedObjectStore requires 32-byte key");
    }
    this.#inner = inner;
    this.#crypto = crypto;
    this.#key = key;
  }

  async put(envelope) {
    // Validate via base class
    ObjectStore.prototype.put.call(this, envelope);

    const id = envelope.header.id;
    const plaintext = new TextEncoder().encode(JSON.stringify(canonicalize(envelope.toJSON())));
    const nonce = this.#crypto.randomBytes(12);
    const aad = new TextEncoder().encode(String(id));
    const ciphertext = this.#crypto.aeadEncrypt({
      key: this.#key,
      nonce,
      plaintext,
      aad,
    });

    // Store a sealed envelope that the inner store writes as-is.
    // We wrap in an Envelope with a marker body so the inner FsObjectStore
    // can still use its normal put() path (which requires an Envelope).
    const sealedJson = {
      encrypted: ENVELOPE_VERSION,
      id,
      n: toBase64(nonce),
      c: toBase64(ciphertext),
    };
    // Write directly to the inner store's filesystem to avoid
    // requiring the sealed format to be a valid Envelope.
    await this.#inner._writeSealed(id, JSON.stringify(sealedJson));
  }

  async get(id) {
    const raw = await this.#inner._readRaw(id);
    if (raw === null) return null;

    const json = JSON.parse(raw);

    // Legacy plaintext: has header/body, no "encrypted" flag
    if (!json || json.encrypted !== ENVELOPE_VERSION) {
      return Envelope.fromJSON(json);
    }

    const nonce = fromBase64(json.n);
    const ciphertext = fromBase64(json.c);
    const aad = new TextEncoder().encode(String(id));
    const plaintext = this.#crypto.aeadDecrypt({
      key: this.#key,
      nonce,
      ciphertext,
      aad,
    });
    const envelopeJson = JSON.parse(new TextDecoder().decode(plaintext));
    return Envelope.fromJSON(envelopeJson);
  }

  async has(id) {
    return this.#inner.has(id);
  }

  async delete(id) {
    return this.#inner.delete(id);
  }

  async listIds() {
    return this.#inner.listIds();
  }
}
