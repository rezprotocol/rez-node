import { StorageProvider } from "@rezprotocol/core";
import { createKeyValueBackedPeerLinkStorage } from "@rezprotocol/sdk/peer-link";
import { PgObjectStore } from "./PgObjectStore.js";
import { PgMailboxStore } from "./PgMailboxStore.js";
import { PgKeyValueStore } from "./PgKeyValueStore.js";
import { EncryptedObjectStore } from "../fs/EncryptedObjectStore.js";
import { EncryptedMailboxStore } from "../fs/EncryptedMailboxStore.js";
import { EncryptedKeyValueStore } from "../fs/EncryptedKeyValueStore.js";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

/**
 * Postgres StorageProvider — the shared-state backend for a hosted cluster.
 *
 * Implements the EXISTING StorageProvider contract (SSOT — no parallel API) and
 * reuses the same Encrypted{Object,Mailbox,KeyValue}Store at-rest wrappers as
 * FsStorageProvider, so at-rest encryption is identical. Unlike Fs, it HONORS
 * the `ownerAccountId` partition (Fs drops it): KV + peer-link stores are
 * per-owner. Owner is a storage-partition handle (claimant pubkey), not a
 * node-visible account correlation — the node stays account-blind.
 *
 * The raw `PgKeyValueStore` (with CAS via setVersioned) is reachable through
 * `rawKeyValueStore(owner)` for registries that need atomic cross-node claims;
 * `getKeyValueStore` returns the encrypted wrapper for ordinary callers.
 */
export class PgStorageProvider extends StorageProvider {
  #connection;
  #crypto;
  #key;
  #objectStore;
  #mailboxStore;
  #rawKvByOwner;
  #kvByOwner;
  #peerLinkByOwner;

  /**
   * @param {{ connection: import("./PgConnection.js").PgConnection, encryptionKey?: Uint8Array|null }} opts
   */
  constructor({ connection, encryptionKey = null } = {}) {
    super();
    if (!connection) {
      throw new Error("PgStorageProvider requires connection");
    }
    this.#connection = connection;
    if (encryptionKey instanceof Uint8Array && encryptionKey.length === 32) {
      this.#crypto = new NodeCryptoProvider();
      this.#key = encryptionKey;
    } else {
      this.#crypto = null;
      this.#key = null;
    }

    const rawObjects = new PgObjectStore({ connection });
    const rawMailbox = new PgMailboxStore({ connection });
    if (this.#key) {
      this.#objectStore = new EncryptedObjectStore({ inner: rawObjects, crypto: this.#crypto, key: this.#key });
      this.#mailboxStore = new EncryptedMailboxStore({ inner: rawMailbox, crypto: this.#crypto, key: this.#key });
    } else {
      this.#objectStore = rawObjects;
      this.#mailboxStore = rawMailbox;
    }

    this.#rawKvByOwner = new Map();
    this.#kvByOwner = new Map();
    this.#peerLinkByOwner = new Map();
  }

  get connection() {
    return this.#connection;
  }

  #normalizeOwner(ownerAccountId) {
    return typeof ownerAccountId === "string" && ownerAccountId.length > 0 ? ownerAccountId : "";
  }

  getObjectStore() {
    return this.#objectStore;
  }

  getMailboxStore() {
    return this.#mailboxStore;
  }

  /**
   * Raw (unencrypted, CAS-capable) KV for one owner — used by registries that
   * need setVersioned()/atomic claims. Plaintext: registry values are routing
   * pointers, not secrets.
   */
  rawKeyValueStore(ownerAccountId = null) {
    const owner = this.#normalizeOwner(ownerAccountId);
    const existing = this.#rawKvByOwner.get(owner);
    if (existing) {
      return existing;
    }
    const raw = new PgKeyValueStore({ connection: this.#connection, ownerAccountId: owner });
    this.#rawKvByOwner.set(owner, raw);
    return raw;
  }

  getKeyValueStore(ownerAccountId = null) {
    const owner = this.#normalizeOwner(ownerAccountId);
    const existing = this.#kvByOwner.get(owner);
    if (existing) {
      return existing;
    }
    const raw = this.rawKeyValueStore(owner);
    const kv = this.#key
      ? new EncryptedKeyValueStore({ inner: raw, crypto: this.#crypto, key: this.#key })
      : raw;
    this.#kvByOwner.set(owner, kv);
    return kv;
  }

  getPeerLinkStorage(ownerAccountId = null) {
    const owner = this.#normalizeOwner(ownerAccountId);
    const existing = this.#peerLinkByOwner.get(owner);
    if (existing) {
      return existing;
    }
    const storage = createKeyValueBackedPeerLinkStorage({ keyValueStore: this.getKeyValueStore(owner) });
    this.#peerLinkByOwner.set(owner, storage);
    return storage;
  }
}
