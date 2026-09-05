import { StorageProvider } from "@rezprotocol/core";
import { createKeyValueBackedPeerLinkStorage } from "@rezprotocol/sdk/peer-link";
import { PgObjectStore } from "./PgObjectStore.js";
import { PgMailboxStore } from "./PgMailboxStore.js";
import { PgKeyValueStore } from "./PgKeyValueStore.js";
import { EncryptedObjectStore } from "../fs/EncryptedObjectStore.js";
import { EncryptedMailboxStore } from "../fs/EncryptedMailboxStore.js";
import { EncryptedKeyValueStore } from "../fs/EncryptedKeyValueStore.js";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import { createHash } from "node:crypto";

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
  #runtimeOwnershipPromises;
  #runtimeClient = null;
  #runtimeRequired = false;

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
    this.#runtimeOwnershipPromises = new Map();
  }

  get connection() {
    return this.#connection;
  }

  // Field-style accessors for parity with FsStorageProvider: several call sites
  // read `storageProvider.objectStore` / `.keyValueStore` directly rather than
  // the get*() methods. These return the owner-less ("") default stores, exactly
  // as the Fs fields do (Fs ignores the owner partition entirely).
  get objectStore() {
    return this.getObjectStore();
  }

  get mailboxStore() {
    return this.getMailboxStore();
  }

  get keyValueStore() {
    return this.getKeyValueStore("");
  }

  get peerLinkStorage() {
    return this.getPeerLinkStorage("");
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
    const connection = {
      query: (...args) => {
        if (!this.#runtimeRequired) return this.#connection.query(...args);
        if (!this.#runtimeClient) throw new Error("delivery runtime ownership is inactive");
        // All protected KV IO uses the SAME session holding the advisory lock.
        // A dead connection can never fall back to another pooled writer.
        return this.#runtimeClient.query(...args);
      },
    };
    const raw = new PgKeyValueStore({ connection, ownerAccountId: owner });
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

  acquireRuntimeOwnership({ namespace = "sdk-delivery" } = {}) {
    const normalizedNamespace = String(namespace || "").trim();
    if (!normalizedNamespace) throw new Error("PgStorageProvider runtime namespace is required");
    const existing = this.#runtimeOwnershipPromises.get(normalizedNamespace);
    if (existing) return existing;
    let grantPromise;
    const acquired = (async () => {
      const client = await this.#connection.pool.connect();
      const lockId = createHash("sha256").update("rez:" + normalizedNamespace, "utf8").digest().readBigInt64BE(0).toString();
      let locked = false;
      try {
        const result = await client.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [lockId]);
        if (!result.rows[0] || result.rows[0].acquired !== true) {
          const err = new Error("Delivery storage is already owned by a live runtime");
          err.code = "DELIVERY_RUNTIME_ALREADY_ACTIVE";
          throw err;
        }
        locked = true;
        this.#runtimeRequired = true;
        this.#runtimeClient = client;
        const kv = this.getKeyValueStore(null);
        const key = "sdk:delivery:runtime-epoch:v1";
        const raw = await kv.getStrict(key);
        const prior = raw === undefined ? 0 : Number(raw);
        if (!Number.isSafeInteger(prior) || prior < 0 || prior === Number.MAX_SAFE_INTEGER) throw new Error("Invalid delivery runtime epoch");
        const runtimeEpoch = prior + 1;
        await kv.set(key, runtimeEpoch);
        return {
          runtimeEpoch,
          assertActive: () => {
            if (this.#runtimeClient !== client) throw new Error("delivery runtime ownership is inactive");
          },
          release: async () => {
            this.#runtimeClient = null;
            try {
              const result = await client.query("SELECT pg_advisory_unlock($1::bigint) AS unlocked", [lockId]);
              if (!result.rows[0] || result.rows[0].unlocked !== true) throw new Error("runtime advisory unlock failed");
            } catch (err) {
              client.release(true);
              throw err;
            }
            client.release();
          },
        };
      } catch (err) {
        if (this.#runtimeClient === client) this.#runtimeClient = null;
        // A session lock must never follow a failed acquisition into the pool.
        // Destroy the connection if unlock cannot be confirmed.
        let destroy = false;
        if (locked) {
          try {
            const result = await client.query("SELECT pg_advisory_unlock($1::bigint) AS unlocked", [lockId]);
            destroy = !result.rows[0] || result.rows[0].unlocked !== true;
          } catch (unlockError) { destroy = true; }
        }
        client.release(destroy);
        throw err;
      }
    })();
    grantPromise = acquired.then((grant) => {
      let released = false;
      return {
        ...grant,
        release: async () => {
          if (released) return;
          released = true;
          try {
            await grant.release();
          } finally {
            if (this.#runtimeOwnershipPromises.get(normalizedNamespace) === grantPromise) {
              this.#runtimeOwnershipPromises.delete(normalizedNamespace);
            }
          }
        },
      };
    }, (err) => {
      if (this.#runtimeOwnershipPromises.get(normalizedNamespace) === grantPromise) {
        this.#runtimeOwnershipPromises.delete(normalizedNamespace);
      }
      throw err;
    });
    this.#runtimeOwnershipPromises.set(normalizedNamespace, grantPromise);
    return grantPromise;
  }
}
