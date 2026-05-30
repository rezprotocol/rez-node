import { randomBytes } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import {
  RSessionStore,
  EncryptedStorageCodec,
  EncryptedStoreEnvelopeV1,
  StorageRecordRegistry,
  SecureSessionRecord,
} from "@rezprotocol/core";

function isBytes(value) {
  return value instanceof Uint8Array;
}

function bytesToBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function base64ToBytes(value, label) {
  if (typeof value !== "string") {
    throw new Error(`FsSessionStore ${label} must be base64 string`);
  }
  return new Uint8Array(Buffer.from(value, "base64"));
}

function sidHex(sidBytes) {
  return Buffer.from(sidBytes).toString("hex");
}

function ensureRecordShape(record) {
  if (!record || typeof record !== "object") {
    throw new Error("FsSessionStore.put(record) requires object");
  }
  if (!isBytes(record.sid)) {
    throw new Error("FsSessionStore.put(record) requires sid Uint8Array");
  }
  if (typeof record.peerId !== "string" || record.peerId.trim().length === 0) {
    throw new Error("FsSessionStore.put(record) requires peerId");
  }
  if (typeof record.includeDh !== "boolean") {
    throw new Error("FsSessionStore.put(record) requires includeDh boolean");
  }
  if (!record.ratchetState || typeof record.ratchetState !== "object") {
    throw new Error("FsSessionStore.put(record) requires ratchetState object");
  }
}

function toDisk(record) {
  return {
    v: 1,
    sid: bytesToBase64(record.sid),
    peerId: record.peerId,
    includeDh: record.includeDh,
    ratchetState: record.ratchetState,
    version: typeof record.version === "number" ? record.version : 1,
  };
}

function fromDisk(json) {
  if (!json || typeof json !== "object") {
    throw new Error("FsSessionStore.fromDisk requires object");
  }
  if (json.v !== 1) {
    throw new Error("FsSessionStore.fromDisk requires v=1");
  }
  return {
    v: 1,
    sid: base64ToBytes(json.sid, "sid"),
    peerId: json.peerId,
    includeDh: Boolean(json.includeDh),
    ratchetState: json.ratchetState,
    version: typeof json.version === "number" && json.version > 0 ? json.version : 1,
  };
}

/**
 * Create the default StorageRecordRegistry for session storage.
 * Only SecureSessionRecord is allowed — anything else on disk is rejected.
 */
function createSessionRegistry() {
  const registry = new StorageRecordRegistry();
  registry.register(SecureSessionRecord);
  return registry;
}

export class FsSessionStore extends RSessionStore {
  #codec = null;
  #registry = null;

  /**
   * @param {object} [options]
   * @param {string} [options.rootDir] — directory for session files
   * @param {RCryptoProvider} [options.crypto] — crypto provider (enables encryption)
   * @param {Uint8Array} [options.encryptionKey] — 32-byte AES-256 key (enables encryption)
   */
  constructor({ rootDir, crypto, encryptionKey } = {}) {
    super();

    const base = rootDir ? path.resolve(rootDir) : path.resolve("data", "sessions");
    this.rootDir = base;

    if (crypto && encryptionKey) {
      this.#codec = new EncryptedStorageCodec({ crypto, key: encryptionKey });
      this.#registry = createSessionRegistry();
    }
  }

  get encryptionEnabled() {
    return this.#codec !== null;
  }

  async ensureRoot() {
    await fs.mkdir(this.rootDir, { recursive: true });
  }

  filePathForSid(sidBytes) {
    return path.join(this.rootDir, `${sidHex(sidBytes)}.json`);
  }

  async get(sidBytes) {
    if (!isBytes(sidBytes)) {
      throw new Error("FsSessionStore.get(sid) requires Uint8Array");
    }
    await this.ensureRoot();
    const filePath = this.filePathForSid(sidBytes);
    try {
      const raw = await fs.readFile(filePath, "utf8");
      const json = JSON.parse(raw);
      return this.#fromDiskDispatch(json);
    } catch (err) {
      if (err && err.code === "ENOENT") return null;
      throw err;
    }
  }

  /**
   * Persist a session record with optimistic locking (legacy plaintext path).
   *
   * @param {object} record — plain object with sid, peerId, includeDh, ratchetState, version
   * @returns {Promise<number>} the new version after write
   */
  async put(record) {
    ensureRecordShape(record);
    const recordVersion = typeof record.version === "number" && record.version > 0 ? record.version : 1;
    const diskRecord = toDisk(record);
    return this.#writeWithVersionCheck(record.sid, recordVersion, (nextVersion) => {
      diskRecord.version = nextVersion;
      return JSON.stringify(diskRecord);
    });
  }

  /**
   * Persist a SecureSessionRecord with encryption and optimistic locking.
   *
   * @param {SecureSessionRecord} record — validated session record
   * @param {object} options
   * @param {number} options.version — expected current version for optimistic lock
   * @returns {Promise<number>} the new version after write
   */
  async putEncrypted(record, { version = 0 } = {}) {
    if (!(record instanceof SecureSessionRecord)) {
      throw new Error("FsSessionStore.putEncrypted requires SecureSessionRecord");
    }
    if (!this.#codec) {
      throw new Error("FsSessionStore.putEncrypted requires encryption to be configured");
    }
    const recordVersion = typeof version === "number" && version > 0 ? version : 1;
    return this.#writeWithVersionCheck(record.sid, recordVersion, (nextVersion) => {
      const envelope = this.#codec.seal(record);
      return JSON.stringify({
        encrypted: 1,
        version: nextVersion,
        envelope: envelope.toJSON(),
      });
    });
  }

  /**
   * Shared write logic: optimistic lock check + atomic file write.
   */
  async #writeWithVersionCheck(sid, recordVersion, buildPayload) {
    await this.ensureRoot();
    const filePath = this.filePathForSid(sid);

    // Read existing version for optimistic lock check
    let diskVersion = 0;
    try {
      const raw = await fs.readFile(filePath, "utf8");
      const existing = JSON.parse(raw);
      diskVersion = this.#extractVersion(existing);
    } catch (err) {
      if (!err || err.code !== "ENOENT") {
        if (err) throw err;
      }
    }

    if (diskVersion > 0 && recordVersion !== diskVersion) {
      throw new Error(
        "FsSessionStore.put: version conflict (expected " + recordVersion
        + " but disk has " + diskVersion + ") — concurrent ratchet state update detected"
      );
    }

    const nextVersion = diskVersion > 0 ? diskVersion + 1 : 1;
    const payload = buildPayload(nextVersion);

    const tmpName = `${filePath}.tmp-${process.pid}-${Buffer.from(randomBytes(8)).toString("hex")}`;
    await fs.writeFile(tmpName, payload, "utf8");
    await fs.rename(tmpName, filePath);
    return nextVersion;
  }

  async delete(sidBytes) {
    if (!isBytes(sidBytes)) {
      throw new Error("FsSessionStore.delete(sid) requires Uint8Array");
    }
    await this.ensureRoot();
    const filePath = this.filePathForSid(sidBytes);
    try {
      await fs.unlink(filePath);
    } catch (err) {
      if (err && err.code === "ENOENT") return;
      throw err;
    }
  }

  async list() {
    await this.ensureRoot();
    const files = await fs.readdir(this.rootDir);
    const records = [];
    for (const name of files) {
      if (!name.endsWith(".json")) continue;
      const filePath = path.join(this.rootDir, name);
      const raw = await fs.readFile(filePath, "utf8");
      const json = JSON.parse(raw);
      records.push(this.#fromDiskDispatch(json));
    }
    return records;
  }

  /**
   * Read a disk JSON object, handling both encrypted and legacy plaintext formats.
   * @param {object} json — parsed JSON from disk
   * @returns {object} — record with { sid, peerId, includeDh, ratchetState, version }
   */
  #fromDiskDispatch(json) {
    if (json && json.encrypted === 1) {
      return this.#fromEncryptedDisk(json);
    }
    return fromDisk(json);
  }

  /**
   * Decrypt an encrypted disk record.
   * Returns a SecureSessionRecord with a `version` property attached
   * so PersistentSessionManager can track optimistic locking.
   *
   * @param {object} json — { encrypted: 1, version: N, envelope: {...} }
   * @returns {SecureSessionRecord} — with .version property
   */
  #fromEncryptedDisk(json) {
    if (!this.#codec || !this.#registry) {
      throw new Error("FsSessionStore: encrypted record found but no encryption key configured");
    }
    const envelope = EncryptedStoreEnvelopeV1.fromJSON(json.envelope);
    const sessionRecord = this.#codec.open(envelope, this.#registry);
    sessionRecord.version = typeof json.version === "number" && json.version > 0 ? json.version : 1;
    return sessionRecord;
  }

  /**
   * Extract version from a disk record (works for both encrypted and plaintext).
   */
  #extractVersion(json) {
    if (!json || typeof json !== "object") return 0;
    const v = json.version;
    return typeof v === "number" && v > 0 ? v : 1;
  }

}
