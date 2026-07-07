import { createHash, randomBytes } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { KeyValueStore } from "@rezprotocol/core";

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true, mode: 0o700 });
}

async function writeJsonAtomic(filePath, data) {
  const tmpPath = `${filePath}.tmp.${process.pid}.${Date.now()}.${Buffer.from(randomBytes(4)).toString("hex")}`;
  await fs.writeFile(tmpPath, data, { mode: 0o600 });
  await fs.rename(tmpPath, filePath);
}

// A key encodes to a `base64url(key)` filename, which grows ~4/3 vs the key. A
// single path component must stay under the filesystem limit (255 bytes on
// APFS/ext4/HFS+) AND leave room for the `.json` suffix plus the `.tmp.<pid>.
// <ts>.<hex>` suffix writeJsonAtomic appends before the rename (~40 bytes). Keys
// whose base64url basename exceeds this bound (e.g. the S2.5 per-device session
// index `peer-link:sessions:by-peer-link-device:<owner>::<peerLinkId>::<deviceId>`
// — ~200 chars → ~270-char base64url) are stored under a fixed-length SHA-256
// digest filename instead, with the original key recorded INSIDE the file so
// `keys(prefix)` can still enumerate them. base64url never emits `.`, so a normal
// filename basename holds exactly one `.` (before `json`); a hashed basename
// carries a second `.` (the `h.` marker) — an unambiguous discriminator.
const MAX_BASENAME_LEN = 200;
const HASHED_MARKER = "__fskv_hashed__";

export class FsKeyValueStore extends KeyValueStore {
  constructor({ rootDir } = {}) {
    super();
    if (!rootDir) {
      throw new Error("FsKeyValueStore requires rootDir");
    }
    this.rootDir = rootDir;
    this.kvDir = path.join(rootDir, "kv");
  }

  _base64UrlBasename(key) {
    return Buffer.from(String(key), "utf8").toString("base64url");
  }

  // The on-disk basename (no `.json`). Short keys keep the historical base64url
  // form for byte-for-byte backward compatibility; only over-long keys are hashed.
  _encodeKey(key) {
    const encoded = this._base64UrlBasename(key);
    if (encoded.length <= MAX_BASENAME_LEN) {
      return encoded;
    }
    return `h.${createHash("sha256").update(String(key), "utf8").digest("hex")}`;
  }

  _isHashedBasename(basename) {
    // base64url output contains no `.`; a hashed basename is `h.<hex>`.
    return basename.includes(".");
  }

  _isKeyHashed(key) {
    return this._base64UrlBasename(key).length > MAX_BASENAME_LEN;
  }

  _decodeBasename(basename) {
    return Buffer.from(basename, "base64url").toString("utf8");
  }

  _pathForKey(key) {
    return path.join(this.kvDir, `${this._encodeKey(key)}.json`);
  }

  async set(key, value) {
    await ensureDir(this.kvDir);
    const filePath = this._pathForKey(key);
    // A hashed filename is not reversible to its key, so persist the key alongside
    // the value; get() unwraps it and keys() reads it back for enumeration.
    const payload = this._isKeyHashed(key)
      ? JSON.stringify({ [HASHED_MARKER]: 1, key: String(key), value })
      : JSON.stringify(value);
    await writeJsonAtomic(filePath, payload);
  }

  async get(key) {
    const filePath = this._pathForKey(key);
    try {
      const data = await fs.readFile(filePath, "utf8");
      const parsed = JSON.parse(data);
      if (this._isKeyHashed(key)) {
        if (parsed && typeof parsed === "object" && parsed[HASHED_MARKER] === 1) {
          return parsed.value;
        }
        // A hashed filename that is not a wrapper is a corrupt/foreign file — fail
        // loud rather than silently returning the wrapper envelope as the value.
        throw new Error(`FsKeyValueStore: hashed record missing wrapper for key ${String(key)}`);
      }
      return parsed;
    } catch (err) {
      if (err && err.code === "ENOENT") return undefined;
      throw err;
    }
  }

  async delete(key) {
    const filePath = this._pathForKey(key);
    try {
      await fs.unlink(filePath);
      return true;
    } catch (err) {
      if (err && err.code === "ENOENT") return false;
      throw err;
    }
  }

  async keys(prefix = "") {
    let entries;
    try {
      entries = await fs.readdir(this.kvDir);
    } catch (err) {
      if (err && err.code === "ENOENT") return [];
      throw err;
    }
    const out = [];
    for (const name of entries) {
      if (!name.endsWith(".json")) continue;
      const basename = name.slice(0, -".json".length);
      let key;
      if (this._isHashedBasename(basename)) {
        // Recover the original key from inside the file (hashed names are not
        // reversible). A hashed file that lost its wrapper is skipped, not decoded.
        key = await this._readHashedKey(name);
        if (key === undefined) continue;
      } else {
        key = this._decodeBasename(basename);
      }
      if (typeof key === "string" && key.startsWith(prefix)) {
        out.push(key);
      }
    }
    return out;
  }

  // Read the stored key out of a hashed record file by its on-disk name.
  async _readHashedKey(filename) {
    const filePath = path.join(this.kvDir, filename);
    try {
      const parsed = JSON.parse(await fs.readFile(filePath, "utf8"));
      if (parsed && typeof parsed === "object" && parsed[HASHED_MARKER] === 1 && typeof parsed.key === "string") {
        return parsed.key;
      }
      return undefined;
    } catch (err) {
      if (err && err.code === "ENOENT") return undefined;
      throw err;
    }
  }
}
