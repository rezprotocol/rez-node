import { createHash, randomBytes } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { KeyValueStore } from "@rezprotocol/core";

// DT-009: fsync scope for durability against power loss (not just process
// crash). writeJsonAtomic fsyncs the temp FILE before the rename and the
// containing DIRECTORY after it; delete() fsyncs the directory after the
// unlink (an unfsynced delete can resurrect the file after power loss —
// DT-006 §7.5). Uniform for ALL keys by design: this helper is shared, and a
// per-prefix opt-out would be a silent durability downgrade.
//
// FAIL CLOSED (DT-006 §7.5 makes this durability mandatory): a directory
// fsync failure fails the whole set()/delete(). There is deliberately NO
// warn-and-continue degradation — EACCES/EPERM are operational
// misconfiguration, and a store that cannot prove durability must not report
// success that a WAL consumer would treat as a commit. Every production
// target (darwin desktop, linux hosted/relay) supports POSIX directory
// fsync; an unsupported platform is a deployment error, not a capability to
// silently accommodate.

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
// WARNING: this constant is part of the ON-DISK layout. Changing it silently
// ORPHANS existing data — a key whose base64url basename straddles the old vs new
// bound moves between its base64url path and its `h.<hash>.json` path, so `get()`
// then reads the wrong (nonexistent) path and returns undefined while the old file
// lingers invisibly. Do not change without a migration that rewrites straddling keys.
const MAX_BASENAME_LEN = 200;
const HASHED_MARKER = "__fskv_hashed__";

export class FsKeyValueStore extends KeyValueStore {
  // `fsImpl` (default: node:fs promises) exists so tests can assert the fsync
  // call sequence without stubbing the module graph; production callers never
  // pass it.
  constructor({ rootDir, fsImpl = fs } = {}) {
    super();
    if (!rootDir) {
      throw new Error("FsKeyValueStore requires rootDir");
    }
    this.rootDir = rootDir;
    this.kvDir = path.join(rootDir, "kv");
    this.fs = fsImpl;
  }

  async #ensureDir(dir) {
    await this.fs.mkdir(dir, { recursive: true, mode: 0o700 });
  }

  // fsync the directory containing a just-renamed or just-unlinked entry so
  // the namespace change itself is durable. FAIL CLOSED: any open/sync
  // failure propagates and fails the caller's operation (see header).
  async #syncDir(dirPath) {
    const handle = await this.fs.open(dirPath, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  }

  // Same fail-closed sync, but tolerant of the directory not existing at all —
  // there is then no namespace change to confirm. Used only by the idempotent
  // delete path; every other error (EACCES, EPERM, sync failure) propagates.
  async #syncDirIfPresent(dirPath) {
    let handle;
    try {
      handle = await this.fs.open(dirPath, "r");
    } catch (err) {
      if (err && err.code === "ENOENT") return;
      throw err;
    }
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  }

  async #writeJsonAtomic(filePath, data) {
    const tmpPath = `${filePath}.tmp.${process.pid}.${Date.now()}.${Buffer.from(randomBytes(4)).toString("hex")}`;
    const handle = await this.fs.open(tmpPath, "w", 0o600);
    try {
      await handle.writeFile(data);
      // Content durability: the temp file's bytes must be on stable storage
      // BEFORE the rename publishes the name, or a power loss can leave the
      // final name pointing at empty/torn content.
      await handle.sync();
    } finally {
      await handle.close();
    }
    await this.fs.rename(tmpPath, filePath);
    // Rename durability: the directory entry itself.
    await this.#syncDir(path.dirname(filePath));
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
    await this.#ensureDir(this.kvDir);
    const filePath = this._pathForKey(key);
    // A hashed filename is not reversible to its key, so persist the key alongside
    // the value; get() unwraps it and keys() reads it back for enumeration.
    const payload = this._isKeyHashed(key)
      ? JSON.stringify({ [HASHED_MARKER]: 1, key: String(key), value })
      : JSON.stringify(value);
    await this.#writeJsonAtomic(filePath, payload);
  }

  async get(key) {
    const filePath = this._pathForKey(key);
    let data;
    try {
      data = await this.fs.readFile(filePath, "utf8");
    } catch (err) {
      if (err && err.code === "ENOENT") return undefined;
      throw err;
    }
    let parsed;
    try {
      parsed = JSON.parse(data);
    } catch (parseErr) {
      // A corrupt/torn/foreign file at this path — treat the key as ABSENT rather
      // than throwing, so one bad file can't wedge callers (and a re-write heals
      // it). A read-integrity error is not the caller's to handle.
      this.#warnCorrupt("get", filePath, parseErr);
      return undefined;
    }
    if (this._isKeyHashed(key)) {
      if (parsed && typeof parsed === "object" && parsed[HASHED_MARKER] === 1) {
        return parsed.value;
      }
      // A hashed filename that is not a wrapper is a corrupt/foreign file — treat as
      // absent (skip-and-warn), consistent with a parse failure above.
      this.#warnCorrupt("get", filePath, new Error("hashed record missing wrapper"));
      return undefined;
    }
    return parsed;
  }

  #warnCorrupt(where, filePath, err) {
    // Log with context; never silently swallow. Callers see `undefined`/skip, not a throw.
    // eslint-disable-next-line no-console
    console.warn(`[FsKeyValueStore] ${where}: skipping unreadable kv file ${filePath}: ${err && err.message ? err.message : err}`);
  }

  async delete(key) {
    const filePath = this._pathForKey(key);
    const dirPath = path.dirname(filePath);
    try {
      await this.fs.unlink(filePath);
    } catch (err) {
      if (err && err.code === "ENOENT") {
        // IDEMPOTENTLY DURABLE (rev-3 review): a failed durable delete cannot
        // be repaired by retry unless the retry also confirms the namespace
        // change. A previous delete() may have unlinked the file and then
        // REJECTED because its directory fsync failed — the retry sees ENOENT,
        // but that unlink is still unconfirmed and can resurrect after power
        // loss. So sync the directory before reporting absence; only a missing
        // directory means there is nothing to confirm.
        await this.#syncDirIfPresent(dirPath);
        return false;
      }
      throw err;
    }
    // Deletion durability: without this, power loss can resurrect the file —
    // for a WAL-style consumer that means a compacted record coming back
    // (DT-006 §7.5 makes resurrection non-destructive but it must stay rare).
    await this.#syncDir(dirPath);
    return true;
  }

  async keys(prefix = "") {
    let entries;
    try {
      entries = await this.fs.readdir(this.kvDir);
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

  // Read the stored key out of a hashed record file by its on-disk name. A corrupt/
  // torn/foreign file is SKIPPED (returns undefined + warns), never thrown — else a
  // single bad file would wedge keys() for the whole kv dir (which is on the
  // peer-link/session enumeration hot path), a regression vs the old
  // filename-only decode that never read file contents.
  async _readHashedKey(filename) {
    const filePath = path.join(this.kvDir, filename);
    let data;
    try {
      data = await this.fs.readFile(filePath, "utf8");
    } catch (err) {
      if (err && err.code === "ENOENT") return undefined;
      throw err;
    }
    try {
      const parsed = JSON.parse(data);
      if (parsed && typeof parsed === "object" && parsed[HASHED_MARKER] === 1 && typeof parsed.key === "string") {
        return parsed.key;
      }
      return undefined;
    } catch (parseErr) {
      this.#warnCorrupt("keys", filePath, parseErr);
      return undefined;
    }
  }
}
