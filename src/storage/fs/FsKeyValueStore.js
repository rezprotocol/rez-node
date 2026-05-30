import { randomBytes } from "node:crypto";
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

export class FsKeyValueStore extends KeyValueStore {
  constructor({ rootDir } = {}) {
    super();
    if (!rootDir) {
      throw new Error("FsKeyValueStore requires rootDir");
    }
    this.rootDir = rootDir;
    this.kvDir = path.join(rootDir, "kv");
  }

  _encodeKey(key) {
    return Buffer.from(String(key), "utf8").toString("base64url");
  }

  _decodeKey(filename) {
    const base = filename.replace(/\.json$/, "");
    return Buffer.from(base, "base64url").toString("utf8");
  }

  _pathForKey(key) {
    return path.join(this.kvDir, `${this._encodeKey(key)}.json`);
  }

  async set(key, value) {
    await ensureDir(this.kvDir);
    const filePath = this._pathForKey(key);
    await writeJsonAtomic(filePath, JSON.stringify(value));
  }

  async get(key) {
    const filePath = this._pathForKey(key);
    try {
      const data = await fs.readFile(filePath, "utf8");
      return JSON.parse(data);
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
    try {
      const entries = await fs.readdir(this.kvDir);
      return entries
        .filter((name) => name.endsWith(".json"))
        .map((name) => this._decodeKey(name))
        .filter((key) => key.startsWith(prefix));
    } catch (err) {
      if (err && err.code === "ENOENT") return [];
      throw err;
    }
  }
}
