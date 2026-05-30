import { promises as fs } from "node:fs";
import path from "node:path";
import { Envelope, canonicalize, ObjectStore } from "@rezprotocol/core";

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function writeJsonAtomic(filePath, data) {
  const tmpPath = `${filePath}.tmp`;
  await fs.writeFile(tmpPath, data);
  await fs.rename(tmpPath, filePath);
}

export class FsObjectStore extends ObjectStore {
  constructor({ rootDir } = {}) {
    super();
    if (!rootDir) {
      throw new Error("FsObjectStore requires rootDir");
    }
    this.rootDir = rootDir;
    this.objectsDir = path.join(rootDir, "objects");
  }

  _encodeId(id) {
    return Buffer.from(String(id), "utf8").toString("base64url");
  }

  _decodeId(filename) {
    const base = filename.replace(/\.json$/, "");
    return Buffer.from(base, "base64url").toString("utf8");
  }

  _pathForId(id) {
    const filename = `${this._encodeId(id)}.json`;
    return path.join(this.objectsDir, filename);
  }

  async put(envelope) {
    super.put(envelope);
    await ensureDir(this.objectsDir);
    const id = envelope.header.id;
    const filePath = this._pathForId(id);
    const json = canonicalize(envelope.toJSON());
    const data = JSON.stringify(json);
    await writeJsonAtomic(filePath, data);
  }

  async get(id) {
    const filePath = this._pathForId(id);
    try {
      const data = await fs.readFile(filePath, "utf8");
      const json = JSON.parse(data);
      return Envelope.fromJSON(json);
    } catch (err) {
      if (err && err.code === "ENOENT") return null;
      throw err;
    }
  }

  async has(id) {
    const filePath = this._pathForId(id);
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  async delete(id) {
    const filePath = this._pathForId(id);
    try {
      await fs.unlink(filePath);
      return true;
    } catch (err) {
      if (err && err.code === "ENOENT") return false;
      throw err;
    }
  }

  /**
   * Write raw string data for the given ID (used by EncryptedObjectStore).
   */
  async _writeSealed(id, data) {
    await ensureDir(this.objectsDir);
    const filePath = this._pathForId(id);
    await writeJsonAtomic(filePath, data);
  }

  /**
   * Read raw string data for the given ID (used by EncryptedObjectStore).
   * Returns null if not found.
   */
  async _readRaw(id) {
    const filePath = this._pathForId(id);
    try {
      return await fs.readFile(filePath, "utf8");
    } catch (err) {
      if (err && err.code === "ENOENT") return null;
      throw err;
    }
  }

  async listIds() {
    try {
      const entries = await fs.readdir(this.objectsDir);
      return entries
        .filter((name) => name.endsWith(".json"))
        .map((name) => this._decodeId(name));
    } catch (err) {
      if (err && err.code === "ENOENT") return [];
      throw err;
    }
  }
}
