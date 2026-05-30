import { promises as fs } from "node:fs";
import path from "node:path";
import { MailboxStore } from "@rezprotocol/core";

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function writeJsonAtomic(filePath, data) {
  const tmpPath = `${filePath}.tmp`;
  await fs.writeFile(tmpPath, data);
  await fs.rename(tmpPath, filePath);
}

export class FsMailboxStore extends MailboxStore {
  constructor({ rootDir } = {}) {
    super();
    if (!rootDir) {
      throw new Error("FsMailboxStore requires rootDir");
    }
    this.rootDir = rootDir;
    this.mailboxesDir = path.join(rootDir, "mailboxes");
  }

  _encodeId(id) {
    return Buffer.from(String(id), "utf8").toString("base64url");
  }

  _pathForMailboxDir(mailboxId) {
    return path.join(this.mailboxesDir, this._encodeId(mailboxId));
  }

  _pathForMailboxIndex(mailboxId) {
    return path.join(this._pathForMailboxDir(mailboxId), "index.json");
  }

  async append(mailboxId, objectId) {
    const dir = this._pathForMailboxDir(mailboxId);
    const indexPath = this._pathForMailboxIndex(mailboxId);
    await ensureDir(dir);

    let items = [];
    try {
      const data = await fs.readFile(indexPath, "utf8");
      items = JSON.parse(data);
    } catch (err) {
      if (!(err && err.code === "ENOENT")) throw err;
    }

    items.push(objectId);
    await writeJsonAtomic(indexPath, JSON.stringify(items));
  }

  async list(mailboxId) {
    const indexPath = this._pathForMailboxIndex(mailboxId);
    try {
      const data = await fs.readFile(indexPath, "utf8");
      const items = JSON.parse(data);
      return Array.isArray(items) ? items : [];
    } catch (err) {
      if (err && err.code === "ENOENT") return [];
      throw err;
    }
  }

  /**
   * Read raw string data for the given mailbox (used by EncryptedMailboxStore).
   * Returns null if not found.
   */
  async _readRaw(mailboxId) {
    const indexPath = this._pathForMailboxIndex(mailboxId);
    try {
      return await fs.readFile(indexPath, "utf8");
    } catch (err) {
      if (err && err.code === "ENOENT") return null;
      throw err;
    }
  }

  /**
   * Write raw string data for the given mailbox (used by EncryptedMailboxStore).
   */
  async _writeSealed(mailboxId, data) {
    const dir = this._pathForMailboxDir(mailboxId);
    const indexPath = this._pathForMailboxIndex(mailboxId);
    await ensureDir(dir);
    await writeJsonAtomic(indexPath, data);
  }

  async deleteMailbox(mailboxId) {
    const dir = this._pathForMailboxDir(mailboxId);
    const indexPath = this._pathForMailboxIndex(mailboxId);
    try {
      await fs.unlink(indexPath);
      await fs.rmdir(dir);
      return true;
    } catch (err) {
      if (err && err.code === "ENOENT") return false;
      throw err;
    }
  }
}
