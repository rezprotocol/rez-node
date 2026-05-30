import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { promises as fs } from "node:fs";
import { FsStorageProvider } from "../src/storage/fs/FsStorageProvider.js";
import { Header, Envelope } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

async function makeTempDir() {
  return fs.mkdtemp(path.join(os.tmpdir(), "rez-fs-"));
}

async function cleanup(dir) {
  await fs.rm(dir, { recursive: true, force: true });
}

test("FsStorageProvider object round-trip", async () => {
  const rootDir = await makeTempDir();
  try {
    const provider = new FsStorageProvider({ rootDir });
    const header = new Header({ id: "fs-1", type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { hello: "world" } });

    await provider.getObjectStore().put(envelope);
    const loaded = await provider.getObjectStore().get("fs-1");

    assert.deepEqual(loaded?.toJSON(), envelope.toJSON());
  } finally {
    await cleanup(rootDir);
  }
});

test("FsStorageProvider mailbox persists across instances", async () => {
  const rootDir = await makeTempDir();
  try {
    const provider1 = new FsStorageProvider({ rootDir });
    await provider1.getMailboxStore().append("mb1", "id-1");

    const provider2 = new FsStorageProvider({ rootDir });
    const items = await provider2.getMailboxStore().list("mb1");

    assert.deepEqual(items, ["id-1"]);
  } finally {
    await cleanup(rootDir);
  }
});

test("FsStorageProvider kv persists across instances", async () => {
  const rootDir = await makeTempDir();
  try {
    const provider1 = new FsStorageProvider({ rootDir });
    await provider1.getKeyValueStore().set("k1", { ok: true });

    const provider2 = new FsStorageProvider({ rootDir });
    const value = await provider2.getKeyValueStore().get("k1");

    assert.deepEqual(value, { ok: true });
  } finally {
    await cleanup(rootDir);
  }
});

// --- Encrypted storage tests ---

function makeEncryptionKey() {
  const crypto = new NodeCryptoProvider();
  return crypto.randomBytes(32);
}

test("Encrypted FsStorageProvider object round-trip", async () => {
  const rootDir = await makeTempDir();
  const encryptionKey = makeEncryptionKey();
  try {
    const provider = new FsStorageProvider({ rootDir, encryptionKey });
    const header = new Header({ id: "enc-obj-1", type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { secret: "data" } });

    await provider.getObjectStore().put(envelope);

    // Verify file on disk is encrypted (not plaintext)
    const objectsDir = path.join(rootDir, "objects");
    const files = await fs.readdir(objectsDir);
    const objFile = files.find((f) => f.endsWith(".json"));
    assert.ok(objFile, "object file should exist");
    const raw = await fs.readFile(path.join(objectsDir, objFile), "utf8");
    const diskJson = JSON.parse(raw);
    assert.equal(diskJson.encrypted, 1, "disk file should be encrypted");
    assert.ok(diskJson.c, "disk file should have ciphertext");
    assert.equal(diskJson.body, undefined, "plaintext body should not be visible");

    // Read back and verify
    const loaded = await provider.getObjectStore().get("enc-obj-1");
    assert.ok(loaded, "should load envelope");
    assert.deepEqual(loaded.toJSON(), envelope.toJSON());
  } finally {
    await cleanup(rootDir);
  }
});

test("Encrypted FsStorageProvider mailbox round-trip", async () => {
  const rootDir = await makeTempDir();
  const encryptionKey = makeEncryptionKey();
  try {
    const provider = new FsStorageProvider({ rootDir, encryptionKey });
    await provider.getMailboxStore().append("mb-enc", "id-a");
    await provider.getMailboxStore().append("mb-enc", "id-b");

    // Verify disk is encrypted
    const mbDir = path.join(rootDir, "mailboxes");
    const dirs = await fs.readdir(mbDir);
    assert.ok(dirs.length > 0, "mailbox directory should exist");
    const indexPath = path.join(mbDir, dirs[0], "index.json");
    const raw = await fs.readFile(indexPath, "utf8");
    const diskJson = JSON.parse(raw);
    assert.equal(diskJson.encrypted, 1, "mailbox index should be encrypted");
    assert.equal(Array.isArray(diskJson), false, "should not be plaintext array");

    // Read back
    const items = await provider.getMailboxStore().list("mb-enc");
    assert.deepEqual(items, ["id-a", "id-b"]);
  } finally {
    await cleanup(rootDir);
  }
});

test("Encrypted FsStorageProvider kv round-trip", async () => {
  const rootDir = await makeTempDir();
  const encryptionKey = makeEncryptionKey();
  try {
    const provider = new FsStorageProvider({ rootDir, encryptionKey });
    await provider.getKeyValueStore().set("enc-k1", { secret: true });

    const value = await provider.getKeyValueStore().get("enc-k1");
    assert.deepEqual(value, { secret: true });

    // Verify disk is encrypted
    const kvDir = path.join(rootDir, "kv");
    const files = await fs.readdir(kvDir);
    const kvFile = files.find((f) => f.endsWith(".json"));
    assert.ok(kvFile, "kv file should exist");
    const raw = await fs.readFile(path.join(kvDir, kvFile), "utf8");
    const diskJson = JSON.parse(raw);
    assert.equal(diskJson.v, 1, "should be encrypted envelope");
    assert.ok(diskJson.c, "should have ciphertext");
    assert.equal(diskJson.secret, undefined, "plaintext should not be visible");
  } finally {
    await cleanup(rootDir);
  }
});

test("Encrypted FsStorageProvider rejects tampered object", async () => {
  const rootDir = await makeTempDir();
  const encryptionKey = makeEncryptionKey();
  try {
    const provider = new FsStorageProvider({ rootDir, encryptionKey });
    const header = new Header({ id: "tamper-1", type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { ok: true } });

    await provider.getObjectStore().put(envelope);

    // Tamper with ciphertext
    const objectsDir = path.join(rootDir, "objects");
    const files = await fs.readdir(objectsDir);
    const objFile = files.find((f) => f.endsWith(".json"));
    const raw = await fs.readFile(path.join(objectsDir, objFile), "utf8");
    const diskJson = JSON.parse(raw);
    diskJson.c = diskJson.c.slice(0, -4) + "XXXX";
    await fs.writeFile(path.join(objectsDir, objFile), JSON.stringify(diskJson), "utf8");

    await assert.rejects(
      () => provider.getObjectStore().get("tamper-1"),
      /authenticate|integrity|decrypt|tag/i,
    );
  } finally {
    await cleanup(rootDir);
  }
});

test("Encrypted FsStorageProvider wrong key rejects", async () => {
  const rootDir = await makeTempDir();
  const key1 = makeEncryptionKey();
  const key2 = makeEncryptionKey();
  try {
    const provider1 = new FsStorageProvider({ rootDir, encryptionKey: key1 });
    const header = new Header({ id: "wrongkey-1", type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { ok: true } });
    await provider1.getObjectStore().put(envelope);

    const provider2 = new FsStorageProvider({ rootDir, encryptionKey: key2 });
    await assert.rejects(
      () => provider2.getObjectStore().get("wrongkey-1"),
      /authenticate|integrity|decrypt|tag/i,
    );
  } finally {
    await cleanup(rootDir);
  }
});

test("Encrypted FsStorageProvider reads legacy plaintext objects", async () => {
  const rootDir = await makeTempDir();
  const encryptionKey = makeEncryptionKey();
  try {
    // Write plaintext (no encryption key)
    const plainProvider = new FsStorageProvider({ rootDir });
    const header = new Header({ id: "legacy-1", type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { legacy: true } });
    await plainProvider.getObjectStore().put(envelope);

    // Read with encryption enabled (progressive migration)
    const encProvider = new FsStorageProvider({ rootDir, encryptionKey });
    const loaded = await encProvider.getObjectStore().get("legacy-1");
    assert.ok(loaded, "should read legacy plaintext");
    assert.deepEqual(loaded.toJSON(), envelope.toJSON());
  } finally {
    await cleanup(rootDir);
  }
});
