import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { EncryptedKeyValueStore } from "../src/storage/fs/EncryptedKeyValueStore.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

class InMemoryKV {
  constructor() { this._store = new Map(); }
  async set(key, value) { this._store.set(key, JSON.parse(JSON.stringify(value))); }
  async get(key) {
    if (!this._store.has(key)) return undefined;
    return JSON.parse(JSON.stringify(this._store.get(key)));
  }
  async getStrict(key) { return this.get(key); }
  async delete(key) {
    if (!this._store.has(key)) return false;
    this._store.delete(key);
    return true;
  }
  async keys(prefix = "") {
    return [...this._store.keys()].filter((k) => k.startsWith(prefix));
  }
}

function makeKey(crypto) {
  return crypto.randomBytes(32);
}

describe("EncryptedKeyValueStore", () => {
  const crypto = new NodeCryptoProvider();

  it("round-trip: set then get returns original value", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    const original = { hello: "world", count: 42, nested: { a: [1, 2, 3] } };
    await store.set("test-key", original);
    const result = await store.get("test-key");
    assert.deepStrictEqual(result, original);
  });

  it("inner store contains encrypted envelope, not plaintext", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    await store.set("secret", { password: "hunter2" });
    const raw = await inner.get("secret");
    assert.equal(raw.v, 1, "envelope version should be 1");
    assert.equal(typeof raw.n, "string", "nonce should be base64 string");
    assert.equal(typeof raw.c, "string", "ciphertext should be base64 string");
    assert.equal(raw.password, undefined, "plaintext field should not exist");
  });

  it("plaintext fallback: legacy data reads correctly", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    // Write plaintext directly to inner (simulating pre-encryption data)
    const legacy = { message: "hello from the past" };
    await inner.set("legacy-key", legacy);

    const result = await store.get("legacy-key");
    assert.deepStrictEqual(result, legacy);
  });

  it("get returns undefined for missing keys", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    const result = await store.get("nonexistent");
    assert.equal(result, undefined);
  });

  it("wrong key fails decryption", async () => {
    const inner = new InMemoryKV();
    const key1 = makeKey(crypto);
    const key2 = makeKey(crypto);
    const store1 = new EncryptedKeyValueStore({ inner, crypto, key: key1 });
    const store2 = new EncryptedKeyValueStore({ inner, crypto, key: key2 });

    await store1.set("bound", { secret: true });
    await assert.rejects(() => store2.get("bound"), "should fail with wrong key");
  });

  it("getStrict wraps unreadable encrypted values in the core-owned error", async () => {
    const inner = new InMemoryKV();
    const key1 = makeKey(crypto);
    const key2 = makeKey(crypto);
    const store1 = new EncryptedKeyValueStore({ inner, crypto, key: key1 });
    const store2 = new EncryptedKeyValueStore({ inner, crypto, key: key2 });

    await store1.set("bound", { secret: true });
    await assert.rejects(
      () => store2.getStrict("bound"),
      (err) => err && err.code === "KEY_VALUE_UNREADABLE" && err.key === "bound",
    );
  });

  it("AAD binding: ciphertext moved to different key fails", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    await store.set("key-a", { data: "value-a" });
    const envelope = await inner.get("key-a");
    // Move envelope to a different storage key
    await inner.set("key-b", envelope);
    await assert.rejects(() => store.get("key-b"), "AAD mismatch should fail");
  });

  it("keys() passthrough works with prefixes", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    await store.set("chat:msg:1", { text: "hi" });
    await store.set("chat:msg:2", { text: "hello" });
    await store.set("contact:alice", { name: "Alice" });

    const chatKeys = await store.keys("chat:");
    assert.equal(chatKeys.length, 2);
    assert.ok(chatKeys.includes("chat:msg:1"));
    assert.ok(chatKeys.includes("chat:msg:2"));

    const allKeys = await store.keys("");
    assert.equal(allKeys.length, 3);
  });

  it("delete() passthrough works", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    await store.set("to-delete", { temp: true });
    assert.deepStrictEqual(await store.get("to-delete"), { temp: true });

    const deleted = await store.delete("to-delete");
    assert.equal(deleted, true);
    assert.equal(await store.get("to-delete"), undefined);

    const deletedAgain = await store.delete("to-delete");
    assert.equal(deletedAgain, false);
  });

  it("handles string values", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    await store.set("str", "just a string");
    assert.equal(await store.get("str"), "just a string");
  });

  it("handles null values", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    await store.set("nil", null);
    assert.equal(await store.get("nil"), null);
  });

  it("handles array values", async () => {
    const inner = new InMemoryKV();
    const key = makeKey(crypto);
    const store = new EncryptedKeyValueStore({ inner, crypto, key });

    const arr = [1, "two", { three: 3 }];
    await store.set("arr", arr);
    assert.deepStrictEqual(await store.get("arr"), arr);
  });

  it("rejects invalid constructor args", () => {
    assert.throws(() => new EncryptedKeyValueStore({}), /inner/);
    assert.throws(() => new EncryptedKeyValueStore({ inner: new InMemoryKV() }), /crypto/);
    assert.throws(
      () => new EncryptedKeyValueStore({ inner: new InMemoryKV(), crypto, key: new Uint8Array(16) }),
      /32-byte/,
    );
  });
});
