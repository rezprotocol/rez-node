import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";

describe("DhtValueStore", () => {
  it("stores and retrieves a route entry", () => {
    const store = new DhtValueStore();
    const entry = { inboxId: "inbox:a", hops: 1, deliveryRelayKeyId: "relay-x" };
    store.store("inbox:a", entry, 1000);
    const result = store.get("inbox:a", 1000);
    assert.deepStrictEqual(result, entry);
  });

  it("returns null for unknown key", () => {
    const store = new DhtValueStore();
    assert.equal(store.get("inbox:unknown", 1000), null);
  });

  it("returns null and evicts when TTL expired", () => {
    const store = new DhtValueStore({ defaultTtlMs: 5000 });
    const entry = { inboxId: "inbox:a", hops: 0 };
    store.store("inbox:a", entry, 1000);

    // Not expired yet
    assert.ok(store.get("inbox:a", 5999));
    // Expired
    assert.equal(store.get("inbox:a", 6000), null);
    // Entry was cleaned up
    assert.equal(store.size, 0);
  });

  it("overwrites existing entry", () => {
    const store = new DhtValueStore();
    store.store("inbox:a", { hops: 1 }, 1000);
    store.store("inbox:a", { hops: 2 }, 2000);
    assert.equal(store.size, 1);
    const result = store.get("inbox:a", 2000);
    assert.equal(result.hops, 2);
  });

  it("remove deletes entry", () => {
    const store = new DhtValueStore();
    store.store("inbox:a", { hops: 0 }, 1000);
    assert.equal(store.remove("inbox:a"), true);
    assert.equal(store.size, 0);
    assert.equal(store.get("inbox:a", 1000), null);
  });

  it("remove returns false for unknown key", () => {
    const store = new DhtValueStore();
    assert.equal(store.remove("inbox:unknown"), false);
  });

  it("evictExpired removes only expired entries", () => {
    const store = new DhtValueStore({ defaultTtlMs: 5000 });
    store.store("inbox:a", { hops: 0 }, 1000);
    store.store("inbox:b", { hops: 1 }, 3000);
    store.store("inbox:c", { hops: 2 }, 5000);

    // At 6000ms: inbox:a expired (stored at 1000, ttl 5000)
    const evicted = store.evictExpired(6000);
    assert.equal(evicted, 1);
    assert.equal(store.size, 2);
    assert.equal(store.get("inbox:a", 6000), null);
    assert.ok(store.get("inbox:b", 6000));
    assert.ok(store.get("inbox:c", 6000));
  });

  it("getAll returns only non-expired entries", () => {
    const store = new DhtValueStore({ defaultTtlMs: 5000 });
    store.store("inbox:a", { hops: 0 }, 1000);
    store.store("inbox:b", { hops: 1 }, 3000);

    const all = store.getAll(6000);
    assert.equal(all.size, 1);
    assert.ok(all.has("inbox:b"));
    assert.ok(!all.has("inbox:a"));
  });

  it("custom TTL per entry overrides default", () => {
    const store = new DhtValueStore({ defaultTtlMs: 10_000 });
    store.store("inbox:short", { hops: 0 }, 1000, { ttlMs: 2000 });
    store.store("inbox:default", { hops: 0 }, 1000);

    // At 3000: short expired, default still valid
    assert.equal(store.get("inbox:short", 3000), null);
    assert.ok(store.get("inbox:default", 3000));
  });

  it("null routeEntry removes existing entry (withdrawal)", () => {
    const store = new DhtValueStore();
    store.store("inbox:a", { hops: 0 }, 1000);
    assert.equal(store.size, 1);

    store.store("inbox:a", null, 2000);
    assert.equal(store.size, 0);
    assert.equal(store.get("inbox:a", 2000), null);
  });

  it("null routeEntry is safe on non-existent key", () => {
    const store = new DhtValueStore();
    store.store("inbox:missing", null, 1000);
    assert.equal(store.size, 0);
  });

  it("rejects invalid arguments", () => {
    const store = new DhtValueStore();
    assert.throws(() => store.store("", { hops: 0 }, 1000), /non-empty/);
    assert.throws(() => store.store("inbox:a", "not-an-object", 1000), /routeEntry/);
    assert.throws(() => store.store("inbox:a", { hops: 0 }, NaN), /finite/);
  });

  it("rejects invalid constructor args", () => {
    assert.throws(() => new DhtValueStore({ defaultTtlMs: 0 }), /positive/);
    assert.throws(() => new DhtValueStore({ defaultTtlMs: -1 }), /positive/);
  });
});
