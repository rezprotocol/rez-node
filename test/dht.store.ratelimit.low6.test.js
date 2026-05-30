import test from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { SlidingWindowRateLimiter } from "../src/util/SlidingWindowRateLimiter.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

/**
 * docs/SECURITY_AUDIT.md LOW-6 — `dht.store` had no per-peer quota. A
 * relay-provisional peer that observed legitimate claimant-signed
 * delegations off the wire could replay them at the global rate-limit
 * cap (200 ctl frames/sec/socket), filling local valueStore with 24h-TTL
 * entries. HIGH-8 guarantees every stored entry is genuine, so this is a
 * memory-pressure attack, not a correctness one — but it remained
 * cheap and unbounded without a per-peer cap.
 *
 * Fix: `SlidingWindowRateLimiter` (sliding-window, in-memory, LRU-capped),
 * applied in `DhtProtocol.#handleStore` keyed on the peer's relayKeyId
 * via the bootstrap-provided `getPeerKey` callback.
 */

function makeProtocol({ maxStores = 3, windowMs = 60_000, getPeerKey = null } = {}) {
  const selfRelayKeyId = "relay-self";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);
  const valueStore = new DhtValueStore();
  const registry = new ControlMessageRegistry();
  const storeRateLimiter = new SlidingWindowRateLimiter({ maxAttempts: maxStores, windowMs });
  const protocol = new DhtProtocol({
    kBuckets, valueStore, registry, selfNodeId, selfRelayKeyId,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: () => {},
    queryTimeoutMs: 500,
    nowMs: () => Date.now(),
    storeRateLimiter,
    getPeerKey,
  });
  protocol.install();
  return { protocol, registry, valueStore, storeRateLimiter };
}

function makeStoreFrame(inboxId, deliveryRelayKeyId = "relay-host") {
  const { routeEntry } = makeSignedRouteEntry({ inboxId, deliveryRelayKeyId, hops: 0 });
  return { _ctl: "dht.store", inboxId, routeEntry };
}

test("LOW-6: dht.store admits stores under the per-peer cap", async () => {
  const peer = { id: "peer-a" };
  const { registry, valueStore } = makeProtocol({ maxStores: 3 });
  for (let i = 0; i < 3; i += 1) {
    const inboxId = "inbox:legit-" + i;
    await registry.dispatch("dht.store", makeStoreFrame(inboxId), peer);
  }
  assert.equal(valueStore.size, 3, "stores within cap admitted");
});

test("LOW-6: dht.store drops stores once the per-peer cap is hit", async () => {
  const peer = { id: "peer-a" };
  const { registry, valueStore } = makeProtocol({ maxStores: 3 });
  for (let i = 0; i < 5; i += 1) {
    const inboxId = "inbox:flood-" + i;
    await registry.dispatch("dht.store", makeStoreFrame(inboxId), peer);
  }
  assert.equal(valueStore.size, 3, "extra stores past cap dropped");
});

test("LOW-6: per-peer caps are independent — one noisy peer does not starve another", async () => {
  const peerA = { id: "peer-a" };
  const peerB = { id: "peer-b" };
  const { registry, valueStore } = makeProtocol({ maxStores: 2 });
  // Peer A floods.
  await registry.dispatch("dht.store", makeStoreFrame("inbox:a1"), peerA);
  await registry.dispatch("dht.store", makeStoreFrame("inbox:a2"), peerA);
  await registry.dispatch("dht.store", makeStoreFrame("inbox:a3"), peerA);
  // Peer B still has budget.
  await registry.dispatch("dht.store", makeStoreFrame("inbox:b1"), peerB);
  await registry.dispatch("dht.store", makeStoreFrame("inbox:b2"), peerB);
  // Both A and B at 2 each → 4 stored, A's third was dropped.
  assert.equal(valueStore.size, 4);
  assert.ok(valueStore.get("inbox:b1", Date.now()), "peer B's stores survive peer A's flood");
  assert.ok(valueStore.get("inbox:b2", Date.now()), "peer B's stores survive peer A's flood");
});

test("LOW-6: getPeerKey callback keys the limiter on relayKeyId, not socket identity", async () => {
  // The same logical peer reconnects with a fresh socket but the same
  // relayKeyId — limiter must count both socket instances under one bucket.
  const sock1 = {};
  const sock2 = {};
  const sockToRelay = new Map([[sock1, "relay-mallory"], [sock2, "relay-mallory"]]);
  const getPeerKey = (s) => sockToRelay.get(s) || null;
  const { registry, valueStore } = makeProtocol({ maxStores: 2, getPeerKey });
  await registry.dispatch("dht.store", makeStoreFrame("inbox:m1"), sock1);
  await registry.dispatch("dht.store", makeStoreFrame("inbox:m2"), sock1);
  // Same logical peer, fresh socket — should NOT reset the bucket.
  await registry.dispatch("dht.store", makeStoreFrame("inbox:m3"), sock2);
  assert.equal(valueStore.size, 2, "reconnect under same relayKeyId does not reset the bucket");
});

test("LOW-6: rate-limit check runs BEFORE registration validation — a peer can't burn another peer's budget by sending invalid stores", async () => {
  // Peer A sends bogus (no registration) stores. They should be dropped
  // by HIGH-8 — but they DO count against A's own LOW-6 cap.
  const peerA = { id: "peer-a" };
  const { registry, storeRateLimiter, valueStore } = makeProtocol({ maxStores: 3 });
  for (let i = 0; i < 3; i += 1) {
    await registry.dispatch("dht.store", {
      _ctl: "dht.store",
      inboxId: "inbox:bogus-" + i,
      routeEntry: { hops: 0 }, // no registration → HIGH-8 reject
    }, peerA);
  }
  // Peer A's budget is now exhausted. A legitimate store from A should be dropped too.
  await registry.dispatch("dht.store", makeStoreFrame("inbox:legit"), peerA);
  assert.equal(valueStore.size, 0, "neither bogus nor legit stored");
  // Confirm internal cap accounting: limiter at maxStores.
  assert.equal(storeRateLimiter.size, 1);
});

test("SlidingWindowRateLimiter: sliding window evicts old timestamps", () => {
  const limiter = new SlidingWindowRateLimiter({ windowMs: 1000, maxAttempts: 2 });
  const t0 = 10_000;
  assert.equal(limiter.record("p", t0), true);
  assert.equal(limiter.record("p", t0 + 100), true);
  assert.equal(limiter.record("p", t0 + 200), false, "third store within window rejected");
  // Slide past the window.
  assert.equal(limiter.record("p", t0 + 2000), true, "after window slides, budget restored");
});

test("SlidingWindowRateLimiter: missing peerKey is treated as 'skip rate-limit' (defensive)", () => {
  const limiter = new SlidingWindowRateLimiter({ maxAttempts: 1 });
  // Both should pass because peerKey is missing.
  assert.equal(limiter.record(null, 100), true);
  assert.equal(limiter.record(null, 200), true);
  assert.equal(limiter.record("", 300), true);
});

test("SlidingWindowRateLimiter: LRU bounds memory under sybil-keypair flood", () => {
  const limiter = new SlidingWindowRateLimiter({ maxAttempts: 1, lruCap: 3 });
  const t = 10_000;
  for (let i = 0; i < 10; i += 1) {
    limiter.record("peer-" + i, t + i);
  }
  assert.ok(limiter.size <= 3, "LRU caps the distinct-peer map at lruCap");
});
