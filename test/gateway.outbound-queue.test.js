import test from "node:test";
import assert from "node:assert/strict";
import { RMailbox, MemoryDataStore, createDefaultRegistry, encodeOuterPacket, newRoutingKey, OutboundQueueEntryV1 } from "@rezprotocol/core";
import {
  GatewayLoop,
  GatewaySender,
  RoutingFailedError,
  PersistentOutboundQueue,
  RetryScheduler,
  GatewayRelaySelector,
  GatewayPathPlanner,
} from "../src/gateway/index.js";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

/** In-memory KV store that satisfies PersistentOutboundQueue's interface. */
class MemoryKV {
  #data = new Map();
  async get(key) { return this.#data.get(key) || null; }
  async set(key, value) { this.#data.set(key, value); }
  async delete(key) { this.#data.delete(key); }
  async keys(prefix) {
    const result = [];
    for (const k of this.#data.keys()) {
      if (k.startsWith(prefix)) result.push(k);
    }
    return result;
  }
}

function makeGatewayLoop({
  inboxRouter = null,
  inboxStore = null,
  isHostedHere = null,
  outboundQueue = null,
  sender = null,
  routePolicy = null,
  routeResolver = null,
} = {}) {
  const routeTable = inboxRouter ? inboxRouter.routeTable : null;
  return new GatewayLoop({
    relaySelector: new GatewayRelaySelector(),
    pathPlanner: new GatewayPathPlanner(),
    sender: sender || new GatewaySender(),
    crypto: new NodeCryptoProvider(),
    routeTable,
    inboxRouter,
    inboxStore,
    isHostedHere,
    outboundQueue,
    routePolicy: routePolicy || undefined,
    routeResolver: routeResolver || undefined,
  });
}

// --- PersistentOutboundQueue tests ---

test("PersistentOutboundQueue enqueue and size", async () => {
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV() });
  assert.equal(queue.size(), 0);
  assert.equal(queue.sizeForInbox("inbox:a"), 0);

  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  assert.equal(queue.size(), 1);
  assert.equal(queue.sizeForInbox("inbox:a"), 1);

  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([2]) });
  assert.equal(queue.size(), 2);
  assert.equal(queue.sizeForInbox("inbox:a"), 2);

  await queue.enqueue({ deliverInboxId: "inbox:b", innerBytes: new Uint8Array([3]) });
  assert.equal(queue.size(), 3);
  assert.equal(queue.sizeForInbox("inbox:b"), 1);
});

test("PersistentOutboundQueue getRetryable returns ready entries", async () => {
  let now = 1000;
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV(), nowMs: () => now });

  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  const retryable = queue.getRetryable();
  assert.equal(retryable.length, 1);
  assert.ok(retryable[0] instanceof OutboundQueueEntryV1);
  assert.equal(retryable[0].deliverInboxId, "inbox:a");
});

test("PersistentOutboundQueue markDelivered removes entry", async () => {
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV() });
  const entry = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  assert.equal(queue.size(), 1);

  await queue.markDelivered(entry.queueId);
  assert.equal(queue.size(), 0);
  assert.equal(queue.sizeForInbox("inbox:a"), 0);
});

test("PersistentOutboundQueue recordAttemptFailure updates backoff", async () => {
  let now = 1000;
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV(), nowMs: () => now });
  const entry = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });

  // First attempt fails at now=1000
  await queue.recordAttemptFailure(entry.queueId);

  // Entry should not be retryable until backoff passes
  const retryable = queue.getRetryable();
  assert.equal(retryable.length, 0, "entry should be in backoff");

  // Advance past first backoff (15s for attempt 1)
  now = 16000;
  const retryable2 = queue.getRetryable();
  assert.equal(retryable2.length, 1, "entry should be retryable after backoff");
});

test("PersistentOutboundQueue getForInbox returns entries for specific inbox", async () => {
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV() });
  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  await queue.enqueue({ deliverInboxId: "inbox:b", innerBytes: new Uint8Array([2]) });
  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([3]) });

  const forA = queue.getForInbox("inbox:a");
  assert.equal(forA.length, 2);
  const forB = queue.getForInbox("inbox:b");
  assert.equal(forB.length, 1);
  const forC = queue.getForInbox("inbox:c");
  assert.equal(forC.length, 0);
});

test("PersistentOutboundQueue pruneExpired removes old entries", async () => {
  let now = 1000;
  const ttlMs = 5000;
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV(), ttlMs, nowMs: () => now });
  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  assert.equal(queue.size(), 1);

  now = 7000; // past TTL
  const pruned = await queue.pruneExpired();
  assert.equal(pruned, 1);
  assert.equal(queue.size(), 0);
});

test("PersistentOutboundQueue respects maxPerInbox", async () => {
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV(), maxPerInbox: 2, maxTotal: 100 });
  for (let i = 0; i < 5; i += 1) {
    await queue.enqueue({ deliverInboxId: "inbox:cap", innerBytes: new Uint8Array([i]) });
  }
  assert.equal(queue.sizeForInbox("inbox:cap"), 2, "should drop oldest and keep maxPerInbox");
  assert.equal(queue.size(), 2);
});

test("PersistentOutboundQueue respects maxTotal", async () => {
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV(), maxPerInbox: 10, maxTotal: 3 });
  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([2]) });
  await queue.enqueue({ deliverInboxId: "inbox:b", innerBytes: new Uint8Array([3]) });
  await queue.enqueue({ deliverInboxId: "inbox:c", innerBytes: new Uint8Array([4]) });
  assert.equal(queue.size(), 3, "should cap total and drop oldest across inboxes");
});

test("PersistentOutboundQueue loadAll restores from KV", async () => {
  const kv = new MemoryKV();
  let now = 1000;
  const queue1 = new PersistentOutboundQueue({ keyValueStore: kv, nowMs: () => now });
  await queue1.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  await queue1.enqueue({ deliverInboxId: "inbox:b", innerBytes: new Uint8Array([2]) });
  assert.equal(queue1.size(), 2);

  // Create a new queue pointing to the same KV store — simulates restart
  const queue2 = new PersistentOutboundQueue({ keyValueStore: kv, nowMs: () => now });
  assert.equal(queue2.size(), 0, "before loadAll, queue is empty");
  await queue2.loadAll();
  assert.equal(queue2.size(), 2, "after loadAll, entries are restored");
  assert.equal(queue2.sizeForInbox("inbox:a"), 1);
  assert.equal(queue2.sizeForInbox("inbox:b"), 1);
});

test("PersistentOutboundQueue status change callback", async () => {
  const statuses = [];
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV() });
  queue.setOnStatusChange((queueId, status, entry) => {
    statuses.push({ queueId, status, deliverInboxId: entry.deliverInboxId });
  });

  const entry = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].status, "queued");

  await queue.markDelivered(entry.queueId);
  assert.equal(statuses.length, 2);
  assert.equal(statuses[1].status, "delivered");
});

// --- RetryScheduler tests ---

test("RetryScheduler flushForInbox attempts delivery", async () => {
  const kv = new MemoryKV();
  const queue = new PersistentOutboundQueue({ keyValueStore: kv });
  await queue.enqueue({ deliverInboxId: "inbox:flush", innerBytes: new Uint8Array([1]) });
  await queue.enqueue({ deliverInboxId: "inbox:flush", innerBytes: new Uint8Array([2]) });

  const sent = [];
  const scheduler = new RetryScheduler({
    queue,
    sendFn: async (entry) => { sent.push(entry.deliverInboxId); },
  });

  await scheduler.flushForInbox("inbox:flush");
  assert.equal(sent.length, 2);
  assert.equal(queue.size(), 0, "delivered entries should be removed");
});

test("RetryScheduler records failure on send error", async () => {
  const kv = new MemoryKV();
  let now = 1000;
  const queue = new PersistentOutboundQueue({ keyValueStore: kv, nowMs: () => now });
  const entry = await queue.enqueue({ deliverInboxId: "inbox:fail", innerBytes: new Uint8Array([1]) });

  const scheduler = new RetryScheduler({
    queue,
    sendFn: async () => { throw new Error("send failed"); },
  });

  await scheduler.flushForInbox("inbox:fail");
  // Entry should still be in the queue with incremented attempts
  assert.equal(queue.size(), 1, "entry should remain after failure");
  const entries = queue.getForInbox("inbox:fail");
  assert.equal(entries[0].attempts, 1);
});

test("PersistentOutboundQueue drops entry and emits expired after maxAttempts", async () => {
  let now = 1000;
  const statuses = [];
  const queue = new PersistentOutboundQueue({
    keyValueStore: new MemoryKV(),
    maxAttempts: 3,
    nowMs: () => now,
  });
  queue.setOnStatusChange((queueId, status) => { statuses.push(status); });

  const entry = await queue.enqueue({ deliverInboxId: "inbox:stuck", innerBytes: new Uint8Array([1]) });
  await queue.recordAttemptFailure(entry.queueId); // attempts -> 1
  await queue.recordAttemptFailure(entry.queueId); // attempts -> 2
  assert.equal(queue.size(), 1, "entry kept while under the cap");

  await queue.recordAttemptFailure(entry.queueId); // attempts -> 3 == cap, give up
  assert.equal(queue.size(), 0, "entry dropped once the attempt cap is reached");
  assert.equal(statuses[statuses.length - 1], "expired", "owner notified the entry was given up on");
});

test("RetryScheduler flushForInbox does not re-attempt entries in backoff (no flap amplification)", async () => {
  let now = 1000;
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV(), nowMs: () => now });
  await queue.enqueue({ deliverInboxId: "inbox:flap", innerBytes: new Uint8Array([1]) });

  let attempts = 0;
  const scheduler = new RetryScheduler({
    queue,
    sendFn: async () => { attempts += 1; throw new Error("no route"); },
  });

  // Fresh entry is due — first flush attempts it once, it fails -> backoff.
  await scheduler.flushForInbox("inbox:flap");
  assert.equal(attempts, 1);

  // Simulate a flapping route: repeated route-added flushes inside the backoff
  // window must NOT re-attempt (this is what amplified deposits before).
  await scheduler.flushForInbox("inbox:flap");
  await scheduler.flushForInbox("inbox:flap");
  await scheduler.flushForInbox("inbox:flap");
  assert.equal(attempts, 1, "entry in backoff must not be re-attempted on every flush");

  // Once the backoff window elapses, the entry becomes due again.
  now = 20_000;
  await scheduler.flushForInbox("inbox:flap");
  assert.equal(attempts, 2, "entry is attempted again only after backoff elapses");
});

// --- GatewayLoop integration tests ---

test("GatewayLoop commits a shared-cluster home before process-local route resolution", async () => {
  const deposited = [];
  let routeResolutions = 0;
  const loop = makeGatewayLoop({
    inboxStore: {
      async depositFromWire(inboxId, bytes) {
        deposited.push({ inboxId, bytes });
      },
    },
    isHostedHere: async (inboxId) => inboxId === "inbox:shared-home",
    routeResolver: {
      async resolve() {
        routeResolutions += 1;
        return null;
      },
    },
  });

  const bytes = new Uint8Array([4, 5, 6]);
  const result = await loop.sendToInbox({
    innerBytes: bytes,
    deliverInboxId: "inbox:shared-home",
  });

  assert.equal(result.local, true);
  assert.equal(routeResolutions, 0, "shared-home delivery must not depend on a process-local route");
  assert.equal(deposited.length, 1);
  assert.equal(deposited[0].inboxId, "inbox:shared-home");
  assert.equal(deposited[0].bytes, bytes);
});

test("GatewayLoop preserves WAN routing for an inbox not hosted by the cluster", async () => {
  let routeResolutions = 0;
  const loop = makeGatewayLoop({
    inboxStore: { async depositFromWire() { throw new Error("must not deposit locally"); } },
    isHostedHere: async () => false,
    routeResolver: {
      async resolve() {
        routeResolutions += 1;
        return null;
      },
    },
  });

  await assert.rejects(
    () => loop.sendToInbox({ innerBytes: new Uint8Array([1]), deliverInboxId: "inbox:foreign" }),
    RoutingFailedError,
  );
  assert.equal(routeResolutions, 1);
});

test("GatewayLoop force-onion policy bypasses the shared-home shortcut", async () => {
  let hostedChecks = 0;
  let deposits = 0;
  const loop = makeGatewayLoop({
    inboxStore: { async depositFromWire() { deposits += 1; } },
    isHostedHere: async () => { hostedChecks += 1; return true; },
    routePolicy: { forceOnionRouting: true },
    routeResolver: { async resolve() { return null; } },
  });

  await assert.rejects(
    () => loop.sendToInbox({ innerBytes: new Uint8Array([1]), deliverInboxId: "inbox:shared-home" }),
    RoutingFailedError,
  );
  assert.equal(hostedChecks, 0);
  assert.equal(deposits, 0);
});

test("GatewayLoop sendToInbox enqueues on RoutingFailedError when outboundQueue present", async () => {
  const inboxRouter = new InboxRouter();
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV() });
  const loop = makeGatewayLoop({ inboxRouter, outboundQueue: queue });

  await assert.rejects(
    () => loop.sendToInbox({
      innerBytes: new Uint8Array([1, 2, 3]),
      deliverInboxId: "inbox:no-route",
    }),
    (err) => err instanceof RoutingFailedError && err.reason === "no route to target"
  );

  assert.equal(queue.sizeForInbox("inbox:no-route"), 1);
  assert.equal(queue.size(), 1);
});

test("GatewayLoop without outboundQueue still throws on RoutingFailedError", async () => {
  const loop = makeGatewayLoop({ inboxRouter: new InboxRouter() });
  await assert.rejects(
    () => loop.sendToInbox({
      innerBytes: new Uint8Array([1]),
      deliverInboxId: "inbox:none",
    }),
    RoutingFailedError
  );
});

test("GatewayLoop throws RoutingFailedError when no local route (no broadcast fallback)", async () => {
  const sender = new GatewaySender({ pool: {} });
  const loop = makeGatewayLoop({
    inboxRouter: new InboxRouter(),
    sender,
  });
  await assert.rejects(
    () => loop.sendToInbox({
      innerBytes: new Uint8Array([1, 2, 3]),
      deliverInboxId: "inbox:unknown",
    }),
    RoutingFailedError,
  );
});

test("GatewayLoop does NOT forward deposit when forceOnionRouting and no route", async () => {
  const forwarded = [];
  const mockPool = {
    sendDepositToAllConnections(deliverInboxId, innerBytes) {
      forwarded.push({ deliverInboxId });
      return Promise.resolve();
    },
  };
  const sender = new GatewaySender({ pool: mockPool });
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryKV() });
  const loop = makeGatewayLoop({
    inboxRouter: new InboxRouter(),
    sender,
    outboundQueue: queue,
    routePolicy: { forceOnionRouting: true },
  });

  await assert.rejects(
    () => loop.sendToInbox({
      innerBytes: new Uint8Array([7, 8, 9]),
      deliverInboxId: "inbox:onion-target",
    }),
    (err) => err instanceof RoutingFailedError && err.reason === "no route to target",
  );

  assert.equal(forwarded.length, 0, "broadcast fallback must be skipped when forceOnionRouting");
  assert.equal(queue.sizeForInbox("inbox:onion-target"), 1, "message should be enqueued for later delivery");
});

// --- InboxRouter route-added callback (still tested here for integration coverage) ---

test("InboxRouter setOnRouteAdded is invoked when registerLocal adds routes", () => {
  const router = new InboxRouter();
  const added = [];
  router.setOnRouteAdded((inboxIds) => { added.push(...inboxIds); });

  router.registerLocal(["inbox:one", "inbox:two"], null);
  assert.deepEqual(added, ["inbox:one", "inbox:two"]);

  added.length = 0;
  router.registerLocal(["inbox:three"], null);
  assert.deepEqual(added, ["inbox:three"]);
});

test("InboxRouter setOnRouteAdded is invoked when addRemoteRoute adds route", () => {
  const router = new InboxRouter();
  const added = [];
  router.setOnRouteAdded((inboxIds) => { added.push(...inboxIds); });

  router.addRemoteRoute("inbox:remote", {
    hops: 1,
    nextHopRelayKeyId: "relay-remote",
    deliveryRelayKeyId: "relay-remote",
  });
  assert.deepEqual(added, ["inbox:remote"]);
});
