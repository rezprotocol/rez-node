import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { HostedInboxRegistry } from "../src/app/HostedInboxRegistry.js";
import { PersistentOutboundQueue } from "../src/gateway/PersistentOutboundQueue.js";
import { DepositPolicyStore } from "../src/inbox/DepositPolicyStore.js";
import { DepositRateLimitStore } from "../src/inbox/DepositRateLimitStore.js";

test("security audit: malformed deposit policy state fails closed and can retry cleanly", async () => {
  const storageProvider = new MemoryStorageProvider();
  const kv = storageProvider.getKeyValueStore(null);
  await kv.set("node:deposit-policy:v1", { policies: [{ inboxId: "inbox:partial" }, null] });
  const store = new DepositPolicyStore({ storageProvider });

  await assert.rejects(() => store.hydrate(), /durable policy is malformed/);
  assert.throws(() => store.get("inbox:partial"), /before hydrate/);

  await kv.set("node:deposit-policy:v1", { policies: [] });
  await store.hydrate();
  assert.equal(store.get("inbox:partial"), null);
});

test("security audit: malformed rate-limit rows fail closed without partial cache state", async () => {
  const storageProvider = new MemoryStorageProvider();
  const kv = storageProvider.getKeyValueStore(null);
  const validKey = "node:deposit-ratelimit:v1:pk:alice|inbox:one";
  const corruptKey = "node:deposit-ratelimit:v1:pk:mallory|inbox:one";
  await kv.set(validKey, { timestamps: [Date.now()] });
  await kv.set(corruptKey, { timestamps: ["not-a-time"] });
  const store = new DepositRateLimitStore({ storageProvider, maxDeposits: 1 });

  await assert.rejects(() => store.hydrate(), /durable timestamps are malformed/);
  await assert.rejects(
    () => store.record({ depositorPubkeyB64: "alice", mailboxId: "inbox:one", nowMs: Date.now() }),
    /before hydrate/,
  );

  await kv.delete(corruptKey);
  await store.hydrate();
  assert.equal(
    await store.record({ depositorPubkeyB64: "alice", mailboxId: "inbox:one", nowMs: Date.now() }),
    false,
  );
});

test("security audit: malformed hosted routing snapshot fails closed without partial routes", async () => {
  const storageProvider = new MemoryStorageProvider();
  const kv = storageProvider.getKeyValueStore(null);
  await kv.set("substrate:hostedInboxRegistry:v2", {
    claimantDelegations: [["claimant-a", { inboxId: "inbox:a" }], null],
  });
  const registry = new HostedInboxRegistry({ storageProvider });

  await assert.rejects(() => registry.hydrate(), /durable entry is malformed/);
  assert.deepEqual(registry.getInboxIds(), []);

  await kv.set("substrate:hostedInboxRegistry:v2", { claimantDelegations: [] });
  await registry.hydrate();
  assert.deepEqual(registry.getInboxIds(), []);
});

test("security audit: malformed outbound queue state is retained and never partially admitted", async () => {
  const storageProvider = new MemoryStorageProvider();
  const kv = storageProvider.getKeyValueStore(null);
  const corruptKey = "outbound:queue:corrupt";
  await kv.set(corruptKey, { queueId: "corrupt" });
  const queue = new PersistentOutboundQueue({ keyValueStore: kv });

  await assert.rejects(() => queue.loadAll(), /durable entry is malformed/);
  assert.equal(queue.size(), 0);
  assert.deepEqual(await kv.getStrict(corruptKey), { queueId: "corrupt" });

  await kv.delete(corruptKey);
  await queue.loadAll();
  assert.equal(queue.size(), 0);
});

test("security audit: outbound queue key and authenticated record identity cannot diverge", async () => {
  const storageProvider = new MemoryStorageProvider();
  const kv = storageProvider.getKeyValueStore(null);
  const writer = new PersistentOutboundQueue({ keyValueStore: kv, nowMs: () => 1_000 });
  const entry = await writer.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  const correctKey = "outbound:queue:" + entry.queueId;
  const durable = await kv.getStrict(correctKey);
  await kv.delete(correctKey);
  await kv.set("outbound:queue:different-id", durable);

  const reader = new PersistentOutboundQueue({ keyValueStore: kv, nowMs: () => 1_000 });
  await assert.rejects(
    () => reader.loadAll(),
    (err) => err && err.cause && /queue key does not match queueId/.test(err.cause.message),
  );
  assert.equal(reader.size(), 0);
});
