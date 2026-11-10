import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { PersistentOutboundQueue } from "../src/gateway/PersistentOutboundQueue.js";
import { RetryScheduler } from "../src/gateway/RetryScheduler.js";

const run = promisify(execFile);
const queueModule = new URL("../src/gateway/PersistentOutboundQueue.js", import.meta.url).href;
const schedulerModule = new URL("../src/gateway/RetryScheduler.js", import.meta.url).href;
const coreModule = new URL("../../rez-core/src/node.js", import.meta.url).href;
const turn = () => new Promise((resolve) => setImmediate(resolve));

test("concurrent route flushes share one send and one durable completion", async () => {
  const kv = new MemoryStorageProvider().getKeyValueStore(null);
  const queue = new PersistentOutboundQueue({ keyValueStore: kv });
  await queue.enqueue({ deliverInboxId: "inbox:concurrent", innerBytes: new Uint8Array([1]) });
  const gate = Promise.withResolvers();
  const completing = Promise.withResolvers();
  const completionGate = Promise.withResolvers();
  const deleteRecord = kv.delete.bind(kv);
  kv.delete = async (key) => {
    completing.resolve();
    await completionGate.promise;
    return deleteRecord(key);
  };
  let sends = 0;
  let completions = 0;
  queue.setOnStatusChange((queueId, status) => {
    if (status === "delivered") completions += 1;
  });
  const scheduler = new RetryScheduler({ queue, sendFn: async () => { sends += 1; await gate.promise; } });
  const flushes = [scheduler.flushForInbox("inbox:concurrent"), scheduler.flushForInbox("inbox:concurrent")];
  try {
    await turn();
    assert.equal(sends, 1);
    assert.equal(queue.size(), 1, "in-flight work remains durable until completion");
    gate.resolve();
    await completing.promise;
    flushes.push(scheduler.flushForInbox("inbox:concurrent"));
    await turn();
    assert.equal(sends, 1, "the guard also covers the durable completion write");
  } finally {
    gate.resolve();
    completionGate.resolve();
    await Promise.all(flushes);
  }
  assert.equal(completions, 1);
  assert.equal(queue.size(), 0);
});

test("periodic polling and a route flush share the same active attempt", async () => {
  const queue = new PersistentOutboundQueue({ keyValueStore: new MemoryStorageProvider().getKeyValueStore(null) });
  await queue.enqueue({ deliverInboxId: "inbox:poll-and-route", innerBytes: new Uint8Array([1]) });
  const gate = Promise.withResolvers();
  let sends = 0;
  const scheduler = new RetryScheduler({ queue, pollIntervalMs: 5, sendFn: async () => {
    sends += 1;
    await gate.promise;
  } });
  let flush;
  scheduler.start();
  try {
    await new Promise((resolve) => setTimeout(resolve, 25));
    assert.equal(sends, 1);
    flush = scheduler.flushForInbox("inbox:poll-and-route");
    await turn();
    assert.equal(sends, 1);
  } finally {
    gate.resolve();
    if (flush) await flush;
    scheduler.stop();
  }
  assert.equal(queue.size(), 0);
});

test("concurrent failed retries charge one attempt and release the guard after backoff", async () => {
  let now = 1000;
  const queue = new PersistentOutboundQueue({
    keyValueStore: new MemoryStorageProvider().getKeyValueStore(null), nowMs: () => now,
  });
  await queue.enqueue({ deliverInboxId: "inbox:backoff", innerBytes: new Uint8Array([1]) });
  const gate = Promise.withResolvers();
  let sends = 0;
  const scheduler = new RetryScheduler({ queue, sendFn: async () => {
    sends += 1;
    await gate.promise;
    throw new Error("route unavailable");
  } });
  const flushes = [scheduler.flushForInbox("inbox:backoff"), scheduler.flushForInbox("inbox:backoff")];
  await turn();
  gate.resolve();
  await Promise.all(flushes);
  assert.equal(sends, 1);
  assert.equal(queue.getForInbox("inbox:backoff")[0].attempts, 1);
  await scheduler.flushForInbox("inbox:backoff");
  assert.equal(sends, 1);
  now = 20_000;
  await scheduler.flushForInbox("inbox:backoff");
  assert.equal(sends, 2);
  assert.equal(queue.getForInbox("inbox:backoff")[0].attempts, 2);
});

test("a retry snapshot cannot send an entry that entered backoff while awaiting another send", async () => {
  const queue = new PersistentOutboundQueue({
    keyValueStore: new MemoryStorageProvider().getKeyValueStore(null), nowMs: () => 1000,
  });
  const first = await queue.enqueue({ deliverInboxId: "inbox:snapshot", innerBytes: new Uint8Array([1]) });
  const second = await queue.enqueue({ deliverInboxId: "inbox:snapshot", innerBytes: new Uint8Array([2]) });
  const gate = Promise.withResolvers();
  const sent = [];
  const scheduler = new RetryScheduler({ queue, sendFn: async (entry) => {
    sent.push(entry.queueId);
    if (entry.queueId === first.queueId) await gate.promise;
  } });
  const flush = scheduler.flushForInbox("inbox:snapshot");
  await turn();
  await queue.recordAttemptFailure(second.queueId);
  gate.resolve();
  await flush;
  assert.deepEqual(sent, [first.queueId]);
  assert.equal(queue.getForInbox("inbox:snapshot")[0].queueId, second.queueId);
});

test("background retry storage failure is reported without terminating the node or blocking other packets", async () => {
  const source = `
    import assert from 'node:assert/strict';
    import { MemoryStorageProvider } from ${JSON.stringify(coreModule)};
    import { PersistentOutboundQueue } from ${JSON.stringify(queueModule)};
    import { RetryScheduler } from ${JSON.stringify(schedulerModule)};
    const kv = new MemoryStorageProvider().getKeyValueStore(null);
    const queue = new PersistentOutboundQueue({ keyValueStore: kv });
    await queue.enqueue({ deliverInboxId: 'inbox:failed', innerBytes: new Uint8Array([1]) });
    await queue.enqueue({ deliverInboxId: 'inbox:healthy', innerBytes: new Uint8Array([2]) });
    kv.set = async () => { throw new Error('private storage detail'); };
    const completed = Promise.withResolvers();
    let healthySends = 0;
    const scheduler = new RetryScheduler({ queue, pollIntervalMs: 5, sendFn: async (entry) => {
      if (entry.deliverInboxId === 'inbox:failed') throw new Error('route unavailable');
      healthySends += 1;
      completed.resolve();
    } });
    const deadline = setTimeout(() => { throw new Error('healthy packet starved'); }, 1500);
    scheduler.start();
    await completed.promise;
    await new Promise(resolve => setImmediate(resolve));
    scheduler.stop();
    clearTimeout(deadline);
    assert.equal(healthySends, 1);
    assert.equal(queue.size(), 1);
    assert.equal(queue.getForInbox('inbox:failed')[0].attempts, 0);
    console.log('retained-and-continued');
  `;
  const result = await run(process.execPath, ["--unhandled-rejections=strict", "--input-type=module", "--eval", source], { timeout: 5000 });
  assert.match(result.stdout, /retained-and-continued/);
  assert.match(result.stderr, /RetryScheduler.*retry.*failed/i);
  assert.doesNotMatch(result.stderr, /private storage detail|inbox:failed/);
});

test("route-discovery flush rejection is reported at the background callback boundary", async () => {
  const source = `
    import { MemoryStorageProvider } from ${JSON.stringify(coreModule)};
    import { bootstrapMesh } from ${JSON.stringify(new URL("../src/gateway/MeshBootstrap.js", import.meta.url).href)};
    import { RelayStore } from ${JSON.stringify(new URL("../src/network/RelayStore.js", import.meta.url).href)};
    import { NodeMetrics } from ${JSON.stringify(new URL("../src/metrics/NodeMetrics.js", import.meta.url).href)};
    import { InboxRouter } from ${JSON.stringify(new URL("../src/relay/InboxRouter.js", import.meta.url).href)};
    import { createNodeTestIdentity } from ${JSON.stringify(new URL("./helpers/wsAuth.js", import.meta.url).href)};
    const router = new InboxRouter();
    const mesh = bootstrapMesh({ relayStore: new RelayStore(), metrics: new NodeMetrics(),
      identity: createNodeTestIdentity(), inboxRouter: router,
      keyValueStore: new MemoryStorageProvider().getKeyValueStore(null) });
    mesh.retryScheduler.flushForInbox = async () => { throw new Error('private storage detail'); };
    router.registerLocal(['inbox:private-route'], null);
    await new Promise(resolve => setImmediate(resolve));
    console.log('route-callback-survived');
  `;
  const result = await run(process.execPath, ["--unhandled-rejections=strict", "--input-type=module", "--eval", source], { timeout: 5000 });
  assert.match(result.stdout, /route-callback-survived/);
  assert.match(result.stderr, /retry.*failed/i);
  assert.doesNotMatch(result.stderr, /private storage detail|inbox:private-route/);
});
