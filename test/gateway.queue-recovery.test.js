import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { FsStorageProvider } from "../src/storage/fs/FsStorageProvider.js";
import { PersistentOutboundQueue } from "../src/gateway/PersistentOutboundQueue.js";

test("concurrent queue admission cannot exceed either capacity or evict accepted packets", async () => {
  const kv = new MemoryStorageProvider().getKeyValueStore(null);
  const queue = new PersistentOutboundQueue({ keyValueStore: kv, maxPerInbox: 2, maxTotal: 3 });
  const results = await Promise.allSettled(Array.from({ length: 12 }, (_, i) => queue.enqueue({
    deliverInboxId: i < 6 ? "inbox:a" : "inbox:b", innerBytes: new Uint8Array([i]),
  })));
  assert.equal(results.filter((result) => result.status === "fulfilled").length, 3);
  for (const result of results.filter((result) => result.status === "rejected")) {
    assert.equal(result.reason.code, "RESOURCE_LIMIT");
    assert.equal(result.reason.retryable, true);
  }
  const restored = new PersistentOutboundQueue({ keyValueStore: kv });
  await restored.loadAll();
  assert.equal(restored.size(), 3);
  assert.equal(restored.sizeForInbox("inbox:a"), 2);
});

test("a failed enqueue never claims custody and does not poison later admission", async () => {
  const kv = new MemoryStorageProvider().getKeyValueStore(null);
  const save = kv.set.bind(kv);
  kv.set = async () => { throw new Error("disk unavailable"); };
  const queue = new PersistentOutboundQueue({ keyValueStore: kv });
  await assert.rejects(() => queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) }));
  assert.equal(queue.size(), 0);
  kv.set = save;
  await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([2]) });
  assert.equal(queue.size(), 1);
});

test("a failure update racing completion cannot resurrect a durably removed packet", async () => {
  const kv = new MemoryStorageProvider().getKeyValueStore(null);
  const queue = new PersistentOutboundQueue({ keyValueStore: kv });
  const entry = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: new Uint8Array([1]) });
  const deleting = Promise.withResolvers();
  const gate = Promise.withResolvers();
  const remove = kv.delete.bind(kv);
  kv.delete = async (key) => { deleting.resolve(); await gate.promise; return remove(key); };
  const completion = queue.markDelivered(entry.queueId);
  await deleting.promise;
  const failure = queue.recordAttemptFailure(entry.queueId);
  gate.resolve();
  await Promise.all([completion, failure]);
  const restored = new PersistentOutboundQueue({ keyValueStore: kv });
  await restored.loadAll();
  assert.equal(queue.size(), 0);
  assert.equal(restored.size(), 0);
});

test("accepted encrypted packets and backoff survive a filesystem restart", async (t) => {
  const rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-queue-recovery-"));
  t.after(() => fs.rm(rootDir, { recursive: true, force: true }));
  const encryptionKey = new Uint8Array(32).fill(9);
  let now = 1000;
  const queue = new PersistentOutboundQueue({ keyValueStore: new FsStorageProvider({ rootDir, encryptionKey }).getKeyValueStore(), nowMs: () => now });
  const packet = new Uint8Array([10, 20, 30]);
  const entry = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: packet });
  await queue.recordAttemptFailure(entry.queueId);
  const restored = new PersistentOutboundQueue({ keyValueStore: new FsStorageProvider({ rootDir, encryptionKey }).getKeyValueStore(), nowMs: () => now });
  await restored.loadAll();
  assert.deepEqual(restored.getForInbox("inbox:a")[0].innerBytes, packet);
  assert.equal(restored.getRetryable().length, 0);
  now = 20_000;
  assert.equal(restored.getRetryable().length, 1);
  await restored.markDelivered(entry.queueId);
  const third = new PersistentOutboundQueue({ keyValueStore: new FsStorageProvider({ rootDir, encryptionKey }).getKeyValueStore() });
  await third.loadAll();
  assert.equal(third.size(), 0);
});

test("SIGKILL after durable queue admission preserves exact ciphertext and backoff", async (t) => {
  const rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-queue-kill-"));
  const source = `
    import { FsStorageProvider } from ${JSON.stringify(new URL("../src/storage/fs/FsStorageProvider.js", import.meta.url).href)};
    import { PersistentOutboundQueue } from ${JSON.stringify(new URL("../src/gateway/PersistentOutboundQueue.js", import.meta.url).href)};
    const provider = new FsStorageProvider({ rootDir: ${JSON.stringify(rootDir)}, encryptionKey: new Uint8Array(32).fill(9) });
    const queue = new PersistentOutboundQueue({ keyValueStore: provider.getKeyValueStore(), nowMs: () => 1000 });
    const entry = await queue.enqueue({ deliverInboxId: 'inbox:crash', innerBytes: new Uint8Array([4, 5, 6]) });
    await queue.recordAttemptFailure(entry.queueId);
    console.log('COMMITTED');
    setInterval(() => {}, 1000);
  `;
  const child = spawn(process.execPath, ["--input-type=module", "--eval", source], { stdio: ["ignore", "pipe", "pipe"] });
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); await fs.rm(rootDir, { recursive: true, force: true }); });
  const [ready] = await once(child.stdout, "data");
  assert.match(ready.toString(), /COMMITTED/);
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  const [code, signal] = await exited;
  assert.equal(signal, "SIGKILL");
  assert.equal(code, null);
  const restored = new PersistentOutboundQueue({ keyValueStore: new FsStorageProvider({ rootDir, encryptionKey: new Uint8Array(32).fill(9) }).getKeyValueStore(), nowMs: () => 1000 });
  await restored.loadAll();
  assert.deepEqual(restored.getForInbox("inbox:crash")[0].innerBytes, new Uint8Array([4, 5, 6]));
  assert.equal(restored.getForInbox("inbox:crash")[0].attempts, 1);
  assert.equal(restored.getRetryable().length, 0);
});
