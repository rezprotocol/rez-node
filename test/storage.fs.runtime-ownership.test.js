import { fork } from "node:child_process";
import { once } from "node:events";
import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { DeliveryCommitStore } from "@rezprotocol/sdk/delivery";
import { FsKeyValueStore } from "../src/storage/fs/FsKeyValueStore.js";
import { FsStorageProvider } from "../src/storage/fs/FsStorageProvider.js";

async function withTempDir(fn) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-runtime-lock-"));
  try {
    await fn(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

test("filesystem runtime ownership excludes a second live runtime and increments epoch after release", async () => {
  await withTempDir(async (rootDir) => {
    const first = new FsStorageProvider({ rootDir });
    const grant1 = await first.acquireRuntimeOwnership({ namespace: "sdk-delivery" });
    assert.equal(grant1.runtimeEpoch, 1);

    const second = new FsStorageProvider({ rootDir });
    await assert.rejects(
      () => second.acquireRuntimeOwnership({ namespace: "sdk-delivery" }),
      (err) => err && err.code === "DELIVERY_RUNTIME_ALREADY_ACTIVE",
    );

    await grant1.release();
    const grant2 = await second.acquireRuntimeOwnership({ namespace: "sdk-delivery" });
    assert.equal(grant2.runtimeEpoch, 2, "a rejected provider retries after the live owner releases");
    await grant2.release();

    const reacquired = await first.acquireRuntimeOwnership({ namespace: "sdk-delivery" });
    assert.equal(reacquired.runtimeEpoch, 3, "a cleanly released provider can acquire a fresh fenced epoch");
    await reacquired.release();
  });
});

test("filesystem runtime ownership removes a provably dead stale lock", async () => {
  await withTempDir(async (rootDir) => {
    const lockPath = path.join(rootDir, ".rez-sdk-delivery.lock");
    await fs.writeFile(lockPath, JSON.stringify({
      namespace: "sdk-delivery",
      pid: 2_147_483_647,
      processStartTime: "Mon Jan  1 00:00:00 1900",
      runtimeEpoch: 1,
      acquiredAtMs: 1,
    }));
    const provider = new FsStorageProvider({ rootDir });
    const grant = await provider.acquireRuntimeOwnership({ namespace: "sdk-delivery" });
    assert.equal(grant.runtimeEpoch, 1);
    await grant.release();
  });
});

test("delivery commit keys remain enumerable when their filesystem value is corrupt", async () => {
  await withTempDir(async (rootDir) => {
    const store = new FsKeyValueStore({ rootDir });
    const key = DeliveryCommitStore.commitKey("rez:acct:layout-owner", "ab".repeat(32));
    assert.equal(store._isKeyHashed(key), false);
    await store.set(key, { ok: true });
    await fs.writeFile(store._pathForKey(key), "{broken-json");
    assert.deepEqual(await store.keys("sdk:delivery:commit:v1:"), [key]);
    await assert.rejects(() => store.getStrict(key), (err) => err && err.code === "KEY_VALUE_UNREADABLE");
  });
});

function startOwner(rootDir) {
  const child = fork(new URL("../scripts/test-support/runtime-lock-child.mjs", import.meta.url), [rootDir], { stdio: ["ignore", "ignore", "inherit", "ipc"] });
  const reply = once(child, "message").then(([message]) => message);
  const exited = once(child, "exit");
  return { child, reply, exited };
}

test("two competing processes cannot take ownership together; crash releases the kernel lock", async () => {
  await withTempDir(async rootDir => {
    const first = startOwner(rootDir), second = startOwner(rootDir);
    let winners = 0;
    try {
      const messages = await Promise.all([first.reply, second.reply]);
      winners = messages.filter(m => m.ready).length;
      assert.ok(winners <= 1, JSON.stringify(messages));
      // Contending zero-timeout acquisitions may both refuse. Refusal is safe
      // and retryable; admitting two writers would corrupt ratchets.
      assert.equal(messages.filter(m => m.error === "DELIVERY_RUNTIME_ALREADY_ACTIVE").length, 2 - winners, JSON.stringify(messages));
    } finally {
      first.child.kill("SIGKILL"); second.child.kill("SIGKILL");
      await Promise.all([first.exited, second.exited]);
    }
    const replacement = new FsStorageProvider({ rootDir });
    const grant = await replacement.acquireRuntimeOwnership();
    assert.equal(grant.runtimeEpoch, winners + 1);
    await grant.release();
    assert.throws(() => grant.assertActive(), /released/);
  });
});

test("a suspended writer retains ownership and can resume without a competing writer", { skip: process.platform === "win32" ? "Windows has no SIGSTOP; exclusion/crash tests above still run" : false }, async () => {
  await withTempDir(async rootDir => {
    const owner = startOwner(rootDir);
    try {
      assert.equal((await owner.reply).ready, true);
      owner.child.kill("SIGSTOP");
      const challenger = new FsStorageProvider({ rootDir });
      await assert.rejects(challenger.acquireRuntimeOwnership(), { code: "DELIVERY_RUNTIME_ALREADY_ACTIVE" });
      owner.child.kill("SIGCONT");
      await assert.rejects(challenger.acquireRuntimeOwnership(), { code: "DELIVERY_RUNTIME_ALREADY_ACTIVE" });
      owner.child.send("release"); await owner.exited;
      const grant = await challenger.acquireRuntimeOwnership(); assert.equal(grant.runtimeEpoch, 2); await grant.release();
    } finally { owner.child.kill("SIGCONT"); owner.child.kill("SIGKILL"); await owner.exited; }
  });
});

test("an epoch write failure releases ownership for the next runtime", async () => {
  await withTempDir(async rootDir => {
    const failed = new FsStorageProvider({ rootDir });
    failed.keyValueStore.set = async () => { throw new Error("disk failure"); };
    await assert.rejects(failed.acquireRuntimeOwnership(), /disk failure/);
    const grant = await new FsStorageProvider({ rootDir }).acquireRuntimeOwnership();
    assert.equal(grant.runtimeEpoch, 1); await grant.release();
  });
});

test("abrupt process death permits exactly one new runtime epoch", async () => {
  await withTempDir(async rootDir => {
    const owner = startOwner(rootDir);
    try { assert.equal((await owner.reply).ready, true); }
    finally { owner.child.kill("SIGKILL"); await owner.exited; }
    const grant = await new FsStorageProvider({ rootDir }).acquireRuntimeOwnership();
    assert.equal(grant.runtimeEpoch, 2); await grant.release();
  });
});
