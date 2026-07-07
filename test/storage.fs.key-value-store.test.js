import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { FsKeyValueStore } from "../src/storage/fs/FsKeyValueStore.js";

// A key whose base64url basename exceeds the on-disk length bound. The real
// trigger is the S2.5 per-device session index (owner + peerLinkId + a 64-hex
// deviceId); reproduce its shape so a regression re-surfaces the exact ENAMETOOLONG.
const LONG_KEY =
  "peer-link:sessions:by-peer-link-device:"
  + "rez:acct:wb2wyvkuys55mtadxzt5oae5ujwuoaai4q4vggy56fpeypu6ge6q"
  + "::pl_oBza-xI0K6DykN3k4QGHSdzB"
  + "::rez:dev:98dcf7865363ae01405da6647ff21475fe968e8c448b92b132739a951d5080d4";

async function withStore(fn) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-"));
  try {
    await fn(new FsKeyValueStore({ rootDir: dir }), dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

test("short keys round-trip in the historical base64url filename (backward compatible)", async () => {
  await withStore(async (store, dir) => {
    await store.set("peer-link:sessions:by-peer-link:rez:acct:short::pl_abc", { sessionId: "s1" });
    assert.deepEqual(await store.get("peer-link:sessions:by-peer-link:rez:acct:short::pl_abc"), { sessionId: "s1" });
    // The on-disk name is the plain base64url of the key (no hashing) — unchanged.
    const files = await fs.readdir(path.join(dir, "kv"));
    const expected = Buffer.from("peer-link:sessions:by-peer-link:rez:acct:short::pl_abc", "utf8").toString("base64url") + ".json";
    assert.ok(files.includes(expected), "short key keeps its base64url filename: " + files.join(","));
  });
});

test("an over-long key round-trips via a hashed filename (was ENAMETOOLONG)", async () => {
  await withStore(async (store, dir) => {
    await store.set(LONG_KEY, "session-id-123");
    assert.equal(await store.get(LONG_KEY), "session-id-123");
    // The filename is bounded (h.<sha256hex>.json), well under the FS limit.
    const files = await fs.readdir(path.join(dir, "kv"));
    assert.equal(files.length, 1);
    assert.ok(files[0].startsWith("h."), "hashed name marker: " + files[0]);
    assert.ok(files[0].length < 80, "hashed filename is bounded: " + files[0].length);
  });
});

test("keys(prefix) enumerates a hashed (over-long) key by recovering it from the file", async () => {
  await withStore(async (store) => {
    await store.set(LONG_KEY, "sid");
    await store.set("peer-link:sessions:by-peer-link:rez:acct:x::pl_y", "sid2");
    await store.set("unrelated:key", 1);

    const byDevice = await store.keys("peer-link:sessions:by-peer-link-device:");
    assert.deepEqual(byDevice, [LONG_KEY], "the hashed long key is enumerable by its real prefix");

    const allSessions = await store.keys("peer-link:sessions:");
    assert.equal(allSessions.length, 2, "both session keys enumerate together");
    assert.ok(allSessions.includes(LONG_KEY));
  });
});

test("delete removes a hashed (over-long) key", async () => {
  await withStore(async (store) => {
    await store.set(LONG_KEY, "sid");
    assert.equal(await store.delete(LONG_KEY), true);
    assert.equal(await store.get(LONG_KEY), undefined);
    assert.deepEqual(await store.keys("peer-link:"), []);
  });
});

test("overwriting a hashed key bumps the stored value, not a duplicate file", async () => {
  await withStore(async (store, dir) => {
    await store.set(LONG_KEY, "v1");
    await store.set(LONG_KEY, "v2");
    assert.equal(await store.get(LONG_KEY), "v2");
    const files = await fs.readdir(path.join(dir, "kv"));
    assert.equal(files.length, 1, "one file per key, not one per write");
  });
});
