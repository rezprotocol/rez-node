import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { FsKeyValueStore } from "../src/storage/fs/FsKeyValueStore.js";
import { KeyValueUnreadableError } from "@rezprotocol/core";

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

test("a corrupt hashed file does NOT wedge keys() — it is skipped, the rest still enumerate", async () => {
  await withStore(async (store, dir) => {
    await store.set(LONG_KEY, "sid");
    await store.set("peer-link:sessions:by-peer-link:rez:acct:x::pl_y", "sid2");
    // Corrupt an unrelated hashed file (write a second over-long key, then truncate it).
    const otherLong = LONG_KEY.replace("d5080d4", "ffffff0");
    await store.set(otherLong, "sid3");
    const files = await fs.readdir(path.join(dir, "kv"));
    const hashed = files.filter((f) => f.startsWith("h."));
    assert.ok(hashed.length >= 2);
    await fs.writeFile(path.join(dir, "kv", hashed[0]), "{ this is not json", "utf8");

    // keys() must still return the good keys, not throw.
    const all = await store.keys("peer-link:sessions:");
    assert.ok(all.includes("peer-link:sessions:by-peer-link:rez:acct:x::pl_y"), "short key still enumerates");
    // Exactly one of the two long keys survives (the corrupted one is skipped).
    const longs = all.filter((k) => k.startsWith("peer-link:sessions:by-peer-link-device:"));
    assert.equal(longs.length, 1, "the corrupt hashed file is skipped, not fatal: " + JSON.stringify(all));
  });
});

test("get() on a corrupt hashed file returns undefined (treats as absent), not throw", async () => {
  await withStore(async (store, dir) => {
    await store.set(LONG_KEY, "sid");
    const files = await fs.readdir(path.join(dir, "kv"));
    const hashed = files.find((f) => f.startsWith("h."));
    await fs.writeFile(path.join(dir, "kv", hashed), "torn", "utf8");
    assert.equal(await store.get(LONG_KEY), undefined, "corrupt hashed file → key reads as absent");
  });
});

test("getStrict() distinguishes a corrupt record from an absent key", async () => {
  await withStore(async (store, dir) => {
    assert.equal(await store.getStrict("absent"), undefined);
    await store.set("present", { ok: true });
    const file = Buffer.from("present", "utf8").toString("base64url") + ".json";
    await fs.writeFile(path.join(dir, "kv", file), "torn", "utf8");
    await assert.rejects(
      () => store.getStrict("present"),
      (err) => err instanceof KeyValueUnreadableError
        && err.code === "KEY_VALUE_UNREADABLE"
        && err.key === "present",
    );
    assert.equal(await store.get("present"), undefined, "legacy permissive read remains unchanged");
  });
});

test("getStrict() rejects a corrupt hashed wrapper with the original logical key", async () => {
  await withStore(async (store, dir) => {
    await store.set(LONG_KEY, "sid");
    const files = await fs.readdir(path.join(dir, "kv"));
    const hashed = files.find((file) => file.startsWith("h."));
    await fs.writeFile(path.join(dir, "kv", hashed), JSON.stringify({ wrong: true }), "utf8");
    await assert.rejects(
      () => store.getStrict(LONG_KEY),
      (err) => err instanceof KeyValueUnreadableError && err.key === LONG_KEY,
    );
  });
});

test("getStrict() wraps filesystem read faults instead of misclassifying absence", async () => {
  const cause = new Error("injected read failure");
  cause.code = "EIO";
  const store = new FsKeyValueStore({
    rootDir: "/unused",
    fsImpl: {
      async readFile() { throw cause; },
    },
  });
  await assert.rejects(
    () => store.getStrict("faulted"),
    (err) => err instanceof KeyValueUnreadableError
      && err.key === "faulted"
      && err.cause === cause,
  );
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
