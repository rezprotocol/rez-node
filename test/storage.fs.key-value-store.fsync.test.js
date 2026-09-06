import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { FsKeyValueStore } from "../src/storage/fs/FsKeyValueStore.js";

// DT-009: fsync hardening. writeAtomic must fsync (1) the temp FILE
// before the rename and (2) the containing DIRECTORY after the rename;
// delete() must fsync the directory after the unlink. The wrapper below is
// the REAL node:fs delegated through a recorder — every operation actually
// hits disk; only the ordering evidence is captured.

function recordingFs(events) {
  return {
    async mkdir(...args) { return fs.mkdir(...args); },
    async readFile(...args) { return fs.readFile(...args); },
    async readdir(...args) { return fs.readdir(...args); },
    async rename(oldPath, newPath) {
      events.push({ op: "rename", from: oldPath, to: newPath });
      return fs.rename(oldPath, newPath);
    },
    async unlink(p) {
      events.push({ op: "unlink", path: p });
      return fs.unlink(p);
    },
    async open(p, flags, mode) {
      const handle = await fs.open(p, flags, mode);
      events.push({ op: "open", path: p, flags });
      return {
        async writeFile(data) { events.push({ op: "writeFile", path: p, data }); return handle.writeFile(data); },
        async sync() { events.push({ op: "sync", path: p }); return handle.sync(); },
        async close() { events.push({ op: "close", path: p }); return handle.close(); },
      };
    },
  };
}

async function withStore(fn) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-fsync-"));
  const events = [];
  try {
    await fn(new FsKeyValueStore({ rootDir: dir, fsImpl: recordingFs(events) }), events, dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

const KEY = "peer-link:sessions:rez:acct:fsync-owner::pls_1";

test("set(): temp file is fsynced BEFORE the rename; the directory is fsynced AFTER it", async () => {
  await withStore(async (store, events, dir) => {
    await store.set(KEY, { hello: "durable" });

    const kvDir = path.join(dir, "kv");
    const tmpSync = events.findIndex((e) => e.op === "sync" && e.path.includes(".tmp."));
    const rename = events.findIndex((e) => e.op === "rename");
    const dirSync = events.findIndex((e) => e.op === "sync" && e.path === kvDir);

    assert.ok(tmpSync >= 0, "temp file fsync happened");
    assert.ok(rename >= 0, "rename happened");
    assert.ok(dirSync >= 0, "directory fsync happened");
    assert.ok(tmpSync < rename, "content fsync precedes the rename (else the name can point at torn bytes)");
    assert.ok(rename < dirSync, "directory fsync follows the rename (the namespace change itself is made durable)");

    // And the write still round-trips through the real filesystem.
    assert.deepEqual(await store.get(KEY), { hello: "durable" });
  });
});

test("delete(): the directory is fsynced AFTER the unlink (no resurrect-after-power-loss)", async () => {
  await withStore(async (store, events, dir) => {
    await store.set(KEY, { hello: "gone" });
    events.length = 0;

    const removed = await store.delete(KEY);
    assert.equal(removed, true);

    const kvDir = path.join(dir, "kv");
    const unlink = events.findIndex((e) => e.op === "unlink");
    const dirSync = events.findIndex((e) => e.op === "sync" && e.path === kvDir);
    assert.ok(unlink >= 0, "unlink happened");
    assert.ok(dirSync >= 0, "directory fsync happened");
    assert.ok(unlink < dirSync, "directory fsync follows the unlink");

    assert.equal(await store.get(KEY), undefined);
  });
});

test("win32 set(): syncs the published file after rename without opening the directory", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-win32-set-"));
  const events = [];
  try {
    const store = new FsKeyValueStore({
      rootDir: dir,
      fsImpl: recordingFs(events),
      platform: "win32",
    });
    await store.set(KEY, { hello: "windows" });

    const kvDir = path.join(dir, "kv");
    const tmpSync = events.findIndex((e) => e.op === "sync" && e.path.includes(".tmp."));
    const rename = events.findIndex((e) => e.op === "rename");
    const publishedSync = events.findIndex((e) => e.op === "sync" && e.path.endsWith(".json"));
    assert.ok(tmpSync >= 0 && rename >= 0 && publishedSync >= 0);
    assert.ok(tmpSync < rename && rename < publishedSync);
    assert.equal(events.some((e) => e.op === "open" && e.path === kvDir), false);
    assert.equal(events.some((e) => e.op === "open" && e.path.endsWith(".json") && e.flags === "r+"), true);
    assert.deepEqual(await store.get(KEY), { hello: "windows" });
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("win32 delete(): publishes a synced tombstone and a later set restores the key", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-win32-delete-"));
  const events = [];
  try {
    const store = new FsKeyValueStore({
      rootDir: dir,
      fsImpl: recordingFs(events),
      platform: "win32",
    });
    await store.set(KEY, { hello: "gone" });
    events.length = 0;

    assert.equal(await store.delete(KEY), true);
    assert.equal(events.some((e) => e.op === "unlink"), false);
    const rename = events.findIndex((e) => e.op === "rename");
    const publishedSync = events.findIndex((e) => e.op === "sync" && e.path.endsWith(".json"));
    assert.ok(rename >= 0 && publishedSync > rename);
    assert.equal(await store.get(KEY), undefined);
    assert.deepEqual(await store.keys("peer-link:"), []);
    assert.equal(await store.delete(KEY), false);

    await store.set(KEY, { hello: "again" });
    assert.deepEqual(await store.get(KEY), { hello: "again" });
    assert.deepEqual(await store.keys("peer-link:"), [KEY]);
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("delete() with no kv directory at all returns false and performs NO fsync (nothing to confirm)", async () => {
  await withStore(async (store, events) => {
    const removed = await store.delete("no:such:key");
    assert.equal(removed, false);
    assert.equal(events.some((e) => e.op === "sync"), false, "no directory exists, so no namespace change to make durable");
  });
});

test("hashed-layout keys get the identical fsync treatment (uniform for ALL keys)", async () => {
  const LONG_KEY = "peer-link:sessions:by-peer-link-device:"
    + "rez:acct:wb2wyvkuys55mtadxzt5oae5ujwuoaai4q4vggy56fpeypu6ge6q"
    + "::pl_oBza-xI0K6DykN3k4QGHSdzB"
    + "::rez:dev:98dcf7865363ae01405da6647ff21475fe968e8c448b92b132739a951d5080d4";
  await withStore(async (store, events) => {
    await store.set(LONG_KEY, { big: true });
    const tmpSync = events.findIndex((e) => e.op === "sync" && e.path.includes(".tmp."));
    const rename = events.findIndex((e) => e.op === "rename");
    assert.ok(tmpSync >= 0 && rename >= 0 && tmpSync < rename);
    assert.deepEqual(await store.get(LONG_KEY), { big: true });
  });
});

// FAIL-CLOSED coverage (rev-2 review of DT-009): durability failures must
// fail the operation — never warn-and-succeed. A WAL consumer treats a
// resolved set()/delete() as a durable commit; EACCES/EPERM on the directory
// are operational misconfiguration and must surface.

function failingDirFs(events, { failOpen = false, failSync = false, code = "EACCES" } = {}) {
  const base = recordingFs(events);
  return {
    ...base,
    async open(p, flags, mode) {
      const isDirOpen = flags === "r" && !p.endsWith(".json") && !p.includes(".tmp.");
      if (isDirOpen && failOpen) {
        const err = new Error("injected dir open failure");
        err.code = code;
        throw err;
      }
      const handle = await base.open(p, flags, mode);
      if (isDirOpen && failSync) {
        return {
          ...handle,
          async sync() {
            const err = new Error("injected dir sync failure");
            err.code = code;
            throw err;
          },
        };
      }
      return handle;
    },
  };
}

function failingPublishedFileFs(events, code = "EPERM") {
  const base = recordingFs(events);
  return {
    ...base,
    async open(p, flags, mode) {
      const handle = await base.open(p, flags, mode);
      if (flags === "r+" && p.endsWith(".json")) {
        return {
          ...handle,
          async sync() {
            const err = new Error("injected published file sync failure");
            err.code = code;
            throw err;
          },
        };
      }
      return handle;
    },
  };
}

test("FAIL CLOSED: win32 set() rejects when the published file fsync fails", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-win32-failclosed-"));
  const events = [];
  try {
    const store = new FsKeyValueStore({
      rootDir: dir,
      fsImpl: failingPublishedFileFs(events),
      platform: "win32",
    });
    await assert.rejects(() => store.set(KEY, { v: 1 }), (err) => err.code === "EPERM");
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("win32 delete() retry repairs a failed tombstone fsync before reporting absence", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-win32-delete-repair-"));
  const events = [];
  try {
    const good = new FsKeyValueStore({
      rootDir: dir,
      fsImpl: recordingFs(events),
      platform: "win32",
    });
    await good.set(KEY, { v: 1 });

    const bad = new FsKeyValueStore({
      rootDir: dir,
      fsImpl: failingPublishedFileFs(events),
      platform: "win32",
    });
    await assert.rejects(() => bad.delete(KEY), (err) => err.code === "EPERM");
    assert.equal(await good.get(KEY), undefined, "the tombstone was published even though its sync failed");

    events.length = 0;
    assert.equal(await good.delete(KEY), false);
    assert.ok(
      events.some((e) => e.op === "sync" && e.path.endsWith(".json")),
      "the retry synced the existing tombstone",
    );
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("FAIL CLOSED: set() rejects when the directory cannot be opened for fsync (EACCES)", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-failclosed-"));
  const events = [];
  try {
    const store = new FsKeyValueStore({ rootDir: dir, fsImpl: failingDirFs(events, { failOpen: true, code: "EACCES" }) });
    await assert.rejects(() => store.set(KEY, { v: 1 }), (err) => err.code === "EACCES");
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("FAIL CLOSED: set() rejects when the directory fsync itself fails (EPERM)", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-failclosed-"));
  const events = [];
  try {
    const store = new FsKeyValueStore({ rootDir: dir, fsImpl: failingDirFs(events, { failSync: true, code: "EPERM" }) });
    await assert.rejects(() => store.set(KEY, { v: 1 }), (err) => err.code === "EPERM");
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("FAIL CLOSED: delete() rejects when the post-unlink directory fsync fails", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-failclosed-"));
  const events = [];
  try {
    const good = new FsKeyValueStore({ rootDir: dir, fsImpl: recordingFs(events) });
    await good.set(KEY, { v: 1 });
    const bad = new FsKeyValueStore({ rootDir: dir, fsImpl: failingDirFs(events, { failSync: true, code: "EACCES" }) });
    await assert.rejects(() => bad.delete(KEY), (err) => err.code === "EACCES");
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

// IDEMPOTENT DURABILITY (rev-3 review): a failed durable delete must be
// REPAIRABLE by retry. The first delete() unlinks and then fails its
// directory fsync (it rejects — correct). The retry sees ENOENT, so without
// this rule it would return false having confirmed nothing, leaving the
// original unlink able to resurrect after power loss.
test("delete() retry after a failed post-unlink fsync still syncs the directory before reporting absence", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-delete-repair-"));
  const events = [];
  try {
    const good = new FsKeyValueStore({ rootDir: dir, fsImpl: recordingFs(events) });
    await good.set(KEY, { v: 1 });

    // Attempt 1: unlink lands, directory fsync fails, delete() rejects.
    const bad = new FsKeyValueStore({ rootDir: dir, fsImpl: failingDirFs(events, { failSync: true, code: "EIO" }) });
    await assert.rejects(() => bad.delete(KEY), (err) => err.code === "EIO");
    assert.equal(await good.get(KEY), undefined, "the file really is unlinked — only its durability is unconfirmed");

    // Attempt 2: the fault has cleared. The key is already absent (ENOENT),
    // but the unconfirmed namespace change MUST still be fsynced.
    events.length = 0;
    const removed = await good.delete(KEY);
    assert.equal(removed, false, "already absent");
    const kvDir = path.join(dir, "kv");
    assert.ok(
      events.some((e) => e.op === "sync" && e.path === kvDir),
      "the retry synced the containing directory, confirming the earlier unlink",
    );
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("FAIL CLOSED: the idempotent delete path still propagates a directory fsync failure", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-delete-repair-fail-"));
  const events = [];
  try {
    const good = new FsKeyValueStore({ rootDir: dir, fsImpl: recordingFs(events) });
    await good.set(KEY, { v: 1 });
    await good.delete(KEY);
    // Absent key + still-broken directory fsync: must reject, not return false.
    const bad = new FsKeyValueStore({ rootDir: dir, fsImpl: failingDirFs(events, { failSync: true, code: "EPERM" }) });
    await assert.rejects(() => bad.delete(KEY), (err) => err.code === "EPERM");
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

test("default construction (no fsImpl) still round-trips against the real filesystem", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-fskv-fsync-real-"));
  try {
    const store = new FsKeyValueStore({ rootDir: dir });
    await store.set(KEY, { real: 1 });
    assert.deepEqual(await store.get(KEY), { real: 1 });
    assert.deepEqual(await store.keys("peer-link:"), [KEY]);
    assert.equal(await store.delete(KEY), true);
    assert.equal(await store.get(KEY), undefined);
  } finally {
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});
