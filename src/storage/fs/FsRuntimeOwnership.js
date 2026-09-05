import { promises as fs } from "node:fs";
import path from "node:path";

const RUNTIME_EPOCH_KEY = "sdk:delivery:runtime-epoch:v1";

/**
 * Kernel-backed single writer, held until the runtime drains and releases.
 * SQLite's rollback-journal EXCLUSIVE transaction owns the lock on Windows
 * and Unix, including process suspension. A crash releases it automatically.
 * Never unlink/rename the database: that would create a second lock identity.
 * No timeout or heartbeat may evict a living (possibly suspended) writer.
 */
export async function acquireFsRuntimeOwnership({ rootDir, keyValueStore, namespace = "sdk-delivery" } = {}) {
  if (!rootDir || !keyValueStore) throw new Error("runtime ownership requires rootDir and keyValueStore");
  if (typeof namespace !== "string" || !/^[a-zA-Z0-9_-]+$/.test(namespace)) {
    throw new Error("invalid runtime namespace");
  }
  await fs.mkdir(rootDir, { recursive: true, mode: 0o700 });
  // A legacy runtime may still own its old lock during an upgrade. Fail closed
  // for a live/unknown PID; a provably dead old owner cannot resume writing.
  const legacyPath = path.join(rootDir, ".rez-" + namespace + ".lock");
  try {
    const prior = JSON.parse(await fs.readFile(legacyPath, "utf8"));
    if (!Number.isSafeInteger(prior.pid) || prior.pid < 1) throw new Error("invalid legacy runtime PID");
    let alive = true;
    try { process.kill(prior.pid, 0); } catch (err) {
      if (err.code === "ESRCH") alive = false;
      else throw err;
    }
    if (alive) {
      const err = new Error("legacy delivery runtime is still active");
      err.code = "DELIVERY_RUNTIME_ALREADY_ACTIVE";
      throw err;
    }
  } catch (err) {
    if (err.code !== "ENOENT") throw err;
  }
  // Dynamic import keeps non-filesystem node/relay users independent of SQLite.
  // The pinned desktop Node 22.15 includes this built-in; no addon is needed.
  const { DatabaseSync } = await import("node:sqlite");
  const lockPath = path.join(await fs.realpath(rootDir), ".rez-runtime-owner.sqlite");
  let db = null;
  try {
    db = new DatabaseSync(lockPath);
    db.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE");
    const raw = await keyValueStore.getStrict(RUNTIME_EPOCH_KEY);
    const prior = raw === undefined ? 0 : Number(raw);
    if (!Number.isSafeInteger(prior) || prior < 0 || prior === Number.MAX_SAFE_INTEGER) {
      throw new Error("Invalid delivery runtime epoch");
    }
    const runtimeEpoch = prior + 1;
    await keyValueStore.set(RUNTIME_EPOCH_KEY, runtimeEpoch);
    let released = false;
    return {
      runtimeEpoch,
      lockPath,
      assertActive() {
        if (released) throw new Error("delivery runtime ownership has been released");
      },
      async release() {
        if (released) return;
        released = true;
        db.close();
      },
    };
  } catch (err) {
    if (db) db.close();
    if (err.errcode === 5 || err.errcode === 6 || /database is locked/.test(err.message)) {
      const busy = new Error("Delivery storage is already owned by a live runtime", { cause: err });
      busy.code = "DELIVERY_RUNTIME_ALREADY_ACTIVE";
      throw busy;
    }
    throw err;
  }
}
