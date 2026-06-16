import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";

import { startRezNode } from "../src/app/startRezNode.js";
import { validateConfig } from "../src/app/NodeConfigValidator.js";
import { createStorageBackend } from "../src/app/createStorageBackend.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";
// A pre-supplied identity makes the boot path write NOTHING to the shared `kv`
// table (ensureNodeIdentity returns early), so this test never races the other
// pg suites that TRUNCATE kv in parallel.
const FIXED_IDENTITY = { accountId: "rez:node:bsel", deviceId: "dev:bsel", localInboxId: "inbox:bsel" };

// Minimal otherwise-valid node config; vary only `storage`.
const withStorage = (storage) => ({
  node: {
    ws: { host: "127.0.0.1", port: 0, path: "/ws" },
    network: { knownRelays: [] },
    storage,
  },
});

// ---- Config: backend selection (no DB needed) ----

test("storage backend defaults to fs", () => {
  const resolved = validateConfig(withStorage({ dataDir: "/tmp/x" }));
  assert.equal(resolved.storage.backend, "fs");
  assert.equal(resolved.storage.pg.connectionString, "");
  assert.equal(resolved.storage.pg.migrateOnBoot, true);
});

test("storage.backend=pg requires a connectionString", () => {
  assert.throws(
    () => validateConfig(withStorage({ dataDir: "/tmp/x", backend: "pg" })),
    /storage\.pg\.connectionString when storage\.backend=pg/,
  );
});

test("storage.backend=pg resolves connectionString + migrateOnBoot", () => {
  const resolved = validateConfig(withStorage({
    dataDir: "/tmp/x",
    backend: "PG", // case-insensitive
    pg: { connectionString: "  postgres://h/db  ", migrateOnBoot: false },
  }));
  assert.equal(resolved.storage.backend, "pg");
  assert.equal(resolved.storage.pg.connectionString, "postgres://h/db");
  assert.equal(resolved.storage.pg.migrateOnBoot, false);
});

test("an unknown storage.backend is rejected", () => {
  assert.throws(
    () => validateConfig(withStorage({ dataDir: "/tmp/x", backend: "redis" })),
    /storage\.backend in fs\|pg/,
  );
});

// ---- createStorageBackend: fs path (no DB) ----

test("createStorageBackend(fs) mints a working provider and closes cleanly", async (t) => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-backend-fs-"));
  t.after(async () => {
    await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
  });
  const resolved = validateConfig(withStorage({ dataDir: tempRoot }));
  const backend = await createStorageBackend({ resolved });
  assert.equal(backend.backend, "fs");
  const provider = backend.makeProvider(null);
  const kv = provider.getKeyValueStore();
  await kv.set("probe", { ok: 1 });
  assert.deepEqual(await kv.get("probe"), { ok: 1 });
  await backend.close(); // no-op for fs, must not throw
});

// ---- The headline: a node actually BOOTS on the pg backend ----

test(
  "startRezNode boots on storage.backend=pg: runs migrations, uses the Pg provider, no FS store",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-backend-pg-"));
    t.after(async () => {
      await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
    });

    const config = {
      node: {
        mode: "relay-only",
        identity: FIXED_IDENTITY,
        storage: {
          dataDir: tempRoot,
          backend: "pg",
          pg: { connectionString: PG_URL, migrateOnBoot: true },
        },
        network: { knownRelays: [] },
        mesh: { mode: "seeded-gossip", seeds: [] },
        relay: {
          listenHost: "127.0.0.1",
          listenPort: 0,
          advertisedHost: "127.0.0.1",
          relayKeyId: "ws:relay-pg-smoke",
        },
      },
    };

    const { PgStorageProvider } = await import("../src/storage/pg/PgStorageProvider.js");
    const app = await startRezNode(config);
    try {
      assert.equal(app.config.storage.backend, "pg", "the running node selected the pg backend");
      assert.ok(app.relayAddress && app.relayAddress.port > 0, "relay listener bound");

      // The running node's provider IS the Postgres provider (not Fs) — the seam
      // actually swapped the backend, not just the config value.
      assert.ok(app.storageProvider instanceof PgStorageProvider, "node is backed by PgStorageProvider");

      // migrateOnBoot actually applied the schema to THIS database. schema_migrations
      // is bookkeeping no other suite truncates, so this read never races them.
      const { PgConnection } = await import("../src/storage/pg/PgConnection.js");
      const probe = new PgConnection({ connectionString: PG_URL });
      try {
        const res = await probe.query("SELECT coalesce(max(version), 0) AS v FROM schema_migrations");
        assert.ok(Number(res.rows[0].v) >= 1, "boot migration recorded in schema_migrations");
      } finally {
        await probe.close();
      }

      // Storage went to Pg, NOT the filesystem: no kv store under the data dir
      // (only the node-local transient relay buffer lives there).
      const entries = await fs.readdir(tempRoot);
      assert.ok(!entries.includes("kv"), "no filesystem kv store was created (storage went to Pg)");
    } finally {
      await app.stop(); // must close the Pg pool without throwing
    }
  },
);
