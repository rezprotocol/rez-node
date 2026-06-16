import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";

import { startRezNode } from "../src/app/startRezNode.js";
import { validateConfig } from "../src/app/NodeConfigValidator.js";
import { createStorageBackend } from "../src/app/createStorageBackend.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";
// An explicit 32-byte at-rest cluster key (base64). pg mode requires one.
const STORAGE_KEY_B64 = Buffer.alloc(32, 7).toString("base64");

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
    encryptionKeyB64: STORAGE_KEY_B64,
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

test("storage.backend=pg REQUIRES an explicit encryptionKeyB64 (no per-node derive)", () => {
  assert.throws(
    () => validateConfig(withStorage({ dataDir: "/tmp/x", backend: "pg", pg: { connectionString: "postgres://h/db" } })),
    /encryptionKeyB64.*when storage\.backend=pg/s,
  );
});

test("a wrong-length storage encryption key is rejected (must be 32 bytes)", () => {
  assert.throws(
    () => validateConfig(withStorage({
      dataDir: "/tmp/x", backend: "pg",
      pg: { connectionString: "postgres://h/db" },
      encryptionKeyB64: Buffer.alloc(16, 1).toString("base64"), // 16 bytes, too short
    })),
    /must decode to exactly 32 bytes/,
  );
});

test("fs mode needs NO explicit key (node-derived)", () => {
  const resolved = validateConfig(withStorage({ dataDir: "/tmp/x" })); // no encryptionKeyB64
  assert.equal(resolved.storage.backend, "fs");
  assert.equal(resolved.storage.encryptionKeyB64, "");
});

// ---- Config: node identity preserves node key material (no boot-time rotation) ----

test("config identity preserves full node key material when supplied", () => {
  const id = {
    accountId: "rez:node:x", deviceId: "dev:x", localInboxId: "inbox:x",
    nodeKeyId: "nodekey:abc", nodePublicKeyB64: "cHVi", nodePrivateKeyB64: "cHJpdg==",
  };
  const resolved = validateConfig({
    node: { ws: { host: "127.0.0.1", port: 0, path: "/ws" }, network: { knownRelays: [] }, identity: id },
  });
  assert.equal(resolved.node.identity.nodeKeyId, "nodekey:abc");
  assert.equal(resolved.node.identity.nodePublicKeyB64, "cHVi");
  assert.equal(resolved.node.identity.nodePrivateKeyB64, "cHJpdg==");
});

test("partial node key material in config identity is rejected (all-or-nothing)", () => {
  assert.throws(
    () => validateConfig({
      node: {
        ws: { host: "127.0.0.1", port: 0, path: "/ws" }, network: { knownRelays: [] },
        identity: { accountId: "a", deviceId: "d", localInboxId: "i", nodeKeyId: "nodekey:abc" }, // missing pub/priv
      },
    }),
    /node key material must be complete/,
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
  "startRezNode boots on storage.backend=pg: migrates, uses Pg, and keeps node identity NODE-LOCAL",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-backend-pg-"));
    t.after(async () => {
      await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
    });

    const config = {
      node: {
        mode: "relay-only",
        // NO explicit identity — the node generates one. The fix under test:
        // that identity must land on LOCAL FS, never in shared Postgres.
        storage: {
          dataDir: tempRoot,
          backend: "pg",
          pg: { connectionString: PG_URL, migrateOnBoot: true },
          encryptionKeyB64: STORAGE_KEY_B64,
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
    const { FsStorageProvider } = await import("../src/storage/fs/FsStorageProvider.js");
    const app = await startRezNode(config);
    try {
      assert.equal(app.config.storage.backend, "pg", "the running node selected the pg backend");
      assert.ok(app.relayAddress && app.relayAddress.port > 0, "relay listener bound");
      assert.ok(app.storageProvider instanceof PgStorageProvider, "node is backed by PgStorageProvider");

      const { PgConnection } = await import("../src/storage/pg/PgConnection.js");
      const probe = new PgConnection({ connectionString: PG_URL });
      try {
        const mig = await probe.query("SELECT coalesce(max(version), 0) AS v FROM schema_migrations");
        assert.ok(Number(mig.rows[0].v) >= 1, "boot migration recorded in schema_migrations");
        // THE FIX: node identity must NOT be written to shared Postgres.
        const idRows = await probe.query("SELECT count(*)::int c FROM kv WHERE key = 'substrate:nodeIdentity:v1'");
        assert.equal(idRows.rows[0].c, 0, "node identity is NOT in shared Pg (would collide across nodes)");
      } finally {
        await probe.close();
      }

      // ...and it IS persisted node-locally on the filesystem, with real mesh keys.
      const localId = await new FsStorageProvider({ rootDir: tempRoot })
        .getKeyValueStore().get("substrate:nodeIdentity:v1");
      assert.ok(localId && typeof localId.nodePublicKeyB64 === "string" && localId.nodePublicKeyB64.length > 0,
        "node identity persisted to LOCAL fs with mesh-auth material");
    } finally {
      await app.stop();
    }
  },
);
