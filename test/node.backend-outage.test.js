import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PgConnection } from "../src/storage/pg/PgConnection.js";
import { startRezNode } from "../src/app/startRezNode.js";
import { pgTestUrl } from "./support/integrationBackends.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";

const PG_URL = pgTestUrl();

test("Postgres statement cancellation releases the connection and the next query succeeds", { skip: !PG_URL }, async (t) => {
  const connection = new PgConnection({ connectionString: PG_URL, poolConfig: { statement_timeout: 25, query_timeout: 1000 } });
  t.after(() => connection.close());
  await assert.rejects(() => connection.query("SELECT pg_sleep(0.2)"), (err) => err.code === "57014");
  assert.equal((await connection.query("SELECT 1 AS ok")).rows[0].ok, 1);
});

test("a node starts degraded with Redis absent and shuts down without waiting for Redis", { skip: !PG_URL, timeout: 15000 }, async (t) => {
  const schema = "test_node_redis_absent";
  const admin = await createIsolatedPgConnection(PG_URL, schema);
  await admin.close();
  const rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-redis-absent-"));
  let app;
  t.after(async () => { if (app) await app.stop(); await dropSchema(PG_URL, schema); await fs.rm(rootDir, { recursive: true, force: true }); });
  const url = new URL(PG_URL);
  url.searchParams.set("options", "-c search_path=" + schema);
  app = await startRezNode({ node: { ws: { host: "127.0.0.1", port: 0, path: "/ws" },
    storage: { dataDir: rootDir, backend: "pg", encryptionKeyB64: Buffer.alloc(32, 9).toString("base64"),
      pg: { connectionString: url.toString(), migrateOnBoot: true } },
    redis: { url: "redis://127.0.0.1:1" }, network: { knownRelays: [] },
    relay: { listenHost: "127.0.0.1", listenPort: 0 }, mesh: { mode: "seed-only", seeds: [] } } });
  const state = await app.runtime.checkReadiness();
  assert.equal(state.ok, true);
  assert.equal(state.degraded, true);
  const started = Date.now();
  await app.stop();
  app = null;
  assert.ok(Date.now() - started < 3000);
});
