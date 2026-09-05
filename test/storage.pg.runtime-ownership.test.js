import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { pgTestUrl } from "./support/integrationBackends.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import test from "node:test";
import assert from "node:assert/strict";

import { PgStorageProvider } from "../src/storage/pg/PgStorageProvider.js";

test("Postgres runtime ownership holds one advisory-lock session and advances the durable epoch", async () => {
  const clientQueries = [];
  let released = false;
  const client = {
    async query(sql, params) {
      clientQueries.push({ sql, params });
      if (sql.includes("pg_advisory_unlock")) return { rows: [{ unlocked: true }] };
      if (sql.includes("pg_try_advisory_lock")) return { rows: [{ acquired: true }] };
      return { rows: [{}] };
    },
    release() { released = true; },
  };
  const connection = {
    pool: { async connect() { return client; } },
  };
  const values = new Map();
  const provider = new PgStorageProvider({ connection });
  provider.getKeyValueStore = () => ({
    async getStrict(key) { return values.get(key); },
    async set(key, value) { values.set(key, value); },
  });

  const grant = await provider.acquireRuntimeOwnership({ namespace: "sdk-delivery" });
  assert.equal(grant.runtimeEpoch, 1);
  assert.match(clientQueries[0].sql, /pg_try_advisory_lock/);
  assert.equal(typeof clientQueries[0].params[0], "string", "the store lock uses a stable signed bigint key");
  assert.equal(values.get("sdk:delivery:runtime-epoch:v1"), 1);
  assert.equal(released, false, "the checked-out connection holds the lock for the runtime lifetime");

  await grant.release();
  assert.match(clientQueries[1].sql, /pg_advisory_unlock/);
  assert.deepEqual(clientQueries[1].params, clientQueries[0].params);
  assert.equal(released, true);

  released = false;
  const reacquired = await provider.acquireRuntimeOwnership({ namespace: "sdk-delivery" });
  assert.equal(reacquired.runtimeEpoch, 2);
  assert.match(clientQueries[2].sql, /pg_try_advisory_lock/);
  await reacquired.release();
  assert.match(clientQueries[3].sql, /pg_advisory_unlock/);
  assert.equal(released, true);
});

for (const failure of ["busy", "read", "write", "unlock"]) {
  test("Pg runtime failure cleanup: " + failure, async () => {
    const queries = []; const releases = [];
    const client = { async query(sql) {
      queries.push(sql);
      if (sql.includes("pg_try_advisory_lock")) return { rows: [{ acquired: failure !== "busy" }] };
      if (sql.includes("pg_advisory_unlock")) {
        if (failure === "unlock") throw new Error("connection lost");
        return { rows: [{ unlocked: true }] };
      }
      return { rows: [] };
    }, release(destroy) { releases.push(Boolean(destroy)); } };
    const provider = new PgStorageProvider({ connection: { pool: { async connect() { return client; } } } });
    provider.getKeyValueStore = () => ({
      async getStrict() { if (failure === "read") throw new Error("read failure"); return 0; },
      async set() { throw new Error("write failure"); },
    });
    await assert.rejects(provider.acquireRuntimeOwnership());
    assert.equal(queries.some(sql => sql.includes("pg_advisory_unlock")), failure !== "busy");
    assert.deepEqual(releases, [failure === "unlock"]);
  });
}

test("Pg protected KV writes use the lock-owning connection and never fall back after release", async () => {
  const queries = [];
  const client = { async query(sql) {
    queries.push(sql);
    if (sql.includes("pg_try_advisory_lock")) return { rows: [{ acquired: true }] };
    if (sql.includes("pg_advisory_unlock")) return { rows: [{ unlocked: true }] };
    if (sql.startsWith("SELECT value")) return { rows: [], rowCount: 0 };
    return { rows: [], rowCount: 1 };
  }, release() {} };
  const provider = new PgStorageProvider({ connection: { pool: { async connect() { return client; } }, query() { throw new Error("pooled fallback used"); } } });
  const kv = provider.getKeyValueStore("owner");
  const grant = await provider.acquireRuntimeOwnership();
  await kv.set("peer-link:test", "value");
  assert.ok(queries.some(sql => sql.includes("INSERT INTO kv")));
  await grant.release();
  await assert.rejects(kv.set("peer-link:test", "stale"), /inactive/);
});

test("real Pg: exclusive owner, failed acquisition cleanup, and protected IO share the lock session", { skip: !pgTestUrl() }, async t => {
  const schema = "audit_runtime_ownership_" + process.pid;
  const conn = await createIsolatedPgConnection(pgTestUrl(), schema);
  t.after(async () => { await conn.close(); await dropSchema(pgTestUrl(),schema); });
  await new MigrationRunner({ connection: conn }).migrate();
  const first = new PgStorageProvider({ connection: conn });
  const second = new PgStorageProvider({ connection: conn });
  const grant = await first.acquireRuntimeOwnership();
  await assert.rejects(second.acquireRuntimeOwnership(), { code: "DELIVERY_RUNTIME_ALREADY_ACTIVE" });
  await first.getKeyValueStore("owner").set("test", "first");
  await grant.release();
  const next = await second.acquireRuntimeOwnership();
  assert.equal(next.runtimeEpoch, 2);
  assert.equal(await second.getKeyValueStore("owner").get("test"), "first");
  await assert.rejects(first.getKeyValueStore("owner").set("test", "stale"), /inactive/);
  await next.release();
  const failure = new PgStorageProvider({ connection: conn });
  failure.getKeyValueStore = () => ({ async getStrict() { throw new Error("injected read failure"); } });
  await assert.rejects(failure.acquireRuntimeOwnership(), /injected/);
  const afterFailure = await new PgStorageProvider({ connection: conn }).acquireRuntimeOwnership();
  assert.equal(afterFailure.runtimeEpoch, 3); await afterFailure.release();
});
