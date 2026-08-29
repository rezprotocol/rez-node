import test from "node:test";
import assert from "node:assert/strict";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgKeyValueStore } from "../src/storage/pg/PgKeyValueStore.js";
import { PgStorageProvider } from "../src/storage/pg/PgStorageProvider.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { pgTestUrl } from "./support/integrationBackends.js";

// Un-mocked integration test — requires a real Postgres. Set REZ_PG_TEST_URL,
// e.g. postgres://rez:rez@localhost:5433/rez_dev (the dev container). Skipped
// (not failed) when unset so the suite stays green on machines without Pg.
const PG_URL = pgTestUrl();

test("PgKeyValueStore getStrict wraps backend read failures", async () => {
  const cause = new Error("injected pg read failure");
  const kv = new PgKeyValueStore({
    connection: {
      async query() { throw cause; },
    },
    ownerAccountId: "owner",
  });
  await assert.rejects(
    () => kv.getStrict("faulted"),
    (err) => err && err.code === "KEY_VALUE_UNREADABLE"
      && err.key === "faulted"
      && err.cause === cause,
  );
});

test(
  "PgKeyValueStore + MigrationRunner against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_kv";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });

    const result = await new MigrationRunner({ connection: conn }).migrate();
    assert.ok(result.shipped >= 1, "at least migration 0001 ships");

    // Idempotent re-run applies nothing new (advisory-locked, version-gated).
    const second = await new MigrationRunner({ connection: conn }).migrate();
    assert.deepEqual(second.appliedNow, [], "re-running migrate is a no-op");

    await conn.query("DELETE FROM kv");

    await t.test("set / get / delete / keys", async () => {
      const kv = new PgKeyValueStore({ connection: conn, ownerAccountId: "claimantA" });
      assert.equal(await kv.get("missing"), undefined);
      assert.equal(await kv.getStrict("missing"), undefined);
      await kv.set("a:1", { n: 1 });
      await kv.set("a:2", { n: 2 });
      await kv.set("b:1", { n: 3 });
      assert.deepEqual(await kv.get("a:1"), { n: 1 });
      assert.deepEqual((await kv.keys("a:")).sort(), ["a:1", "a:2"]);
      assert.deepEqual((await kv.keys("")).sort(), ["a:1", "a:2", "b:1"]);
      assert.equal(await kv.delete("a:1"), true);
      assert.equal(await kv.delete("a:1"), false);
      assert.equal(await kv.get("a:1"), undefined);
    });

    await t.test("owner isolation (partition by claimant)", async () => {
      const a = new PgKeyValueStore({ connection: conn, ownerAccountId: "ownerX" });
      const b = new PgKeyValueStore({ connection: conn, ownerAccountId: "ownerY" });
      await a.set("shared", { who: "X" });
      await b.set("shared", { who: "Y" });
      assert.deepEqual(await a.get("shared"), { who: "X" });
      assert.deepEqual(await b.get("shared"), { who: "Y" });
      assert.deepEqual(await a.keys(""), ["shared"], "owner A sees only its own keys");
    });

    await t.test("CAS: setVersioned conflict + success", async () => {
      const kv = new PgKeyValueStore({ connection: conn, ownerAccountId: "casOwner" });
      const created = await kv.setVersioned("claim", { v: 0 }, null);
      assert.equal(created.ok, true);
      assert.equal(created.version, 1);

      const dup = await kv.setVersioned("claim", { v: 0 }, null);
      assert.equal(dup.ok, false, "create-if-absent conflicts when row exists");

      const upd = await kv.setVersioned("claim", { v: 1 }, 1);
      assert.equal(upd.ok, true);
      assert.equal(upd.version, 2);

      const stale = await kv.setVersioned("claim", { v: 99 }, 1);
      assert.equal(stale.ok, false, "stale expected version conflicts");
      assert.deepEqual(await kv.get("claim"), { v: 1 }, "conflicting write did not land");

      const versioned = await kv.getVersioned("claim");
      assert.equal(versioned.version, 2);
    });

    await t.test("LIKE-special chars in keys/prefix are literal", async () => {
      const kv = new PgKeyValueStore({ connection: conn, ownerAccountId: "likeOwner" });
      await kv.set("50%_off", { ok: true });
      await kv.set("50Xoff", { ok: false });
      assert.deepEqual(await kv.keys("50%_"), ["50%_off"], "% and _ matched literally");
    });

    await t.test("at-rest cluster key sharing: SAME key reads cross-node, DIFFERENT key cannot", async () => {
      // Two "nodes" (distinct providers, as distinct cluster members would be)
      // sharing one explicit cluster key must read each other's encrypted rows;
      // a node with a different key must NOT be able to decrypt them.
      const K1 = new Uint8Array(32).fill(7);
      const K2 = new Uint8Array(32).fill(9);
      const owner = "xnet-share-owner";
      const nodeA = new PgStorageProvider({ connection: conn, encryptionKey: K1 });
      const nodeB = new PgStorageProvider({ connection: conn, encryptionKey: K1 }); // same cluster key
      const nodeC = new PgStorageProvider({ connection: conn, encryptionKey: K2 }); // different key

      await nodeA.getKeyValueStore(owner).set("xnet:secret", { ok: 42 });
      assert.deepEqual(await nodeB.getKeyValueStore(owner).get("xnet:secret"), { ok: 42 },
        "a node with the same cluster key reads the shared encrypted row");
      await assert.rejects(
        () => nodeC.getKeyValueStore(owner).get("xnet:secret"),
        "a node with a different key cannot decrypt the row (AEAD auth fails)",
      );
    });
  },
);
