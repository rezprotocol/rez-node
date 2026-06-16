import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { mkdtempSync, rmSync } from "node:fs";
import { Header, Envelope } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { FsStorageProvider } from "../src/storage/fs/FsStorageProvider.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgStorageProvider } from "../src/storage/pg/PgStorageProvider.js";

/**
 * Interface-conformance harness: the SAME assertions run against both
 * FsStorageProvider and PgStorageProvider, proving the Pg backend satisfies the
 * existing StorageProvider contract identically (SSOT — no parallel API).
 *
 * Registered as ONE parent test per provider (async setup + subtests inside) so
 * gated/async Pg setup can't race --test-force-exit.
 *
 * `newProvider({ encryptionKey })` must return a provider sharing the SAME
 * backing store across calls (so "persists across instances" is meaningful).
 */
function makeKey() {
  return new NodeCryptoProvider().randomBytes(32);
}

async function runProviderConformance(t, label, newProvider) {
  await t.test(`[${label}] object put/get/has/delete round-trip`, async () => {
    const store = newProvider().getObjectStore();
    const header = new Header({ id: `${label}-obj-1`, type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { hello: "world" } });
    await store.put(envelope);
    const loaded = await store.get(`${label}-obj-1`);
    assert.ok(loaded, "object loads");
    assert.deepEqual(loaded.toJSON(), envelope.toJSON());
    assert.equal(await store.has(`${label}-obj-1`), true);
    assert.equal(await store.delete(`${label}-obj-1`), true);
    assert.equal(await store.delete(`${label}-obj-1`), false);
    assert.equal(await store.get(`${label}-obj-1`), null);
  });

  await t.test(`[${label}] mailbox append persists across instances + ordering`, async () => {
    await newProvider().getMailboxStore().append(`${label}-mb`, "id-1");
    await newProvider().getMailboxStore().append(`${label}-mb`, "id-2");
    const items = await newProvider().getMailboxStore().list(`${label}-mb`);
    assert.deepEqual(items, ["id-1", "id-2"]);
  });

  await t.test(`[${label}] kv persists across instances`, async () => {
    await newProvider().getKeyValueStore().set(`${label}-k1`, { ok: true });
    const value = await newProvider().getKeyValueStore().get(`${label}-k1`);
    assert.deepEqual(value, { ok: true });
  });

  await t.test(`[${label}] encrypted object + kv + mailbox round-trip`, async () => {
    const key = makeKey();
    const provider = newProvider({ encryptionKey: key });
    const header = new Header({ id: `${label}-enc-obj`, type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { secret: "data" } });
    await provider.getObjectStore().put(envelope);
    assert.deepEqual((await provider.getObjectStore().get(`${label}-enc-obj`)).toJSON(), envelope.toJSON());

    await provider.getKeyValueStore().set(`${label}-enc-k`, { secret: true });
    assert.deepEqual(await provider.getKeyValueStore().get(`${label}-enc-k`), { secret: true });

    await provider.getMailboxStore().append(`${label}-enc-mb`, "a");
    await provider.getMailboxStore().append(`${label}-enc-mb`, "b");
    assert.deepEqual(await provider.getMailboxStore().list(`${label}-enc-mb`), ["a", "b"]);
  });

  await t.test(`[${label}] encrypted: wrong key rejects (AEAD integrity)`, async () => {
    const provider1 = newProvider({ encryptionKey: makeKey() });
    const header = new Header({ id: `${label}-wrongkey`, type: "message", createdAt: 1 });
    await provider1.getObjectStore().put(new Envelope({ header, body: { ok: true } }));
    const provider2 = newProvider({ encryptionKey: makeKey() });
    await assert.rejects(
      () => provider2.getObjectStore().get(`${label}-wrongkey`),
      /authenticate|integrity|decrypt|tag/i,
    );
  });

  await t.test(`[${label}] encrypted reads legacy plaintext (progressive migration)`, async () => {
    const header = new Header({ id: `${label}-legacy`, type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { legacy: true } });
    await newProvider().getObjectStore().put(envelope); // plaintext write
    const loaded = await newProvider({ encryptionKey: makeKey() }).getObjectStore().get(`${label}-legacy`);
    assert.ok(loaded, "reads legacy plaintext under an encrypted provider");
    assert.deepEqual(loaded.toJSON(), envelope.toJSON());
  });

  await t.test(`[${label}] peer-link storage is available`, () => {
    assert.ok(newProvider().getPeerLinkStorage(), "getPeerLinkStorage returns a bundle");
  });
}

// Fs: always runs, proving the harness matches shipped Fs behavior.
test("FsStorageProvider conformance", async (t) => {
  const root = mkdtempSync(path.join(os.tmpdir(), "rez-conf-fs-"));
  try {
    await runProviderConformance(t, "Fs", ({ encryptionKey = null } = {}) =>
      new FsStorageProvider({ rootDir: root, encryptionKey }));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// Pg: gated on a real Postgres (un-mocked).
const PG_URL = process.env.REZ_PG_TEST_URL || "";
test(
  "PgStorageProvider conformance",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_conformance";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    try {
      await new MigrationRunner({ connection: conn }).migrate();
      await conn.query("TRUNCATE objects, mailbox_index, kv");
      await runProviderConformance(t, "Pg", ({ encryptionKey = null } = {}) =>
        new PgStorageProvider({ connection: conn, encryptionKey }));
    } finally {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    }
  },
);
