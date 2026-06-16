import test from "node:test";
import assert from "node:assert/strict";
import { ensureNodeIdentity } from "../src/identity/NodeIdentity.js";

// Minimal in-memory KV mirroring the node-local FS store across "boots".
function fakeProvider() {
  const store = new Map();
  return {
    getKeyValueStore() {
      return {
        async get(k) { return store.has(k) ? store.get(k) : undefined; },
        async set(k, v) { store.set(k, v); },
      };
    },
  };
}

const PARTIAL = { accountId: "rez:node:a", deviceId: "dev:a", localInboxId: "inbox:a" };

test("REGRESSION: a partial config identity gets STABLE node keys across boots (persisted)", async () => {
  const provider = fakeProvider();
  const boot1 = await ensureNodeIdentity({ storageProvider: provider, configuredIdentity: PARTIAL });
  const boot2 = await ensureNodeIdentity({ storageProvider: provider, configuredIdentity: PARTIAL });
  assert.ok(boot1.nodeKeyId && boot1.nodePrivateKeyB64, "node keys were generated");
  assert.equal(boot2.nodeKeyId, boot1.nodeKeyId, "nodeKeyId is stable across boots (no rotation)");
  assert.equal(boot2.nodePrivateKeyB64, boot1.nodePrivateKeyB64, "node private key is stable (fs storage key won't rotate)");
  // Config ids are honored.
  assert.equal(boot2.accountId, "rez:node:a");
});

test("a config identity WITH complete node keys is returned verbatim (pinned, not persisted)", async () => {
  const full = {
    ...PARTIAL,
    nodeKeyId: "nodekey:fixed",
    nodePublicKeyB64: "cHVi",
    nodePrivateKeyB64: "cHJpdg==",
  };
  // No provider: a fully-pinned identity must not need storage.
  const id = await ensureNodeIdentity({ storageProvider: null, configuredIdentity: full });
  assert.equal(id.nodeKeyId, "nodekey:fixed");
  assert.equal(id.nodePrivateKeyB64, "cHJpdg==");
});

test("no config identity: generated identity persists and is reused", async () => {
  const provider = fakeProvider();
  const boot1 = await ensureNodeIdentity({ storageProvider: provider });
  const boot2 = await ensureNodeIdentity({ storageProvider: provider });
  assert.equal(boot2.accountId, boot1.accountId, "generated identity is reused, not regenerated");
  assert.equal(boot2.nodeKeyId, boot1.nodeKeyId);
});

test("legacy persisted identity WITHOUT node keys is upgraded with stable keys", async () => {
  const provider = fakeProvider();
  // Simulate an old persisted identity that predates mesh keys.
  await provider.getKeyValueStore().set("substrate:nodeIdentity:v1", { ...PARTIAL });
  const boot1 = await ensureNodeIdentity({ storageProvider: provider });
  const boot2 = await ensureNodeIdentity({ storageProvider: provider });
  assert.ok(boot1.nodeKeyId, "mesh keys were added to the legacy identity");
  assert.equal(boot2.nodeKeyId, boot1.nodeKeyId, "and they are stable thereafter");
});
