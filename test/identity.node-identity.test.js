import test from "node:test";
import assert from "node:assert/strict";
import { ensureNodeIdentity } from "../src/identity/NodeIdentity.js";
import { deriveRelayIdentity, RelayIdentityMismatchError } from "../src/util/relayKeyId.js";
import { makeRelayIdentity } from "./support/relayIdentity.js";

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

test("a config identity WITH valid node keys is accepted pinned (not persisted) and gains the derived relayKeyId", async () => {
  const minted = makeRelayIdentity();
  const full = {
    ...PARTIAL,
    nodeKeyId: minted.nodeKeyId,
    nodePublicKeyB64: minted.nodePublicKeyB64,
    nodePrivateKeyB64: minted.nodePrivateKeyB64,
  };
  // No provider: a fully-pinned identity must not need storage.
  const id = await ensureNodeIdentity({ storageProvider: null, configuredIdentity: full });
  assert.equal(id.nodeKeyId, minted.nodeKeyId);
  assert.equal(id.nodePrivateKeyB64, minted.nodePrivateKeyB64);
  assert.equal(id.relayKeyId, minted.relayKeyId, "relayKeyId is derived from the pinned key");
});

test("ADR-RELAY-IDENTITY: a pinned identity whose nodeKeyId does not re-derive from its key is rejected", async () => {
  const minted = makeRelayIdentity();
  const forged = {
    ...PARTIAL,
    nodeKeyId: "nodekey:00000000000000000000000000000000",
    nodePublicKeyB64: minted.nodePublicKeyB64,
    nodePrivateKeyB64: minted.nodePrivateKeyB64,
  };
  await assert.rejects(
    ensureNodeIdentity({ storageProvider: null, configuredIdentity: forged }),
    RelayIdentityMismatchError,
  );
});

test("ADR-RELAY-IDENTITY: a pinned identity with a garbage public key is rejected", async () => {
  const garbage = {
    ...PARTIAL,
    nodeKeyId: "nodekey:fixed",
    nodePublicKeyB64: "cHVi",
    nodePrivateKeyB64: "cHJpdg==",
  };
  await assert.rejects(
    ensureNodeIdentity({ storageProvider: null, configuredIdentity: garbage }),
    RelayIdentityMismatchError,
  );
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

test("ADR-RELAY-IDENTITY: every returned identity carries the relayKeyId derived from its node key", async () => {
  const provider = fakeProvider();
  const id = await ensureNodeIdentity({ storageProvider: provider });
  const derived = deriveRelayIdentity(id.nodePublicKeyB64);
  assert.equal(id.relayKeyId, derived.relayKeyId);
  assert.match(id.relayKeyId, /^rez:relay:[0-9a-f]{64}$/);
  assert.equal(id.nodeKeyId, derived.nodeKeyId);
  // Stable across boots, and never persisted (derivable).
  const again = await ensureNodeIdentity({ storageProvider: provider });
  assert.equal(again.relayKeyId, id.relayKeyId);
  const stored = await provider.getKeyValueStore().get("substrate:nodeIdentity:v1");
  assert.equal(Object.prototype.hasOwnProperty.call(stored, "relayKeyId"), false,
    "relayKeyId is derived on load, not persisted");
});

test("ADR-RELAY-IDENTITY: renaming device/account metadata does not change relay identity", async () => {
  const minted = makeRelayIdentity();
  const a = await ensureNodeIdentity({
    storageProvider: null,
    configuredIdentity: { ...PARTIAL, ...pickMeshAuth(minted) },
  });
  const b = await ensureNodeIdentity({
    storageProvider: null,
    configuredIdentity: {
      accountId: "rez:node:renamed", deviceId: "dev:renamed", localInboxId: "inbox:renamed",
      ...pickMeshAuth(minted),
    },
  });
  assert.equal(a.relayKeyId, b.relayKeyId);
  assert.equal(a.nodeKeyId, b.nodeKeyId);
});

function pickMeshAuth(id) {
  return {
    nodeKeyId: id.nodeKeyId,
    nodePublicKeyB64: id.nodePublicKeyB64,
    nodePrivateKeyB64: id.nodePrivateKeyB64,
  };
}
