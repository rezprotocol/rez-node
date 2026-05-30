import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider, OnionKeyRecordV1, RelayDescriptorV1 } from "@rezprotocol/core";

import { RelayStore } from "../src/network/RelayStore.js";

function makeDescriptor({ relayKeyId, nowMs, port }) {
  return new RelayDescriptorV1({
    relayKeyId,
    endpoints: [{ host: "127.0.0.1", port }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: `k-${relayKeyId}`,
        publicKeyBytes: new Uint8Array(32).fill(7),
        format: "raw",
        createdAt: nowMs - 500,
        notBefore: nowMs - 500,
        notAfter: nowMs + 60_000,
        status: "active",
      }),
    ],
    expiresAt: nowMs + 60_000,
    nowMs,
  }).toJSON();
}

test("RelayStore persists non-expired descriptors across instances", async () => {
  const nowMs = Date.now();
  const storageProvider = new MemoryStorageProvider();
  const descriptor = makeDescriptor({
    relayKeyId: "node-dev:persisted",
    nowMs,
    port: 9001,
  });

  const store1 = new RelayStore({
    storageProvider,
    nowMs: () => nowMs,
  });
  store1.upsertDescriptor(descriptor, {
    source: "peer",
    receivedAtMs: nowMs,
  });
  await store1.flushPersistence();

  const store2 = new RelayStore({
    storageProvider,
    nowMs: () => nowMs,
  });
  await store2.hydratePersistentDescriptors();

  const loaded = store2.getDescriptor("node-dev:persisted", { nowMs });
  assert.ok(loaded);
  assert.equal(loaded.relayKeyId, "node-dev:persisted");
  assert.equal(store2.getAll().length, 1);
});
