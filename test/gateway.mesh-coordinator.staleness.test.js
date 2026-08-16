import test from "node:test";
import assert from "node:assert/strict";
import { RelayStore } from "../src/network/RelayStore.js";
import { MeshCoordinator } from "../src/gateway/MeshCoordinator.js";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";

test("RelayStore.evictExpired removes entries with no usable onion key", () => {
  const nowMs = Date.now();
  const expiredKey = new OnionKeyRecordV1({
    onionKeyId: "expired",
    publicKeyBytes: new Uint8Array(32),
    format: "raw",
    createdAt: nowMs - 2000,
    notBefore: nowMs - 2000,
    notAfter: nowMs - 1000,
    status: "active",
  });
  const validKey = new OnionKeyRecordV1({
    onionKeyId: "valid",
    publicKeyBytes: new Uint8Array(32).fill(1),
    format: "raw",
    createdAt: nowMs - 100,
    notBefore: nowMs - 100,
    notAfter: nowMs + 60_000,
    status: "active",
  });
  const expiredDesc = new RelayDescriptorV1({
    relayKeyId: "expired-relay",
    endpoints: [{ host: "127.0.0.1", port: 1 }],
    onionKeys: [expiredKey],
    expiresAt: nowMs + 60_000,
    nowMs,
  }).toJSON();
  const validDesc = new RelayDescriptorV1({
    relayKeyId: "valid-relay",
    endpoints: [{ host: "127.0.0.1", port: 2 }],
    onionKeys: [validKey],
    expiresAt: nowMs + 60_000,
    nowMs,
  }).toJSON();

  const store = new RelayStore();
  store.upsertDescriptor(expiredDesc, { source: "discovery", receivedAtMs: nowMs });
  store.upsertDescriptor(validDesc, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(store.getAll().length, 2);
  assert.equal(store.listDescriptors({ nowMs }).length, 1);

  const evicted = store.evictExpired({ nowMs });
  assert.equal(evicted, 1);
  assert.equal(store.getAll().length, 1);
  assert.equal(store.listDescriptors({ nowMs }).length, 1);
  assert.equal(store.getAll()[0].relayKeyId, "valid-relay");
});

test("RelayStore.evictExpired removes entries with expired descriptor envelope", () => {
  const nowMs = Date.now();
  const key = new OnionKeyRecordV1({
    onionKeyId: "k",
    publicKeyBytes: new Uint8Array(32),
    format: "raw",
    createdAt: nowMs - 100,
    notBefore: nowMs - 100,
    notAfter: nowMs + 60_000,
    status: "active",
  });
  // P2 canonical admission rejects an already-expired descriptor outright, so
  // the eviction path is set up with a controllable clock: valid at insert,
  // expired at evict.
  const desc = new RelayDescriptorV1({
    relayKeyId: "stale-envelope",
    endpoints: [{ host: "127.0.0.1", port: 1 }],
    onionKeys: [key],
    expiresAt: nowMs - 1,
    nowMs: nowMs - 1000,
  }).toJSON();

  const store = new RelayStore({ nowMs: () => nowMs - 1000 });
  store.upsertDescriptor(desc, { source: "discovery", receivedAtMs: nowMs - 2000 });
  assert.equal(store.getAll().length, 1);

  const evicted = store.evictExpired({ nowMs });
  assert.equal(evicted, 1);
  assert.equal(store.getAll().length, 0);
});

test("MeshCoordinator refresh evicts stale descriptors and updates routeStats.evicted", async () => {
  let nowMs = Date.now();
  const validKey = new OnionKeyRecordV1({
    onionKeyId: "valid",
    publicKeyBytes: new Uint8Array(32).fill(1),
    format: "raw",
    createdAt: nowMs - 100,
    notBefore: nowMs - 100,
    notAfter: nowMs + 60_000,
    status: "active",
  });
  const validDesc = new RelayDescriptorV1({
    relayKeyId: "fresh-relay",
    endpoints: [{ host: "127.0.0.1", port: 2 }],
    onionKeys: [validKey],
    expiresAt: nowMs + 60_000,
    nowMs,
  }).toJSON();

  const relayStore = new RelayStore();
  // Pre-populate with a fresh descriptor (simulating TCP gossip)
  relayStore.upsertDescriptor(validDesc, { source: "gossip", receivedAtMs: nowMs });

  const expiredKey = new OnionKeyRecordV1({
    onionKeyId: "expired",
    publicKeyBytes: new Uint8Array(32),
    format: "raw",
    createdAt: nowMs - 2000,
    notBefore: nowMs - 2000,
    notAfter: nowMs - 1000,
    status: "active",
  });
  const staleDesc = new RelayDescriptorV1({
    relayKeyId: "stale-relay",
    endpoints: [{ host: "127.0.0.1", port: 1 }],
    onionKeys: [expiredKey],
    expiresAt: nowMs + 60_000,
    nowMs: nowMs - 1000,
  }).toJSON();
  relayStore.upsertDescriptor(staleDesc, { source: "discovery", receivedAtMs: nowMs - 2000 });
  assert.equal(relayStore.getAll().length, 2);

  const coordinator = new MeshCoordinator({
    relayStore,
    nowMs: () => nowMs,
    meshConfig: {
      enabled: true,
      seeds: [],
      discoveryIntervalMs: 10_000,
    },
  });

  await coordinator.refresh();

  assert.equal(relayStore.getAll().length, 1, "stale relay evicted, only fresh from gossip remains");
  assert.equal(relayStore.getAll()[0].relayKeyId, "fresh-relay");
  const status = coordinator.getStatus();
  assert.equal(typeof status.routeStats.evicted, "number");
  assert.ok(status.routeStats.evicted >= 1, "at least one descriptor evicted");
});
