import test from "node:test";
import assert from "node:assert/strict";
import { MeshCoordinator } from "../src/gateway/MeshCoordinator.js";
import { RelayStore } from "../src/network/RelayStore.js";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";

function makeDescriptor({ relayKeyId, nowMs, port }) {
  return new RelayDescriptorV1({
    relayKeyId,
    endpoints: [{ host: "127.0.0.1", port }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: `k-${relayKeyId}`,
        publicKeyBytes: new Uint8Array(32).fill(2),
        format: "raw",
        createdAt: nowMs - 500,
        notBefore: nowMs - 500,
        notAfter: nowMs + 60_000,
        status: "active",
      }),
    ],
    expiresAt: nowMs + 60_000,
    nowMs,
    meta: {
      v: 1,
      capabilities: {
        transports: ["http", "tcp"],
      },
    },
  }).toJSON();
}

test("MeshCoordinator refreshes from relay store and reports status", async () => {
  const nowMs = Date.now();
  const descriptor = makeDescriptor({ relayKeyId: "relay-seed-1", nowMs, port: 9091 });
  const relayStore = new RelayStore();
  relayStore.upsertDescriptor(descriptor, { source: "gossip", receivedAtMs: nowMs });

  const coordinator = new MeshCoordinator({
    relayStore,
    nowMs: () => nowMs,
    meshConfig: {
      enabled: true,
      mode: "seeded-gossip",
      seeds: ["http://127.0.0.1:9091"],
      discoveryIntervalMs: 10_000,
      policy: { failureThreshold: 8 },
    },
  });

  await coordinator.refresh();
  const status = coordinator.getStatus();
  assert.equal(status.enabled, true);
  assert.equal(status.mode, "seeded-gossip");
  assert.equal(status.peerCount, 1);
  assert.equal(status.seedReachable["http://127.0.0.1:9091"], true);
  assert.equal(Array.isArray(status.peers), true);
  assert.equal(status.peers[0].nodeId, "relay-seed-1");
});

test("MeshCoordinator refresh replays registrations, routes, and descriptors", async () => {
  const nowMs = Date.now();
  let registrations = 0;
  let replays = 0;
  let descriptorSyncs = 0;
  let connections = 0;
  const relayStore = new RelayStore();
  const coordinator = new MeshCoordinator({
    relayStore,
    relayConnectionPool: {
      async connectToKnownRelays(records) {
        connections += 1;
        assert.deepEqual(records, []);
      },
      async updateInboxIds() {
        registrations += 1;
      },
    },
    inboxRouter: {
      reannounceAllRoutesToPeers() {
        replays += 1;
      },
    },
    nowMs: () => nowMs,
    meshConfig: {
      enabled: true,
      mode: "seeded-gossip",
      seeds: ["http://127.0.0.1:9999"],
      discoveryIntervalMs: 10_000,
      policy: { failureThreshold: 8 },
    },
  });
  coordinator.setDescriptorExchange({
    announceToAllPeers() {
      descriptorSyncs += 1;
    },
  });

  await coordinator.refresh();

  assert.equal(connections, 1);
  assert.equal(registrations, 1);
  assert.equal(replays, 1);
  assert.equal(descriptorSyncs, 1);
});

test("MeshCoordinator retries startup discovery before the full discovery interval", async () => {
  const relayStore = new RelayStore();
  relayStore.load([
    {
      id: "internet-relay",
      host: "127.0.0.1",
      port: 19091,
      transport: "tcp",
    },
  ]);

  let nowMs = Date.now();
  let connections = 0;
  const timeouts = [];
  const intervals = [];

  const relayConnectionPool = {
    connectionCount: 0,
    async connectToKnownRelays(records) {
      connections += 1;
      assert.equal(records.length, 1);
      if (connections >= 2) {
        this.connectionCount = 1;
      }
    },
  };

  const coordinator = new MeshCoordinator({
    relayStore,
    relayConnectionPool,
    nowMs: () => nowMs,
    meshConfig: {
      enabled: true,
      mode: "seeded-gossip",
      seeds: [],
      discoveryIntervalMs: 30_000,
      startupRetryIntervalMs: 1_000,
      startupRetryWindowMs: 5_000,
      policy: { failureThreshold: 8 },
    },
    setTimer(fn, ms) {
      const handle = { fn, ms, cleared: false };
      timeouts.push(handle);
      return handle;
    },
    clearTimer(handle) {
      if (!handle) return;
      handle.cleared = true;
    },
    setIntervalFn(fn, ms) {
      const handle = { fn, ms, unref() {} };
      intervals.push(handle);
      return handle;
    },
    clearIntervalFn(handle) {
      if (!handle) return;
      handle.cleared = true;
    },
  });

  await coordinator.start();

  assert.equal(connections, 1);
  assert.equal(intervals.length, 1);
  assert.equal(intervals[0].ms, 30_000);
  assert.equal(timeouts.length, 1);
  assert.equal(timeouts[0].ms, 1_000);

  nowMs += 1_000;
  await timeouts[0].fn();

  assert.equal(connections, 2);
  assert.equal(relayConnectionPool.connectionCount, 1);
  assert.equal(timeouts.length, 1);

  await coordinator.stop();
});

test("MeshCoordinator marks configured seeds reachable from active relay connections", async () => {
  const relayStore = new RelayStore();
  const coordinator = new MeshCoordinator({
    relayStore,
    relayConnectionPool: {
      async connectToKnownRelays() {},
      listActiveConnectionEndpoints() {
        return [
          { host: "r1.rezprotocol.io", port: 8443, tls: true },
        ];
      },
    },
    meshConfig: {
      enabled: true,
      mode: "seeded-gossip",
      seeds: ["http://r1.rezprotocol.io:18081"],
      discoveryIntervalMs: 30_000,
      policy: { failureThreshold: 8 },
    },
  });

  await coordinator.refresh();

  const status = coordinator.getStatus();
  assert.equal(status.seedReachable["http://r1.rezprotocol.io:18081"], true);
});
