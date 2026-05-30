import test from "node:test";
import assert from "node:assert/strict";
import { RelayStore } from "../src/network/RelayStore.js";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";

function descriptor({ relayKeyId, nowMs, expiresAt }) {
  return new RelayDescriptorV1({
    relayKeyId,
    endpoints: [{ host: "127.0.0.1", port: 1000 }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: `${relayKeyId}-onion`,
        publicKeyBytes: new Uint8Array(32).fill(1),
        format: "raw",
        createdAt: nowMs - 1000,
        notBefore: nowMs - 1000,
        notAfter: nowMs + 60_000,
        status: "active",
      }),
    ],
    expiresAt,
    nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  });
}

test("RelayStore mergeDescriptors prefers newer descriptor metadata and returns sanitized peer snapshot", () => {
  const nowMs = Date.now();
  const store = new RelayStore();

  const oldDesc = descriptor({ relayKeyId: "relay-a", nowMs, expiresAt: nowMs + 1000 });
  const newDesc = descriptor({ relayKeyId: "relay-a", nowMs, expiresAt: nowMs + 2000 });

  const first = store.mergeDescriptors([oldDesc], { source: "seed", receivedAtMs: nowMs });
  assert.equal(first.accepted, 1);
  const second = store.mergeDescriptors([newDesc], { source: "gossip", receivedAtMs: nowMs + 10 });
  assert.equal(second.accepted, 1);

  const descriptors = store.listDescriptors({ nowMs });
  assert.equal(descriptors.length, 1);
  assert.equal(descriptors[0].expiresAt, newDesc.expiresAt);

  const peers = store.snapshotPeers({ nowMs, failureThreshold: 8 });
  assert.equal(peers.length, 1);
  assert.equal(peers[0].nodeId, "relay-a");
  assert.equal(peers[0].source, "gossip");
  assert.equal(peers[0].health, "healthy");
});

test("RelayStore getSelfDescriptor returns descriptor for source self", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const selfDesc = descriptor({ relayKeyId: "self-relay", nowMs, expiresAt: nowMs + 60_000 });
  store.upsertDescriptor(selfDesc.toJSON ? selfDesc.toJSON() : selfDesc, { source: "self", receivedAtMs: nowMs });

  const got = store.getSelfDescriptor({ nowMs });
  assert.ok(got, "should return a descriptor");
  assert.equal(got.relayKeyId, "self-relay");
});

test("RelayStore getSelfDescriptor returns null when no self record", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const desc = descriptor({ relayKeyId: "other", nowMs, expiresAt: nowMs + 60_000 });
  store.upsertDescriptor(desc.toJSON ? desc.toJSON() : desc, { source: "discovery", receivedAtMs: nowMs });

  assert.equal(store.getSelfDescriptor({ nowMs }), null);
});

test("RelayStore getSelfDescriptor returns null when self descriptor expired", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const selfDesc = descriptor({ relayKeyId: "self-relay", nowMs, expiresAt: nowMs + 1000 });
  store.upsertDescriptor(selfDesc.toJSON ? selfDesc.toJSON() : selfDesc, { source: "self", receivedAtMs: nowMs });

  assert.ok(store.getSelfDescriptor({ nowMs }), "should return when not expired");
  assert.equal(store.getSelfDescriptor({ nowMs: nowMs + 2000 }), null, "should return null when expired");
});

test("RelayStore listDescriptors excludes descriptors with no usable onion key at nowMs", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const descWithValidKey = descriptor({ relayKeyId: "relay-valid", nowMs, expiresAt: nowMs + 60_000 });
  const descWithExpiredKeys = new RelayDescriptorV1({
    relayKeyId: "relay-expired-keys",
    endpoints: [{ host: "127.0.0.1", port: 1000 }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: "k1",
        publicKeyBytes: new Uint8Array(32).fill(2),
        format: "raw",
        createdAt: nowMs - 2000,
        notBefore: nowMs - 2000,
        notAfter: nowMs - 1000,
        status: "active",
      }),
    ],
    expiresAt: nowMs + 60_000,
    nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  });
  store.mergeDescriptors([descWithValidKey, descWithExpiredKeys], { source: "discovery", receivedAtMs: nowMs });

  const list = store.listDescriptors({ nowMs });
  assert.equal(list.length, 1, "only descriptor with usable key should be returned");
  assert.equal(list[0].relayKeyId, "relay-valid");
});

test("RelayStore getSelfDescriptor returns null when self has no usable onion key at nowMs", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const selfDescNoKey = new RelayDescriptorV1({
    relayKeyId: "self-relay",
    endpoints: [{ host: "127.0.0.1", port: 1000 }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: "k1",
        publicKeyBytes: new Uint8Array(32).fill(3),
        format: "raw",
        createdAt: nowMs - 2000,
        notBefore: nowMs - 2000,
        notAfter: nowMs - 1000,
        status: "active",
      }),
    ],
    expiresAt: nowMs + 60_000,
    nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  });
  store.upsertDescriptor(selfDescNoKey.toJSON ? selfDescNoKey.toJSON() : selfDescNoKey, {
    source: "self",
    receivedAtMs: nowMs,
  });

  assert.equal(store.getSelfDescriptor({ nowMs }), null, "should not return self when no key valid at nowMs");
});

test("RelayStore preserves tls on legacy configured relay endpoints", () => {
  const store = new RelayStore();
  store.load([
    { id: "relay-tls", host: "relay.example", port: 443, tls: true, transport: "tcp" },
  ]);

  const rows = store.getAll();
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0].endpoint, { host: "relay.example", port: 443, tls: true });
});
