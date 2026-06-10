// TRUST-7: a config-supplied relay node-key pin is preserved and exposed only for
// CONFIG-trusted bindings (never gossip/discovery), so the connection pool can
// assert an upstream relay presents exactly the operator-pinned identity key.

import test from "node:test";
import assert from "node:assert/strict";
import { RelayStore } from "../src/network/RelayStore.js";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";

test("config knownRelay nodePublicKeyB64 is preserved and exposed as a pin", () => {
  const store = new RelayStore();
  store.load([
    { id: "ws:relay1", relayKeyId: "ws:relay1", host: "r1.example", port: 8443, tls: true,
      nodeKeyId: "nodekey:abc", nodePublicKeyB64: "PINNEDKEY==" },
  ]);
  assert.equal(store.getPinnedNodePublicKeyB64("ws:relay1"), "PINNEDKEY==");
});

test("a relay with no configured pin returns empty (TOFU, unchanged)", () => {
  const store = new RelayStore();
  store.load([{ id: "ws:relay2", relayKeyId: "ws:relay2", host: "r2.example", port: 8443, tls: true }]);
  assert.equal(store.getPinnedNodePublicKeyB64("ws:relay2"), "");
  assert.equal(store.getPinnedNodePublicKeyB64("unknown"), "");
});

test("a gossiped/discovered descriptor is NOT treated as a pin", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const desc = new RelayDescriptorV1({
    relayKeyId: "relay-gossip",
    endpoints: [{ host: "127.0.0.1", port: 1000 }],
    onionKeys: [new OnionKeyRecordV1({
      onionKeyId: "relay-gossip-onion", publicKeyBytes: new Uint8Array(32).fill(1), format: "raw",
      createdAt: nowMs - 1000, notBefore: nowMs - 1000, notAfter: nowMs + 60_000, status: "active",
    })],
    expiresAt: nowMs + 60_000, nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] }, node: { keyId: "nodekey:g", publicKeyB64: "GOSSIPKEY==" } },
  });
  store.mergeDescriptors([desc], { source: "gossip", receivedAtMs: nowMs });
  // Even though the gossiped descriptor carries a node key, it must NOT be a pin.
  assert.equal(store.getPinnedNodePublicKeyB64("relay-gossip"), "");
});
