// TRUST-7: a config-supplied relay node-key pin is preserved and exposed only for
// CONFIG-trusted bindings (never gossip/discovery), so the connection pool can
// assert an upstream relay presents exactly the operator-pinned identity key.

import test from "node:test";
import assert from "node:assert/strict";
import { RelayStore } from "../src/network/RelayStore.js";
import { makeSignedDescriptor } from "./support/relayIdentity.js";

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
  // A validly bound AND validly signed descriptor (re-audit R1: admission now
  // requires the signature) so it is actually admitted — the pin check must
  // then still refuse to treat a gossiped key as a pin.
  const { identity, descriptor } = makeSignedDescriptor({ nowMs });
  const results = store.mergeDescriptors([descriptor], { source: "gossip", receivedAtMs: nowMs });
  assert.equal(results.accepted, 1, "validly bound gossip descriptor should be admitted");
  // Even though the gossiped descriptor carries a node key, it must NOT be a pin.
  assert.equal(store.getPinnedNodePublicKeyB64(identity.relayKeyId), "");
});
