/**
 * P1.3 adversarial tests — ADR-RELAY-IDENTITY enforcement at admission points.
 * A relayKeyId is valid only as the self-certifying identity of the presented
 * node key; every admission surface fails closed on a broken binding.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { validateRelayIdentityBinding, isCanonicalRelayKeyId } from "@rezprotocol/core";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { RelayStore } from "../src/network/RelayStore.js";
import { HostedInboxRegistry } from "../src/app/HostedInboxRegistry.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { makeRelayIdentity, makeSignedDescriptor } from "./support/relayIdentity.js";
import { GOLDEN_INBOX_ID, GOLDEN_INBOX_DHT_POSITION_HEX } from "../../rez-core/test/support/goldenVectors.js";

function fakeSocket() {
  return { destroyed: false, write() { return true; } };
}

test("peer directory refuses a relay auth whose relayKeyId belongs to a different key", () => {
  const a = makeRelayIdentity();
  const b = makeRelayIdentity();
  const dir = new RelayPeerDirectory();

  // Correct binding authenticates.
  const ok = dir.authenticate(fakeSocket(), {
    relayKeyId: a.relayKeyId, nodeKeyId: a.nodeKeyId, nodePublicKeyB64: a.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  assert.ok(ok && ok.authenticated === true);

  // Same relay ID presented by a SECOND key: binding cannot hold.
  const stolen = dir.authenticate(fakeSocket(), {
    relayKeyId: a.relayKeyId, nodeKeyId: b.nodeKeyId, nodePublicKeyB64: b.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  assert.equal(stolen, null);

  // Wrong nodeKeyId with the right public key: rejected.
  const wrongNodeKey = dir.authenticate(fakeSocket(), {
    relayKeyId: a.relayKeyId, nodeKeyId: b.nodeKeyId, nodePublicKeyB64: a.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  assert.equal(wrongNodeKey, null);

  // Leaf nodes (no relayKeyId) are unaffected.
  const leaf = dir.authenticate(fakeSocket(), {
    relayKeyId: null, nodeKeyId: b.nodeKeyId, nodePublicKeyB64: b.nodePublicKeyB64,
    authLevel: "node",
  });
  assert.ok(leaf && leaf.authenticated === true);
});

test("descriptor admission rejects an identity-conflicting descriptor and accepts a bound one", () => {
  const nowMs = Date.now();
  const store = new RelayStore();

  const { identity, descriptor } = makeSignedDescriptor({ nowMs });
  const accepted = store.upsertDescriptor(descriptor, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(accepted.accepted, true);

  // Descriptor whose meta.node key belongs to A but claims B's relayKeyId.
  const other = makeRelayIdentity();
  const forged = { ...descriptor, relayKeyId: other.relayKeyId };
  const rejected = store.upsertDescriptor(forged, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(rejected.accepted, false);
  // P2 layered admission: with key material present the canonical schema
  // validator owns the binding check, so the rejection surfaces as
  // descriptor-invalid; the store-level relay-identity-binding check remains
  // the gate for key-LESS canonical ids (below).
  assert.match(rejected.reason, /^descriptor-invalid:.*binding invalid/);

  // A canonical rez:relay: id without any key material is not admissible.
  const keyless = {
    relayKeyId: identity.relayKeyId,
    endpoints: [{ host: "127.0.0.1", port: 4700 }],
    onionKeys: descriptor.onionKeys,
    expiresAt: nowMs + 60_000,
    meta: { v: 1 },
  };
  const keylessResult = store.upsertDescriptor(keyless, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(keylessResult.accepted, false);
  assert.match(keylessResult.reason, /^relay-identity-binding:/);
});

test("config pin conflicting with the derived identity cannot be admitted", () => {
  const nowMs = Date.now();
  const store = new RelayStore();
  const a = makeRelayIdentity();
  const b = makeRelayIdentity();
  // A config row that pins A's relayKeyId to B's key material must be refused
  // at admission — a bad operator pin must not become an identity.
  const { descriptor } = makeSignedDescriptor({ identity: b, nowMs });
  const conflicted = { ...descriptor, relayKeyId: a.relayKeyId };
  const result = store.upsertDescriptor(conflicted, { source: "config", receivedAtMs: nowMs });
  assert.equal(result.accepted, false);
  assert.match(result.reason, /^descriptor-invalid:.*binding invalid/);
  assert.equal(store.getPinnedNodePublicKeyB64(a.relayKeyId), "");
});

test("hosted-inbox registration naming a relay identity must carry a valid binding", async () => {
  const registryOk = new HostedInboxRegistry({});
  const id = makeRelayIdentity();
  await registryOk.add("claimant-1", {
    inboxId: "inbox:hosted:1",
    relayKeyId: id.relayKeyId,
    nodeKeyId: id.nodeKeyId,
    nodePublicKeyB64: id.nodePublicKeyB64,
    issuedAtMs: 1,
    expiresAtMs: Date.now() + 60_000,
    delegationSigB64: "c2ln",
  });
  assert.deepEqual(registryOk.getInboxIds(), ["inbox:hosted:1"]);

  const registryBad = new HostedInboxRegistry({});
  const other = makeRelayIdentity();
  await registryBad.add("claimant-2", {
    inboxId: "inbox:hosted:2",
    relayKeyId: other.relayKeyId, // someone else's ID
    nodeKeyId: id.nodeKeyId,
    nodePublicKeyB64: id.nodePublicKeyB64,
    issuedAtMs: 1,
    expiresAtMs: Date.now() + 60_000,
    delegationSigB64: "c2ln",
  });
  assert.deepEqual(registryBad.getInboxIds(), [], "identity-conflicting registration is dropped");

  // Registrations with NO relay identity remain valid local records.
  const registryLocal = new HostedInboxRegistry({});
  await registryLocal.add("claimant-3", { inboxId: "inbox:hosted:3" });
  assert.deepEqual(registryLocal.getInboxIds(), ["inbox:hosted:3"]);
});

test("restart and metadata changes preserve identity and DHT position", () => {
  const id = makeRelayIdentity();
  const pos1 = DhtNodeId.fromRelayKeyId(id.relayKeyId);
  // Identity is a pure function of the key: recomputing (a "restart") and
  // changing labels/endpoints/deviceId cannot move the DHT position.
  const again = validateRelayIdentityBinding({
    relayKeyId: id.relayKeyId, nodeKeyId: id.nodeKeyId, nodePublicKeyB64: id.nodePublicKeyB64,
  });
  assert.equal(again.ok, true);
  const pos2 = DhtNodeId.fromRelayKeyId(id.relayKeyId);
  assert.equal(pos1.hex, pos2.hex);
});

test("inbox-ID hashing semantics are untouched by relay-identity derivation (shared keyspace invariant)", () => {
  // DhtNodeId.fromRelayKeyId is also the inbox-ID hasher. Re-audit R7: the
  // expected value is a FROZEN literal, not recomputed from the function
  // under test — if the derivation is ever split or canonicalized, the inbox
  // path must keep exactly these bytes or route announce/resolve breaks for
  // every existing inbox.
  assert.equal(DhtNodeId.fromRelayKeyId(GOLDEN_INBOX_ID).hex, GOLDEN_INBOX_DHT_POSITION_HEX);
  assert.equal(isCanonicalRelayKeyId(GOLDEN_INBOX_ID), false,
    "inbox IDs are NOT canonical relay IDs — the shared hasher must not format-gate them");
});
