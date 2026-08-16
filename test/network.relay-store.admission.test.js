/**
 * P2.3 — canonical descriptor admission at the RelayStore choke point
 * (ATLAS_PREREQUISITES). upsertDescriptor is the ONE gate: no ingress,
 * hydration, trust-restoration, or gossip path stores or re-emits a
 * descriptor that has not passed the canonical rez-core validator.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { RelayStore } from "../src/network/RelayStore.js";
import { makeRelayIdentity, makeSignedDescriptor } from "./support/relayIdentity.js";

const STORE_KEY = "substrate:relayStore:descriptors:v1";

function memoryStorage() {
  const kv = new Map();
  return {
    kv,
    provider: {
      getKeyValueStore() {
        return {
          async get(k) { return kv.has(k) ? kv.get(k) : undefined; },
          async set(k, v) { kv.set(k, v); },
        };
      },
    },
  };
}

test("admission rejects an expired descriptor outright (not admit-then-filter)", () => {
  const nowMs = Date.now();
  const { descriptor } = makeSignedDescriptor({ nowMs: nowMs - 7_200_000, ttlMs: 3_600_000 });
  const store = new RelayStore({ nowMs: () => nowMs });
  const result = store.upsertDescriptor(descriptor, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(result.accepted, false);
  assert.match(result.reason, /^descriptor-invalid:/);
  assert.equal(store.getAll().length, 0, "nothing was stored");
});

test("admission closes the peer.bind gap: empty onionKeys and malformed shapes are rejected", () => {
  const nowMs = Date.now();
  const { descriptor } = makeSignedDescriptor({ nowMs });
  const store = new RelayStore({ nowMs: () => nowMs });

  const noKeys = { ...descriptor, onionKeys: [] };
  assert.equal(store.upsertDescriptor(noKeys, { source: "peer-bind-tofu", receivedAtMs: nowMs }).accepted, false);

  const badEndpoint = { ...descriptor, endpoints: [{ host: "" }] };
  assert.equal(store.upsertDescriptor(badEndpoint, { source: "peer-bind-tofu", receivedAtMs: nowMs }).accepted, false);

  const unknownMeta = { ...descriptor, meta: { ...descriptor.meta, evil: true } };
  assert.equal(store.upsertDescriptor(unknownMeta, { source: "peer-bind-tofu", receivedAtMs: nowMs }).accepted, false);

  // The unmodified, validly signed descriptor is admitted.
  assert.equal(store.upsertDescriptor(descriptor, { source: "peer-bind-tofu", receivedAtMs: nowMs }).accepted, true);
});

test("admission rejects the non-empty reserved top-level capabilities blob", () => {
  const nowMs = Date.now();
  const { descriptor } = makeSignedDescriptor({ nowMs });
  const store = new RelayStore({ nowMs: () => nowMs });
  const blob = { ...descriptor, capabilities: { cpu: 64, battery: "full", nested: { deep: [1, 2, 3] } } };
  const result = store.upsertDescriptor(blob, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(result.accepted, false);
  assert.match(result.reason, /^descriptor-invalid:/);
});

test("hydration re-derives trust: a KV write cannot resurrect an operator pin or a self descriptor", async () => {
  const nowMs = Date.now();
  const { identity, descriptor } = makeSignedDescriptor({ nowMs });
  const { kv, provider } = memoryStorage();
  // Forge a persisted snapshot claiming config trust and self source.
  kv.set(STORE_KEY, {
    descriptors: [
      { descriptor, source: "config", bindingTrust: "config", receivedAtMs: nowMs },
      { descriptor: makeSignedDescriptor({ nowMs }).descriptor, source: "self", bindingTrust: "self", receivedAtMs: nowMs },
    ],
  });
  const store = new RelayStore({ storageProvider: provider, nowMs: () => nowMs });
  await store.hydratePersistentDescriptors();

  // The descriptor is admitted (it is valid) but with capped trust:
  assert.ok(store.getDescriptor(identity.relayKeyId, { nowMs }), "valid descriptor rehydrates");
  assert.equal(store.getPinnedNodePublicKeyB64(identity.relayKeyId), "",
    "persisted 'config' trust must NOT rehydrate into an operator pin");
  assert.equal(store.getSelfDescriptor({ nowMs }), null,
    "persisted 'self' source must NOT rehydrate into self authority");
});

test("re-audit R1: admission verifies the descriptor SIGNATURE — a tampered field is rejected at every ingress", () => {
  const nowMs = Date.now();
  const { descriptor } = makeSignedDescriptor({ nowMs });
  const store = new RelayStore({ nowMs: () => nowMs });

  // The reproduced attack: mutate a signed descriptor's endpoint without
  // re-signing. Shape and identity binding still pass; only the signature
  // catches it.
  const tampered = { ...descriptor, endpoints: [{ host: "attacker.invalid", port: 4900 }] };
  const result = store.upsertDescriptor(tampered, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(result.accepted, false);
  assert.equal(result.reason, "descriptor-signature:invalid");
  assert.equal(store.getAll().length, 0);

  // A descriptor carrying key material but no signature at all is rejected.
  const { sig, ...unsigned } = descriptor;
  assert.equal(sig !== undefined, true, "fixture descriptor is signed");
  const unsignedResult = store.upsertDescriptor(unsigned, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(unsignedResult.accepted, false);
  assert.equal(unsignedResult.reason, "descriptor-signature:missing");
});

test("re-audit R1: hydration cannot restore a tampered persisted descriptor as verified/gossip-eligible", async () => {
  const nowMs = Date.now();
  const good = makeSignedDescriptor({ nowMs });
  const poisoned = makeSignedDescriptor({ nowMs });
  const tamperedDescriptor = {
    ...poisoned.descriptor,
    endpoints: [{ host: "attacker.invalid", port: 4900 }],
  };
  const { kv, provider } = memoryStorage();
  kv.set(STORE_KEY, {
    descriptors: [
      { descriptor: good.descriptor, source: "gossip", bindingTrust: "verified", receivedAtMs: nowMs },
      { descriptor: tamperedDescriptor, source: "gossip", bindingTrust: "verified", receivedAtMs: nowMs },
    ],
  });
  const store = new RelayStore({ storageProvider: provider, nowMs: () => nowMs });
  await store.hydratePersistentDescriptors();

  assert.ok(store.getDescriptor(good.identity.relayKeyId, { nowMs }), "intact persisted descriptor rehydrates");
  assert.equal(store.getDescriptor(poisoned.identity.relayKeyId, { nowMs }), null,
    "tampered persisted descriptor must NOT rehydrate");
  const gossip = store.listDescriptors({ nowMs });
  assert.equal(gossip.length, 1);
  assert.equal(gossip[0].relayKeyId, good.identity.relayKeyId,
    "tampered persisted descriptor must NOT become gossip-eligible");
});

test("re-audit R1: the stored form is the CANONICAL serialization — unsigned extra top-level fields are stripped", () => {
  const nowMs = Date.now();
  const { identity, descriptor } = makeSignedDescriptor({ nowMs });
  const store = new RelayStore({ nowMs: () => nowMs });

  // Unknown top-level keys are outside both the schema allowlist's scope and
  // the signing payload; before R1 they survived verbatim into storage,
  // persistence, and re-gossip.
  const padded = { ...descriptor, pad: "x".repeat(4096), extra: { nested: true } };
  const result = store.upsertDescriptor(padded, { source: "discovery", receivedAtMs: nowMs });
  assert.equal(result.accepted, true, "signed descriptor with junk riders is still admitted");

  const stored = store.getDescriptor(identity.relayKeyId, { nowMs });
  assert.equal(Object.hasOwn(stored, "pad"), false, "junk field must not reach the store");
  assert.equal(Object.hasOwn(stored, "extra"), false);
  const gossip = store.listDescriptors({ nowMs });
  assert.equal(gossip.length, 1);
  assert.equal(Object.hasOwn(gossip[0], "pad"), false, "junk field must not be re-gossiped");
});

test("only admitted descriptors are ever re-gossiped", () => {
  const nowMs = Date.now();
  const store = new RelayStore({ nowMs: () => nowMs });
  const good = makeSignedDescriptor({ nowMs });
  const badIdentity = makeRelayIdentity();
  const forged = { ...good.descriptor, relayKeyId: badIdentity.relayKeyId };

  store.upsertDescriptor(good.descriptor, { source: "discovery", receivedAtMs: nowMs });
  store.upsertDescriptor(forged, { source: "discovery", receivedAtMs: nowMs });

  const gossip = store.listDescriptors({ nowMs });
  assert.equal(gossip.length, 1);
  assert.equal(gossip[0].relayKeyId, good.identity.relayKeyId);
});
