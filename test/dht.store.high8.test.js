import test from "node:test";
import assert from "node:assert/strict";
import { bytesToBase64, canonicalJSONStringify } from "@rezprotocol/core";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtProtocol, validateStoredRouteEntry } from "../src/routing/dht/DhtProtocol.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

/**
 * docs/SECURITY_AUDIT.md HIGH-8 — `dht.store` previously accepted any
 * routeEntry from any peer relay. Any DHT-peer relay could pollute the
 * route layer with arbitrary `inboxId → delivery` mappings.
 *
 * The fix: every stored routeEntry must carry a claimant-signed
 * `registration` (validated via `verifyClaimantNodeDelegation`) whose
 * `inboxId` matches the store key and whose `nodeKeyId` matches the
 * entry's `deliveryRelayKeyId`. Tombstones are rejected outright until a
 * withdraw-proof schema lands.
 */

const CRYPTO = new NodeCryptoProvider();

function createProtocol() {
  const selfRelayKeyId = "relay-self";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);
  const valueStore = new DhtValueStore();
  const registry = new ControlMessageRegistry();
  const protocol = new DhtProtocol({
    kBuckets, valueStore, registry, selfNodeId, selfRelayKeyId,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: () => {},
    queryTimeoutMs: 500,
    nowMs: () => Date.now(),
  });
  protocol.install();
  return { protocol, registry, valueStore };
}

test("HIGH-8: dht.store accepts a routeEntry with a valid claimant-signed registration", async () => {
  const { registry, valueStore } = createProtocol();
  const { routeEntry } = makeSignedRouteEntry({
    inboxId: "inbox:legit",
    deliveryRelayKeyId: "relay-host",
    hops: 0,
  });
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:legit",
    routeEntry,
  }, { id: "p" });
  assert.deepStrictEqual(valueStore.get("inbox:legit", Date.now()), routeEntry);
});

test("HIGH-8: dht.store REJECTS a routeEntry with no registration (the original bug)", async () => {
  const { registry, valueStore } = createProtocol();
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:victim",
    routeEntry: { inboxId: "inbox:victim", hops: 0, deliveryRelayKeyId: "relay-evil" },
  }, { id: "p" });
  assert.equal(valueStore.size, 0, "unsigned routeEntry must NOT be stored");
});

test("HIGH-8: dht.store REJECTS a routeEntry whose registration inboxId mismatches the store key", async () => {
  const { registry, valueStore } = createProtocol();
  // Legitimate delegation for inbox:A, but Mallory tries to file it under inbox:B.
  const { routeEntry } = makeSignedRouteEntry({
    inboxId: "inbox:A",
    deliveryRelayKeyId: "relay-host",
    hops: 0,
  });
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:B",
    routeEntry,
  }, { id: "p" });
  assert.equal(valueStore.size, 0);
});

test("HIGH-8: dht.store REJECTS a routeEntry whose deliveryRelayKeyId doesn't match the delegation's nodeKeyId", async () => {
  const { registry, valueStore } = createProtocol();
  // Legit delegation for relay-real, rewrap as delivery via relay-evil.
  const { routeEntry } = makeSignedRouteEntry({
    inboxId: "inbox:rewrap",
    deliveryRelayKeyId: "relay-real",
    hops: 0,
  });
  const rewrapped = { ...routeEntry, deliveryRelayKeyId: "relay-evil", relayKeyId: "relay-evil" };
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:rewrap",
    routeEntry: rewrapped,
  }, { id: "p" });
  assert.equal(valueStore.size, 0);
});

test("HIGH-8: dht.store REJECTS a routeEntry whose registration signature does not verify", async () => {
  const { registry, valueStore } = createProtocol();
  const { routeEntry } = makeSignedRouteEntry({
    inboxId: "inbox:tampered",
    deliveryRelayKeyId: "relay-host",
    hops: 0,
  });
  // Tamper the issuedAtMs after signing — the embedded sig was over the
  // original payload, so verification must now fail.
  const tampered = {
    ...routeEntry,
    registration: { ...routeEntry.registration, issuedAtMs: routeEntry.registration.issuedAtMs - 1000 },
  };
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:tampered",
    routeEntry: tampered,
  }, { id: "p" });
  assert.equal(valueStore.size, 0);
});

test("HIGH-8: dht.store REJECTS an expired delegation", async () => {
  const { registry, valueStore } = createProtocol();
  // Build a delegation that's already expired.
  const claimantKp = CRYPTO.generateSigningKeyPair();
  const claimantPublicKeyB64 = bytesToBase64(claimantKp.publicKey);
  const issuedAtMs = Date.now() - 60_000;
  const expiresAtMs = Date.now() - 1_000; // already expired
  const payload = {
    kind: "inbox-node-delegation",
    inboxId: "inbox:expired",
    claimantPublicKeyB64,
    nodeKeyId: "relay-host",
    nodePublicKeyB64: "relay-host",
    issuedAtMs,
    expiresAtMs,
  };
  const msg = new TextEncoder().encode(canonicalJSONStringify(payload));
  const sig = CRYPTO.sign({ privateKey: claimantKp.privateKey, msg });
  const routeEntry = {
    inboxId: "inbox:expired",
    deliveryRelayKeyId: "relay-host",
    relayKeyId: "relay-host",
    nextHopRelayKeyId: "relay-host",
    hops: 0,
    registration: {
      inboxId: "inbox:expired",
      claimantPublicKeyB64,
      nodeKeyId: "relay-host",
      nodePublicKeyB64: "relay-host",
      issuedAtMs,
      expiresAtMs,
      delegationSigB64: bytesToBase64(sig),
    },
  };
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:expired",
    routeEntry,
  }, { id: "p" });
  assert.equal(valueStore.size, 0);
});

test("HIGH-8: tombstone (null routeEntry) from a hostile peer cannot evict a legit route", async () => {
  const { registry, valueStore } = createProtocol();
  const { routeEntry } = makeSignedRouteEntry({
    inboxId: "inbox:protected",
    deliveryRelayKeyId: "relay-real",
    hops: 0,
  });
  valueStore.store("inbox:protected", routeEntry, Date.now());

  // Mallory sends a tombstone — must be ignored.
  await registry.dispatch("dht.store", {
    _ctl: "dht.store",
    inboxId: "inbox:protected",
    routeEntry: null,
  }, { id: "mallory" });

  assert.deepStrictEqual(valueStore.get("inbox:protected", Date.now()), routeEntry);
});

test("HIGH-8: dht.find_value evicts and refuses to serve a value that fails revalidation", async () => {
  // If something slipped into the value store via a different code path
  // (eg a future bug, or a stale entry whose delegation expired), the
  // read-path revalidation drops it instead of replying with bad data.
  const { protocol, registry, valueStore } = createProtocol();
  protocol.install();

  // Plant an unsigned entry directly into the store, bypassing the
  // store-handler's validation.
  valueStore.store("inbox:rogue", { inboxId: "inbox:rogue", hops: 0, deliveryRelayKeyId: "relay-evil" }, Date.now());
  assert.equal(valueStore.size, 1);

  const replies = [];
  // Re-create the protocol so trySendFrame captures replies.
  const selfRelayKeyId = "relay-self-2";
  const selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
  const kBuckets = new KBucketTable(selfNodeId);
  const reg2 = new ControlMessageRegistry();
  const proto2 = new DhtProtocol({
    kBuckets, valueStore, registry: reg2, selfNodeId, selfRelayKeyId,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: (_s, bytes) => replies.push(JSON.parse(new TextDecoder().decode(bytes))),
    queryTimeoutMs: 500,
    nowMs: () => Date.now(),
  });
  proto2.install();

  await reg2.dispatch("dht.find_value", {
    _ctl: "dht.find_value",
    queryId: "qx",
    targetIdHex: DhtNodeId.fromRelayKeyId("inbox:rogue").hex,
    inboxId: "inbox:rogue",
  }, { id: "requester" });

  // Reply is sent (k-closest-nodes form, value=null) — NOT serving the rogue value.
  assert.equal(replies.length, 1);
  assert.equal(replies[0].value, null);
  // And the rogue entry has been evicted.
  assert.equal(valueStore.get("inbox:rogue", Date.now()), null);
});

test("HIGH-8: validateStoredRouteEntry returns null for missing fields (defensive)", () => {
  assert.equal(validateStoredRouteEntry("inbox:x", null), null);
  assert.equal(validateStoredRouteEntry("inbox:x", {}), null);
  assert.equal(validateStoredRouteEntry("inbox:x", { registration: null }), null);
  assert.equal(validateStoredRouteEntry("inbox:x", { registration: "not-an-object" }), null);
});
