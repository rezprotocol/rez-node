import test from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { KBucketTable } from "../src/routing/dht/KBucketTable.js";
import { DhtValueStore } from "../src/routing/dht/DhtValueStore.js";
import { DhtProtocol } from "../src/routing/dht/DhtProtocol.js";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";
import { makeSignedRouteEntry } from "./support/dhtRouteEntry.js";

/**
 * docs/SECURITY_AUDIT.md pass-3 observation: "Delegation-rebroadcast-overwrite,
 * no monotonic version."
 *
 * DhtValueStore.store was previously last-write-wins. If a claimant
 * rotated from nodeA to nodeB and issued a new delegation, the old nodeA
 * delegation was still cryptographically valid until its own
 * `expiresAtMs`. A peer that observed the old delegation could rebroadcast it
 * via `dht.store`, overwriting the current nodeB entry. Receivers would
 * route to nodeA (dead, or serving stale state) instead of nodeB.
 *
 * Fix: DhtValueStore.store now rejects an incoming routeEntry whose
 * `registration.issuedAtMs` is older than the existing entry's.
 */

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

test("monotonicity: a newer delegation overwrites an older one", () => {
  const store = new DhtValueStore();
  const t = Date.now();
  const older = { registration: { issuedAtMs: 100 }, deliveryRelayKeyId: "relay-A" };
  const newer = { registration: { issuedAtMs: 200 }, deliveryRelayKeyId: "relay-B" };
  assert.deepEqual(store.store("inbox:x", older, t), { stored: true, reason: null });
  assert.deepEqual(store.store("inbox:x", newer, t), { stored: true, reason: null });
  assert.equal(store.get("inbox:x", t).deliveryRelayKeyId, "relay-B");
});

test("monotonicity: an older delegation is rejected when a newer one is already stored", () => {
  const store = new DhtValueStore();
  const t = Date.now();
  const older = { registration: { issuedAtMs: 100 }, deliveryRelayKeyId: "relay-A" };
  const newer = { registration: { issuedAtMs: 200 }, deliveryRelayKeyId: "relay-B" };
  store.store("inbox:x", newer, t);
  const result = store.store("inbox:x", older, t);
  assert.equal(result.stored, false);
  assert.equal(result.reason, "older-delegation");
  assert.equal(store.get("inbox:x", t).deliveryRelayKeyId, "relay-B", "newer entry preserved");
});

test("monotonicity: equal issuedAtMs is allowed (idempotent re-store)", () => {
  const store = new DhtValueStore();
  const t = Date.now();
  const entry = { registration: { issuedAtMs: 100 }, deliveryRelayKeyId: "relay-A" };
  store.store("inbox:x", entry, t);
  const result = store.store("inbox:x", entry, t + 1000);
  assert.equal(result.stored, true);
});

test("monotonicity: missing issuedAtMs on either side falls back to last-write-wins (defensive)", () => {
  const store = new DhtValueStore();
  const t = Date.now();
  const noStamp = { deliveryRelayKeyId: "relay-A" };
  const withStamp = { registration: { issuedAtMs: 100 }, deliveryRelayKeyId: "relay-B" };
  store.store("inbox:x", withStamp, t);
  assert.equal(store.store("inbox:x", noStamp, t).stored, true, "no incoming stamp → can't compare → admit");
  assert.equal(store.get("inbox:x", t).deliveryRelayKeyId, "relay-A");
});

test("end-to-end via DhtProtocol: rebroadcast of older delegation is rejected", async () => {
  const { registry, valueStore } = createProtocol();
  // Build TWO claimant-signed delegations for the same inbox, with different
  // issuedAtMs. (They have different claimant keypairs because the helper
  // generates a fresh kp per call, but the monotonicity check operates on
  // issuedAtMs regardless of who signed it — that's the right behavior: a
  // hostile peer doesn't know which claimant identity rotated, only that
  // they have a stale entry.)
  const peer = { id: "peer-mallory" };
  const t = Date.now();
  const earlier = makeSignedRouteEntry({ inboxId: "inbox:rotate", deliveryRelayKeyId: "relay-old", issuedAtMs: t });
  const later = makeSignedRouteEntry({ inboxId: "inbox:rotate", deliveryRelayKeyId: "relay-new", issuedAtMs: t + 1000 });

  // The "current" delegation lands first.
  await registry.dispatch("dht.store", { _ctl: "dht.store", inboxId: "inbox:rotate", routeEntry: later.routeEntry }, peer);
  assert.equal(valueStore.get("inbox:rotate", Date.now()).deliveryRelayKeyId, "relay-new");

  // Mallory rebroadcasts the older one.
  await registry.dispatch("dht.store", { _ctl: "dht.store", inboxId: "inbox:rotate", routeEntry: earlier.routeEntry }, peer);
  assert.equal(
    valueStore.get("inbox:rotate", Date.now()).deliveryRelayKeyId,
    "relay-new",
    "the older delegation must not overwrite the newer one",
  );
});
