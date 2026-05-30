import test from "node:test";
import assert from "node:assert/strict";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { createClaimantNodeDelegation, createSessionIdentity } from "./helpers/wsAuth.js";

/**
 * docs/SECURITY_AUDIT.md MED-8 — `_handleRoute` previously accepted any
 * hops>0 entry from any authenticated peer relay. An attacker peer could
 * advertise itself as the next hop for any inboxId at any hop count,
 * intercepting / DoSing deposits. Cross-mesh discovery is the DHT's job
 * (HIGH-8-anchored); gossip now only carries hops=0 owner-host
 * announcements.
 */

function makeSocket(label) {
  return { id: label, destroyed: false };
}

function setup() {
  const directory = new RelayPeerDirectory();
  const router = new InboxRouter({
    relayPeerDirectory: directory,
    selfRelayKeyId: "relay-self",
    logger: { error: () => {}, warn: () => {}, info: () => {}, log: () => {} },
  });
  return { router, directory };
}

function authPeerRelay(directory, socket, relayKeyId) {
  const auth = {
    relayKeyId,
    nodeKeyId: relayKeyId,
    nodePublicKeyB64: `${relayKeyId}-pub`,
    authLevel: "relay-verified",
  };
  directory.authenticate(socket, auth);
  return auth;
}

test("MED-8: inbox.route with hops>0 is rejected (transitive trust killed)", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-mallory");
  authPeerRelay(directory, peerSocket, "relay-mallory");

  const handled = router.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:alice",
      hops: 2,
      nextHopRelayKeyId: "relay-mallory",
      deliveryRelayKeyId: "relay-real",
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  assert.equal(router._routeTable.get("inbox:alice"), null,
    "transitive hops=2 advertisement must NOT install");
});

test("MED-8: inbox.route with hops=1 from an authenticated peer is also rejected", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-x");
  authPeerRelay(directory, peerSocket, "relay-x");

  const handled = router.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:target",
      hops: 1,
      nextHopRelayKeyId: "relay-x",
      deliveryRelayKeyId: "relay-other",
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  assert.equal(router._routeTable.get("inbox:target"), null);
});

test("MED-8: inbox.route with hops=0 + valid registration is still accepted (the legit path)", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-host");
  const auth = authPeerRelay(directory, peerSocket, "relay-host");

  const claimant = createSessionIdentity();
  const registration = createClaimantNodeDelegation({
    claimantIdentity: claimant,
    inboxId: "inbox:hosted",
    nodeKeyId: auth.nodeKeyId,
    nodePublicKeyB64: auth.nodePublicKeyB64,
    relayKeyId: auth.relayKeyId,
  });

  const handled = router.handleControlMessage({
    _ctl: "inbox.route",
    entries: [{
      inboxId: "inbox:hosted",
      hops: 0,
      nextHopRelayKeyId: "relay-host",
      deliveryRelayKeyId: "relay-host",
      registration,
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  const route = router._routeTable.get("inbox:hosted");
  assert.ok(route, "hops=0 with valid registration must still install");
  assert.equal(route.deliveryRelayKeyId, "relay-host");
});

test("MED-8: a malicious peer mixing valid hops=0 + a transitive hops=1 hijack — only the valid one installs", async () => {
  // Real attack shape: Mallory hosts ONE legit inbox, then bundles in a
  // hops=1 entry claiming to be the next hop for inbox:victim. Without
  // MED-8, both would install; the inbox:victim hijack would succeed.
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-mixed");
  const auth = authPeerRelay(directory, peerSocket, "relay-mixed");

  const claimant = createSessionIdentity();
  const legitRegistration = createClaimantNodeDelegation({
    claimantIdentity: claimant,
    inboxId: "inbox:legit",
    nodeKeyId: auth.nodeKeyId,
    nodePublicKeyB64: auth.nodePublicKeyB64,
    relayKeyId: auth.relayKeyId,
  });

  const handled = router.handleControlMessage({
    _ctl: "inbox.route",
    entries: [
      {
        inboxId: "inbox:legit",
        hops: 0,
        nextHopRelayKeyId: "relay-mixed",
        deliveryRelayKeyId: "relay-mixed",
        registration: legitRegistration,
      },
      {
        inboxId: "inbox:victim",
        hops: 1,
        nextHopRelayKeyId: "relay-mixed",
        deliveryRelayKeyId: "relay-real-victim-host",
      },
    ],
  }, peerSocket);
  await Promise.resolve(handled);

  assert.ok(router._routeTable.get("inbox:legit"), "legit hops=0 should install");
  assert.equal(router._routeTable.get("inbox:victim"), null,
    "transitive hops=1 hijack must NOT install");
});
