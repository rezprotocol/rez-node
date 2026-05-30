import test from "node:test";
import assert from "node:assert/strict";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { createClaimantNodeDelegation, createSessionIdentity } from "./helpers/wsAuth.js";

/**
 * docs/SECURITY_AUDIT.md MED-7 — `_handleQueryReply` previously installed
 * any hops=0 entry as a "direct-hosted route via the replying peer"
 * without checking the claimant-signed registration that `_handleRoute`
 * required. An authenticated peer could race a query.reply for any
 * inboxId with `hops: 0, deliveryRelayKeyId: <self>` and hijack delivery
 * for that inbox at the querying relay.
 *
 * The remediation copies the verifyHostedInboxRegistration block from
 * `_handleRoute`: hops=0 entries must carry a claimant delegation that
 * names this inboxId and the same nodeKey the peer authenticated as.
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

test("MED-7: hops=0 reply WITHOUT a registration is rejected (the original bug)", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-malicious");
  authPeerRelay(directory, peerSocket, "relay-mallory");

  const handled = router.handleControlMessage({
    _ctl: "inbox.query.reply",
    queryId: "q-1",
    entries: [{
      inboxId: "inbox:victim",
      hops: 0,
      deliveryRelayKeyId: "relay-mallory",
      // NO registration field
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  // The malicious entry must NOT have been installed.
  assert.equal(router._routeTable.get("inbox:victim"), null,
    "hops=0 entry without registration must not be installed");
});

test("MED-7: hops=0 reply WITH a valid claimant-signed registration is installed", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-host");
  const auth = authPeerRelay(directory, peerSocket, "relay-host");

  const claimant = createSessionIdentity();
  const registration = createClaimantNodeDelegation({
    claimantIdentity: claimant,
    inboxId: "inbox:legit",
    nodeKeyId: auth.nodeKeyId,
    nodePublicKeyB64: auth.nodePublicKeyB64,
    relayKeyId: auth.relayKeyId,
  });

  const handled = router.handleControlMessage({
    _ctl: "inbox.query.reply",
    queryId: "q-2",
    entries: [{
      inboxId: "inbox:legit",
      hops: 0,
      deliveryRelayKeyId: "relay-host",
      registration,
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  const route = router._routeTable.get("inbox:legit");
  assert.ok(route, "valid hops=0 entry should be installed");
  assert.equal(route.deliveryRelayKeyId, "relay-host");
});

test("MED-7: hops=0 reply whose registration inboxId mismatches the entry is rejected", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-host");
  const auth = authPeerRelay(directory, peerSocket, "relay-host");

  const claimant = createSessionIdentity();
  // Registration is for inbox:A, but entry advertises it under inbox:B.
  const registration = createClaimantNodeDelegation({
    claimantIdentity: claimant,
    inboxId: "inbox:A",
    nodeKeyId: auth.nodeKeyId,
    nodePublicKeyB64: auth.nodePublicKeyB64,
    relayKeyId: auth.relayKeyId,
  });

  const handled = router.handleControlMessage({
    _ctl: "inbox.query.reply",
    queryId: "q-3",
    entries: [{
      inboxId: "inbox:B",
      hops: 0,
      deliveryRelayKeyId: "relay-host",
      registration,
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  assert.equal(router._routeTable.get("inbox:A"), null);
  assert.equal(router._routeTable.get("inbox:B"), null);
});

test("MED-7: hops=0 reply whose deliveryRelayKeyId doesn't match the peer's auth is rejected", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-mallory");
  const auth = authPeerRelay(directory, peerSocket, "relay-mallory");

  const claimant = createSessionIdentity();
  // Registration says nodeKeyId = relay-real, but Mallory claims to be relay-real
  // while authenticating as relay-mallory. Either the entry's
  // deliveryRelayKeyId mismatches peer auth, or the registration mismatches it.
  const registration = createClaimantNodeDelegation({
    claimantIdentity: claimant,
    inboxId: "inbox:rewrap",
    nodeKeyId: "relay-real",
    nodePublicKeyB64: "relay-real-pub",
    relayKeyId: "relay-real",
  });

  const handled = router.handleControlMessage({
    _ctl: "inbox.query.reply",
    queryId: "q-4",
    entries: [{
      inboxId: "inbox:rewrap",
      hops: 0,
      deliveryRelayKeyId: "relay-real",
      registration,
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  // Auth mismatch (registration nodeKeyId ≠ peer auth nodeKeyId)
  // → verifyHostedInboxRegistration returns null.
  assert.equal(router._routeTable.get("inbox:rewrap"), null);
});

test("MED-7/MED-8: hops>0 entries are rejected entirely (no transitive-trust gossip)", async () => {
  const { router, directory } = setup();
  const peerSocket = makeSocket("peer-relay");
  authPeerRelay(directory, peerSocket, "relay-peer");

  const handled = router.handleControlMessage({
    _ctl: "inbox.query.reply",
    queryId: "q-5",
    entries: [{
      inboxId: "inbox:far",
      hops: 3,
      deliveryRelayKeyId: "relay-distant",
    }],
  }, peerSocket);
  await Promise.resolve(handled);

  assert.equal(router._routeTable.get("inbox:far"), null,
    "transitive hops>0 gossip must NOT install — MED-8 (DHT is the cross-mesh discovery path)");
});
