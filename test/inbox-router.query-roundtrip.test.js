import test from "node:test";
import assert from "node:assert/strict";

import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { createClaimantNodeDelegation, createSessionIdentity } from "./helpers/wsAuth.js";
import { makeRelayIdentity } from "./support/relayIdentity.js";

/**
 * The inbox.query round trip: the real `_handleQuery` output fed into the real
 * `_handleQueryReply`.
 *
 * rez-node#7 — these two halves were each well covered, but only ever against
 * HAND-AUTHORED fixtures, so they drifted apart while both suites stayed green.
 * `_handleQuery` emitted `hops: route.hops + 1` (never 0) and omitted
 * `registration` entirely; `_handleQueryReply` drops anything with `hops !== 0`
 * or without a verifiable registration. Every reply was therefore
 * un-installable and `queryRoute` resolved false 100% of the time — an
 * inert fallback that read as a working one.
 *
 * Same trap as the CHAT_BRIDGE_SPEC drift in CLAUDE.md: if two sides of a
 * protocol are only ever tested against fixtures, nothing holds them together.
 * These tests wire them to each other.
 */

const QUIET = { error: () => {}, warn: () => {}, info: () => {}, log: () => {} };

function makeSocket(label) {
  const writes = [];
  return { id: label, destroyed: false, writes, write(buf) { writes.push(Buffer.from(buf)); } };
}

function authAs(directory, socket, identity) {
  const auth = {
    relayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    authLevel: "relay-verified",
  };
  directory.authenticate(socket, auth);
  return auth;
}

/** Decode the length-prefixed control frames a socket received. */
function replyFrames(socket) {
  const out = [];
  for (const buf of socket.writes) {
    let offset = 0;
    while (offset + 4 <= buf.length) {
      const len = buf.readUInt32BE(offset);
      out.push(JSON.parse(buf.subarray(offset + 4, offset + 4 + len).toString("utf8")));
      offset += 4 + len;
    }
  }
  return out.filter((f) => f._ctl === "inbox.query.reply");
}

function makeRouter(identity) {
  const directory = new RelayPeerDirectory();
  const router = new InboxRouter({
    relayPeerDirectory: directory,
    selfRelayKeyId: identity.relayKeyId,
    logger: QUIET,
  });
  return { router, directory };
}

/** A relay that directly hosts `inboxId`, registered against its own identity. */
function hostingRelay(inboxId) {
  const identity = makeRelayIdentity({ label: "host" });
  const { router, directory } = makeRouter(identity);
  const socket = makeSocket("registrant");
  const auth = authAs(directory, socket, identity);
  const registration = createClaimantNodeDelegation({
    claimantIdentity: createSessionIdentity(),
    inboxId,
    nodeKeyId: auth.nodeKeyId,
    nodePublicKeyB64: auth.nodePublicKeyB64,
    relayKeyId: auth.relayKeyId,
  });
  const accepted = router.handleControlMessage({ _ctl: "inbox.register", registrations: [registration] }, socket);
  assert.equal(accepted, true, "precondition: the relay accepted the registration");
  return { identity, router, directory, registration };
}

/** Ask `host` for `inboxId` and return the reply it sends back. */
function askFor(host, inboxId, queryId) {
  const querierSocket = makeSocket("querier");
  authAs(host.directory, querierSocket, makeRelayIdentity({ label: "querier" }));
  host.router.handleControlMessage({ _ctl: "inbox.query", queryId, inboxIds: [inboxId] }, querierSocket);
  const replies = replyFrames(querierSocket);
  assert.equal(replies.length, 1, "a query is always answered, even when the answer is 'no'");
  return replies[0];
}

test("a reply from the hosting relay installs a route (the round trip that was broken)", async () => {
  const host = hostingRelay("inbox:X");
  const reply = askFor(host, "inbox:X", "q-1");

  assert.equal(reply.entries.length, 1);
  assert.equal(reply.entries[0].hops, 0, "the receiver drops anything but hops=0");
  assert.ok(reply.entries[0].registration, "and requires a claimant-signed registration");

  // Feed that exact reply into a real querier.
  const querier = makeRouter(makeRelayIdentity({ label: "q-self" }));
  const socketToHost = makeSocket("to-host");
  authAs(querier.directory, socketToHost, host.identity);

  const pending = querier.router.waitForQueryReply("q-1", 500);
  querier.router.setQueryExpectedReplies("q-1", 1);
  querier.router.handleControlMessage(reply, socketToHost);

  assert.equal(await pending, true, "queryRoute must report the route was found");
  const route = querier.router._routeTable.get("inbox:X");
  assert.ok(route, "and the route must actually be installed");
  assert.equal(route.deliveryRelayKeyId, host.identity.relayKeyId);
});

test("a relay answers 'not found' for a route it only knows transitively", () => {
  const identity = makeRelayIdentity({ label: "middle" });
  const { router, directory } = makeRouter(identity);
  const upstream = makeSocket("upstream");
  authAs(directory, upstream, makeRelayIdentity({ label: "upstream" }));
  // Learned second-hand: no claimant signature this relay can offer as its own.
  router.addRemoteRoute("inbox:far", {
    hops: 0,
    peerSocket: upstream,
    nextHopRelayKeyId: identity.relayKeyId,
    deliveryRelayKeyId: makeRelayIdentity({ label: "far" }).relayKeyId,
  });
  assert.ok(router._routeTable.get("inbox:far"), "precondition: the relay does know this route");

  const querierSocket = makeSocket("querier");
  authAs(directory, querierSocket, makeRelayIdentity({ label: "querier" }));
  router.handleControlMessage({ _ctl: "inbox.query", queryId: "q-2", inboxIds: ["inbox:far"] }, querierSocket);

  const reply = replyFrames(querierSocket)[0];
  assert.deepEqual(reply.entries, [],
    "the querier cannot verify a claim about a third party — MED-8 leaves that to the DHT");
});

test("an unknown inbox is answered, not ignored", () => {
  const host = hostingRelay("inbox:X");
  const reply = askFor(host, "inbox:absent", "q-3");
  assert.deepEqual(reply.entries, []);
});

test("a negative reply does NOT resolve a query another relay may still answer", async () => {
  const host = hostingRelay("inbox:X");
  const affirmative = askFor(host, "inbox:X", "q-4");

  const querier = makeRouter(makeRelayIdentity({ label: "q-self" }));
  const emptySocket = makeSocket("to-relay-without-it");
  const hostSocket = makeSocket("to-host");
  authAs(querier.directory, emptySocket, makeRelayIdentity({ label: "relay-without-it" }));
  authAs(querier.directory, hostSocket, host.identity);

  const pending = querier.router.waitForQueryReply("q-4", 500);
  querier.router.setQueryExpectedReplies("q-4", 2);

  // The relay that does NOT have it answers first — the common case, since it
  // has no work to do. Resolving here would discard the real answer in flight.
  querier.router.handleControlMessage({ _ctl: "inbox.query.reply", queryId: "q-4", entries: [] }, emptySocket);
  querier.router.handleControlMessage(affirmative, hostSocket);

  assert.equal(await pending, true, "the affirmative answer must win regardless of arrival order");
  assert.ok(querier.router._routeTable.get("inbox:X"));
});

test("once every relay has answered 'not found', the query fails without waiting out its timeout", async () => {
  const querier = makeRouter(makeRelayIdentity({ label: "q-self" }));
  const a = makeSocket("relay-a");
  const b = makeSocket("relay-b");
  authAs(querier.directory, a, makeRelayIdentity({ label: "a" }));
  authAs(querier.directory, b, makeRelayIdentity({ label: "b" }));

  // A timeout long enough that resolving via it would hang this test.
  const pending = querier.router.waitForQueryReply("q-5", 60_000);
  querier.router.setQueryExpectedReplies("q-5", 2);
  querier.router.handleControlMessage({ _ctl: "inbox.query.reply", queryId: "q-5", entries: [] }, a);
  querier.router.handleControlMessage({ _ctl: "inbox.query.reply", queryId: "q-5", entries: [] }, b);

  assert.equal(await pending, false);
});

test("a query that reached no relay fails immediately", async () => {
  const querier = makeRouter(makeRelayIdentity({ label: "q-self" }));
  const pending = querier.router.waitForQueryReply("q-6", 60_000);
  querier.router.setQueryExpectedReplies("q-6", 0);
  assert.equal(await pending, false);
});

test("replies counted before the reach is known still settle the query", async () => {
  // The sends settle after the broadcast, so a reply can beat
  // setQueryExpectedReplies. The count must be re-checked when it arrives.
  const querier = makeRouter(makeRelayIdentity({ label: "q-self" }));
  const a = makeSocket("relay-a");
  authAs(querier.directory, a, makeRelayIdentity({ label: "a" }));

  const pending = querier.router.waitForQueryReply("q-7", 60_000);
  querier.router.handleControlMessage({ _ctl: "inbox.query.reply", queryId: "q-7", entries: [] }, a);
  querier.router.setQueryExpectedReplies("q-7", 1);

  assert.equal(await pending, false);
});

test("a relay does not answer for an inbox whose registration names someone else", () => {
  // A node registers its inbox at an UPSTREAM relay: the delegation names the
  // node's own relayKeyId, not the upstream's. The upstream cannot present
  // that as its own — the querier would reject it — so it must not try.
  const upstreamIdentity = makeRelayIdentity({ label: "upstream" });
  const { router, directory } = makeRouter(upstreamIdentity);
  const nodeIdentity = makeRelayIdentity({ label: "leaf-node" });
  const nodeSocket = makeSocket("leaf");
  const auth = authAs(directory, nodeSocket, nodeIdentity);
  const registration = createClaimantNodeDelegation({
    claimantIdentity: createSessionIdentity(),
    inboxId: "inbox:leaf-hosted",
    nodeKeyId: auth.nodeKeyId,
    nodePublicKeyB64: auth.nodePublicKeyB64,
    relayKeyId: auth.relayKeyId,
  });
  router.handleControlMessage({ _ctl: "inbox.register", registrations: [registration] }, nodeSocket);
  assert.ok(router._routeTable.get("inbox:leaf-hosted"), "precondition: the upstream holds the route");

  const querierSocket = makeSocket("querier");
  authAs(directory, querierSocket, makeRelayIdentity({ label: "querier" }));
  router.handleControlMessage(
    { _ctl: "inbox.query", queryId: "q-8", inboxIds: ["inbox:leaf-hosted"] }, querierSocket,
  );

  assert.deepEqual(replyFrames(querierSocket)[0].entries, [],
    "an entry the receiver is guaranteed to drop must not be sent as if it were an answer");
});

test("MED-7 still holds: a relay cannot re-serve another relay's registration as its own", async () => {
  const host = hostingRelay("inbox:X");
  const stolen = askFor(host, "inbox:X", "q-9");

  // Mallory replays the host's genuine reply verbatim over her own socket.
  const querier = makeRouter(makeRelayIdentity({ label: "q-self" }));
  const mallorySocket = makeSocket("mallory");
  authAs(querier.directory, mallorySocket, makeRelayIdentity({ label: "mallory" }));

  const pending = querier.router.waitForQueryReply("q-9", 500);
  querier.router.setQueryExpectedReplies("q-9", 1);
  querier.router.handleControlMessage(stolen, mallorySocket);

  assert.equal(await pending, false);
  assert.equal(querier.router._routeTable.get("inbox:X"), null,
    "the registration names the host, not Mallory — hijacking delivery must stay impossible");
});
