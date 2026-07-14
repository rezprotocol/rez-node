import test from "node:test";
import assert from "node:assert/strict";

import { GatewaySession } from "../src/protocol/GatewaySession.js";

// audit R4 L2c review round-7 P2: session authentication must be serialized. WS
// message callbacks are not ordered, so two authenticate frames can arrive for one
// connection and, without a guard, both capture the same one-time challenge and both
// verify/adopt (repeated crypto + authority/PG work, duplicate session.ready,
// nondeterministic authority). The challenge is consumed atomically before the first
// await and a competing frame is refused, so exactly one verification and one
// adoption occur.

function fakeWs() {
  const closes = [];
  return {
    closes,
    OPEN: 1, readyState: 1,
    send() {}, on() {}, once() {}, off() {}, removeListener() {},
    close(code, reason) { closes.push({ code, reason }); },
  };
}

function primePendingChallenge(session) {
  session._pendingSessionAuth = {
    challengeId: "c1", nonceB64: "AA", nodeKeyId: "nk", nodePublicKeyB64: "np",
    relayKeyId: "rk", accountIdentityPublicKeyB64: "acct", sessionDeviceId: "rez:dev:x",
    wsPath: "/ws", expiresAtMs: Date.now() + 60_000,
  };
}

test("two concurrent authenticate frames verify + adopt the one-time challenge EXACTLY once", async () => {
  let verifyCalls = 0;
  let adoptCalls = 0;
  const errors = [];
  const session = new GatewaySession({ runtime: {}, ws: fakeWs() });
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  // A DEFERRED verifier: it yields before resolving, so both frames genuinely race
  // across the async boundary — the second must still be refused.
  session._verifyDirectSessionAuth = async () => {
    verifyCalls += 1;
    await Promise.resolve();
    return { ok: true, mode: "direct", accountIdentityPublicKeyB64: "acct" };
  };
  // Stub adoption + the build so the test isolates the concurrency guard, not the
  // ready-payload internals.
  session._adoptAuthenticatedSession = async () => { adoptCalls += 1; };
  primePendingChallenge(session);
  const body = { challengeId: "c1", signatureB64: "AAAA" };

  // Fire both frames without awaiting between them — the classic un-serialized race.
  const p1 = session._handleSessionAuthenticate("r1", body);
  const p2 = session._handleSessionAuthenticate("r2", body);
  await Promise.all([p1, p2]);

  assert.equal(verifyCalls, 1, "the one-time challenge is verified exactly once");
  assert.equal(adoptCalls, 1, "the session is adopted exactly once");
  assert.equal(session._pendingSessionAuth, null, "the challenge was consumed");
  assert.equal(session._sessionAuthInFlight, false, "the in-flight slot is released after completion");
  assert.ok(
    errors.some((e) => e.message === "session authentication already in progress"),
    "the racing frame was explicitly refused",
  );
});

test("a hello racing an IN-FLIGHT authenticate installs NO stale challenge (round-8 TOCTOU)", async () => {
  // The reported ordering: authenticate claims + consumes the challenge and parks at
  // verification; a second hello arrives mid-verification. With a check-only guard the
  // hello would pass its (pre-await) check, sign, and publish a fresh challenge onto
  // the session that authentication is about to adopt — leaving a stale pending
  // challenge on an authenticated socket. Holding the slot across signing refuses it.
  let adoptCalls = 0;
  const errors = [];
  const session = new GatewaySession({ runtime: {}, ws: fakeWs() });
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  let releaseVerify;
  session._verifyDirectSessionAuth = () => new Promise((resolve) => {
    releaseVerify = () => resolve({ ok: true, mode: "direct", accountIdentityPublicKeyB64: "acct" });
  });
  session._adoptAuthenticatedSession = async () => { adoptCalls += 1; };
  primePendingChallenge(session);

  // authenticate consumes the challenge + claims the slot, then parks at verify.
  const authP = session._handleSessionAuthenticate("auth-1", { challengeId: "c1", signatureB64: "AAAA" });
  // A racing hello for a DIFFERENT account arrives mid-verification.
  await session._beginSessionAuthentication({ sessionDeviceId: "rez:dev:y", accountIdentityPublicKeyB64: "acct-new" }, "hello-2");

  assert.equal(session._pendingSessionAuth, null, "the racing hello did NOT install a new challenge mid-auth");
  assert.ok(errors.some((e) => e.message === "session authentication already in progress"), "the racing hello was refused");

  releaseVerify();
  await authP;
  assert.equal(adoptCalls, 1, "authenticated exactly once");
  assert.equal(session._pendingSessionAuth, null, "no stale challenge remains on the authenticated session");
  assert.equal(session._sessionAuthInFlight, false, "the slot is released after completion");
});

test("a session.hello racing an in-flight authentication is refused (challenge not reset)", async () => {
  const errors = [];
  const session = new GatewaySession({ runtime: {}, ws: fakeWs() });
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  session._sessionAuthInFlight = true; // an authentication is already running

  await session._beginSessionAuthentication({ sessionDeviceId: "rez:dev:x", accountIdentityPublicKeyB64: "acct" }, "hello-2");

  assert.equal(session._pendingSessionAuth, null, "no new challenge was minted");
  assert.ok(
    errors.some((e) => e.message === "session authentication already in progress"),
    "the racing hello was refused",
  );
});
