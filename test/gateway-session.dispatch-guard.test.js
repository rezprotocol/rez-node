import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  DeviceRegistrationV1,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
} from "@rezprotocol/core";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

// SESSION_AUTH_V5 slice 1: session identity lives in one frozen SessionPrincipal;
// tests install it through the real commit mechanism instead of poking fields.
function installAccountPrincipal(session, { owner = "acct", deviceId = "rez:dev:" + "a".repeat(64), authority }) {
  session._commitPrincipal(new SessionPrincipal({
    kind: SessionPrincipal.KINDS.ACCOUNT,
    accountPublicKeyB64: owner,
    sessionDeviceId: deviceId,
    authority,
  }));
}

const crypto = new NodeCryptoProvider();
const ISSUED = Date.now() - 1000;
const EXPIRES = Date.now() + 3_600_000;

async function genKey() {
  const kp = await crypto.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}
async function sign(priv, msg) {
  return { alg: "ed25519", sigB64: bytesToBase64(await crypto.sign({ privateKey: priv, msg })) };
}
async function buildLeafCert({ account, grantee, capabilities, issuedAtMs = ISSUED, expiresAtMs = EXPIRES }) {
  const fields = {
    v: 1, purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE, accountIdentityPublicKeyB64: account.pubB64,
    parentCertId: null, granteeDevicePublicKeyB64: grantee.pubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(grantee.pubB64), capabilities,
    maxDelegationDepth: 0, issuedAtMs, expiresAtMs, signerPublicKeyB64: account.pubB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = await sign(account.priv, AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }));
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig });
}

function fakeWs() {
  const closes = [];
  return {
    closes,
    OPEN: 1, readyState: 1,
    send() {}, on() {}, once() {}, off() {}, removeListener() {},
    close(code, reason) { closes.push({ code, reason }); },
  };
}

// A runtime device-registry stub. As of L5 review-4 finding 1 the guard NO LONGER reads a runtime
// accountDeviceRegistry — terminal status is resolved inside the coherent snapshot (via the
// serializer's own canonical registry). Tests that still wire this prove it is a harmless no-op.
const cleanRegistry = { async isTerminallyRevokedInTx() { return false; } };
// An epoch-aware cache stub. The L5 guard fast path reads currentEpoch(); the slow path (and
// connect-time admission) read resolveDelegatedSnapshot() for ONE coherent {state, epoch, terminal}.
// Counters expose which path ran. `terminal` lets a test simulate a mid-session device revoke.
function epochCache({ epoch = 0, state = { revokedCertIds: [], minValidIssuedAtMs: 0 }, terminal = false } = {}) {
  const calls = { currentEpoch: 0, resolveDelegatedSnapshot: 0 };
  return {
    calls,
    async currentEpoch() { calls.currentEpoch += 1; return epoch; },
    async resolveDelegatedSnapshot() { calls.resolveDelegatedSnapshot += 1; return { state, epoch, terminal }; },
  };
}
const cleanCache = epochCache();

// ---- Dispatch plumbing (audit R4 F3-remediation round-5 finding 1): the guard is CALLED for
// delegated sessions and, on refusal, sends UNAUTHORIZED + closes the socket without dispatching.
// The guard's own logic is tested separately below. ----
function makePlumbingSession({ mode, authorized }) {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  installAccountPrincipal(session, { authority: { mode, signerPublicKeyB64: "C", accountIdentityPublicKeyB64: "acct" } });
  let dispatched = false;
  session._registry = { async dispatch() { dispatched = true; } };
  let guardCalled = false;
  session._delegatedSessionStillAuthorized = async () => { guardCalled = true; return authorized; };
  session._frameCodec = { decodeFrame: () => ({ id: "req1", type: "peerLink.create", body: {} }) };
  return { run: () => session._handleSocketMessage(Buffer.from("{}")), ws, errors, isDispatched: () => dispatched, wasGuardCalled: () => guardCalled };
}

test("round-5 finding 1: a DELEGATED session refused by the guard is UNAUTHORIZED + closed, not dispatched", async () => {
  const s = makePlumbingSession({ mode: "delegated", authorized: false });
  await s.run();
  assert.equal(s.wasGuardCalled(), true);
  assert.equal(s.isDispatched(), false, "not forwarded to a handler");
  assert.ok(s.errors.some((e) => e.code === "UNAUTHORIZED"));
  assert.ok(s.ws.closes.some((c) => c.code === 1008), "socket closed (authority_revoked)");
});

test("round-5 finding 1: a DELEGATED session the guard authorizes is dispatched normally", async () => {
  const s = makePlumbingSession({ mode: "delegated", authorized: true });
  await s.run();
  assert.equal(s.wasGuardCalled(), true);
  assert.equal(s.isDispatched(), true);
});

test("round-5 finding 1: a DIRECT (account-root) session skips the guard and dispatches", async () => {
  const s = makePlumbingSession({ mode: "direct", authorized: false });
  await s.run();
  assert.equal(s.wasGuardCalled(), false, "guard not consulted for a direct session");
  assert.equal(s.isDispatched(), true);
});

// ---- Per-op capability enforcement (audit leaf-3c F2) ----
// After the revocation guard proves the (immutable) chain still verifies, an op that maps to a
// required capability (account.outbox.lease.* → deviceSet.publish) must find it in the frozen
// chain-derived grantedCapabilities. A MISSING capability is FORBIDDEN (socket STAYS OPEN, not a
// revocation-close); a PRESENT capability dispatches; an UNMAPPED op needs only membership.
function makeCapSession({ opType, grantedCapabilities }) {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  installAccountPrincipal(session, { authority: { mode: "delegated", signerPublicKeyB64: "C", accountIdentityPublicKeyB64: "acct", grantedCapabilities } });
  let dispatched = false;
  session._registry = { async dispatch() { dispatched = true; } };
  session._delegatedSessionStillAuthorized = async () => true; // revocation guard passes
  session._frameCodec = { decodeFrame: () => ({ id: "req1", type: opType, body: {} }) };
  return { run: () => session._handleSocketMessage(Buffer.from("{}")), ws, errors, isDispatched: () => dispatched };
}

test("leaf-3c F2: a delegated session LACKING the op's required capability is FORBIDDEN, socket stays OPEN, not dispatched", async () => {
  const s = makeCapSession({ opType: "account.outbox.lease.claim", grantedCapabilities: ["device.add"] });
  await s.run();
  assert.equal(s.isDispatched(), false, "the privileged op is NOT forwarded to a handler");
  assert.ok(s.errors.some((e) => e.code === "FORBIDDEN"), "answered FORBIDDEN (a capability denial)");
  assert.equal(s.ws.closes.length, 0, "socket STAYS OPEN — a capability denial is NOT a revocation");
});

test("leaf-3c F2: a delegated session HOLDING the required capability dispatches the op", async () => {
  const s = makeCapSession({ opType: "account.outbox.lease.claim", grantedCapabilities: ["deviceSet.publish"] });
  await s.run();
  assert.equal(s.isDispatched(), true);
  assert.ok(!s.errors.some((e) => e.code === "FORBIDDEN"));
});

test("leaf-3c F2: an UNMAPPED op (no required capability) dispatches on membership alone, even with empty capabilities", async () => {
  const s = makeCapSession({ opType: "peerLink.create", grantedCapabilities: [] });
  await s.run();
  assert.equal(s.isDispatched(), true, "unmapped ops keep the pre-existing membership-only behavior");
});

// ---- Guard logic: _delegatedSessionStillAuthorized (round-6 finding 1 + round-7 finding 2) ----
// `chainExpiresAtMs` mirrors what real admission derives from the chain (leaf-3c review-2 F1) —
// these certs are built with EXPIRES, so that is their deadline. It is NOT optional: the guard
// fails closed without it, which is the point of the F1 fix.
function guardSession({ registry, cache, certChain, signer, chainExpiresAtMs = EXPIRES, now = Date.now }) {
  const runtime = {};
  if (registry) runtime.accountDeviceRegistry = registry;
  if (cache) runtime.accountAuthorityRevocationCache = cache;
  const session = new GatewaySession({ runtime, ws: fakeWs(), now });
  installAccountPrincipal(session, { authority: { mode: "delegated", signerPublicKeyB64: signer, accountIdentityPublicKeyB64: "acct", certChain, chainExpiresAtMs } });
  return session;
}

test("round-6 finding 1: the guard fails on a revoked LEAF cert even when the device is not tombstoned", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({
    registry: cleanRegistry,
    cache: epochCache({ epoch: 1, state: { revokedCertIds: [leaf.certId], minValidIssuedAtMs: 0 } }),
    certChain: [leaf.toJSON()], signer: delegate.pubB64,
  });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  // No admitted watermark ⇒ the guard always takes the full re-verify path.
  assert.equal(await session._delegatedSessionStillAuthorized(), false);
});

test("L5 review finding 1 (fast path): an UNCHANGED epoch since admission returns true WITHOUT the heavy read/verify", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  let terminalReads = 0;
  const registry = { async isTerminallyRevokedInTx() { terminalReads += 1; return false; } };
  const cache = epochCache({ epoch: 7 });
  const session = guardSession({ registry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7; // admitted at epoch 7; nothing has changed

  assert.equal(await session._delegatedSessionStillAuthorized(), true, "epoch unchanged ⇒ still authorized");
  assert.equal(cache.calls.currentEpoch, 1, "read only the cheap epoch");
  assert.equal(cache.calls.resolveDelegatedSnapshot, 0, "did NOT do the coherent snapshot read");
  assert.equal(terminalReads, 0, "did NOT do the terminal registry read");
});

test("L5 review finding 1 (slow path): an ADVANCED epoch triggers a full re-verify that catches a mid-session revoke", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // Admitted at epoch 7; a revoke bumped the epoch to 8 and put the leaf in the revoked set.
  const cache = epochCache({ epoch: 8, state: { revokedCertIds: [leaf.certId], minValidIssuedAtMs: 0 } });
  const session = guardSession({ registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;

  assert.equal(await session._delegatedSessionStillAuthorized(), false, "the advance forced a re-verify that saw the revoke");
  assert.equal(cache.calls.resolveDelegatedSnapshot, 1, "the coherent snapshot WAS read on the advance");
});

test("L5 review finding 1 (TOCTOU fix): a terminal cert_id=NULL device (no revoked cert) is caught via the snapshot's terminal flag", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // The exploit shape: epoch advanced 7->8, the device is TERMINAL, but the cert set is CLEAN
  // (cert_id was NULL so the revoke auto-revoked nothing). The chain would still verify — only the
  // coherent snapshot's terminal flag catches it, and because terminal + epoch come from ONE
  // snapshot the watermark can never arm to 8 while terminal reads false.
  const cache = epochCache({ epoch: 8, state: { revokedCertIds: [], minValidIssuedAtMs: 0 }, terminal: true });
  const session = guardSession({ registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;

  assert.equal(await session._delegatedSessionStillAuthorized(), false, "terminal-in-snapshot rejects the revoked NULL-cert device");
  assert.notEqual(session._admittedAuthorityEpoch, 8, "the watermark was NOT armed to the revoke epoch (no poisoning)");
});

test("L5 review finding 1 (watermark advance): an epoch advance that does NOT revoke this device passes and re-arms the fast path", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // Epoch advanced 7 -> 8 (some OTHER device changed); this leaf is still clean.
  const cache = epochCache({ epoch: 8 });
  const session = guardSession({ registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;

  assert.equal(await session._delegatedSessionStillAuthorized(), true, "still authorized after an unrelated mutation");
  assert.equal(session._admittedAuthorityEpoch, 8, "watermark advanced to the re-verified epoch");
  assert.equal(cache.calls.resolveDelegatedSnapshot, 1, "one coherent re-verify happened on the advance");

  // The next call fast-paths again (epoch now matches the advanced watermark) — no new heavy read.
  cache.calls.resolveDelegatedSnapshot = 0;
  assert.equal(await session._delegatedSessionStillAuthorized(), true);
  assert.equal(cache.calls.resolveDelegatedSnapshot, 0, "re-armed: a subsequent frame fast-paths");
});

test("round-6 finding 1: the guard passes an un-revoked chain with BOTH sources present", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({ registry: cleanRegistry, cache: cleanCache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  assert.equal(await session._delegatedSessionStillAuthorized(), true);
});

test("L5 review-4 finding 1: the coherent resolver is the combined authority source — a cache-only guard passes a clean chain (redundant registry gate removed)", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // No runtime accountDeviceRegistry wired. The coherent snapshot resolves BOTH revocation dimensions
  // (terminal via the serializer's own canonical registry), so the resolver's presence alone is the
  // combined authority source — a clean snapshot + un-revoked chain ⇒ still authorized.
  const cache = epochCache();
  const session = guardSession({ cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 }); // no registry
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  assert.equal(await session._delegatedSessionStillAuthorized(), true, "resolver present ⇒ combined authority source; clean chain authorized");
});

test("L5 review finding 4: resolveDelegatedSnapshot throwing on the slow path surfaces as REVOCATION_BACKEND_UNAVAILABLE (never a false 'revoked')", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // currentEpoch advances past the watermark ⇒ slow path; then the coherent snapshot read throws
  // (the registry's in-tx terminal read, or the Pg snapshot, is unavailable).
  const cache = {
    async currentEpoch() { return 9; },
    async resolveDelegatedSnapshot() { throw new Error("db down"); },
  };
  const session = guardSession({ registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;
  await assert.rejects(
    () => session._delegatedSessionStillAuthorized(),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
    "an availability failure throws a coded error, never returns false (which would close a valid socket)",
  );
});

test("L5 review finding 4: currentEpoch throwing (the fast-path read itself) surfaces as REVOCATION_BACKEND_UNAVAILABLE", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const cache = {
    async currentEpoch() { throw new Error("pool exhausted"); },
    async resolveDelegatedSnapshot() { return { state: null, epoch: 0, terminal: false }; },
  };
  const session = guardSession({ registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;
  await assert.rejects(
    () => session._delegatedSessionStillAuthorized(),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
  );
});

test("L5 review finding 4: a REVOCATION_BACKEND_UNAVAILABLE guard failure → SERVICE_UNAVAILABLE retryable, socket STAYS OPEN, not dispatched", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  installAccountPrincipal(session, { authority: { mode: "delegated", signerPublicKeyB64: "C", accountIdentityPublicKeyB64: "acct" } });
  let dispatched = false;
  session._registry = { async dispatch() { dispatched = true; } };
  session._delegatedSessionStillAuthorized = async () => { const e = new Error("db down"); e.code = "REVOCATION_BACKEND_UNAVAILABLE"; throw e; };
  session._frameCodec = { decodeFrame: () => ({ id: "req1", type: "peerLink.create", body: {} }) };

  await session._handleSocketMessage(Buffer.from("{}"));

  assert.equal(dispatched, false, "the privileged op is NOT dispatched while authority is unprovable");
  assert.equal(ws.closes.length, 0, "the socket STAYS OPEN (availability failure ≠ revocation)");
  const su = errors.find((e) => e.code === "SERVICE_UNAVAILABLE");
  assert.ok(su, "answered SERVICE_UNAVAILABLE");
  assert.equal(su.retryable, true, "and it is retryable");
  assert.ok(!errors.some((e) => e.code === "INTERNAL"), "NOT the generic INTERNAL/non-retryable mapping");
});

test("L5 review-4 finding 1: the guard fails CLOSED when NO coherent resolver is wired (registry alone is not consulted)", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({ registry: cleanRegistry, certChain: [leaf.toJSON()], signer: delegate.pubB64 }); // no cache
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "no resolver ⇒ fail closed; the runtime registry is not an authority source");
});

// ---- Delegated auth fail-close (round-6 finding 3 + round-7 finding 2 require-both) ----
async function verifyDelegated({ runtime }) {
  const delegate = await genKey();
  const deviceId = DeviceRegistrationV1.deviceIdFor(delegate.pubB64);
  const payloadBytes = new TextEncoder().encode("session-auth-payload");
  const signatureBytes = await crypto.sign({ privateKey: delegate.priv, msg: payloadBytes });
  const session = new GatewaySession({ runtime, ws: fakeWs() });
  return session._verifyDelegatedSessionAuth({
    pending: { accountIdentityPublicKeyB64: "acct", sessionDeviceId: deviceId },
    body: { signerPublicKeyB64: delegate.pubB64 },
    payloadBytes, signatureBytes, certChain: [{ any: "chain" }],
  });
}

test("round-6 finding 3: delegated auth FAILS CLOSED with neither cache nor registry", async () => {
  const res = await verifyDelegated({ runtime: {} });
  assert.equal(res.ok, false);
});

test("L5 review-4 finding 1: delegated auth with cache only is NO LONGER gate-rejected (the resolver is the combined authority source)", async () => {
  // The redundant registry gate is gone: cache-only now PASSES the wiring gate and consults the
  // coherent snapshot. res.ok is false here only because this unit stub presents a non-verifying chain.
  const cache = epochCache();
  const res = await verifyDelegated({ runtime: { accountAuthorityRevocationCache: cache } });
  assert.equal(cache.calls.resolveDelegatedSnapshot, 1, "the wiring gate passed — the coherent snapshot WAS consulted (not gate-rejected)");
  assert.equal(res.ok, false, "rejected only because this stub chain does not verify");
});

test("L5 review-4 finding 1: delegated auth FAILS CLOSED with registry only (no resolver)", async () => {
  const res = await verifyDelegated({ runtime: { accountDeviceRegistry: cleanRegistry } });
  assert.equal(res.ok, false, "no coherent resolver ⇒ fail closed; the runtime registry is not an authority source");
});

// ---- L5 review-4 finding P1: a PUBLIC/hand-built resolver may return an INCOMPLETE snapshot; the
// consumption boundary must validate the complete contract and fail closed (availability), NOT
// coerce a missing `terminal` to false and fail OPEN. ----
test("L5 review-4 finding P1 (guard): an INCOMPLETE snapshot (missing terminal) fails as REVOCATION_BACKEND_UNAVAILABLE — never a false 'authorized'", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // Epoch advances (forces the slow path) but the resolver omits `terminal`. The pre-fix code
  // coerced that to false and AUTHORIZED the valid chain (the reported fail-open).
  const cache = {
    async currentEpoch() { return 9; },
    async resolveDelegatedSnapshot() { return { state: null, epoch: 9 }; }, // no `terminal`
  };
  const session = guardSession({ cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;
  await assert.rejects(
    () => session._delegatedSessionStillAuthorized(),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
    "an incomplete snapshot is an availability failure, never a silent authorize",
  );
});

test("L5 review-4 finding P1 (guard): a MALFORMED revocation state (revokedCertIds not an array) fails as REVOCATION_BACKEND_UNAVAILABLE", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const cache = {
    async currentEpoch() { return 9; },
    async resolveDelegatedSnapshot() { return { state: { revokedCertIds: "nope", minValidIssuedAtMs: 0 }, epoch: 9, terminal: false }; },
  };
  const session = guardSession({ cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;
  await assert.rejects(
    () => session._delegatedSessionStillAuthorized(),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
  );
});

test("L5 review-4 finding P1 (admission): an INCOMPLETE snapshot fails as unavailable (→ SERVICE_UNAVAILABLE), never admitted", async () => {
  const cache = {
    async resolveDelegatedSnapshot() { return { state: null, epoch: 1 }; }, // no `terminal`
  };
  const res = await verifyDelegated({ runtime: { accountAuthorityRevocationCache: cache } });
  assert.equal(res.ok, false, "not admitted on a partial snapshot");
  assert.equal(res.unavailable, true, "signaled as an availability failure, not a plain UNAUTHORIZED");
});

test("L5 review-4 finding P1 (admission): a resolver that THROWS fails as unavailable, never admitted", async () => {
  const cache = {
    async resolveDelegatedSnapshot() { throw new Error("db down"); },
  };
  const res = await verifyDelegated({ runtime: { accountAuthorityRevocationCache: cache } });
  assert.equal(res.ok, false);
  assert.equal(res.unavailable, true, "a backend throw at admission is an availability failure");
});

// ---- Round-7 finding 1: bounded intake / serialization ----
test("round-8 finding 1+3: a flood behind a blocked head is bounded, LATCHED, and closes ONCE (no amplification)", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  session._safeSendRawRecord = () => {};
  let errorEmits = 0;
  session._sendErrorRecord = () => { errorEmits += 1; }; // count REAL emits (round-8 finding 3)
  let releaseHead;
  session._handleSocketMessage = () => new Promise((r) => { releaseHead = r; }); // block the head forever
  for (let i = 0; i < 10000; i += 1) {
    session._onSocketMessage(Buffer.from("x"));
  }
  assert.ok(session._msgQueue.length < 1000, "queue bounded, not 9999: " + session._msgQueue.length);
  assert.equal(session._intakeClosed, true, "intake latched after the terminal decision");
  assert.equal(ws.closes.length, 1, "socket closed EXACTLY once (no close amplification): " + ws.closes.length);
  assert.ok(errorEmits < 50, "error emits are latched-bounded, not ~9800: " + errorEmits);
  if (releaseHead) releaseHead();
});

test("round-8 finding 1: 1 MiB frames trip the per-session BYTE cap (not the frame-count cap) and backpressure-close", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  session._safeSendRawRecord = () => {};
  session._sendErrorRecord = () => {};
  let releaseHead;
  session._handleSocketMessage = () => new Promise((r) => { releaseHead = r; });
  const oneMiB = Buffer.alloc(1024 * 1024); // realistic max WS payload; ~9 exceed the 8 MiB cap
  for (let i = 0; i < 12; i += 1) session._onSocketMessage(oneMiB);
  assert.equal(session._intakeClosed, true, "per-session byte cap tripped");
  assert.ok(session._msgQueue.length < 512, "closed on BYTES, far below the 512 frame-count cap: " + session._msgQueue.length);
  assert.ok(ws.closes.some((c) => c.reason === "backpressure"), "closed for backpressure");
  if (releaseHead) releaseHead();
  await new Promise((r) => setImmediate(r)); // let the in-flight finally release its charge
});

// Round-9 finding: IN-FLIGHT frames stay charged to the process-wide cap, so many sessions each
// parking one max-sized blocked request cannot bypass it — later sessions are rejected. (The
// process cap is 256 MiB; each session holds one 1 MiB in-flight frame behind a blocked head.)
test("round-9 finding: the process-wide byte cap counts IN-FLIGHT frames across sessions (later sessions rejected)", async () => {
  const oneMiB = Buffer.alloc(1024 * 1024);

  // A wave of sessions each parking one 1 MiB in-flight frame behind a blocked head. Returns the
  // built sessions + their release fns + how many were backpressure-rejected once the cap filled.
  const runWave = (count) => {
    const releases = [];
    const built = [];
    let rejected = 0;
    for (let i = 0; i < count; i += 1) {
      const ws = fakeWs();
      const session = new GatewaySession({ runtime: {}, ws });
      session._safeSendRawRecord = () => {};
      session._sendErrorRecord = () => {};
      session._handleSocketMessage = () => new Promise((r) => { releases.push(r); }); // block head: one in-flight frame
      session._onSocketMessage(oneMiB);
      if (ws.closes.some((c) => c.reason === "backpressure")) rejected += 1;
      built.push(session);
    }
    return { built, releases, rejected };
  };

  const settle = async () => { await new Promise((r) => setImmediate(r)); await new Promise((r) => setImmediate(r)); };

  // 256 MiB process budget / 1 MiB per in-flight frame ⇒ EXACTLY 256 accepted (one release fn each),
  // 44 of the 300 rejected. Pin the exact split, not just >0 — a loose check also passes if wave 1
  // leaked most of the budget.
  const wave1 = runWave(300);
  assert.equal(wave1.releases.length, 256, "exactly 256 x 1 MiB in-flight frames fill the 256 MiB process budget");
  assert.equal(wave1.rejected, 44, "the remaining 44 sessions are backpressure-rejected");

  // Release wave 1's blocked heads, let the finallys settle so the process counter returns to ~0,
  // then stop() every session — exercising the real cleanup path (FloodGate.release + intake close),
  // not leaving 300 buckets allocated for the rest of the process (round-9 P3 test hygiene).
  for (const r of wave1.releases) r();
  await settle();
  for (const s of wave1.built) s.stop();
  await settle();

  // A second wave must reuse the FULL process budget — its accepted/rejected split must EQUAL wave 1's.
  // A partial leak in wave 1 would leave wave 2 with less budget ⇒ fewer accepted, more rejected.
  const wave2 = runWave(300);
  assert.equal(wave2.releases.length, wave1.releases.length, "wave 2 accepts the SAME count ⇒ the full budget was released");
  assert.equal(wave2.rejected, wave1.rejected, "wave 2 rejects the SAME count ⇒ no budget leaked across waves");
  for (const r of wave2.releases) r();
  await settle();
  for (const s of wave2.built) s.stop();
  await settle();
});

test("round-7 finding 1: frames arriving after stop() are discarded", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  session._safeSendRawRecord = () => {};
  let handled = 0;
  session._handleSocketMessage = async () => { handled += 1; };
  session.stop();
  session._onSocketMessage(Buffer.from("x"));
  await Promise.resolve();
  assert.equal(handled, 0, "no handler runs after stop()");
});

test("round-6 finding 4: concurrent socket frames are processed sequentially (not interleaved)", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  session._safeSendRawRecord = () => {};
  session._sendErrorRecord = () => {};
  let active = 0;
  let maxConcurrent = 0;
  session._handleSocketMessage = async () => {
    active += 1;
    maxConcurrent = Math.max(maxConcurrent, active);
    await new Promise((r) => setTimeout(r, 5));
    active -= 1;
  };
  const p1 = session._onSocketMessage(Buffer.from("a"));
  const p2 = session._onSocketMessage(Buffer.from("b"));
  await Promise.all([p1, p2]);
  assert.equal(maxConcurrent, 1, "at most one handler runs at a time (serialized)");
});

// ---- Chain expiry vs the epoch fast path (leaf-3c review-2 F1) ----
// The epoch watermark proves the account's REVOCATION state is unchanged. It says nothing about the
// clock, and no mutation bumps it merely because time passed — so before this fix a session admitted
// moments before its leaf cert lapsed fast-pathed on an unchanged epoch forever and kept privileged
// access for the life of the socket. These pin the deadline against the REAL admission path: the
// authority (and its deadline) is produced by _verifyDelegatedSessionAuth verifying an actual signed
// chain, not hand-injected, so the test cannot pass by fabricating a convenient deadline.
async function admitDelegated({ leaf, account, delegate, epoch, now }) {
  const cache = epochCache({ epoch });
  const session = new GatewaySession({
    runtime: { accountAuthorityRevocationCache: cache, accountDeviceRegistry: cleanRegistry },
    ws: fakeWs(),
    now,
  });
  const deviceId = DeviceRegistrationV1.deviceIdFor(delegate.pubB64);
  const payloadBytes = Buffer.from("session-auth-payload");
  const signatureBytes = Buffer.from((await sign(delegate.priv, payloadBytes)).sigB64, "base64");
  const authority = await session._verifyDelegatedSessionAuth({
    pending: { sessionDeviceId: deviceId, accountIdentityPublicKeyB64: account.pubB64 },
    body: { signerPublicKeyB64: delegate.pubB64 },
    payloadBytes,
    signatureBytes,
    certChain: [leaf.toJSON()],
  });
  // Commit exactly as the authenticate path does: one frozen principal (the
  // SessionPrincipal constructor deep-freezes the authority), then arm the
  // fast-path watermark.
  if (authority.ok === true) {
    session._commitPrincipal(SessionPrincipal.accountDelegated({
      accountPublicKeyB64: account.pubB64,
      sessionDeviceId: deviceId,
      authority,
    }));
    session._admittedAuthorityEpoch = authority.admittedAuthorityEpoch;
  }
  return { session, authority, cache };
}

test("leaf-3c review-2 F1: real admission derives the chain deadline from the verified chain", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const T = Date.now();
  const lapsesAt = T + 60_000;
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"], issuedAtMs: T - 1000, expiresAtMs: lapsesAt });
  const { authority } = await admitDelegated({ leaf, account, delegate, epoch: 7, now: () => T });

  assert.equal(authority.ok, true, "admitted while the cert is still valid");
  assert.equal(authority.chainExpiresAtMs, lapsesAt, "the deadline came from the chain that was actually verified");
});

test("leaf-3c review-2 F1: a session admitted before its cert lapses is REFUSED once the clock passes the deadline, on an UNCHANGED epoch", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const T = Date.now();
  const lapsesAt = T + 60_000;
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"], issuedAtMs: T - 1000, expiresAtMs: lapsesAt });

  let now = T;
  const { session, cache } = await admitDelegated({ leaf, account, delegate, epoch: 7, now: () => now });

  // Nothing is revoked; the epoch NEVER moves. Only the clock advances.
  now = lapsesAt - 1;
  assert.equal(await session._delegatedSessionStillAuthorized(), true, "still inside the window ⇒ authorized (the fast path still works)");

  // Baselines, not zero: admission itself already read the snapshot once.
  const epochReadsBefore = cache.calls.currentEpoch;
  const snapshotReadsBefore = cache.calls.resolveDelegatedSnapshot;
  now = lapsesAt;
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "the cert has lapsed ⇒ REFUSED even though the epoch is unchanged");
  assert.equal(cache.calls.currentEpoch, epochReadsBefore, "refused BEFORE the epoch fast path — no backend read needed to know time passed");
  assert.equal(cache.calls.resolveDelegatedSnapshot, snapshotReadsBefore, "expiry is decided locally, never by a re-verify");

  now = lapsesAt + 86_400_000; // a day later, still nothing revoked
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "stays refused — the session cannot outlive its cert");
});

test("leaf-3c review-2 F1: a LAPSED session is refused outright, not reported as a retryable backend blip", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const T = Date.now();
  const lapsesAt = T + 60_000;
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"], issuedAtMs: T - 1000, expiresAtMs: lapsesAt });
  let now = T;
  const { session } = await admitDelegated({ leaf, account, delegate, epoch: 7, now: () => now });

  // A backend that THROWS normally produces REVOCATION_BACKEND_UNAVAILABLE (retryable, socket stays
  // open). Expiry is local and deterministic, so it must be decided BEFORE any backend read: a
  // lapsed session is a definitive refusal, never a retryable "try again". This is what pins the
  // ORDERING — if the deadline were checked after the epoch read, this would throw instead.
  session.runtime.accountAuthorityRevocationCache = {
    async currentEpoch() { throw new Error("pg down"); },
    async resolveDelegatedSnapshot() { throw new Error("pg down"); },
  };
  now = lapsesAt;
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "lapsed ⇒ refused without consulting the backend at all");
});

test("leaf-3c review-2 F1: a delegated authority carrying NO deadline fails closed", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // A malformed authority (no deadline to enforce) must never be treated as "never expires".
  // Passed as null, not undefined: an omitted arg would take guardSession's default deadline.
  const session = guardSession({ registry: cleanRegistry, cache: epochCache({ epoch: 7 }), certChain: [leaf.toJSON()], signer: delegate.pubB64, chainExpiresAtMs: null });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
  session._admittedAuthorityEpoch = 7;
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "no deadline ⇒ malformed authority ⇒ closed");
});

// ---- Malformed injected clock fails CLOSED (leaf-3c review-3 F1) ----
// The deadline is a valid finite number; only the CLOCK is malformed. `NaN >= deadline` is false, so
// an unchecked clock read would skip the expiry return and reach the unchanged-epoch fast path — a
// fail-OPEN. A non-finite or throwing clock must instead refuse authorization. The epoch is UNCHANGED
// (7 == 7): the fast path WOULD authorize if the clock check didn't stop it first, so a `true` result
// here is exactly the bug. `deadline` is far in the future, proving it is the clock — not expiry —
// doing the refusing.
for (const [label, clock] of [
  ["NaN", () => NaN],
  ["+Infinity", () => Infinity],
  ["-Infinity", () => -Infinity],
  ["a numeric string", () => "1700000000000"],
  ["undefined (no return)", () => undefined],
  ["a throwing clock", () => { throw new Error("clock exploded"); }],
]) {
  test("leaf-3c review-3 F1: a clock returning " + label + " fails CLOSED, not into the epoch fast path", async () => {
    const account = await genKey();
    const delegate = await genKey();
    const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
    const cache = epochCache({ epoch: 7 });
    const session = guardSession({
      registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64,
      chainExpiresAtMs: Date.now() + 3_600_000, // valid, far-future deadline — only the clock is bad
      now: clock,
    });
    installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority: session.sessionAuthority });
    session._admittedAuthorityEpoch = 7; // unchanged epoch: the fast path would authorize if reached

    assert.equal(await session._delegatedSessionStillAuthorized(), false, "malformed clock ⇒ never authorized");
    assert.equal(cache.calls.currentEpoch, 0, "refused before the epoch fast path — the clock is read first");
  });
}

test("leaf-3c review-3 F1: a malformed clock at ADMISSION refuses to admit (no authority is minted)", async () => {
  // The dispatch-guard cases above cover a live session. This pins the OTHER strict-read site: the
  // clock read that feeds verifyAccountAuthority during _verifyDelegatedSessionAuth. A NaN clock must
  // yield { ok: false } — never an admitted authority carrying a bogus verification time.
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const { authority, session } = await admitDelegated({ leaf, account, delegate, epoch: 7, now: () => NaN });
  assert.equal(authority.ok, false, "admission refused on a non-finite clock");
  assert.equal(session.sessionAuthority, null, "no authority was committed to the session (no principal ⇒ the derived view is null)");
});

test("leaf-3c review-3 F1: a clock that fails on the SLOW-PATH second read fails closed, not authorized", async () => {
  // A STATEFUL clock: finite on the expiry pre-check (read #1), then broken on the re-verify read
  // (read #2) that only happens when the epoch advanced. The pre-check passes (still inside the
  // window), the epoch mismatch forces the slow path, and the second read throws. That must surface
  // as REVOCATION_BACKEND_UNAVAILABLE (the guard's catch) — a fail-closed throw, NEVER a silent
  // `true`. Structurally the slow-path read is #nowMs(); this proves the failure mode end to end.
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const T = Date.now();
  let reads = 0;
  const clock = () => {
    reads += 1;
    if (reads === 1) return T;                 // pre-check read: valid, before the deadline
    throw new Error("clock died mid-guard");   // slow-path re-verify read
  };
  // Admit at epoch 7 with a good clock to mint a real frozen authority, then drive the guard against
  // a cache whose epoch has ADVANCED (8 != 7) so it must take the slow path — where read #2 lives.
  const { authority } = await admitDelegated({ leaf, account, delegate, epoch: 7, now: () => T });
  const session = new GatewaySession({
    runtime: { accountAuthorityRevocationCache: epochCache({ epoch: 8 }), accountDeviceRegistry: cleanRegistry },
    ws: fakeWs(),
    now: clock,
  });
  installAccountPrincipal(session, { owner: account.pubB64, deviceId: DeviceRegistrationV1.deviceIdFor(delegate.pubB64), authority });
  session._admittedAuthorityEpoch = 7;

  await assert.rejects(
    () => session._delegatedSessionStillAuthorized(),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
    "a slow-path clock failure is a fail-closed throw, not an authorization",
  );
  assert.equal(reads, 2, "the pre-check read succeeded; the slow-path re-verify read is the one that failed");
});
