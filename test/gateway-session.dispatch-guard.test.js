import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  DeviceRegistrationV1,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
} from "@rezprotocol/core";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

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
async function buildLeafCert({ account, grantee, capabilities }) {
  const fields = {
    v: 1, purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE, accountIdentityPublicKeyB64: account.pubB64,
    parentCertId: null, granteeDevicePublicKeyB64: grantee.pubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(grantee.pubB64), capabilities,
    maxDelegationDepth: 0, issuedAtMs: ISSUED, expiresAtMs: EXPIRES, signerPublicKeyB64: account.pubB64,
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

const cleanRegistry = { async isTerminallyRevoked() { return false; } };
const cleanCache = { async resolve() { return { revokedCertIds: [], minValidIssuedAtMs: 0 }; } };

// ---- Dispatch plumbing (audit R4 F3-remediation round-5 finding 1): the guard is CALLED for
// delegated sessions and, on refusal, sends UNAUTHORIZED + closes the socket without dispatching.
// The guard's own logic is tested separately below. ----
function makePlumbingSession({ mode, authorized }) {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  session.authenticated = true;
  session.sessionAuthority = { mode, signerPublicKeyB64: "C", accountIdentityPublicKeyB64: "acct" };
  session.ownerPublicKeyB64 = "acct";
  session.sessionDeviceId = "rez:dev:" + "a".repeat(64);
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

// ---- Guard logic: _delegatedSessionStillAuthorized (round-6 finding 1 + round-7 finding 2) ----
function guardSession({ registry, cache, certChain, signer }) {
  const runtime = {};
  if (registry) runtime.accountDeviceRegistry = registry;
  if (cache) runtime.accountAuthorityRevocationCache = cache;
  const session = new GatewaySession({ runtime, ws: fakeWs() });
  session.ownerPublicKeyB64 = "acct";
  session.sessionDeviceId = "rez:dev:" + "a".repeat(64);
  session.sessionAuthority = { mode: "delegated", signerPublicKeyB64: signer, accountIdentityPublicKeyB64: "acct", certChain };
  return session;
}

test("round-6 finding 1: the guard fails on a revoked LEAF cert even when the device is not tombstoned", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({
    registry: cleanRegistry,
    cache: { async resolve() { return { revokedCertIds: [leaf.certId], minValidIssuedAtMs: 0 }; } },
    certChain: [leaf.toJSON()], signer: delegate.pubB64,
  });
  session.ownerPublicKeyB64 = account.pubB64;
  session.sessionDeviceId = DeviceRegistrationV1.deviceIdFor(delegate.pubB64);
  assert.equal(await session._delegatedSessionStillAuthorized(), false);
});

test("round-6 finding 1: the guard passes an un-revoked chain with BOTH sources present", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({ registry: cleanRegistry, cache: cleanCache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  session.ownerPublicKeyB64 = account.pubB64;
  session.sessionDeviceId = DeviceRegistrationV1.deviceIdFor(delegate.pubB64);
  assert.equal(await session._delegatedSessionStillAuthorized(), true);
});

test("round-7 finding 2: the guard fails CLOSED when only the cache is present (no registry)", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({ cache: cleanCache, certChain: [leaf.toJSON()], signer: delegate.pubB64 }); // no registry
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "cache-only cannot resolve the device-tombstone dimension");
});

test("round-7 finding 2: the guard fails CLOSED when only the registry is present (no cache)", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  const session = guardSession({ registry: cleanRegistry, certChain: [leaf.toJSON()], signer: delegate.pubB64 }); // no cache
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "registry-only cannot resolve the revoked-cert dimension");
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

test("round-7 finding 2: delegated auth FAILS CLOSED with cache only (no registry)", async () => {
  const res = await verifyDelegated({ runtime: { accountAuthorityRevocationCache: cleanCache } });
  assert.equal(res.ok, false);
});

test("round-7 finding 2: delegated auth FAILS CLOSED with registry only (no cache)", async () => {
  const res = await verifyDelegated({ runtime: { accountDeviceRegistry: cleanRegistry } });
  assert.equal(res.ok, false);
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

test("round-8 finding 1: large frames trip the BYTE cap (not just the frame-count cap) and backpressure-close", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  session._safeSendRawRecord = () => {};
  session._sendErrorRecord = () => {};
  let releaseHead;
  session._handleSocketMessage = () => new Promise((r) => { releaseHead = r; });
  const big = Buffer.alloc(2 * 1024 * 1024); // 2 MiB — a handful exceeds the 8 MiB per-session cap
  for (let i = 0; i < 20; i += 1) session._onSocketMessage(big);
  assert.equal(session._intakeClosed, true, "byte cap tripped");
  assert.ok(session._msgQueue.length < 512, "closed on BYTES, far below the 512 frame-count cap: " + session._msgQueue.length);
  assert.ok(ws.closes.some((c) => c.reason === "backpressure"), "closed for backpressure");
  if (releaseHead) releaseHead();
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
