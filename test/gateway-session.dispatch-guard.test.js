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
// Both resolve() (connect-time delegated auth) and resolveFresh() (the always-fresh L5 dispatch
// guard) return the same clean state, so a cache is a drop-in for either consumer.
const cleanCache = {
  async resolve() { return { revokedCertIds: [], minValidIssuedAtMs: 0 }; },
  async resolveFresh() { return { revokedCertIds: [], minValidIssuedAtMs: 0 }; },
};

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
    cache: { async resolveFresh() { return { revokedCertIds: [leaf.certId], minValidIssuedAtMs: 0 }; } },
    certChain: [leaf.toJSON()], signer: delegate.pubB64,
  });
  session.ownerPublicKeyB64 = account.pubB64;
  session.sessionDeviceId = DeviceRegistrationV1.deviceIdFor(delegate.pubB64);
  assert.equal(await session._delegatedSessionStillAuthorized(), false);
});

test("audit R4 L5 (full): the guard reads ALWAYS-FRESH state — a WARM cache carrying pre-revoke state does NOT save a revoked leaf", async () => {
  const account = await genKey();
  const delegate = await genKey();
  const leaf = await buildLeafCert({ account, grantee: delegate, capabilities: ["deviceSet.publish"] });
  // The 30s-TTL warm entry still says CLEAN (the revoke landed after admission); resolveFresh()
  // sees the revoke. The guard MUST consult resolveFresh, so it rejects despite the stale resolve().
  let resolveCalls = 0;
  const cache = {
    async resolve() { resolveCalls += 1; return { revokedCertIds: [], minValidIssuedAtMs: 0 }; },
    async resolveFresh() { return { revokedCertIds: [leaf.certId], minValidIssuedAtMs: 0 }; },
  };
  const session = guardSession({ registry: cleanRegistry, cache, certChain: [leaf.toJSON()], signer: delegate.pubB64 });
  session.ownerPublicKeyB64 = account.pubB64;
  session.sessionDeviceId = DeviceRegistrationV1.deviceIdFor(delegate.pubB64);
  assert.equal(await session._delegatedSessionStillAuthorized(), false, "fresh read enforces the mid-session revoke");
  assert.equal(resolveCalls, 0, "the guard NEVER trusts the warm-cache resolve() — it reads fresh only");
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
