import test from "node:test";
import assert from "node:assert/strict";
import {
  REZ_CONTRACT_TYPES,
  bytesToBase64,
  base64ToBytes,
  DeviceRegistrationV1,
  AccountAuthorityStateV1,
  ACCOUNT_AUTHORITY_STATE_PURPOSE,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  DURABLE_RECORD_V2_VERSION,
  durableRecordV2SignableBytes,
} from "@rezprotocol/core";
import { PropagationOutboxHandler, OUTBOX_LEASE_MAX_PER_MINUTE } from "../src/protocol/handlers/PropagationOutboxHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

const crypto = new NodeCryptoProvider();

// P1#3 leaf 3b — the wire/auth surface for the head-advancing account lease. These are pure
// boundary unit tests (fake ctx + spy outbox); the lease STATE MACHINE is proven against real
// Postgres in storage.pg.propagation-outbox.test.js. Here we pin the boundary invariants:
//   req 2 — account + owner come from the SESSION, never the body.
//   F2   — authority fails CLOSED: only an explicit direct|delegated shape bound to THIS account.
//   req 3 — primary holds all; delegated needs deviceSet.publish + the full chain shape.
//   req 1 — the lease token is size-bounded (in the RRecord contract layer).
//   req 8 — per-account rate limit; the token never appears in an error; transient → retryable.
const T = REZ_CONTRACT_TYPES;
const DEV = "rez:dev:" + "a".repeat(64);           // a canonical session device id
const DEV2 = "rez:dev:" + "b".repeat(64);
// A delegated signer key + the session device id it self-certifies (F2 audit leaf-3c: the handler
// binds the delegated signer to the session/lease-owner device via deviceIdFor). Derived from a REAL
// key, not hardcoded, so the binding holds on the delegated success path.
const DELEGATE_SIGNER_B64 = bytesToBase64((await new NodeCryptoProvider().generateSigningKeyPair()).publicKey);
const DELEGATE_DEVICE_ID = DeviceRegistrationV1.deviceIdFor(DELEGATE_SIGNER_B64);

function makeOutbox(overrides = {}) {
  const calls = [];
  const rec = (name) => (...args) => {
    calls.push({ name, args });
    const r = overrides[name];
    if (typeof r === "function") return Promise.resolve(r(...args));
    return Promise.resolve(r === undefined ? null : r);
  };
  return { calls, claim: rec("claim"), preparePublication: rec("preparePublication"), release: rec("release"), fail: rec("fail"), completePublication: rec("completePublication") };
}

function makeCtx({ account = "ACCT-b64", deviceId = DEV, authority, outbox = makeOutbox(), session = true, now, recordDht, serializer } = {}) {
  // Default authority = a valid DIRECT session bound to this exact account (the fail-closed gate
  // now requires an explicit shape + matching account).
  const auth = authority !== undefined ? authority : { mode: "direct", accountIdentityPublicKeyB64: account, signerPublicKeyB64: account };
  const sent = [];
  const runtime = { propagationOutbox: outbox };
  if (recordDht !== undefined) runtime.recordDht = recordDht;
  if (serializer !== undefined) runtime.accountMutationSerializer = serializer;
  const ctx = {
    runtime,
    ownerPublicKeyB64: account,
    sessionDeviceId: deviceId,
    sessionAuthority: auth,
    requireSession(requestId) {
      if (!session) { this.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session required", retryable: false }); return false; }
      return true;
    },
    sendError(opts) { sent.push({ kind: "error", ...opts }); },
    sendResponse(requestId, type, body) { sent.push({ kind: "response", requestId, type, body }); },
  };
  if (now !== undefined) ctx.now = () => now;
  return { ctx, sent, outbox };
}
const lastError = (sent) => sent.filter((s) => s.kind === "error").at(-1);
const lastResponse = (sent) => sent.filter((s) => s.kind === "response").at(-1);
// A complete delegated authority bound to `account`, holding the given capabilities.
const delegatedAuthority = (account, caps) => ({
  mode: "delegated",
  accountIdentityPublicKeyB64: account,
  grantedCapabilities: caps,
  signerPublicKeyB64: DELEGATE_SIGNER_B64,
  certChain: [{ certId: "rez:cap:" + "0".repeat(64) }],
});

test("leaf-3b claim: a primary session leases the head; owner = the SESSION device, not the body", async () => {
  const outbox = makeOutbox({ claim: { token: "srv-tok", anchorEpoch: 3, headEpoch: 5, leaseExpiresAtMs: 111, attempts: 0 } });
  const { ctx, sent } = makeCtx({ account: "ACCT-1", deviceId: DEV, outbox });
  const h = new PropagationOutboxHandler(ctx);
  // A HOSTILE body tries to steer the account + owner — it must be ignored.
  await h.handleClaim("r1", { accountIdentityPublicKeyB64: "EVIL", ownerDeviceId: "rez:dev:evil" });
  assert.deepEqual(outbox.calls[0], { name: "claim", args: ["ACCT-1", DEV] }, "claim uses the session account + device, never the body");
  const res = lastResponse(sent);
  assert.equal(res.type, T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES);
  assert.deepEqual(res.body, { leased: true, awaitingRootSignature: false, token: "srv-tok", anchorEpoch: 3, headEpoch: 5, leaseExpiresAtMs: 111, attempts: 0 });
});

test("leaf-3b claim: null (nothing publishable / busy / backing off) → { leased: false }", async () => {
  const { ctx, sent } = makeCtx({ account: "ACCT-2", outbox: makeOutbox({ claim: null }) });
  await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
  // awaitingRootSignature is FALSE here and that distinction matters: this is the steady-state
  // "nothing to do", not "an obligation is stuck waiting for the primary device".
  assert.deepEqual(lastResponse(sent).body, { leased: false, awaitingRootSignature: false });
});

test("leaf-3b prepare: token from body, account+owner from session; success → { prepared, anchorEpoch, headEpoch }", async () => {
  const outbox = makeOutbox({ preparePublication: { anchorEpoch: 3, headEpoch: 7 } });
  const { ctx, sent } = makeCtx({ account: "ACCT-3", deviceId: DEV, outbox });
  await new PropagationOutboxHandler(ctx).handlePrepare("r1", { leaseToken: "tokX", accountIdentityPublicKeyB64: "EVIL" });
  assert.deepEqual(outbox.calls[0], { name: "preparePublication", args: ["ACCT-3", "tokX", DEV] });
  assert.deepEqual(lastResponse(sent).body, { prepared: true, anchorEpoch: 3, headEpoch: 7 });
});

test("leaf-3b release + fail: shapes and session-derived owner", async () => {
  const outbox = makeOutbox({ release: true, fail: { attemptedEpoch: 2, anchorEpoch: 1, attempts: 4, backoffMs: 8000, blocked: false } });
  const { ctx, sent } = makeCtx({ account: "ACCT-4", deviceId: DEV, outbox });
  const h = new PropagationOutboxHandler(ctx);
  await h.handleRelease("r1", { leaseToken: "tokR" });
  assert.deepEqual(outbox.calls[0], { name: "release", args: ["ACCT-4", "tokR", DEV] });
  assert.deepEqual(lastResponse(sent).body, { released: true });
  await h.handleFail("r2", { leaseToken: "tokF" });
  assert.deepEqual(outbox.calls[1], { name: "fail", args: ["ACCT-4", "tokF", DEV] });
  assert.deepEqual(lastResponse(sent).body, { recorded: true, attemptedEpoch: 2, anchorEpoch: 1, attempts: 4, backoffMs: 8000, blocked: false });
  // release returning false (no live lease) → { released: false }.
  const { ctx: ctx2, sent: sent2 } = makeCtx({ account: "ACCT-4b", outbox: makeOutbox({ release: false }) });
  await new PropagationOutboxHandler(ctx2).handleRelease("r3", { leaseToken: "tokR" });
  assert.deepEqual(lastResponse(sent2).body, { released: false });
});

test("leaf-3b req 2: a non-canonical (or missing) session device → UNAUTHORIZED, outbox untouched", async () => {
  for (const bad of ["rez:dev:short", "", null]) {
    const { ctx, sent, outbox } = makeCtx({ deviceId: bad });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED", "device " + JSON.stringify(bad) + " rejected");
    assert.equal(outbox.calls.length, 0, "outbox never called without a canonical session device");
  }
});

test("leaf-3b auth gates: no session, empty account, and missing outbox each short-circuit", async () => {
  {
    const { ctx, sent, outbox } = makeCtx({ session: false });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  {
    const { ctx, sent, outbox } = makeCtx({ account: "  " });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  {
    const { ctx, sent } = makeCtx({ outbox: null });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "SERVICE_UNAVAILABLE");
  }
});

test("leaf-3b F2: authority FAILS CLOSED — null/unknown-mode/mismatched-account never grant the lease surface", async () => {
  // null authority → UNAUTHORIZED (never implicitly primary).
  {
    const { ctx, sent, outbox } = makeCtx({ authority: null });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  // unknown mode → UNAUTHORIZED.
  {
    const { ctx, sent, outbox } = makeCtx({ authority: { mode: "root", accountIdentityPublicKeyB64: "ACCT-b64" } });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  // account mismatch (authority for a DIFFERENT account than the session's) → UNAUTHORIZED.
  {
    const { ctx, sent, outbox } = makeCtx({ account: "ACCT-me", authority: { mode: "direct", accountIdentityPublicKeyB64: "ACCT-other" } });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
});

test("leaf-3b req 3: delegated needs deviceSet.publish AND the full chain shape; primary holds all", async () => {
  // Missing capability → FORBIDDEN.
  {
    const { ctx, sent, outbox } = makeCtx({ account: "ACCT-d1", authority: delegatedAuthority("ACCT-d1", ["device.add"]) });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "FORBIDDEN");
    assert.match(lastError(sent).message, /deviceSet\.publish/);
    assert.equal(outbox.calls.length, 0);
  }
  // Has the capability but an INCOMPLETE authority (no cert chain) → UNAUTHORIZED (fail closed).
  {
    const incomplete = { mode: "delegated", accountIdentityPublicKeyB64: "ACCT-d2", grantedCapabilities: ["deviceSet.publish"], signerPublicKeyB64: "sk", certChain: [] };
    const { ctx, sent, outbox } = makeCtx({ account: "ACCT-d2", authority: incomplete });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  // Full delegated authority WITH the capability now stops at AWAITING-ROOT-SIGNATURE.
  //
  // This block previously asserted that such a session proceeds to the outbox and receives the
  // lease. That was rewritten, not repaired: since the P0 fix the authority state is root-signed
  // only, so the lease would hand this device an obligation it could only fail out of — burning an
  // attempt and backing the account off toward BLOCKED for a revocation that was never broken. The
  // capability check above still runs FIRST, so a delegated device without deviceSet.publish is
  // still FORBIDDEN rather than told to wait.
  {
    const outbox = makeOutbox({ claim: { token: "t", anchorEpoch: 1, headEpoch: 1, leaseExpiresAtMs: 9, attempts: 0 } });
    // deviceId = the signer's self-certified id, so the F2 signer→owner binding holds.
    const { ctx, sent } = makeCtx({ account: "ACCT-d3", deviceId: DELEGATE_DEVICE_ID, authority: delegatedAuthority("ACCT-d3", ["deviceSet.publish"]), outbox });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(outbox.calls.length, 0, "no lease is taken, so nothing is attempted or backed off");
    assert.deepEqual(lastResponse(sent).body, { leased: false, awaitingRootSignature: true });
  }
});

test("Option A: a delegated drain is AWAITING-ROOT-SIGNATURE and costs the obligation nothing", async () => {
  // The requirement in full: an explicit state, WITHOUT consuming retry budget, incrementing
  // failure counts, or becoming blocked. All three follow from never taking the lease — so the
  // assertion is on the outbox call log, not just the response.
  const outbox = makeOutbox({ claim: { token: "t", anchorEpoch: 4, headEpoch: 9, leaseExpiresAtMs: 9, attempts: 0 } });
  const { ctx, sent } = makeCtx({
    account: "ACCT-w",
    deviceId: DELEGATE_DEVICE_ID,
    authority: delegatedAuthority("ACCT-w", ["deviceSet.publish"]),
    outbox,
  });
  const h = new PropagationOutboxHandler(ctx);

  // Repeated polling must stay free: a delegated device that keeps checking back never degrades
  // the account's state.
  for (let i = 0; i < 5; i += 1) {
    await h.handleClaim("r" + i, {});
    assert.deepEqual(lastResponse(sent).body, { leased: false, awaitingRootSignature: true });
  }
  assert.equal(outbox.calls.length, 0, "claim/fail/prepare are never reached: no attempts, no backoff, never blocked");

  // And the head stays claimable — the SAME account, from a PRIMARY session, still leases it.
  const { ctx: rootCtx, sent: rootSent } = makeCtx({ account: "ACCT-w", deviceId: DEV, outbox });
  await new PropagationOutboxHandler(rootCtx).handleClaim("r9", {});
  assert.deepEqual(rootSent.at(-1).body, { leased: true, awaitingRootSignature: false, token: "t", anchorEpoch: 4, headEpoch: 9, leaseExpiresAtMs: 9, attempts: 0 });
});

test("Option A: a delegated session cannot COMPLETE a publication (refused before any crypto)", async () => {
  // Defense in depth — claim already refuses the lease, so a delegated session cannot hold a valid
  // token. The point is the ANSWER: a structural refusal, not "publication verification failed",
  // and no Ed25519 verify spent on a submission that cannot pass by construction.
  const outbox = makeOutbox({ completePublication: { completed: true, doneThroughEpoch: 5 } });
  const dht = { putRecord: async () => ({ storedLocally: true, localId: "L", acknowledgedRemote: 1 }) };
  const serializer = { getAuthorityState: async () => ({ revokedCertIds: [], minValidIssuedAtMs: 0 }) };
  const { ctx, sent } = makeCtx({
    account: "ACCT-c",
    deviceId: DELEGATE_DEVICE_ID,
    authority: delegatedAuthority("ACCT-c", ["deviceSet.publish"]),
    outbox,
    recordDht: dht,
    serializer,
  });
  await new PropagationOutboxHandler(ctx).handleComplete("r1", { leaseToken: "tok", record: { v: 2 } });
  const err = lastError(sent);
  assert.equal(err.code, "FORBIDDEN");
  assert.match(err.message, /root-signed only/);
  assert.equal(outbox.calls.length, 0, "nothing completed");
});

test("leaf-3b req 1: the lease token is required and size-bounded (BAD_REQUEST via the contract, outbox untouched)", async () => {
  {
    const { ctx, sent, outbox } = makeCtx({});
    await new PropagationOutboxHandler(ctx).handlePrepare("r1", {});
    assert.equal(lastError(sent).code, "BAD_REQUEST");
    assert.match(lastError(sent).message, /leaseToken is required/);
    assert.equal(outbox.calls.length, 0);
  }
  {
    const { ctx, sent, outbox } = makeCtx({});
    await new PropagationOutboxHandler(ctx).handleRelease("r1", { leaseToken: "z".repeat(129) });
    assert.equal(lastError(sent).code, "BAD_REQUEST");
    assert.match(lastError(sent).message, /128-byte limit/);
    assert.equal(outbox.calls.length, 0);
  }
});

test("leaf-3b req 8: per-account rate limit trips after the budget is exhausted", async () => {
  const NOW = 1_000_000;
  const ACCT = "ACCT-ratelimit-unique";
  const { ctx, sent } = makeCtx({ account: ACCT, outbox: makeOutbox({ claim: null }), now: NOW });
  const h = new PropagationOutboxHandler(ctx);
  for (let i = 0; i < OUTBOX_LEASE_MAX_PER_MINUTE; i += 1) {
    await h.handleClaim("r" + i, {});
  }
  assert.equal(sent.filter((s) => s.kind === "error").length, 0, "all attempts within the budget are admitted");
  await h.handleClaim("over", {});
  const err = lastError(sent);
  assert.equal(err.code, "RATE_LIMITED");
  assert.equal(err.retryable, true);
});

test("leaf-3b req 8 (F6): a transient backend SQLSTATE → retryable SERVICE_UNAVAILABLE; other → INTERNAL", async () => {
  // A pg connection-exception SQLSTATE (class 08) is transient/availability → retryable.
  {
    const outbox = makeOutbox({ claim: () => { const e = new Error("connection reset"); e.code = "08006"; throw e; } });
    const { ctx, sent } = makeCtx({ account: "ACCT-t1", outbox });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    const err = lastError(sent);
    assert.equal(err.code, "SERVICE_UNAVAILABLE");
    assert.equal(err.retryable, true);
  }
  // A non-transient error (e.g. a constraint violation, or no code) → non-retryable INTERNAL.
  {
    const outbox = makeOutbox({ claim: () => { const e = new Error("boom"); e.code = "23505"; throw e; } });
    const { ctx, sent } = makeCtx({ account: "ACCT-t2", outbox });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    const err = lastError(sent);
    assert.equal(err.code, "INTERNAL");
    assert.equal(err.retryable, false);
  }
});

test("leaf-3b req 8: a lease token NEVER appears in an error, even if the backend error text contains it", async () => {
  const SECRET = "super-secret-lease-token-value";
  const outbox = makeOutbox({ preparePublication: () => { throw new Error("db failed with token=" + SECRET); } });
  const { ctx, sent } = makeCtx({ account: "ACCT-hy", outbox });
  await new PropagationOutboxHandler(ctx).handlePrepare("r1", { leaseToken: SECRET });
  const err = lastError(sent);
  assert.equal(err.code, "INTERNAL");
  assert.equal(err.message, "outbox lease operation failed", "a FIXED message is sent (never err.message)");
  for (const s of sent) {
    assert.ok(!JSON.stringify(s).includes(SECRET), "the lease token appears in NO outbound record");
  }
});

test("leaf-3c F2: a DIRECT session whose signer is not the account is UNAUTHORIZED (signer→account bound)", async () => {
  const auth = { mode: "direct", accountIdentityPublicKeyB64: "ACCT-x", signerPublicKeyB64: "some-other-key" };
  const { ctx, sent, outbox } = makeCtx({ account: "ACCT-x", authority: auth });
  await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
  assert.equal(lastError(sent).code, "UNAUTHORIZED");
  assert.match(lastError(sent).message, /signer must be the account/);
  assert.equal(outbox.calls.length, 0);
});

test("leaf-3c F2: a full DELEGATED session whose signer does NOT self-certify the session device is UNAUTHORIZED", async () => {
  // Has deviceSet.publish + a full chain, but the session device is NOT the signer's derived id.
  const { ctx, sent, outbox } = makeCtx({ account: "ACCT-mb", deviceId: DEV, authority: delegatedAuthority("ACCT-mb", ["deviceSet.publish"]) });
  await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
  assert.equal(lastError(sent).code, "UNAUTHORIZED");
  assert.match(lastError(sent).message, /not bound to the session device/);
  assert.equal(outbox.calls.length, 0);
});

test("leaf-3c F4: additional transient backend codes (serialization/deadlock/lock/transport) → retryable SERVICE_UNAVAILABLE", async () => {
  for (const code of ["40001", "40P01", "55P03", "ECONNRESET", "ETIMEDOUT"]) {
    const outbox = makeOutbox({ claim: () => { const e = new Error("transient"); e.code = code; throw e; } });
    const { ctx, sent } = makeCtx({ account: "ACCT-" + code, outbox });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    const err = lastError(sent);
    assert.equal(err.code, "SERVICE_UNAVAILABLE", code + " must be retryable");
    assert.equal(err.retryable, true, code + " must be retryable");
  }
});

test("leaf-3c F5: a non-string lease token is REJECTED (BAD_REQUEST), not coerced via String()", async () => {
  const { ctx, sent, outbox } = makeCtx({});
  await new PropagationOutboxHandler(ctx).handleRelease("r1", { leaseToken: 12345 });
  assert.equal(lastError(sent).code, "BAD_REQUEST");
  assert.match(lastError(sent).message, /leaseToken must be a string/);
  assert.equal(outbox.calls.length, 0, "a coerced '12345' must NEVER reach the backend");
});

// ── leaf 3c: handleComplete — the VERIFIED completion (the ONE crypto-bearing outbox op) ──────────
// These build a REAL direct-mode publication (inner AccountAuthorityStateV1 + DurableRecordV2
// envelope, both signed by the account key) so the handler's ACTUAL verification runs — NO mocked
// crypto. Direct mode (signer == owner == account, no cert chain) is the simplest authentic
// publication; the delegated + storage path runs end-to-end against real Postgres in the storage suite.
const T2 = REZ_CONTRACT_TYPES;

async function genAccount() {
  const kp = await crypto.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}

async function buildPublication(account, epoch, { nowMs = Date.now(), innerSigner = null, tamperEnvelopeSig = false } = {}) {
  const innerPub = innerSigner ? innerSigner.pubB64 : account.pubB64;
  const innerPriv = innerSigner ? innerSigner.priv : account.priv;
  const stateBody = {
    v: 1, purpose: ACCOUNT_AUTHORITY_STATE_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64, epoch,
    revokedCertIds: [], minValidIssuedAtMs: 0, issuedAtMs: nowMs,
    signerPublicKeyB64: innerPub,
  };
  const stateSigB64 = bytesToBase64(await crypto.sign({ privateKey: innerPriv, msg: AccountAuthorityStateV1.signableBytes(stateBody) }));
  const authorityState = new AccountAuthorityStateV1({ ...stateBody, sig: { alg: "ed25519", sigB64: stateSigB64 } });
  const payloadB64 = bytesToBase64(new TextEncoder().encode(JSON.stringify(authorityState.toJSON())));
  const envelope = {
    v: DURABLE_RECORD_V2_VERSION, recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND, recordId: "v1",
    ownerPublicKeyB64: account.pubB64, signerPublicKeyB64: account.pubB64,
    issuedAtMs: nowMs, expiresAtMs: nowMs + 3_600_000, payloadB64,
  };
  const goodSig = bytesToBase64(await crypto.sign({ privateKey: account.priv, msg: durableRecordV2SignableBytes(envelope) }));
  const badSig = bytesToBase64(await crypto.sign({ privateKey: account.priv, msg: new TextEncoder().encode("wrong") }));
  return { ...envelope, sigB64: tamperEnvelopeSig ? badSig : goodSig };
}

// Spies sharing one `order` log so a test can assert store-BEFORE-complete. The outbox carries a
// `claim` stub because #authorize gates on it (SERVICE_UNAVAILABLE otherwise), even though
// handleComplete only calls completePublication.
function makeCompleteDeps({ putResult = { storedLocally: true, localId: "L", acknowledgedRemote: 1 }, completeResult = { completed: true, doneThroughEpoch: 5 }, authorityState = { epoch: 5, revokedCertIds: [], minValidIssuedAtMs: 0 } } = {}) {
  const order = [];
  const outbox = {
    calls: [],
    claim: () => Promise.resolve(null),
    completePublication: (...args) => { order.push("complete"); outbox.calls.push({ name: "completePublication", args }); return Promise.resolve(typeof completeResult === "function" ? completeResult(...args) : completeResult); },
  };
  const recordDht = {
    putCalls: [],
    putRecord: (rec) => { order.push("put"); recordDht.putCalls.push(rec); return Promise.resolve(typeof putResult === "function" ? putResult(rec) : putResult); },
  };
  const serializer = { getAuthorityState: () => Promise.resolve(authorityState) };
  return { order, outbox, recordDht, serializer };
}

test("leaf-3c complete: a VALID publication stores BEFORE completing and returns the done watermark", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5);
  const deps = makeCompleteDeps({ completeResult: { completed: true, doneThroughEpoch: 5 } });
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c1", { leaseToken: "tok-123", record });

  const res = lastResponse(sent);
  assert.equal(res.type, T2.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES);
  assert.deepEqual(res.body, { completed: true, doneThroughEpoch: 5 });
  assert.deepEqual(deps.order, ["put", "complete"], "store happens BEFORE the done-mark (done ⇒ retrievable)");
  assert.equal(deps.recordDht.putCalls.length, 1, "the record was stored");
  assert.deepEqual(deps.outbox.calls[0].args, [account.pubB64, "tok-123", DEV, 5], "complete(account, token, ownerDevice, M) — M from the verified inner epoch");
});

test("leaf-3c complete: a tampered envelope signature is BAD_REQUEST and NEVER stores or completes", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5, { tamperEnvelopeSig: true });
  const deps = makeCompleteDeps();
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c2", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "BAD_REQUEST");
  assert.match(lastError(sent).message, /verification failed/);
  assert.equal(deps.recordDht.putCalls.length, 0, "an unverified publication is NEVER stored");
  assert.equal(deps.outbox.calls.length, 0, "…and NEVER completed");
});

test("leaf-3c complete: a publication owned by a DIFFERENT account is rejected", async () => {
  const account = await genAccount();
  const other = await genAccount();
  const record = await buildPublication(other, 5); // valid, but owned+signed by `other`
  const deps = makeCompleteDeps();
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c3", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "BAD_REQUEST");
  assert.match(lastError(sent).message, /not this account/);
  assert.equal(deps.recordDht.putCalls.length, 0);
  assert.equal(deps.outbox.calls.length, 0);
});

test("leaf-3c complete: an inner payload whose signer disagrees with the envelope is rejected (same-signer binding)", async () => {
  const account = await genAccount();
  const other = await genAccount();
  // envelope signed by `account`; inner AccountAuthorityStateV1 signed by `other` → binding mismatch.
  const record = await buildPublication(account, 5, { innerSigner: other });
  const deps = makeCompleteDeps();
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c4", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "BAD_REQUEST");
  assert.match(lastError(sent).message, /not bound to the verified envelope/);
  assert.equal(deps.recordDht.putCalls.length, 0);
});

test("leaf-3c complete: a publication epoch of 0 identifies no obligation and is rejected", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 0);
  const deps = makeCompleteDeps({ authorityState: { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 } });
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c5", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "BAD_REQUEST");
  assert.match(lastError(sent).message, /does not identify an obligation/);
  assert.equal(deps.recordDht.putCalls.length, 0, "epoch is checked before storing");
});

test("leaf-3c complete: a record REJECTED by the store does not complete", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5);
  const deps = makeCompleteDeps({ putResult: { storedLocally: false, reason: "quota" } });
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c6", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "RECORD_REJECTED");
  assert.equal(deps.outbox.calls.length, 0, "a store rejection stops before the done-mark");
});

test("leaf-3c complete: a lease lost during verification (null) is a benign completed:false", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5);
  const deps = makeCompleteDeps({ completeResult: null });
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c7", { leaseToken: "tok", record });

  const res = lastResponse(sent);
  assert.equal(res.type, T2.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES);
  assert.deepEqual(res.body, { completed: false }, "no epoch on the benign lease-lost race");
  assert.equal(deps.recordDht.putCalls.length, 1, "the authentic record was still stored");
});

test("leaf-3c complete: an epoch that does not match the frozen prepared_epoch is CONFLICT", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5);
  const deps = makeCompleteDeps({ completeResult: { completed: false, expectedEpoch: 4 } });
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c8", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "CONFLICT");
  assert.match(lastError(sent).message, /does not match the prepared epoch/);
});

test("leaf-3c complete: no record store wired ⇒ SERVICE_UNAVAILABLE, nothing verified", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5);
  const deps = makeCompleteDeps();
  // recordDht omitted entirely.
  const { ctx, sent } = makeCtx({ account: account.pubB64, outbox: deps.outbox, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c9", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "SERVICE_UNAVAILABLE");
  assert.equal(deps.outbox.calls.length, 0);
});

test("leaf-3c complete: a delegated session WITHOUT deviceSet.publish is refused before any verification", async () => {
  const account = await genAccount();
  const record = await buildPublication(account, 5);
  const deps = makeCompleteDeps();
  const authority = { mode: "delegated", accountIdentityPublicKeyB64: account.pubB64, signerPublicKeyB64: DELEGATE_SIGNER_B64, grantedCapabilities: ["device.add"], certChain: [{}] };
  const { ctx, sent } = makeCtx({ account: account.pubB64, deviceId: DELEGATE_DEVICE_ID, authority, outbox: deps.outbox, recordDht: deps.recordDht, serializer: deps.serializer });
  await new PropagationOutboxHandler(ctx).handleComplete("c10", { leaseToken: "tok", record });

  assert.equal(lastError(sent).code, "FORBIDDEN", "the #authorize spine gates the crypto op on the capability");
  assert.equal(deps.recordDht.putCalls.length, 0, "no verification/store for an unauthorized session");
  assert.equal(deps.outbox.calls.length, 0);
});
