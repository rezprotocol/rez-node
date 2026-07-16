import test from "node:test";
import assert from "node:assert/strict";
import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { PropagationOutboxHandler, OUTBOX_LEASE_MAX_PER_MINUTE } from "../src/protocol/handlers/PropagationOutboxHandler.js";

// P1#3 leaf 3b — the wire/auth surface for the head-advancing account lease. These are pure
// boundary unit tests (fake ctx + spy outbox); the lease STATE MACHINE is proven against real
// Postgres in storage.pg.propagation-outbox.test.js. Here we pin the boundary invariants:
//   req 2 — account + owner come from the SESSION, never the body.
//   req 3 — primary holds all; delegated needs deviceSet.publish.
//   req 1 — the lease token is size-bounded.
//   req 8 — per-account rate limit; the token never appears in an error.
const T = REZ_CONTRACT_TYPES;
const DEV = "rez:dev:" + "a".repeat(64);           // a canonical session device id
const DEV2 = "rez:dev:" + "b".repeat(64);

function makeOutbox(overrides = {}) {
  const calls = [];
  const rec = (name) => (...args) => {
    calls.push({ name, args });
    const r = overrides[name];
    if (typeof r === "function") return Promise.resolve(r(...args));
    return Promise.resolve(r === undefined ? null : r);
  };
  return { calls, claim: rec("claim"), preparePublication: rec("preparePublication"), release: rec("release"), fail: rec("fail") };
}

function makeCtx({ account = "ACCT-b64", deviceId = DEV, authority = { mode: "direct" }, outbox = makeOutbox(), session = true, now } = {}) {
  const sent = [];
  const ctx = {
    runtime: { propagationOutbox: outbox },
    ownerPublicKeyB64: account,
    sessionDeviceId: deviceId,
    sessionAuthority: authority,
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

test("leaf-3b claim: a primary session leases the head; owner = the SESSION device, not the body", async () => {
  const outbox = makeOutbox({ claim: { token: "srv-tok", anchorEpoch: 3, headEpoch: 5, leaseExpiresAtMs: 111, attempts: 0 } });
  const { ctx, sent } = makeCtx({ account: "ACCT-1", deviceId: DEV, outbox });
  const h = new PropagationOutboxHandler(ctx);
  // A HOSTILE body tries to steer the account + owner — it must be ignored.
  await h.handleClaim("r1", { accountIdentityPublicKeyB64: "EVIL", ownerDeviceId: "rez:dev:evil" });
  assert.deepEqual(outbox.calls[0], { name: "claim", args: ["ACCT-1", DEV] }, "claim uses the session account + device, never the body");
  const res = lastResponse(sent);
  assert.equal(res.type, T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES);
  assert.deepEqual(res.body, { leased: true, token: "srv-tok", anchorEpoch: 3, headEpoch: 5, leaseExpiresAtMs: 111, attempts: 0 });
});

test("leaf-3b claim: null (nothing publishable / busy / backing off) → { leased: false }", async () => {
  const { ctx, sent } = makeCtx({ account: "ACCT-2", outbox: makeOutbox({ claim: null }) });
  await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
  assert.deepEqual(lastResponse(sent).body, { leased: false });
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
  const { ctx: ctx2, sent: sent2, outbox: ob2 } = makeCtx({ account: "ACCT-4b", outbox: makeOutbox({ release: false }) });
  await new PropagationOutboxHandler(ctx2).handleRelease("r3", { leaseToken: "tokR" });
  assert.deepEqual(lastResponse(sent2).body, { released: false });
  void ob2;
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
  // No session → requireSession sends UNAUTHORIZED, handler returns.
  {
    const { ctx, sent, outbox } = makeCtx({ session: false });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  // Empty account identity → UNAUTHORIZED.
  {
    const { ctx, sent, outbox } = makeCtx({ account: "  " });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "UNAUTHORIZED");
    assert.equal(outbox.calls.length, 0);
  }
  // fs/desktop (no outbox on the runtime) → SERVICE_UNAVAILABLE.
  {
    const { ctx, sent } = makeCtx({ outbox: null });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "SERVICE_UNAVAILABLE");
  }
});

test("leaf-3b req 3: a delegated device WITHOUT deviceSet.publish → FORBIDDEN; WITH it → allowed", async () => {
  // Missing capability → FORBIDDEN, outbox untouched.
  {
    const { ctx, sent, outbox } = makeCtx({ authority: { mode: "delegated", grantedCapabilities: ["device.add"] } });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.equal(lastError(sent).code, "FORBIDDEN");
    assert.match(lastError(sent).message, /deviceSet\.publish/);
    assert.equal(outbox.calls.length, 0);
  }
  // Holds deviceSet.publish → proceeds to the outbox.
  {
    const outbox = makeOutbox({ claim: { token: "t", anchorEpoch: 1, headEpoch: 1, leaseExpiresAtMs: 9, attempts: 0 } });
    const { ctx, sent } = makeCtx({ account: "ACCT-del", deviceId: DEV2, authority: { mode: "delegated", grantedCapabilities: ["deviceSet.publish"] }, outbox });
    await new PropagationOutboxHandler(ctx).handleClaim("r1", {});
    assert.deepEqual(outbox.calls[0], { name: "claim", args: ["ACCT-del", DEV2] });
    assert.equal(lastResponse(sent).body.leased, true);
  }
});

test("leaf-3b req 1: the lease token is required and size-bounded (BAD_REQUEST, outbox untouched)", async () => {
  // Missing token.
  {
    const { ctx, sent, outbox } = makeCtx({});
    await new PropagationOutboxHandler(ctx).handlePrepare("r1", {});
    assert.equal(lastError(sent).code, "BAD_REQUEST");
    assert.equal(outbox.calls.length, 0);
  }
  // Oversized token (> 128 bytes).
  {
    const { ctx, sent, outbox } = makeCtx({});
    await new PropagationOutboxHandler(ctx).handleRelease("r1", { leaseToken: "z".repeat(129) });
    assert.equal(lastError(sent).code, "BAD_REQUEST");
    assert.match(lastError(sent).message, /128-byte limit/);
    assert.equal(outbox.calls.length, 0);
  }
});

test("leaf-3b req 8: per-account rate limit trips after the budget is exhausted", async () => {
  const NOW = 1_000_000; // fixed clock so the sliding window never advances mid-test
  const ACCT = "ACCT-ratelimit-unique"; // a distinct subject so no other test consumes its budget
  const outbox = makeOutbox({ claim: null });
  const { ctx, sent } = makeCtx({ account: ACCT, outbox, now: NOW });
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

test("leaf-3b req 8: a lease token NEVER appears in an error, even if the backend error text contains it", async () => {
  const SECRET = "super-secret-lease-token-value";
  // The outbox throws an error whose message embeds the token — the handler must NOT forward it.
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
