import test from "node:test";
import assert from "node:assert/strict";
import { bytesToBase64, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { HandlerRegistry } from "../src/protocol/HandlerRegistry.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";
import { AuthorityRequirement } from "../src/protocol/AuthorityRequirement.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;
const crypto = new NodeCryptoProvider();

const OWNER_A = "owner-A-pub-b64";
const OWNER_B = "owner-B-pub-b64";
const DEVICE = "rez:dev:" + "a".repeat(64);

function directPrincipal(owner, deviceId = DEVICE) {
  return SessionPrincipal.accountDirect({
    accountPublicKeyB64: owner,
    sessionDeviceId: deviceId,
    authority: { mode: "direct", accountIdentityPublicKeyB64: owner, signerPublicKeyB64: owner },
  });
}

function claimantPrincipal(key = "KA") {
  return SessionPrincipal.claimant({ claimantPublicKeyB64: key });
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

/** A recording session registry: every add/remove lands in `log`; `owners()` is the live set. */
function recordingSessionRegistry() {
  const log = [];
  const live = new Set();
  return {
    log,
    owners: () => new Set(live),
    addSession({ ownerPublicKeyB64 }) { log.push(["add", ownerPublicKeyB64]); live.add(ownerPublicKeyB64); },
    removeSession({ ownerPublicKeyB64 }) { log.push(["remove", ownerPublicKeyB64]); live.delete(ownerPublicKeyB64); },
    broadcastToOwner() { return 0; },
  };
}

/**
 * The REAL registration surface with counting stub handlers: drives
 * GatewaySession._registerHandlers exactly as the constructor does (the same
 * technique as architecture.wire-manifest.test.js), so these dispatch tests
 * exercise every actually-registered operation, not a hand-copied list.
 */
function realRegistryWithCountingHandlers({ nodeEnabled = true } = {}) {
  const registry = new HandlerRegistry();
  const invocations = [];
  const stub = new Proxy({}, { get: (_t, method) => (requestId, body) => { invocations.push({ method, requestId, body }); } });
  const session = Object.create(GatewaySession.prototype);
  for (const slot of [
    "_mailboxHandler", "_inboxClaimHandler", "_inboxCloseHandler", "_deviceHandler", "_depositPolicyHandler",
    "_handleHandler", "_recordHandler", "_meshStatusHandler",
    "_accountMutationHandler", "_accountDeviceBundleHandler", "_propagationOutboxHandler",
  ]) session[slot] = stub;
  session._nodeEnabled = nodeEnabled;
  session._registry = registry;
  session._registerHandlers();
  return { registry, invocations };
}

// ---- Boot-time failure: an operation cannot register without declared authority ----

test("register() without an AuthorityRequirement THROWS — an unclassified op cannot exist", () => {
  const r = new HandlerRegistry();
  const handler = { handle() {} };
  assert.throws(() => r.register("some.op", handler, "handle"), /AuthorityRequirement/);
  assert.throws(() => r.register("some.op", handler, "handle", "SUPERUSER"), /AuthorityRequirement/);
  assert.throws(() => r.register("some.op", handler, "handle", null), /AuthorityRequirement/);
  assert.equal(r.has("some.op"), false, "nothing was registered by the failed attempts");
  r.register("some.op", handler, "handle", AuthorityRequirement.ANY_PRINCIPAL);
  assert.equal(r.has("some.op"), true);
});

// ---- Dispatcher enforcement: principal presence, then class, BEFORE the handler ----

test("dispatch with no principal throws UNAUTHORIZED and never invokes the handler", async () => {
  const r = new HandlerRegistry();
  let invoked = 0;
  r.register("some.op", { handle() { invoked += 1; } }, "handle", AuthorityRequirement.ANY_PRINCIPAL);
  for (const missing of [undefined, null, { kind: "ACCOUNT" }]) {
    await assert.rejects(() => r.dispatch("some.op", "r1", {}, missing), (err) => err.code === "UNAUTHORIZED");
  }
  assert.equal(invoked, 0);
});

test("a CLAIMANT principal is FORBIDDEN on every ACCOUNT-classified op; admitted on every ANY_PRINCIPAL op — across the REAL registration surface", async () => {
  const { registry, invocations } = realRegistryWithCountingHandlers({ nodeEnabled: true });
  const claimant = claimantPrincipal("KA");
  const accountOps = registry.listTypes().filter((t) => registry.requiredAuthority(t) === AuthorityRequirement.ACCOUNT);
  const anyOps = registry.listTypes().filter((t) => registry.requiredAuthority(t) === AuthorityRequirement.ANY_PRINCIPAL);
  assert.ok(accountOps.length > 0 && anyOps.length > 0, "both classes are populated");

  for (const op of accountOps) {
    await assert.rejects(
      () => registry.dispatch(op, "r1", { deliberately: "malformed" }, claimant),
      (err) => err.code === "FORBIDDEN",
      op + " must refuse a claimant",
    );
  }
  assert.equal(invocations.length, 0, "no ACCOUNT handler observed the claimant's request — not even to parse it");

  for (const op of anyOps) {
    await registry.dispatch(op, "r2", {}, claimant);
  }
  assert.equal(invocations.length, anyOps.length, "every ANY_PRINCIPAL handler was reached");
});

test("an ACCOUNT principal passes the class gate on EVERY registered op (resource scope stays the handler's job)", async () => {
  const { registry, invocations } = realRegistryWithCountingHandlers({ nodeEnabled: true });
  const account = directPrincipal(OWNER_A);
  for (const op of registry.listTypes()) {
    await registry.dispatch(op, "r1", {}, account);
  }
  assert.equal(invocations.length, registry.listTypes().length);
});

// ---- Error-shape centrality: FORBIDDEN, never a deep-validation distinction ----

test("wire path: a CLAIMANT hitting an ACCOUNT op with a malformed body gets exactly FORBIDDEN — no handler-internal error codes leak", async () => {
  const ws = fakeWs();
  const session = new GatewaySession({ runtime: {}, ws });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  session._commitPrincipal(claimantPrincipal("KA"));
  // A malformed body that, if AccountMutationHandler ran, would answer
  // SERVICE_UNAVAILABLE (no serializer) — seeing FORBIDDEN instead proves the
  // authority gate fired first and the handler never parsed anything.
  session._frameCodec = { decodeFrame: () => ({ id: "r1", type: T.ACCOUNT_DEVICE_MUTATION_SUBMIT, body: { mutation: 42 } }) };

  await session._handleSocketMessage(Buffer.from("{}"));

  assert.equal(errors.length, 1, "exactly one error frame");
  assert.equal(errors[0].code, "FORBIDDEN");
  assert.equal(ws.closes.length, 0, "an authority denial is an answer, not a socket close");
});

// ---- Binding note 1: v4 principal replacement (P → P′) ----

test("completed re-authentication REPLACES the principal atomically: B is authoritative, no A fragment survives, no dual registration", () => {
  const sessionRegistry = recordingSessionRegistry();
  const session = new GatewaySession({ runtime: {}, ws: fakeWs(), sessionRegistry });

  session._commitPrincipal(directPrincipal(OWNER_A));
  assert.equal(session.ownerPublicKeyB64, OWNER_A);
  assert.deepEqual([...sessionRegistry.owners()], [OWNER_A]);

  session._commitPrincipal(directPrincipal(OWNER_B, "rez:dev:" + "b".repeat(64)));

  assert.equal(session.principal.accountPublicKeyB64, OWNER_B, "principal === B");
  assert.equal(session.ownerPublicKeyB64, OWNER_B);
  assert.equal(session.sessionDeviceId, "rez:dev:" + "b".repeat(64));
  assert.equal(session.authenticated, true);
  assert.deepEqual([...sessionRegistry.owners()], [OWNER_B], "A's registry entry is GONE — no identity fragment remains");
  assert.deepEqual(sessionRegistry.log, [
    ["add", OWNER_A],
    ["remove", OWNER_A],
    ["add", OWNER_B],
  ], "old registration removed in the same replacement step");
});

test("_commitPrincipal refuses anything that is not a whole SessionPrincipal — no partial identity can ever be committed", () => {
  const session = new GatewaySession({ runtime: {}, ws: fakeWs() });
  for (const bad of [undefined, null, {}, { kind: "ACCOUNT", accountPublicKeyB64: OWNER_A }]) {
    assert.throws(() => session._commitPrincipal(bad), /SessionPrincipal/);
  }
  assert.equal(session.authenticated, false, "nothing was committed");
});

// ---- Binding note 1: partial re-auth never changes the principal ----

test("session.hello + challenge on an authenticated session does NOT change the principal — replacement commits only on COMPLETED authentication", async () => {
  const nodeKp = await crypto.generateSigningKeyPair();
  const identity = {
    nodeKeyId: "nk-test",
    nodePublicKeyB64: bytesToBase64(nodeKp.publicKey),
    nodePrivateKeyB64: bytesToBase64(nodeKp.privateKey),
    relayKeyId: "rk-test",
  };
  const session = new GatewaySession({ runtime: { getIdentity: () => identity }, ws: fakeWs() });
  const sentRaw = [];
  session._safeSendRawRecord = (type, opts) => sentRaw.push({ type, opts });
  session._sendErrorRecord = () => {};

  session._commitPrincipal(directPrincipal(OWNER_A));
  const principalBefore = session.principal;

  // Re-hello: a full challenge is issued for owner B…
  await session._beginSessionAuthentication(
    { accountIdentityPublicKeyB64: OWNER_B, sessionDeviceId: "rez:dev:" + "b".repeat(64) },
    "req-hello",
  );
  assert.ok(sentRaw.some((s) => s.type === T.SESSION_CHALLENGE), "a challenge went out");
  assert.equal(session.principal, principalBefore, "A stays authoritative through hello + challenge");
  assert.equal(session.ownerPublicKeyB64, OWNER_A);

  // …and a FAILED authenticate (garbage signature) still leaves A authoritative.
  await session._handleSessionAuthenticate("req-auth", { challengeId: "wrong", signatureB64: "AAAA" });
  assert.equal(session.principal, principalBefore, "a partial/failed re-auth never replaces the principal");
  assert.equal(session.authenticated, true);
});

// ---- ACCOUNT does not implicitly gain claimant scope; KA cannot exercise KB scope ----

test("an ACCOUNT session with no binding for an inbox is denied mailbox scope on it, and a KA binding grants nothing on another inbox", async () => {
  const sessionRegistry = recordingSessionRegistry();
  const session = new GatewaySession({ runtime: {}, ws: fakeWs(), sessionRegistry });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._commitPrincipal(directPrincipal(OWNER_A));
  const ctx = session._ctx;

  // Account identity alone is NOT mailbox authority.
  const denied = await ctx.authorize({ requestId: "r1", action: "read", resource: "mailbox:inboxX" });
  assert.equal(denied, null);
  assert.equal(errors.at(-1).code, "FORBIDDEN");

  // Prove claimant KA for inboxA — that scope is inboxA's, not the account's.
  ctx.bindInboxToSession("inboxA", "KA");
  const granted = await ctx.authorize({ requestId: "r2", action: "read", resource: "mailbox:inboxA" });
  assert.ok(granted, "the proven binding authorizes its own inbox");

  const crossScope = await ctx.authorize({ requestId: "r3", action: "read", resource: "mailbox:inboxB" });
  assert.equal(crossScope, null, "KA's proof authorizes nothing on inboxB");
  assert.equal(errors.at(-1).code, "FORBIDDEN");
});
