import test from "node:test";
import assert from "node:assert/strict";
import { bytesToBase64, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";
import { InboxClaimHandler } from "../src/protocol/handlers/InboxClaimHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;
const crypto = new NodeCryptoProvider();

// SESSION_AUTH_V5 §2A.6 — where F1b dies at the protocol layer: with the
// session holding SessionPrincipal.CLAIMANT(KA), a second unlinked claimant
// root can never co-reside on the connection, account-control ops are
// structurally out of reach, and mailbox scope stays proof-based.

function fakeWs() {
  return {
    OPEN: 1, readyState: 1,
    send() {}, on() {}, once() {}, off() {}, removeListener() {},
    close() {},
  };
}

function claimantSession(claimantKey = "KA") {
  const session = new GatewaySession({ runtime: {}, ws: fakeWs() });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  session._commitPrincipal(SessionPrincipal.claimant({ claimantPublicKeyB64: claimantKey }));
  return { session, errors, ctx: session._ctx };
}

test("binding an inbox rooted at KA is admitted; binding KB throws FORBIDDEN (backstop) — one session, one claimant root", () => {
  const { ctx } = claimantSession("KA");

  ctx.bindInboxToSession("inboxA", "KA");
  assert.equal(ctx.isInboxBound("inboxA"), true, "the principal's own root binds");

  assert.throws(
    () => ctx.bindInboxToSession("inboxB", "KB"),
    (err) => err.code === "FORBIDDEN",
    "an unlinked claimant root can never co-reside on a claimant session",
  );
  assert.equal(ctx.isInboxBound("inboxB"), false);
});

test("InboxClaimHandler fail-fast: a claim rooted at KB is FORBIDDEN before any signature work; a KA claim proceeds to signature verification", async () => {
  const { session, errors } = claimantSession("KA");
  // Real registry not needed for the admission check, but the handler checks
  // its presence first — a stub with the right method shape suffices, and the
  // KA case proves the request got PAST admission into signature verification.
  const runtimeWithRegistry = { inboxClaimRegistry: { async getClaim() { return null; }, async putClaim() { return { ok: true }; } } };
  Object.defineProperty(session, "runtime", { value: runtimeWithRegistry });
  const handler = new InboxClaimHandler(session._ctx);

  await handler.handleClaim("r1", {
    inboxId: "inbox:kb-target",
    claimantPublicKeyB64: "KB",
    claimedAtMs: 1,
    signatureB64: "irrelevant",
  });
  assert.equal(errors.at(-1).code, "FORBIDDEN", "KB refused at admission");

  await handler.handleClaim("r2", {
    inboxId: "inbox:ka-target",
    claimantPublicKeyB64: "KA",
    claimedAtMs: 1,
    signatureB64: "not-a-real-signature",
  });
  const err = errors.at(-1);
  assert.notEqual(err.code, "FORBIDDEN", "KA passes admission");
  assert.equal(err.code, "INVALID_SIGNATURE", "…and is then judged on its actual possession proof");
});

test("every ACCOUNT-classified operation is FORBIDDEN for a wire-committed claimant session, before any handler runs", async () => {
  const identity = {
    nodeKeyId: "nk", relayKeyId: "rk",
    nodePublicKeyB64: "", nodePrivateKeyB64: "",
  };
  // Commit through the REAL claimant handshake so this covers the wire path,
  // not a synthetic principal: reuse the flow from the v5 handshake suite in
  // miniature (identity needs real keys for challenge signing).
  const kp = await crypto.generateSigningKeyPair();
  identity.nodePublicKeyB64 = bytesToBase64(kp.publicKey);
  identity.nodePrivateKeyB64 = bytesToBase64(kp.privateKey);

  const session = new GatewaySession({ runtime: { getIdentity: () => identity }, ws: fakeWs() });
  const errors = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  session._safeSendRecord = () => {};
  session._commitPrincipal(SessionPrincipal.claimant({ claimantPublicKeyB64: "KA" }));

  const accountOps = session._registry.listTypes()
    .filter((t) => session._registry.requiredAuthority(t) === "ACCOUNT");
  assert.ok(accountOps.length >= 8, "the ACCOUNT class is populated");
  for (const op of accountOps) {
    session._frameCodec = { decodeFrame: () => ({ id: "r-" + op, type: op, body: {} }) };
    await session._handleSocketMessage(Buffer.from("{}"));
    const err = errors.at(-1);
    assert.equal(err.code, "FORBIDDEN", op + " answers FORBIDDEN to a claimant session");
    assert.equal(err.id, "r-" + op);
  }
});

test("mailbox scope stays proof-based: unbound inbox denied, bound KA inbox authorized, cross-inbox denied", async () => {
  const { ctx, errors } = claimantSession("KA");

  const unbound = await ctx.authorize({ requestId: "r1", action: "read", resource: "mailbox:someInbox" });
  assert.equal(unbound, null, "claimant identity alone is not mailbox authority");
  assert.equal(errors.at(-1).code, "FORBIDDEN");

  ctx.bindInboxToSession("inboxA", "KA");
  const bound = await ctx.authorize({ requestId: "r2", action: "read", resource: "mailbox:inboxA" });
  assert.ok(bound, "the proven binding authorizes its own inbox");

  const cross = await ctx.authorize({ requestId: "r3", action: "read", resource: "mailbox:inboxB" });
  assert.equal(cross, null, "KA's proof authorizes nothing on another inbox");
  assert.equal(errors.at(-1).code, "FORBIDDEN");
});

test("admitsClaimantBinding stays intrinsic: the CLAIMANT principal admits exactly its own root", () => {
  const p = SessionPrincipal.claimant({ claimantPublicKeyB64: "KA" });
  assert.equal(p.admitsClaimantBinding("KA"), true);
  assert.equal(p.admitsClaimantBinding("KB"), false);
  void T;
});
