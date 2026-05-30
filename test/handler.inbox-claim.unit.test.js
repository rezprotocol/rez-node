import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider, bytesToBase64, canonicalJSONStringify } from "@rezprotocol/core";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { InboxClaimHandler } from "../src/protocol/handlers/InboxClaimHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

const CRYPTO = new NodeCryptoProvider();

const NODE_IDENTITY = (() => {
  const kp = CRYPTO.generateSigningKeyPair();
  return {
    nodeKeyId: "node-key-id-unit-test",
    nodePublicKeyB64: bytesToBase64(kp.publicKey),
    relayKeyId: "relay-key-id-unit-test",
  };
})();

function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

function signClaim({ inboxId, claimantPublicKeyB64, claimedAtMs, privateKey }) {
  return bytesToBase64(CRYPTO.sign({
    privateKey,
    msg: signedPayloadBytes({ inboxId, claimantPublicKeyB64, claimedAtMs }),
  }));
}

function signNodeDelegation({
  inboxId,
  claimantPublicKeyB64,
  privateKey,
  nodeKeyId = NODE_IDENTITY.nodeKeyId,
  nodePublicKeyB64 = NODE_IDENTITY.nodePublicKeyB64,
  relayKeyId = NODE_IDENTITY.relayKeyId,
  issuedAtMs = Date.now(),
  expiresAtMs = issuedAtMs + (7 * 24 * 60 * 60 * 1000),
}) {
  const payload = {
    kind: "inbox-node-delegation",
    inboxId,
    claimantPublicKeyB64,
    nodeKeyId,
    nodePublicKeyB64,
    relayKeyId,
    issuedAtMs,
    expiresAtMs,
  };
  const sig = CRYPTO.sign({ privateKey, msg: signedPayloadBytes(payload) });
  return { nodeKeyId, nodePublicKeyB64, relayKeyId, issuedAtMs, expiresAtMs, delegationSigB64: bytesToBase64(sig) };
}

function makeMockCtx({ registry, sessionEstablished = true, ownerPublicKeyB64 = "" } = {}) {
  const responses = [];
  const errors = [];
  const bindings = [];
  const sessionInbox = [];
  const hostedRegistrations = [];
  return {
    runtime: {
      inboxClaimRegistry: registry,
      getIdentity() { return { ...NODE_IDENTITY }; },
      async registerHostedSession(pubkey, registration) {
        hostedRegistrations.push({ pubkey, registration });
      },
    },
    ownerPublicKeyB64,
    requireSession() { return sessionEstablished; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
    bindInboxToSession(inboxId, claimantPublicKeyB64) {
      bindings.push({ inboxId, claimantPublicKeyB64 });
    },
    setSessionInbox(inboxId) { sessionInbox.push(inboxId); },
    _responses: responses,
    _errors: errors,
    _bindings: bindings,
    _sessionInbox: sessionInbox,
    _hostedRegistrations: hostedRegistrations,
  };
}

async function freshRegistry() {
  const storageProvider = new MemoryStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();
  return registry;
}

function buildBody({ inboxId, kp, claimedAtMs }) {
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);
  const signatureB64 = signClaim({ inboxId, claimantPublicKeyB64, claimedAtMs, privateKey: kp.privateKey });
  const nodeDelegation = signNodeDelegation({ inboxId, claimantPublicKeyB64, privateKey: kp.privateKey });
  return { inboxId, claimantPublicKeyB64, claimedAtMs, signatureB64, nodeDelegation };
}

test("first-time claim binds the inbox to the calling session", async () => {
  const registry = await freshRegistry();
  const kp = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:test-bind-1";
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);
  const ctx = makeMockCtx({ registry, ownerPublicKeyB64: claimantPublicKeyB64 });
  const handler = new InboxClaimHandler(ctx);
  const body = buildBody({ inboxId, kp, claimedAtMs: 1700000000000 });

  await handler.handleClaim("req-1", body);

  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._responses.length, 1);
  assert.equal(ctx._bindings.length, 1);
  assert.equal(ctx._bindings[0].inboxId, inboxId);
  assert.equal(ctx._bindings[0].claimantPublicKeyB64, claimantPublicKeyB64);
  assert.equal(ctx._sessionInbox.length, 1);
  assert.equal(ctx._sessionInbox[0], inboxId);
  assert.equal(ctx._hostedRegistrations.length, 1);
  assert.equal(ctx._hostedRegistrations[0].pubkey, claimantPublicKeyB64);
});

test("re-claim by same pubkey is idempotent and rebinds to the calling session", async () => {
  const registry = await freshRegistry();
  const kp = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:test-bind-2";
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);

  const ctxA = makeMockCtx({ registry, ownerPublicKeyB64: claimantPublicKeyB64 });
  const handlerA = new InboxClaimHandler(ctxA);
  await handlerA.handleClaim("req-a", buildBody({ inboxId, kp, claimedAtMs: 1 }));
  assert.equal(ctxA._bindings.length, 1, "session A is bound");

  const ctxB = makeMockCtx({ registry, ownerPublicKeyB64: claimantPublicKeyB64 });
  const handlerB = new InboxClaimHandler(ctxB);
  await handlerB.handleClaim("req-b", buildBody({ inboxId, kp, claimedAtMs: 2 }));

  assert.equal(ctxB._errors.length, 0, "re-claim by same pubkey should succeed");
  assert.equal(ctxB._responses.length, 1);
  assert.equal(ctxB._bindings.length, 1, "session B is bound");
  assert.equal(ctxB._bindings[0].inboxId, inboxId);
});

test("re-claim by a different pubkey rejects and does NOT bind", async () => {
  const registry = await freshRegistry();
  const inboxId = "inbox:test-bind-3";

  const owner = CRYPTO.generateSigningKeyPair();
  const ownerPubB64 = bytesToBase64(owner.publicKey);
  const ctxOwner = makeMockCtx({ registry, ownerPublicKeyB64: ownerPubB64 });
  const handlerOwner = new InboxClaimHandler(ctxOwner);
  await handlerOwner.handleClaim("req-owner", buildBody({ inboxId, kp: owner, claimedAtMs: 1 }));
  assert.equal(ctxOwner._bindings.length, 1);

  const attacker = CRYPTO.generateSigningKeyPair();
  const attackerPubB64 = bytesToBase64(attacker.publicKey);
  const ctxAttacker = makeMockCtx({ registry, ownerPublicKeyB64: attackerPubB64 });
  const handlerAttacker = new InboxClaimHandler(ctxAttacker);
  await handlerAttacker.handleClaim("req-attacker", buildBody({ inboxId, kp: attacker, claimedAtMs: 2 }));

  assert.equal(ctxAttacker._errors.length, 1);
  assert.equal(ctxAttacker._errors[0].code, "INBOX_ALREADY_CLAIMED");
  assert.equal(ctxAttacker._bindings.length, 0, "attacker session must NOT be bound to the inbox");

  assert.equal(registry.getClaimantPublicKey(inboxId), ownerPubB64);
});

test("invalid claim signature on re-claim does not bind", async () => {
  const registry = await freshRegistry();
  const inboxId = "inbox:test-bind-4";
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerPubB64 = bytesToBase64(owner.publicKey);

  const ctxA = makeMockCtx({ registry, ownerPublicKeyB64: ownerPubB64 });
  const handlerA = new InboxClaimHandler(ctxA);
  await handlerA.handleClaim("req-a", buildBody({ inboxId, kp: owner, claimedAtMs: 1 }));
  assert.equal(ctxA._bindings.length, 1);

  // Corrupt the claim sig: signed over a different inboxId.
  const body = buildBody({ inboxId, kp: owner, claimedAtMs: 2 });
  body.signatureB64 = signClaim({
    inboxId: "inbox:wrong",
    claimantPublicKeyB64: ownerPubB64,
    claimedAtMs: 2,
    privateKey: owner.privateKey,
  });
  const ctxB = makeMockCtx({ registry, ownerPublicKeyB64: ownerPubB64 });
  const handlerB = new InboxClaimHandler(ctxB);
  await handlerB.handleClaim("req-b", body);

  assert.equal(ctxB._errors.length, 1);
  assert.equal(ctxB._errors[0].code, "INVALID_SIGNATURE");
  assert.equal(ctxB._bindings.length, 0);
});

test("HIGH-2: a single session may claim multiple inboxes under unlinked keypairs", async () => {
  // Privacy primitive (docs/CAPABILITY_MODEL.md §8): the session-auth
  // identity must NOT force every inbox-claim in a session to share one
  // key. Verifying the per-claim signature is the only required proof.
  const registry = await freshRegistry();
  const sessionAuthKp = CRYPTO.generateSigningKeyPair();
  const sessionOwnerB64 = bytesToBase64(sessionAuthKp.publicKey);

  // Two unrelated claimant keypairs operated by the same human, in the
  // same session. The attacker model is a passive observer; if both
  // claims worked we proved that the relay cannot link them via session.
  const aliceKp = CRYPTO.generateSigningKeyPair();
  const aliceB64 = bytesToBase64(aliceKp.publicKey);
  const anonKp = CRYPTO.generateSigningKeyPair();
  const anonB64 = bytesToBase64(anonKp.publicKey);

  const ctx = makeMockCtx({ registry, ownerPublicKeyB64: sessionOwnerB64 });
  const handler = new InboxClaimHandler(ctx);

  await handler.handleClaim("req-alice", buildBody({
    inboxId: "inbox:alice-irl",
    kp: aliceKp,
    claimedAtMs: 1,
  }));
  await handler.handleClaim("req-anon", buildBody({
    inboxId: "inbox:alice-anon",
    kp: anonKp,
    claimedAtMs: 2,
  }));

  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._bindings.length, 2);
  assert.deepEqual(
    ctx._bindings.map((b) => b.claimantPublicKeyB64).sort(),
    [aliceB64, anonB64].sort(),
  );
  // Each claimant is registered with the hosted-inbox registry under its
  // OWN pubkey, NOT the session-auth identity — so a deposit router
  // looking up "alice-irl" doesn't accidentally route to "alice-anon".
  assert.equal(ctx._hostedRegistrations.length, 2);
  assert.deepEqual(
    ctx._hostedRegistrations.map((r) => r.pubkey).sort(),
    [aliceB64, anonB64].sort(),
  );
});

test("claim with missing inboxClaimRegistry returns SERVICE_UNAVAILABLE", async () => {
  const ctx = makeMockCtx({ registry: null });
  const handler = new InboxClaimHandler(ctx);
  await handler.handleClaim("req", {
    inboxId: "inbox:x", claimantPublicKeyB64: "AAAA", claimedAtMs: 1, signatureB64: "BBBB",
  });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "SERVICE_UNAVAILABLE");
  assert.equal(ctx._bindings.length, 0);
});

test("claim is blocked when session is not established", async () => {
  const registry = await freshRegistry();
  const ctx = makeMockCtx({ registry, sessionEstablished: false });
  const handler = new InboxClaimHandler(ctx);
  const kp = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:test-no-session";

  await handler.handleClaim("req", buildBody({ inboxId, kp, claimedAtMs: 1 }));

  assert.equal(ctx._responses.length, 0);
  assert.equal(ctx._bindings.length, 0);
});

test("claim with expired node-delegation rejects with INVALID_SIGNATURE", async () => {
  const registry = await freshRegistry();
  const kp = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:test-expired";
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);
  const ctx = makeMockCtx({ registry, ownerPublicKeyB64: claimantPublicKeyB64 });
  const handler = new InboxClaimHandler(ctx);
  const body = buildBody({ inboxId, kp, claimedAtMs: 1 });
  // Replace the delegation with a past-expiry one.
  body.nodeDelegation = signNodeDelegation({
    inboxId,
    claimantPublicKeyB64,
    privateKey: kp.privateKey,
    issuedAtMs: Date.now() - 100_000,
    expiresAtMs: Date.now() - 1_000,
  });

  await handler.handleClaim("req", body);
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "INVALID_SIGNATURE");
});

test("claim with delegation naming a different node rejects with INVALID_SIGNATURE", async () => {
  const registry = await freshRegistry();
  const kp = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:test-wrong-node";
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);
  const ctx = makeMockCtx({ registry, ownerPublicKeyB64: claimantPublicKeyB64 });
  const handler = new InboxClaimHandler(ctx);
  const body = buildBody({ inboxId, kp, claimedAtMs: 1 });
  body.nodeDelegation = signNodeDelegation({
    inboxId,
    claimantPublicKeyB64,
    privateKey: kp.privateKey,
    nodeKeyId: "different-node",
    nodePublicKeyB64: NODE_IDENTITY.nodePublicKeyB64,
  });

  await handler.handleClaim("req", body);
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "INVALID_SIGNATURE");
});
