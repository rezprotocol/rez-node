import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  signHandleOwnershipProof,
} from "@rezprotocol/core";
import { HandleHandler } from "../src/handle/HandleHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

/**
 * Unit tests for HandleHandler ownership-proof verification. Closes
 * docs/SECURITY_AUDIT.md CRITICAL-3 — handle.register/release used to
 * accept any caller who knew the handle's keyId (which handle.resolve
 * returned publicly), letting anyone hijack any handle by reading + writing.
 *
 * After the fix, every mutation requires an Ed25519 signature by the
 * private key matching the claimed keyId (keyId IS the base64 pubkey).
 */

const CRYPTO = new NodeCryptoProvider();
const RELAY_KEY_ID = "test-relay-key";

function makeFakeRegistry() {
  const claims = new Map();
  const registered = [];
  const released = [];
  return {
    selfRelayKeyId: RELAY_KEY_ID,
    async register(handle, keyId) {
      registered.push({ handle, keyId });
      const claim = {
        handle,
        keyId,
        relayKeyId: RELAY_KEY_ID,
        createdAtMs: Date.now(),
        expiresAtMs: Date.now() + 365 * 24 * 60 * 60 * 1000,
        previousKeyId: null,
      };
      claims.set(handle, claim);
      return claim;
    },
    async resolve(handle) {
      return claims.get(handle) || null;
    },
    async release(handle, keyId) {
      const existing = claims.get(handle);
      if (!existing) return false;
      if (existing.keyId !== keyId) return false;
      claims.delete(handle);
      released.push({ handle, keyId });
      return true;
    },
    _registered: registered,
    _released: released,
    _claims: claims,
  };
}

function makeCtx({ registry, clock = () => Date.now() } = {}) {
  const responses = [];
  const errors = [];
  return {
    runtime: { handleRegistry: registry, handleExchange: null },
    requireSession() { return true; },
    async authorize() { return { id: "cap-test", actions: ["read", "write"] }; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
    _responses: responses,
    _errors: errors,
    _clock: clock,
  };
}

function newHandlerForClock(ctx, clockFn) {
  return new HandleHandler(ctx, { crypto: CRYPTO, clock: clockFn });
}

async function buildOwnershipBody({ kind, handle, kp, tsMs, relayKeyId = RELAY_KEY_ID, signWithPrivateKey = null }) {
  const keyId = bytesToBase64(kp.publicKey);
  const signatureB64 = await signHandleOwnershipProof({
    kind,
    handle,
    keyId,
    tsMs,
    relayKeyId,
    crypto: CRYPTO,
    signingPrivateKey: signWithPrivateKey || kp.privateKey,
  });
  return { handle, keyId, tsMs, relayKeyId, signatureB64 };
}

// --- legitimate flows ---

test("handle.register with a valid ownership proof succeeds", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const kp = CRYPTO.generateSigningKeyPair();

  const body = await buildOwnershipBody({
    kind: "handle.register",
    handle: "alice",
    kp,
    tsMs: 1_700_000_000_000,
  });
  await handler.handleRegister("r1", body);

  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._responses.length, 1);
  assert.equal(registry._registered.length, 1);
  assert.equal(registry._registered[0].handle, "alice");
});

test("handle.release with a valid ownership proof by the owner succeeds", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const kp = CRYPTO.generateSigningKeyPair();
  const keyId = bytesToBase64(kp.publicKey);

  // Pre-populate a claim owned by kp.
  registry._claims.set("alice", { handle: "alice", keyId, relayKeyId: RELAY_KEY_ID });

  const body = await buildOwnershipBody({
    kind: "handle.release",
    handle: "alice",
    kp,
    tsMs: 1_700_000_000_000,
  });
  await handler.handleRelease("r2", body);

  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._responses.length, 1);
  assert.equal(ctx._responses[0].body.released, true);
  assert.equal(registry._released.length, 1);
});

// --- hijack defenses ---

test("handle.register without a signature is rejected", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const kp = CRYPTO.generateSigningKeyPair();

  await handler.handleRegister("r1", {
    handle: "alice",
    keyId: bytesToBase64(kp.publicKey),
    // no tsMs, no relayKeyId, no signatureB64
  });

  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(registry._registered.length, 0);
});

test("handle.release without a signature is rejected (closes the public-keyId hijack)", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const aliceKp = CRYPTO.generateSigningKeyPair();
  const aliceKeyId = bytesToBase64(aliceKp.publicKey);
  registry._claims.set("alice", { handle: "alice", keyId: aliceKeyId, relayKeyId: RELAY_KEY_ID });

  // Mallory learns aliceKeyId via handle.resolve and tries to release the
  // handle. With no signature she's blocked at the proof check.
  await handler.handleRelease("r-mal", {
    handle: "alice",
    keyId: aliceKeyId,
  });

  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(registry._released.length, 0);
});

test("handle.release with a signature by a DIFFERENT keypair is rejected", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const aliceKp = CRYPTO.generateSigningKeyPair();
  const aliceKeyId = bytesToBase64(aliceKp.publicKey);
  registry._claims.set("alice", { handle: "alice", keyId: aliceKeyId, relayKeyId: RELAY_KEY_ID });

  // Mallory signs a release proof with HER OWN privkey but claims keyId =
  // aliceKeyId. Forged sig fails verification against alice's pubkey.
  const malloryKp = CRYPTO.generateSigningKeyPair();
  // Use the helper at low level (bypass the sanity check) to construct the
  // malformed proof — sign with Mallory's privkey but claim Alice's keyId.
  const { canonicalHandleProofBytes } = await import("@rezprotocol/core");
  const msg = canonicalHandleProofBytes({
    kind: "handle.release",
    handle: "alice",
    keyId: aliceKeyId,
    tsMs: 1_700_000_000_000,
    relayKeyId: RELAY_KEY_ID,
  });
  const sigBytes = CRYPTO.sign({ privateKey: malloryKp.privateKey, msg });

  await handler.handleRelease("r-forge", {
    handle: "alice",
    keyId: aliceKeyId,
    tsMs: 1_700_000_000_000,
    relayKeyId: RELAY_KEY_ID,
    signatureB64: bytesToBase64(sigBytes),
  });

  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(registry._released.length, 0);
});

test("handle.register with a stale timestamp is rejected", async () => {
  const registry = makeFakeRegistry();
  const now = 1_700_000_000_000;
  const ctx = makeCtx({ registry, clock: () => now });
  const handler = newHandlerForClock(ctx, () => now);
  const kp = CRYPTO.generateSigningKeyPair();

  // Sign with a tsMs 10 minutes in the past — outside the 5-minute skew.
  const staleTs = now - (10 * 60 * 1000);
  const body = await buildOwnershipBody({
    kind: "handle.register",
    handle: "alice",
    kp,
    tsMs: staleTs,
  });
  await handler.handleRegister("r-stale", body);

  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(registry._registered.length, 0);
});

test("handle.register pinned to a different relayKeyId is rejected", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const kp = CRYPTO.generateSigningKeyPair();

  // Sign a proof pinned to "wrong-relay" — this relay's keyId is RELAY_KEY_ID.
  const body = await buildOwnershipBody({
    kind: "handle.register",
    handle: "alice",
    kp,
    tsMs: 1_700_000_000_000,
    relayKeyId: "wrong-relay",
  });
  await handler.handleRegister("r-wrong-relay", body);

  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(registry._registered.length, 0);
});

test("handle.register signed for a different action (release) is rejected at register", async () => {
  const registry = makeFakeRegistry();
  const ctx = makeCtx({ registry });
  const handler = newHandlerForClock(ctx, () => 1_700_000_000_000);
  const kp = CRYPTO.generateSigningKeyPair();

  // Sign a release-kind proof but submit it through the register handler.
  // The kind binding in canonical bytes means the signature won't verify
  // when the verifier reconstructs the bytes with kind="handle.register".
  const body = await buildOwnershipBody({
    kind: "handle.release",
    handle: "alice",
    kp,
    tsMs: 1_700_000_000_000,
  });
  await handler.handleRegister("r-wrong-kind", body);

  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(registry._registered.length, 0);
});
