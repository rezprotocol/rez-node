import test from "node:test";
import assert from "node:assert/strict";
import {
  MemoryStorageProvider,
  bytesToBase64,
  signDepositPolicy,
  DepositPolicyV1,
  canonicalDepositPolicyBytes,
} from "@rezprotocol/core";
import { DepositPolicyStore } from "../src/inbox/DepositPolicyStore.js";
import { DepositRateLimitStore } from "../src/inbox/DepositRateLimitStore.js";
import { DepositPolicyHandler } from "../src/protocol/handlers/DepositPolicyHandler.js";
import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";

/**
 * Unit tests for docs/SECURITY_AUDIT.md HIGH-1 — per-inbox deposit policy
 * + per-(depositor, inbox) rate limit.
 */

const CRYPTO = new NodeCryptoProvider();
const INBOX_ID = "inbox:policy-test:1";

async function makeStores() {
  const storageProvider = new MemoryStorageProvider();
  const claimRegistry = new InboxClaimRegistry({ storageProvider });
  await claimRegistry.hydrate();
  const policyStore = new DepositPolicyStore({ storageProvider });
  await policyStore.hydrate();
  const rateLimitStore = new DepositRateLimitStore({ storageProvider });
  await rateLimitStore.hydrate();
  return { storageProvider, claimRegistry, policyStore, rateLimitStore };
}

function makePolicyCtx({ claimRegistry, policyStore, ownerPublicKeyB64 = "", boundClaimants = [] }) {
  const responses = [];
  const errors = [];
  return {
    runtime: { inboxClaimRegistry: claimRegistry, depositPolicyStore: policyStore },
    ownerPublicKeyB64,
    boundClaimantPublicKeys: new Set(boundClaimants),
    requireSession() { return true; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
    _responses: responses,
    _errors: errors,
  };
}

function makeMailboxCtx({ policyStore, rateLimitStore = null, ownerPublicKeyB64 = "", principal } = {}) {
  const responses = [];
  const errors = [];
  const deposits = [];
  // SESSION_AUTH_V5 slice 3: the deposit path consults the session principal.
  // Default = an ACCOUNT principal for the given depositor key (today's
  // shipped shape); pass `principal` explicitly for CLAIMANT depositors.
  const resolvedPrincipal = principal !== undefined
    ? principal
    : (ownerPublicKeyB64
      ? new SessionPrincipal({
        kind: SessionPrincipal.KINDS.ACCOUNT,
        accountPublicKeyB64: ownerPublicKeyB64,
        sessionDeviceId: "rez:dev:" + "0".repeat(64),
        authority: { mode: "direct", accountIdentityPublicKeyB64: ownerPublicKeyB64, signerPublicKeyB64: ownerPublicKeyB64 },
      })
      : null);
  return {
    principal: resolvedPrincipal,
    runtime: {
      depositPolicyStore: policyStore,
      depositRateLimitStore: rateLimitStore,
      gatewayLoop: {
        async sendToInbox(opts) {
          deposits.push(opts);
          return { packetId: "pkt:" + deposits.length };
        },
      },
    },
    ownerPublicKeyB64,
    requireSession() { return true; },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
    _responses: responses,
    _errors: errors,
    _deposits: deposits,
  };
}

async function bootstrapInbox({ claimRegistry, inboxId = INBOX_ID } = {}) {
  const kp = CRYPTO.generateSigningKeyPair();
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);
  await claimRegistry.claim({ inboxId, claimantPublicKeyB64, claimedAtMs: Date.now() });
  return { kp, claimantPublicKeyB64, inboxId };
}

// --- inbox.setDepositPolicy: legit + rejection paths ---

test("inbox.setDepositPolicy stores a claimant-signed policy", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { kp, claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  const ctx = makePolicyCtx({
    claimRegistry,
    policyStore,
    ownerPublicKeyB64: claimantPublicKeyB64,
    boundClaimants: [claimantPublicKeyB64],
  });
  const handler = new DepositPolicyHandler(ctx, { crypto: CRYPTO });

  const policy = await signDepositPolicy({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: ["mallory-pubkey"],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });

  await handler.handleSet("r1", { policy: policy.toJSON() });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._responses.length, 1);
  const stored = policyStore.get(inboxId);
  assert.ok(stored);
  assert.equal(stored.policyVersion, 1);
});

test("inbox.setDepositPolicy with wrong session pubkey is rejected", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { kp, claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  // Session has NOT proven possession of the inbox claimant key. Even
  // though the policy itself is correctly signed, the rule is that only
  // a session bound to the claimant may submit it.
  const ctx = makePolicyCtx({
    claimRegistry,
    policyStore,
    ownerPublicKeyB64: "rogue-pubkey",
    boundClaimants: ["rogue-pubkey"],
  });
  const handler = new DepositPolicyHandler(ctx, { crypto: CRYPTO });

  const policy = await signDepositPolicy({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: [],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });

  await handler.handleSet("r1", { policy: policy.toJSON() });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "UNAUTHORIZED");
  assert.equal(policyStore.get(inboxId), null);
});

test("inbox.setDepositPolicy with a forged signature is rejected", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  const ctx = makePolicyCtx({
    claimRegistry,
    policyStore,
    ownerPublicKeyB64: claimantPublicKeyB64,
    boundClaimants: [claimantPublicKeyB64],
  });
  const handler = new DepositPolicyHandler(ctx, { crypto: CRYPTO });

  // Build a policy JSON directly with an attacker-controlled signature.
  const attackerKp = CRYPTO.generateSigningKeyPair();
  const issuedAtMs = Date.now();
  const expiresAtMs = issuedAtMs + 60_000;
  const msg = canonicalDepositPolicyBytes({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: [],
    allowedDepositorPubkeys: [],
    issuedAtMs,
    expiresAtMs,
  });
  const forgedSig = CRYPTO.sign({ privateKey: attackerKp.privateKey, msg });
  const policyJson = new DepositPolicyV1({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: [],
    issuedAtMs,
    expiresAtMs,
    claimantPublicKeyB64, // claims it's signed by the real claimant
    signatureB64: bytesToBase64(forgedSig), // but actually signed by attacker
  }).toJSON();

  await handler.handleSet("r1", { policy: policyJson });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "INVALID_SIGNATURE");
  assert.equal(policyStore.get(inboxId), null);
});

test("inbox.setDepositPolicy rejects a non-monotonic version", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { kp, claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  const ctx = makePolicyCtx({
    claimRegistry,
    policyStore,
    ownerPublicKeyB64: claimantPublicKeyB64,
    boundClaimants: [claimantPublicKeyB64],
  });
  const handler = new DepositPolicyHandler(ctx, { crypto: CRYPTO });

  const v2 = await signDepositPolicy({
    inboxId,
    policyVersion: 2,
    blockedDepositorPubkeys: ["mallory"],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });
  await handler.handleSet("r1", { policy: v2.toJSON() });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));

  // Now try to push v1 (older).
  const v1 = await signDepositPolicy({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: [],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });
  await handler.handleSet("r2", { policy: v1.toJSON() });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "POLICY_VERSION_STALE");
  // Stored policy still v2.
  assert.equal(policyStore.get(inboxId).policyVersion, 2);
});

test("HIGH-2: one session with two unlinked claimants can publish each one's policy", async () => {
  // The session-auth identity is a third pubkey unrelated to either
  // claimant — proves the per-inbox cap-model authorization is decoupled
  // from session identity. See docs/CAPABILITY_MODEL.md §8.
  const { claimRegistry, policyStore } = await makeStores();
  const { kp: kpA, claimantPublicKeyB64: aB64, inboxId: inboxA } = await bootstrapInbox({
    claimRegistry,
    inboxId: "inbox:high2-a",
  });
  const { kp: kpB, claimantPublicKeyB64: bB64, inboxId: inboxB } = await bootstrapInbox({
    claimRegistry,
    inboxId: "inbox:high2-b",
  });
  const ctx = makePolicyCtx({
    claimRegistry,
    policyStore,
    ownerPublicKeyB64: "session-auth-identity-unrelated",
    boundClaimants: [aB64, bB64],
  });
  const handler = new DepositPolicyHandler(ctx, { crypto: CRYPTO });

  const policyA = await signDepositPolicy({
    inboxId: inboxA,
    policyVersion: 1,
    blockedDepositorPubkeys: ["mallory-a"],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64: aB64,
    crypto: CRYPTO,
    signingPrivateKey: kpA.privateKey,
  });
  const policyB = await signDepositPolicy({
    inboxId: inboxB,
    policyVersion: 1,
    blockedDepositorPubkeys: ["mallory-b"],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64: bB64,
    crypto: CRYPTO,
    signingPrivateKey: kpB.privateKey,
  });

  await handler.handleSet("r1", { policy: policyA.toJSON() });
  await handler.handleSet("r2", { policy: policyB.toJSON() });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(policyStore.get(inboxA).blockedDepositorPubkeys[0], "mallory-a");
  assert.equal(policyStore.get(inboxB).blockedDepositorPubkeys[0], "mallory-b");
});

// --- mailbox.deposit: policy enforcement ---

test("mailbox.deposit accepts deposits when no policy is registered (default-allow)", async () => {
  const { policyStore } = await makeStores();
  const ctx = makeMailboxCtx({ policyStore, ownerPublicKeyB64: "any-depositor" });
  const handler = new MailboxHandler(ctx);
  await handler.handleDeposit("d1", { mailboxId: "inbox:no-policy", ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._deposits.length, 1);
});

test("mailbox.deposit rejects deposits from blocklisted depositor pubkey", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { kp, claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  const malloryKp = CRYPTO.generateSigningKeyPair();
  const malloryPubkeyB64 = bytesToBase64(malloryKp.publicKey);
  const policy = await signDepositPolicy({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: [malloryPubkeyB64],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });
  await policyStore.put(policy);

  const ctx = makeMailboxCtx({ policyStore, ownerPublicKeyB64: malloryPubkeyB64 });
  const handler = new MailboxHandler(ctx);
  await handler.handleDeposit("d1", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "DEPOSIT_BLOCKED");
  assert.equal(ctx._deposits.length, 0);
});

test("mailbox.deposit allowlist: only listed senders may deposit", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { kp, claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  const aliceKp = CRYPTO.generateSigningKeyPair();
  const alicePubkeyB64 = bytesToBase64(aliceKp.publicKey);
  const bobKp = CRYPTO.generateSigningKeyPair();
  const bobPubkeyB64 = bytesToBase64(bobKp.publicKey);

  const policy = await signDepositPolicy({
    inboxId,
    policyVersion: 1,
    allowedDepositorPubkeys: [alicePubkeyB64],
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });
  await policyStore.put(policy);

  // Alice on the allowlist — accepted.
  const aliceCtx = makeMailboxCtx({ policyStore, ownerPublicKeyB64: alicePubkeyB64 });
  await new MailboxHandler(aliceCtx).handleDeposit("d1", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(aliceCtx._errors.length, 0, JSON.stringify(aliceCtx._errors));
  assert.equal(aliceCtx._deposits.length, 1);

  // Bob NOT on the allowlist — rejected.
  const bobCtx = makeMailboxCtx({ policyStore, ownerPublicKeyB64: bobPubkeyB64 });
  await new MailboxHandler(bobCtx).handleDeposit("d2", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(bobCtx._errors.length, 1);
  assert.equal(bobCtx._errors[0].code, "DEPOSIT_BLOCKED");
  assert.equal(bobCtx._deposits.length, 0);
});

// --- SESSION_AUTH_V5 slice 3: identity-bearing policy × CLAIMANT depositor ---
// The pinned live fail-open: `ctx.ownerPublicKeyB64 || ""` +
// `isDepositorBlocked("") === false` let a claimant session bypass BOTH list
// types. The verdicts stay distinct forever: DEPOSIT_BLOCKED (identity
// available, policy evaluated, denied) vs DEPOSITOR_IDENTITY_REQUIRED (this
// principal cannot supply what evaluation needs).

async function storedPolicy({ claimRegistry, policyStore, blocked = [], allowed = [] }) {
  const { kp, claimantPublicKeyB64, inboxId } = await bootstrapInbox({ claimRegistry });
  const policy = await signDepositPolicy({
    inboxId,
    policyVersion: 1,
    blockedDepositorPubkeys: blocked,
    allowedDepositorPubkeys: allowed,
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    claimantPublicKeyB64,
    crypto: CRYPTO,
    signingPrivateKey: kp.privateKey,
  });
  await policyStore.put(policy);
  return { inboxId };
}

const CLAIMANT_DEPOSITOR = () => SessionPrincipal.claimant({ claimantPublicKeyB64: "K-depositor" });

test("slice 3: CLAIMANT deposit against an ALLOWLIST-only policy → DEPOSITOR_IDENTITY_REQUIRED (the live fail-open, pinned)", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { inboxId } = await storedPolicy({ claimRegistry, policyStore, allowed: ["account-A-pubkey"] });
  const ctx = makeMailboxCtx({ policyStore, principal: CLAIMANT_DEPOSITOR() });
  await new MailboxHandler(ctx).handleDeposit("d1", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "DEPOSITOR_IDENTITY_REQUIRED", "not DEPOSIT_BLOCKED — the policy was never evaluated");
  assert.equal(ctx._deposits.length, 0, "nothing reached storage");
});

test("slice 3: CLAIMANT deposit against a BLOCKLIST-only policy → DEPOSITOR_IDENTITY_REQUIRED (both list types count — never allowlist-only)", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { inboxId } = await storedPolicy({ claimRegistry, policyStore, blocked: ["account-A-pubkey"] });
  const ctx = makeMailboxCtx({ policyStore, principal: CLAIMANT_DEPOSITOR() });
  await new MailboxHandler(ctx).handleDeposit("d1", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "DEPOSITOR_IDENTITY_REQUIRED");
  assert.equal(ctx._deposits.length, 0);
});

test("slice 3: a policy with NO identity criteria (both lists empty) evaluates normally for a CLAIMANT depositor", async () => {
  const { claimRegistry, policyStore } = await makeStores();
  const { inboxId } = await storedPolicy({ claimRegistry, policyStore });
  const ctx = makeMailboxCtx({ policyStore, principal: CLAIMANT_DEPOSITOR() });
  await new MailboxHandler(ctx).handleDeposit("d1", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._deposits.length, 1, "no identity needed ⇒ normal path");
});

test("slice 3: no policy at all → CLAIMANT deposits proceed on the anonymous default", async () => {
  const { policyStore } = await makeStores();
  const ctx = makeMailboxCtx({ policyStore, principal: CLAIMANT_DEPOSITOR() });
  await new MailboxHandler(ctx).handleDeposit("d1", { mailboxId: "inbox:anonymous-default", ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._deposits.length, 1);
});

test("mailbox.deposit rate limit kicks in after threshold", async () => {
  const { policyStore, rateLimitStore } = await makeStores();
  const depositorKp = CRYPTO.generateSigningKeyPair();
  const depositorPubkeyB64 = bytesToBase64(depositorKp.publicKey);
  const inboxId = "inbox:rate-limit-target:" + bytesToBase64(CRYPTO.randomBytes(8));
  const ctx = makeMailboxCtx({ policyStore, rateLimitStore, ownerPublicKeyB64: depositorPubkeyB64 });
  const handler = new MailboxHandler(ctx);

  // The default cap is 120 per minute — burn through it.
  for (let i = 0; i < 120; i += 1) {
    await handler.handleDeposit("d" + i, { mailboxId: inboxId, ciphertextB64: "AQ==" });
  }
  assert.equal(ctx._errors.length, 0, "120 deposits all under the cap");
  assert.equal(ctx._deposits.length, 120);

  // The 121st must be rate-limited.
  await handler.handleDeposit("d121", { mailboxId: inboxId, ciphertextB64: "AQ==" });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "RATE_LIMITED");
  assert.equal(ctx._deposits.length, 120);
});

test("DepositRateLimitStore persists counters across restart", async () => {
  const storageProvider = new MemoryStorageProvider();
  const depositorKp = CRYPTO.generateSigningKeyPair();
  const depositorPubkeyB64 = bytesToBase64(depositorKp.publicKey);
  const inboxId = "inbox:persist-target:" + bytesToBase64(CRYPTO.randomBytes(8));

  const store1 = new DepositRateLimitStore({ storageProvider });
  await store1.hydrate();
  for (let i = 0; i < 120; i += 1) {
    const ok = await store1.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: Date.now() });
    assert.equal(ok, true);
  }
  // 121st should be denied.
  const denied = await store1.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: Date.now() });
  assert.equal(denied, false);

  // Simulate restart: hydrate a fresh store from the same KV.
  const store2 = new DepositRateLimitStore({ storageProvider });
  await store2.hydrate();
  // The persisted state should still deny since the window is unchanged.
  const stillDenied = await store2.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: Date.now() });
  assert.equal(stillDenied, false, "rate limit state survives restart");
});

test("DepositRateLimitStore drops entries outside the window on hydrate", async () => {
  const storageProvider = new MemoryStorageProvider();
  const depositorKp = CRYPTO.generateSigningKeyPair();
  const depositorPubkeyB64 = bytesToBase64(depositorKp.publicKey);
  const inboxId = "inbox:window-drop:" + bytesToBase64(CRYPTO.randomBytes(8));

  // Use a tiny window so we can test pruning behavior without sleeping.
  const store1 = new DepositRateLimitStore({ storageProvider, windowMs: 5_000, maxDeposits: 3 });
  await store1.hydrate();
  const t0 = Date.now();
  await store1.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: t0 - 10_000 }); // outside window
  await store1.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: t0 - 9_000 }); // outside window
  await store1.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: t0 }); // inside

  // Restart and hydrate: only the in-window timestamp survives.
  const store2 = new DepositRateLimitStore({ storageProvider, windowMs: 5_000, maxDeposits: 3 });
  await store2.hydrate();
  // Two more in-window deposits should succeed (cap = 3 minus the 1 persisted).
  assert.equal(await store2.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: t0 + 100 }), true);
  assert.equal(await store2.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: t0 + 200 }), true);
  // Fourth denial.
  assert.equal(await store2.record({ depositorPubkeyB64, mailboxId: inboxId, nowMs: t0 + 300 }), false);
});
