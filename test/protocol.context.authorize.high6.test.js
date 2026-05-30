import test from "node:test";
import assert from "node:assert/strict";
import {
  MemoryStorageProvider,
  CapabilityValidator,
  CapabilitySigner,
  bytesToBase64,
} from "@rezprotocol/core";
import { CapabilityMiddleware } from "../src/protocol/CapabilityMiddleware.js";
import { ProtocolContext } from "../src/protocol/ProtocolContext.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

/**
 * docs/SECURITY_AUDIT.md HIGH-6 — `CapabilityMiddleware.resolveChain` had
 * the trust-root anchor implemented but was never reached from production.
 * Every authz request fell through the legacy id-based `resolve()`. This
 * suite drives `ProtocolContext.authorize` (the wire-side SSOT every
 * handler calls) and asserts the chain path lands in `resolveChain` with
 * the inbox-claim-registry anchor applied.
 *
 * Pair with `middleware.capability.chain.unit.test.js` which exercises
 * `resolveChain` in isolation — this file proves the wiring.
 */

const CRYPTO = new NodeCryptoProvider();

async function freshRegistry({ inboxId, claimantPublicKeyB64 } = {}) {
  const storageProvider = new MemoryStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();
  if (inboxId && claimantPublicKeyB64) {
    await registry.claim({ inboxId, claimantPublicKeyB64, claimedAtMs: Date.now() });
  }
  return registry;
}

function makeMockProtocol({ ownerPublicKeyB64 = "", boundInboxIds = [], errors }) {
  return {
    authenticated: true,
    ownerPublicKeyB64,
    sessionDeviceId: "dev-test",
    localInboxId: null,
    boundInboxIds: new Set(boundInboxIds),
    boundClaimantPublicKeys: new Set(),
    runtime: null,
    sessionRegistry: null,
    _sendErrorRecord(opts) { errors.push(opts); },
    _bindClaimantSession() {},
  };
}

function makeCtx({ registry, ownerPublicKeyB64, boundInboxIds = [] }) {
  const errors = [];
  const protocol = makeMockProtocol({ ownerPublicKeyB64, boundInboxIds, errors });
  const ctx = new ProtocolContext(protocol);
  const middleware = new CapabilityMiddleware({
    validator: new CapabilityValidator({ crypto: CRYPTO }),
    inboxClaimRegistry: registry,
  });
  ctx.setCapabilityMiddleware(middleware);
  return { ctx, errors };
}

async function rootCapSignedBy({ kp, resource, actions }) {
  const signer = new CapabilitySigner({ crypto: CRYPTO });
  return signer.createRootCapability({
    resource,
    actions,
    signerPublicKeyB64: bytesToBase64(kp.publicKey),
    privateKeyBytes: kp.privateKey,
  });
}

test("HIGH-6: a valid chain rooted at the inbox claimant is accepted via authorize()", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const inboxId = "inbox:high6-ok";
  const registry = await freshRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  const cap = await rootCapSignedBy({ kp: owner, resource: `mailbox:${inboxId}`, actions: ["read"] });

  const { ctx, errors } = makeCtx({ registry, ownerPublicKeyB64: ownerB64 });
  const result = await ctx.authorize({
    requestId: "req-ok",
    capabilityChain: [cap],
    action: "read",
    resource: `mailbox:${inboxId}`,
  });

  assert.ok(result, `expected truthy result, got ${result}; errors=${JSON.stringify(errors)}`);
  assert.equal(errors.length, 0);
});

test("HIGH-6: a chain rooted at a NON-claimant key is rejected (trust-root anchor fires through authorize)", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const attacker = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:high6-rooted-wrong";
  const registry = await freshRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  // Attacker mints a fully valid chain — by their own key.
  const cap = await rootCapSignedBy({ kp: attacker, resource: `mailbox:${inboxId}`, actions: ["read"] });

  const { ctx, errors } = makeCtx({ registry, ownerPublicKeyB64: bytesToBase64(attacker.publicKey) });
  const result = await ctx.authorize({
    requestId: "req-attacker",
    capabilityChain: [cap],
    action: "read",
    resource: `mailbox:${inboxId}`,
  });

  assert.equal(result, null);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].code, "FORBIDDEN");
  assert.match(errors[0].message, /root cap signer does not match inbox claimant/);
});

test("HIGH-6: session-binding shortcut authorizes free inbox ops with no chain", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const inboxId = "inbox:high6-bound";
  const registry = await freshRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });

  const { ctx, errors } = makeCtx({
    registry,
    ownerPublicKeyB64: ownerB64,
    boundInboxIds: [inboxId],
  });
  const result = await ctx.authorize({
    requestId: "req-bound",
    capabilityChain: null,
    action: "read",
    resource: `mailbox:${inboxId}`,
  });

  assert.ok(result, "session-binding shortcut should return a truthy sentinel");
  assert.equal(errors.length, 0);
});

test("HIGH-6: no chain AND no session binding → FORBIDDEN with explanatory message", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const inboxId = "inbox:high6-unbound";
  const registry = await freshRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });

  const { ctx, errors } = makeCtx({ registry, ownerPublicKeyB64: ownerB64, boundInboxIds: [] });
  const result = await ctx.authorize({
    requestId: "req-unbound",
    capabilityChain: null,
    action: "read",
    resource: `mailbox:${inboxId}`,
  });

  assert.equal(result, null);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].code, "FORBIDDEN");
  assert.match(errors[0].message, /capability chain required/);
});

test("HIGH-6: paid service without a chain is rejected even if the inbox is session-bound", async () => {
  // Closes a subtle bypass: the session-binding shortcut is free-ops only.
  // Allowing it on paid services would let an attacker run paid services on
  // any inbox they bound without the claimant's explicit spend authorization.
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const inboxId = "inbox:high6-paid";
  const registry = await freshRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });

  const { ctx, errors } = makeCtx({
    registry,
    ownerPublicKeyB64: ownerB64,
    boundInboxIds: [inboxId],
  });
  const result = await ctx.authorize({
    requestId: "req-paid",
    capabilityChain: null,
    action: "read",
    resource: `mailbox:${inboxId}`,
    serviceId: "mailbox.read.metered",
  });

  assert.equal(result, null);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].code, "FORBIDDEN");
  assert.match(errors[0].message, /paid service/);
});

test("HIGH-6: chain with widened scope is rejected (validateChain runs through authorize)", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const delegate = CRYPTO.generateSigningKeyPair();
  const delegateB64 = bytesToBase64(delegate.publicKey);
  const inboxId = "inbox:high6-widen";
  const registry = await freshRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });

  const signer = new CapabilitySigner({ crypto: CRYPTO });
  const root = await signer.createRootCapability({
    resource: `mailbox:${inboxId}`,
    actions: ["read"],
    signerPublicKeyB64: ownerB64,
    granteePublicKeyB64: delegateB64,
    privateKeyBytes: owner.privateKey,
  });
  const widened = await signer.delegateCapability({
    parentCapability: root,
    actions: ["read", "write"],
    signerPublicKeyB64: delegateB64,
    privateKeyBytes: delegate.privateKey,
  });

  const { ctx, errors } = makeCtx({ registry, ownerPublicKeyB64: delegateB64 });
  const result = await ctx.authorize({
    requestId: "req-widen",
    capabilityChain: [root, widened],
    action: "write",
    resource: `mailbox:${inboxId}`,
  });

  assert.equal(result, null);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].code, "FORBIDDEN");
  assert.match(errors[0].message, /chain invalid/);
});
