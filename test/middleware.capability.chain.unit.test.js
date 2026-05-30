import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider, CapabilityValidator, CapabilitySigner, bytesToBase64 } from "@rezprotocol/core";
import { CapabilityMiddleware } from "../src/protocol/CapabilityMiddleware.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

/**
 * Unit tests for docs/SECURITY_AUDIT.md MED-3 — CapabilityMiddleware MUST
 * call validator.validateChain AND anchor the root signer to the inbox
 * claimant pubkey from InboxClaimRegistry. Without these, an attacker who
 * mints a well-formed cap signed by ANY key could pass authorization.
 */

const CRYPTO = new NodeCryptoProvider();

async function setupRegistry({ inboxId, claimantPublicKeyB64 } = {}) {
  const storageProvider = new MemoryStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();
  if (inboxId && claimantPublicKeyB64) {
    await registry.claim({ inboxId, claimantPublicKeyB64, claimedAtMs: Date.now() });
  }
  return registry;
}

function makeMiddleware(registry) {
  return new CapabilityMiddleware({
    validator: new CapabilityValidator({ crypto: CRYPTO }),
    inboxClaimRegistry: registry,
  });
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

test("MED-3: chain anchored to the inbox claimant is accepted", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const inboxId = "inbox:legit";
  const registry = await setupRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  const middleware = makeMiddleware(registry);

  const cap = await rootCapSignedBy({
    kp: owner,
    resource: `mailbox:${inboxId}`,
    actions: ["read"],
  });

  const result = await middleware.resolveChain({
    capabilityChain: [cap],
    requiredAction: "read",
    requiredResource: `mailbox:${inboxId}`,
  });
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.equal(result.capability.capId, cap.capId);
});

test("MED-3: chain with a valid sig but wrong root signer is rejected", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const attacker = CRYPTO.generateSigningKeyPair();
  const inboxId = "inbox:claimed-by-owner";
  const registry = await setupRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  const middleware = makeMiddleware(registry);

  // Attacker mints a perfectly-shaped, validly-signed cap — by their own key.
  const attackerCap = await rootCapSignedBy({
    kp: attacker,
    resource: `mailbox:${inboxId}`,
    actions: ["read"],
  });

  const result = await middleware.resolveChain({
    capabilityChain: [attackerCap],
    requiredAction: "read",
    requiredResource: `mailbox:${inboxId}`,
  });
  assert.equal(result.ok, false);
  assert.match(result.error, /root cap signer does not match inbox claimant/);
});

test("MED-3: chain on an unclaimed inbox is rejected (no trust root)", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  // Registry has no claim for the inbox the cap targets.
  const registry = await setupRegistry();
  const middleware = makeMiddleware(registry);

  const cap = await rootCapSignedBy({
    kp: owner,
    resource: "mailbox:inbox:nobody-claimed",
    actions: ["read"],
  });

  const result = await middleware.resolveChain({
    capabilityChain: [cap],
    requiredAction: "read",
    requiredResource: "mailbox:inbox:nobody-claimed",
  });
  assert.equal(result.ok, false);
  assert.match(result.error, /not claimed/);
});

test("MED-3: chain with a tampered signature is rejected by validateChain", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const inboxId = "inbox:tampered";
  const registry = await setupRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  const middleware = makeMiddleware(registry);

  const cap = await rootCapSignedBy({
    kp: owner,
    resource: `mailbox:${inboxId}`,
    actions: ["read"],
  });
  // Mutate the cap's actions AFTER signing — signature should no longer verify.
  const tampered = cap.toJSON();
  tampered.actions = ["read", "write"];
  const { RCapability } = await import("@rezprotocol/core");
  const tamperedCap = new RCapability(tampered);

  const result = await middleware.resolveChain({
    capabilityChain: [tamperedCap],
    requiredAction: "read",
    requiredResource: `mailbox:${inboxId}`,
  });
  assert.equal(result.ok, false);
  assert.match(result.error, /chain invalid/);
});

test("MED-3: delegation chain anchored to claimant + scoped down is accepted", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const delegate = CRYPTO.generateSigningKeyPair();
  const delegateB64 = bytesToBase64(delegate.publicKey);
  const inboxId = "inbox:delegated";
  const registry = await setupRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  const middleware = makeMiddleware(registry);

  const signer = new CapabilitySigner({ crypto: CRYPTO });
  const root = await signer.createRootCapability({
    resource: `mailbox:${inboxId}`,
    actions: ["read", "write"],
    signerPublicKeyB64: ownerB64,
    granteePublicKeyB64: delegateB64,
    privateKeyBytes: owner.privateKey,
  });
  // Delegate narrows actions to just "read".
  const delegated = await signer.delegateCapability({
    parentCapability: root,
    actions: ["read"],
    signerPublicKeyB64: delegateB64,
    privateKeyBytes: delegate.privateKey,
  });

  const result = await middleware.resolveChain({
    capabilityChain: [root, delegated],
    requiredAction: "read",
    requiredResource: `mailbox:${inboxId}`,
    presenterPublicKeyB64: null, // leaf has no grantee → bearer
  });
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.equal(result.capability.capId, delegated.capId);
});

test("MED-3: delegation chain that widens scope is rejected by validateChain", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  const delegate = CRYPTO.generateSigningKeyPair();
  const delegateB64 = bytesToBase64(delegate.publicKey);
  const inboxId = "inbox:scope-widen";
  const registry = await setupRegistry({ inboxId, claimantPublicKeyB64: ownerB64 });
  const middleware = makeMiddleware(registry);

  const signer = new CapabilitySigner({ crypto: CRYPTO });
  // Root grants ONLY read.
  const root = await signer.createRootCapability({
    resource: `mailbox:${inboxId}`,
    actions: ["read"],
    signerPublicKeyB64: ownerB64,
    granteePublicKeyB64: delegateB64,
    privateKeyBytes: owner.privateKey,
  });
  // Delegate tries to widen to "write" too.
  const widened = await signer.delegateCapability({
    parentCapability: root,
    actions: ["read", "write"],
    signerPublicKeyB64: delegateB64,
    privateKeyBytes: delegate.privateKey,
  });

  const result = await middleware.resolveChain({
    capabilityChain: [root, widened],
    requiredAction: "write",
    requiredResource: `mailbox:${inboxId}`,
  });
  assert.equal(result.ok, false);
  assert.match(result.error, /chain invalid/);
});

test("MED-3: chain on a non-inbox resource skips the inbox anchor (channel/object reserved)", async () => {
  const owner = CRYPTO.generateSigningKeyPair();
  const ownerB64 = bytesToBase64(owner.publicKey);
  // Empty registry — should not matter for channel-scoped resource.
  const registry = await setupRegistry();
  const middleware = makeMiddleware(registry);

  const cap = await rootCapSignedBy({
    kp: owner,
    resource: "channel:ch:1",
    actions: ["read"],
  });
  const result = await middleware.resolveChain({
    capabilityChain: [cap],
    requiredAction: "read",
    requiredResource: "channel:ch:1",
  });
  // No inbox anchor required for non-inbox kinds — current scope of MED-3
  // is the inbox trust root; channel/object resources will land in their
  // own audit pass.
  assert.equal(result.ok, true, JSON.stringify(result));
});
