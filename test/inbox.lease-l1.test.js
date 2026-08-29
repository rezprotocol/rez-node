import test from "node:test";
import assert from "node:assert/strict";
import {
  MemoryStorageProvider,
  bytesToBase64,
  canonicalJSONStringify,
  relayKeyIdForNodePublicKeyB64,
  nodeKeyIdForNodePublicKeyB64,
} from "@rezprotocol/core";
import { InboxClaimStore } from "@rezprotocol/sdk/client";

import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { InboxClaimHandler } from "../src/protocol/handlers/InboxClaimHandler.js";
import { InboxCloseHandler } from "../src/protocol/handlers/InboxCloseHandler.js";
import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";

// Portable inbox lease L1 (plans/PORTABLE_INBOX_LEASE_SPEC.md) — records +
// admission semantics over the REAL sdk claim store and the REAL node
// handlers/registry: v2 claims (close key + generation inside the signed
// payload), the lease fields on the delegation, TerminalInboxClose, the
// generation kill rule, and the admission ≠ retention split at the deposit
// gate. Time-driven retention (grace windows, reclamation) is L2, built with
// the adversarial spike.

const CRYPTO = new NodeCryptoProvider();

const NODE_IDENTITY = (() => {
  const kp = CRYPTO.generateSigningKeyPair();
  const nodePublicKeyB64 = bytesToBase64(kp.publicKey);
  return {
    nodeKeyId: nodeKeyIdForNodePublicKeyB64(nodePublicKeyB64),
    nodePublicKeyB64,
    relayKeyId: relayKeyIdForNodePublicKeyB64(nodePublicKeyB64),
  };
})();

async function makeRegistry() {
  const registry = new InboxClaimRegistry({ storageProvider: new MemoryStorageProvider() });
  await registry.hydrate();
  return registry;
}

async function makeClaimStore() {
  const store = new InboxClaimStore({ storageProvider: new MemoryStorageProvider(), cryptoProvider: CRYPTO });
  await store.hydrate();
  return store;
}

function unitPrincipal() {
  return new SessionPrincipal({
    kind: SessionPrincipal.KINDS.ACCOUNT,
    accountPublicKeyB64: "unit-owner",
    sessionDeviceId: "rez:dev:" + "0".repeat(64),
    authority: { mode: "direct", accountIdentityPublicKeyB64: "unit-owner", signerPublicKeyB64: "unit-owner" },
  });
}

function makeCtx({ registry }) {
  const responses = [];
  const errors = [];
  return {
    runtime: {
      inboxClaimRegistry: registry,
      getIdentity() { return { ...NODE_IDENTITY }; },
    },
    principal: unitPrincipal(),
    ownerPublicKeyB64: "unit-owner",
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
    bindInboxToSession() {},
    setSessionInbox() {},
    _responses: responses,
    _errors: errors,
  };
}

function makeDepositCtx({ registry }) {
  const responses = [];
  const errors = [];
  const deposits = [];
  return {
    runtime: {
      inboxClaimRegistry: registry,
      gatewayLoop: {
        async sendToInbox(opts) { deposits.push(opts); return { packetId: "pkt:" + deposits.length }; },
      },
    },
    principal: unitPrincipal(),
    ownerPublicKeyB64: "unit-owner",
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
    _responses: responses,
    _errors: errors,
    _deposits: deposits,
  };
}

async function claimBodyFor(store, claim, { legacyShape = false } = {}) {
  const delegation = await store.createNodeDelegation({
    inboxId: claim.inboxId,
    nodeKeyId: NODE_IDENTITY.nodeKeyId,
    nodePublicKeyB64: NODE_IDENTITY.nodePublicKeyB64,
    relayKeyId: NODE_IDENTITY.relayKeyId,
  });
  const body = {
    inboxId: claim.inboxId,
    claimantPublicKeyB64: claim.claimantPublicKeyB64,
    claimedAtMs: claim.claimedAtMs,
    signatureB64: claim.claimSignatureB64,
    nodeDelegation: {
      nodeKeyId: delegation.nodeKeyId,
      nodePublicKeyB64: delegation.nodePublicKeyB64,
      relayKeyId: delegation.relayKeyId,
      issuedAtMs: delegation.issuedAtMs,
      expiresAtMs: delegation.expiresAtMs,
      delegationSigB64: delegation.delegationSigB64,
    },
  };
  if (!legacyShape && Number.isInteger(claim.generation)) {
    body.closePublicKeyB64 = claim.closePublicKeyB64;
    body.generation = claim.generation;
    body.nodeDelegation.generation = delegation.generation;
    body.nodeDelegation.retentionClass = delegation.retentionClass;
  }
  return body;
}

async function claimThrough(registry, store, claim) {
  const ctx = makeCtx({ registry });
  await new InboxClaimHandler(ctx, { crypto: CRYPTO }).handleClaim("rq", await claimBodyFor(store, claim));
  return ctx;
}

// ---- v2 claim + lease acceptance ----

test("L1: a v2 claim (close key + generation inside the signed payload, lease fields on the delegation) is accepted and stored", async () => {
  const registry = await makeRegistry();
  const store = await makeClaimStore();
  const claim = await store.persist(await store.createClaim());
  assert.notEqual(claim.closePublicKeyB64, claim.claimantPublicKeyB64, "close key is its own random keypair");
  assert.equal(claim.generation, 1);

  const ctx = await claimThrough(registry, store, claim);
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._responses.length, 1);

  const stored = registry.getClaim(claim.inboxId);
  assert.equal(stored.closePublicKeyB64, claim.closePublicKeyB64);
  assert.equal(stored.generation, 1);
});

test("L1 compat: a LEGACY claim (3-field payload, legacy delegation) is still accepted, stored without lease fields", async () => {
  const registry = await makeRegistry();
  const store = await makeClaimStore();
  // Hand-build a legacy claim: sign the legacy payload with a fresh key.
  const kp = CRYPTO.generateSigningKeyPair();
  const claimantPublicKeyB64 = bytesToBase64(kp.publicKey);
  const inboxId = "inbox:" + "d".repeat(24);
  const claimedAtMs = Date.now();
  const signatureB64 = bytesToBase64(CRYPTO.sign({
    privateKey: kp.privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify({ inboxId, claimantPublicKeyB64, claimedAtMs })),
  }));
  const delegationPayload = {
    kind: "inbox-node-delegation",
    inboxId,
    claimantPublicKeyB64,
    nodeKeyId: NODE_IDENTITY.nodeKeyId,
    nodePublicKeyB64: NODE_IDENTITY.nodePublicKeyB64,
    relayKeyId: NODE_IDENTITY.relayKeyId,
    issuedAtMs: claimedAtMs,
    expiresAtMs: claimedAtMs + 60_000,
  };
  const delegationSigB64 = bytesToBase64(CRYPTO.sign({
    privateKey: kp.privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify(delegationPayload)),
  }));
  const ctx = makeCtx({ registry });
  await new InboxClaimHandler(ctx, { crypto: CRYPTO }).handleClaim("rq", {
    inboxId,
    claimantPublicKeyB64,
    claimedAtMs,
    signatureB64,
    nodeDelegation: {
      nodeKeyId: delegationPayload.nodeKeyId,
      nodePublicKeyB64: delegationPayload.nodePublicKeyB64,
      relayKeyId: delegationPayload.relayKeyId,
      issuedAtMs: delegationPayload.issuedAtMs,
      expiresAtMs: delegationPayload.expiresAtMs,
      delegationSigB64,
    },
  });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  const stored = registry.getClaim(inboxId);
  assert.equal(stored.generation, undefined, "legacy claims stay legacy");
  void store;
});

test("L1 fail-closed pairing: a v2 claim with a LEGACY delegation, a mismatched lease generation, or an unknown retentionClass is refused", async () => {
  const registry = await makeRegistry();
  const store = await makeClaimStore();
  const claim = await store.persist(await store.createClaim());

  // v2 claim + legacy-shaped lease → refused.
  const legacyLease = await claimBodyFor(store, claim);
  delete legacyLease.nodeDelegation.generation;
  delete legacyLease.nodeDelegation.retentionClass;
  const ctx1 = makeCtx({ registry });
  await new InboxClaimHandler(ctx1, { crypto: CRYPTO }).handleClaim("r1", legacyLease);
  assert.equal(ctx1._errors.at(-1).code, "INVALID_SIGNATURE", "claim/lease version mismatch is refused");

  // Tampered lease generation → signature reconstruction fails.
  const tampered = await claimBodyFor(store, claim);
  tampered.nodeDelegation.generation = 2;
  const ctx2 = makeCtx({ registry });
  await new InboxClaimHandler(ctx2, { crypto: CRYPTO }).handleClaim("r2", tampered);
  assert.equal(ctx2._errors.at(-1).code, "INVALID_SIGNATURE");

  // Unknown retention class → refused, never silently downgraded.
  const unknownClass = await claimBodyFor(store, claim);
  unknownClass.nodeDelegation.retentionClass = "platinum";
  const ctx3 = makeCtx({ registry });
  await new InboxClaimHandler(ctx3, { crypto: CRYPTO }).handleClaim("r3", unknownClass);
  assert.equal(ctx3._errors.at(-1).code, "INVALID_SIGNATURE");
});

test("L1 reattestation consistency: same v2 fields re-attest fine; a downgraded (legacy-shaped) reattestation of a v2 claim is refused", async () => {
  const registry = await makeRegistry();
  const store = await makeClaimStore();
  const claim = await store.persist(await store.createClaim());
  await claimThrough(registry, store, claim);

  // Same fields (real reattestation) → idempotent success.
  const attestation = await store.createReattestation(claim.inboxId);
  const reBody = await claimBodyFor(store, { ...attestation, closePublicKeyB64: attestation.closePublicKeyB64, generation: attestation.generation });
  const ctx1 = makeCtx({ registry });
  await new InboxClaimHandler(ctx1, { crypto: CRYPTO }).handleClaim("r1", reBody);
  assert.equal(ctx1._errors.length, 0, JSON.stringify(ctx1._errors));

  // Downgrade: a FULLY consistent legacy-shaped submission (legacy claim
  // payload AND legacy delegation payload, both correctly signed with the
  // SAME claimant key) — every signature verifies, so the refusal comes from
  // the stored-record consistency check and nothing earlier.
  const claimantPriv = Uint8Array.from(Buffer.from(store.get(claim.inboxId).claimantPrivateKeyB64, "base64"));
  const claimedAtMs = Date.now();
  const legacySig = bytesToBase64(CRYPTO.sign({
    privateKey: claimantPriv,
    msg: new TextEncoder().encode(canonicalJSONStringify({
      inboxId: claim.inboxId,
      claimantPublicKeyB64: claim.claimantPublicKeyB64,
      claimedAtMs,
    })),
  }));
  const legacyDelegationPayload = {
    kind: "inbox-node-delegation",
    inboxId: claim.inboxId,
    claimantPublicKeyB64: claim.claimantPublicKeyB64,
    nodeKeyId: NODE_IDENTITY.nodeKeyId,
    nodePublicKeyB64: NODE_IDENTITY.nodePublicKeyB64,
    relayKeyId: NODE_IDENTITY.relayKeyId,
    issuedAtMs: claimedAtMs,
    expiresAtMs: claimedAtMs + 60_000,
  };
  const downgraded = {
    inboxId: claim.inboxId,
    claimantPublicKeyB64: claim.claimantPublicKeyB64,
    claimedAtMs,
    signatureB64: legacySig,
    nodeDelegation: {
      nodeKeyId: legacyDelegationPayload.nodeKeyId,
      nodePublicKeyB64: legacyDelegationPayload.nodePublicKeyB64,
      relayKeyId: legacyDelegationPayload.relayKeyId,
      issuedAtMs: legacyDelegationPayload.issuedAtMs,
      expiresAtMs: legacyDelegationPayload.expiresAtMs,
      delegationSigB64: bytesToBase64(CRYPTO.sign({
        privateKey: claimantPriv,
        msg: new TextEncoder().encode(canonicalJSONStringify(legacyDelegationPayload)),
      })),
    },
  };
  const ctx2 = makeCtx({ registry });
  await new InboxClaimHandler(ctx2, { crypto: CRYPTO }).handleClaim("r2", downgraded);
  assert.equal(ctx2._errors.at(-1).code, "CLAIM_RECORD_MISMATCH", "role/version downgrade of a stored v2 claim is refused");
});

// ---- Terminal close: the kill switch ----

test("L1 close lifecycle: TerminalInboxClose is accepted (self-authorizing), idempotent, and admission dies — deposits, reattestation, and any claim at ≤ G are refused", async () => {
  const registry = await makeRegistry();
  const store = await makeClaimStore();
  const claim = await store.persist(await store.createClaim());
  await claimThrough(registry, store, claim);

  const close = await store.createTerminalClose(claim.inboxId);
  const closeCtx = makeCtx({ registry });
  const closeHandler = new InboxCloseHandler(closeCtx, { crypto: CRYPTO });
  await closeHandler.handleClose("c1", close.toJSON());
  assert.equal(closeCtx._errors.length, 0, JSON.stringify(closeCtx._errors));
  assert.equal(closeCtx._responses.at(-1).body.closed, true);

  // Idempotent.
  await closeHandler.handleClose("c2", close.toJSON());
  assert.equal(closeCtx._responses.at(-1).body.closed, true);

  // Deposits refused immediately and permanently.
  const depositCtx = makeDepositCtx({ registry });
  await new MailboxHandler(depositCtx).handleDeposit("d1", { mailboxId: claim.inboxId, ciphertextB64: "AQ==" });
  assert.equal(depositCtx._errors.at(-1).code, "INBOX_CLOSED");
  assert.equal(depositCtx._deposits.length, 0);

  // Reattestation (same generation) refused: nothing at ≤ G becomes active.
  const reBody = await claimBodyFor(store, claim);
  const reCtx = makeCtx({ registry });
  await new InboxClaimHandler(reCtx, { crypto: CRYPTO }).handleClaim("r1", reBody);
  assert.equal(reCtx._errors.at(-1).code, "INBOX_CLOSED");

  // A FRESH claim of the same inboxId at generation 1 (stale-lifetime replay)
  // is refused by the tombstone.
  const otherStore = await makeClaimStore();
  const replay = await otherStore.persist(await otherStore.createClaim({ inboxId: claim.inboxId }));
  const replayCtx = makeCtx({ registry });
  await new InboxClaimHandler(replayCtx, { crypto: CRYPTO }).handleClaim("r2", await claimBodyFor(otherStore, replay));
  assert.equal(replayCtx._errors.at(-1).code, "INBOX_CLOSED");

  // The claim record is KEPT (CLOSED = drain-your-mail-then-die): claimant
  // reads stay resolvable through the grace window.
  assert.equal(registry.getClaimantPublicKey(claim.inboxId), claim.claimantPublicKeyB64);
  assert.equal(registry.getTombstone(claim.inboxId).finalGeneration, 1);
});

test("L1 close refusals: wrong key, wrong generation, unknown inbox, legacy claim", async () => {
  const registry = await makeRegistry();
  const store = await makeClaimStore();
  const claim = await store.persist(await store.createClaim());
  await claimThrough(registry, store, claim);
  const ctx = makeCtx({ registry });
  const handler = new InboxCloseHandler(ctx, { crypto: CRYPTO });

  // Signed by a random key that is NOT the registered close key — the claim
  // key itself must also fail (renew-capable ≠ kill-capable), which this
  // covers by construction since only the close key verifies.
  const rogue = CRYPTO.generateSigningKeyPair();
  const forged = await store.createTerminalClose(claim.inboxId);
  const forgedJson = forged.toJSON();
  forgedJson.signatureB64 = bytesToBase64(CRYPTO.sign({
    privateKey: rogue.privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify({
      kind: "terminal-inbox-close",
      inboxId: forgedJson.inboxId,
      finalGeneration: forgedJson.finalGeneration,
      closedAtMs: forgedJson.closedAtMs,
    })),
  }));
  await handler.handleClose("c1", forgedJson);
  assert.equal(ctx._errors.at(-1).code, "INVALID_SIGNATURE");

  // Wrong generation.
  const wrongGen = (await store.createTerminalClose(claim.inboxId)).toJSON();
  wrongGen.finalGeneration = 2;
  await handler.handleClose("c2", wrongGen);
  assert.equal(ctx._errors.at(-1).code, "BAD_REQUEST");

  // Unknown inbox.
  const unknown = (await store.createTerminalClose(claim.inboxId)).toJSON();
  unknown.inboxId = "inbox:" + "e".repeat(24);
  await handler.handleClose("c3", unknown);
  assert.equal(ctx._errors.at(-1).code, "UNKNOWN_INBOX");

  // Legacy claim: no close key registered — not closable by record.
  await registry.claim({ inboxId: "inbox:" + "f".repeat(24), claimantPublicKeyB64: "legacy-K", claimedAtMs: 1 });
  await handler.handleClose("c4", { v: 1, inboxId: "inbox:" + "f".repeat(24), finalGeneration: 1, closedAtMs: 2, signatureB64: "AA==" });
  assert.equal(ctx._errors.at(-1).code, "INBOX_NOT_CLOSABLE");

  // Nothing was closed by any of the refusals.
  assert.equal(registry.getTombstone(claim.inboxId), null);
});

test("L1 registry durability: claims (v2 fields) and tombstones survive rehydration", async () => {
  const storageProvider = new MemoryStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();
  await registry.claim({
    inboxId: "inbox:" + "1".repeat(24),
    claimantPublicKeyB64: "K",
    claimedAtMs: 5,
    closePublicKeyB64: "CK",
    generation: 1,
    retentionClass: "standard",
    leaseExpiresAtMs: 10_000,
  });
  await registry.recordTerminalClose({ inboxId: "inbox:" + "2".repeat(24), finalGeneration: 3, closedAtMs: 9 });

  const reloaded = new InboxClaimRegistry({ storageProvider });
  await reloaded.hydrate();
  assert.equal(reloaded.getClaim("inbox:" + "1".repeat(24)).closePublicKeyB64, "CK");
  assert.equal(reloaded.getClaim("inbox:" + "1".repeat(24)).generation, 1);
  assert.equal(reloaded.getTombstone("inbox:" + "2".repeat(24)).finalGeneration, 3);
});
