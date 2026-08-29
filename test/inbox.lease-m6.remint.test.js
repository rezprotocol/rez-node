import test from "node:test";
import assert from "node:assert/strict";
import {
  MemoryStorageProvider,
  bytesToBase64,
  relayKeyIdForNodePublicKeyB64,
  nodeKeyIdForNodePublicKeyB64,
} from "@rezprotocol/core";
import { InboxClaimStore } from "@rezprotocol/sdk/client";

import { InboxClaimRegistry, INBOX_LIFECYCLE } from "../src/inbox/InboxClaimRegistry.js";
import { RetentionPolicy } from "../src/inbox/RetentionPolicy.js";
import { InboxLifecycleSweeper } from "../src/inbox/InboxLifecycleSweeper.js";
import { InboxClaimHandler } from "../src/protocol/handlers/InboxClaimHandler.js";
import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";

// M6 (rez-chat plans/MOBILE_LIFECYCLE_ADAPTER_PLAN.md §7e) — same-inboxId
// re-mint at generation+1 after EXPIRY RECLAMATION, never after terminal
// close. Real sdk InboxClaimStore against the real handlers, registry, and
// sweeper — the §7b registry verification turned into acceptance tests:
//   (1) tombstone blocks ≤ finalGeneration          (L1, re-pinned here)
//   (2) claim at finalGeneration+1 is admissible AND LIVES (the lifecycle
//       verdict evaluates the fresh lifetime — the gate found this false
//       before the reason-scoped lifecycleFor fix)
//   (3)/(4) generation isolation: the dead generation's ciphertext can never
//       surface under the new lifetime — including the crash window where
//       the registry recorded the reclamation but the purge never ran.
// Terminal tombstones kill the LINEAGE: every future claim refused, any
// generation (a malicious G+1 over a terminal close must not resurrect the
// inbox — that would undermine the close key).

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

// The handler validates delegation windows against REAL Date.now()
// (`expiresAtMs <= Date.now()` is refused), so the logical lease timeline is
// anchored at the real clock: claims at BASE, expiry/grace as offsets, and
// sweeps/lifecycle reads at injected logical-future instants.
const BASE = Date.now();
const LEASE_TTL = 60_000;
const LEASE_GRACE = 60_000;
const TERMINAL_GRACE = 60_000;
const policy = new RetentionPolicy({
  standardLeaseGraceMs: LEASE_GRACE,
  standardTerminalGraceMs: TERMINAL_GRACE,
  transientTerminalGraceMs: TERMINAL_GRACE,
});

function unitPrincipal() {
  return new SessionPrincipal({
    kind: SessionPrincipal.KINDS.ACCOUNT,
    accountPublicKeyB64: "unit-owner",
    sessionDeviceId: "rez:dev:" + "0".repeat(64),
    authority: { mode: "direct", accountIdentityPublicKeyB64: "unit-owner", signerPublicKeyB64: "unit-owner" },
  });
}

// In-memory inbox store with the surface the sweeper + purge-on-claim use.
function makeInboxStore() {
  const byInbox = new Map();
  return {
    deposit(inboxId, eventId) {
      if (!byInbox.has(inboxId)) byInbox.set(inboxId, []);
      byInbox.get(inboxId).push({ eventId });
    },
    async list(inboxId, { limit = 50 } = {}) {
      const items = byInbox.get(inboxId) || [];
      return { items: items.slice(0, limit) };
    },
    async ack(inboxId, eventId) {
      const items = byInbox.get(inboxId) || [];
      const idx = items.findIndex((i) => i.eventId === eventId);
      if (idx >= 0) items.splice(idx, 1);
    },
    count(inboxId) {
      return (byInbox.get(inboxId) || []).length;
    },
  };
}

function makeCtx({ registry, inboxStore = null }) {
  const responses = [];
  const errors = [];
  return {
    runtime: {
      inboxClaimRegistry: registry,
      inboxStore,
      getIdentity() { return { ...NODE_IDENTITY }; },
      gatewayLoop: {
        async sendToInbox({ inboxId }) {
          if (inboxStore) inboxStore.deposit(inboxId, "evt:" + (inboxStore.count(inboxId) + 1));
          return { packetId: "pkt" };
        },
      },
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

async function makeHarness() {
  const registry = new InboxClaimRegistry({ storageProvider: new MemoryStorageProvider(), retentionPolicy: policy });
  await registry.hydrate();
  const store = new InboxClaimStore({ storageProvider: new MemoryStorageProvider(), cryptoProvider: CRYPTO });
  await store.hydrate();
  const inboxStore = makeInboxStore();
  return { registry, store, inboxStore };
}

async function claimBodyFor(store, inboxId, { nowMs }) {
  const record = store.get(inboxId);
  const attestation = await store.createReattestation(inboxId, { clock: () => nowMs });
  const delegation = await store.createNodeDelegation({
    inboxId,
    nodeKeyId: NODE_IDENTITY.nodeKeyId,
    nodePublicKeyB64: NODE_IDENTITY.nodePublicKeyB64,
    relayKeyId: NODE_IDENTITY.relayKeyId,
    ttlMs: LEASE_TTL,
    retentionClass: "standard",
    clock: () => nowMs,
  });
  const body = {
    inboxId,
    claimantPublicKeyB64: attestation.claimantPublicKeyB64,
    claimedAtMs: attestation.claimedAtMs,
    signatureB64: attestation.claimSignatureB64,
    closePublicKeyB64: attestation.closePublicKeyB64,
    generation: attestation.generation,
    nodeDelegation: {
      nodeKeyId: delegation.nodeKeyId,
      nodePublicKeyB64: delegation.nodePublicKeyB64,
      relayKeyId: delegation.relayKeyId,
      issuedAtMs: delegation.issuedAtMs,
      expiresAtMs: delegation.expiresAtMs,
      delegationSigB64: delegation.delegationSigB64,
      generation: delegation.generation,
      retentionClass: delegation.retentionClass,
    },
  };
  assert.equal(Number.isInteger(record.generation), true, "harness expects a v2 claim");
  return body;
}

async function claimThrough({ registry, inboxStore }, store, inboxId, { nowMs }) {
  const ctx = makeCtx({ registry, inboxStore });
  await new InboxClaimHandler(ctx, { crypto: CRYPTO }).handleClaim("rq", await claimBodyFor(store, inboxId, { nowMs }));
  return ctx;
}

// Establish a standard-class generation-1 claim at t=0 (lease expires at
// LEASE_TTL; RECLAIMABLE at LEASE_TTL + LEASE_GRACE).
async function establishedGen1(h) {
  const claim = await h.store.createClaim({ clock: () => BASE });
  await h.store.persist(claim);
  const ctx = await claimThrough(h, h.store, claim.inboxId, { nowMs: BASE });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  return claim.inboxId;
}

// Claims are established at BASE, so the lease expires at BASE+TTL and the
// grace lapses at BASE+TTL+GRACE — this is safely past that boundary.
const RECLAIM_AT = BASE + LEASE_TTL + LEASE_GRACE + 1;

test("M6 wire semantics: a reattestation refused over a RECLAIMED tombstone carries typed {closeReason:'reclaimed', finalGeneration}", async () => {
  const h = await makeHarness();
  const inboxId = await establishedGen1(h);
  h.inboxStore.deposit(inboxId, "evt:old-mail");

  const sweeper = new InboxLifecycleSweeper({ registry: h.registry, inboxStore: h.inboxStore, now: () => RECLAIM_AT });
  const swept = await sweeper.sweepOnce();
  assert.deepEqual(swept.reclaimed, [inboxId]);
  assert.equal(h.inboxStore.count(inboxId), 0, "the sweep purged the dead generation's mail");

  // The phone wakes months later, still holding generation 1.
  const ctx = await claimThrough(h, h.store, inboxId, { nowMs: RECLAIM_AT + 1 });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "INBOX_CLOSED");
  assert.deepEqual(ctx._errors[0].detail, { closeReason: "reclaimed", finalGeneration: 1 });
});

test("M6 the full re-mint round-trip: reclaimed G=1 → remintGeneration → G=2 admitted, lifecycle evaluates the FRESH lifetime, deposits flow", async () => {
  const h = await makeHarness();
  const inboxId = await establishedGen1(h);
  const sweeper = new InboxLifecycleSweeper({ registry: h.registry, inboxStore: h.inboxStore, now: () => RECLAIM_AT });
  await sweeper.sweepOnce();

  const reminted = await h.store.remintGeneration({ inboxId, finalGeneration: 1, clock: () => RECLAIM_AT + 5 });
  assert.deepEqual(reminted, { inboxId, fromGeneration: 1, toGeneration: 2 });

  const ctx = await claimThrough(h, h.store, inboxId, { nowMs: RECLAIM_AT + 10 });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(ctx._responses.length, 1, "generation 2 admitted over the reclaimed tombstone");

  // The gate's original finding, now fixed: the lifecycle verdict evaluates
  // the fresh lifetime instead of declaring it CLOSED_TERMINAL.
  const verdict = h.registry.lifecycleFor(inboxId, RECLAIM_AT + 11);
  assert.equal(verdict.state, INBOX_LIFECYCLE.ACTIVE, "fresh lifetime is ACTIVE under its own lease");

  // Deposits to the re-minted address flow (the MailboxHandler lifecycle gate
  // sees the fresh lifetime). The tombstone still exists — it governs ≤ 1.
  assert.ok(h.registry.getTombstone(inboxId), "the tombstone is retained");
  const depositCtx = makeCtx({ registry: h.registry, inboxStore: h.inboxStore });
  await new MailboxHandler(depositCtx).handleDeposit("rq-dep", {
    mailboxId: inboxId,
    ciphertextB64: "bmV3LWxpZmV0aW1l",
  });
  assert.equal(depositCtx._errors.length, 0, JSON.stringify(depositCtx._errors));
});

test("M6 crash window (§7e pin 7): registry reclaimed but purge never ran — the G+1 claim purges residual bytes BEFORE admission; nothing bleeds into the new lifetime", async () => {
  const h = await makeHarness();
  const inboxId = await establishedGen1(h);
  h.inboxStore.deposit(inboxId, "evt:orphan-1");
  h.inboxStore.deposit(inboxId, "evt:orphan-2");

  // The crash shape: markReclaimed committed, process died before the purge.
  const marked = await h.registry.markReclaimed(inboxId, RECLAIM_AT);
  assert.equal(marked.reclaimed, true);
  assert.equal(h.inboxStore.count(inboxId), 2, "orphaned dead-generation bytes remain");

  await h.store.remintGeneration({ inboxId, finalGeneration: 1, clock: () => RECLAIM_AT + 5 });
  const ctx = await claimThrough(h, h.store, inboxId, { nowMs: RECLAIM_AT + 10 });
  assert.equal(ctx._errors.length, 0, JSON.stringify(ctx._errors));
  assert.equal(h.inboxStore.count(inboxId), 0,
    "purge-on-claim removed the orphans before the fresh lifetime could exist — old bytes can never appear under G+1");
});

test("M6 terminal kills the LINEAGE: even a valid higher-generation claim is refused over a terminal tombstone (the close key cannot be undermined)", async () => {
  const h = await makeHarness();
  const inboxId = await establishedGen1(h);
  const close = await h.store.createTerminalClose(inboxId, { clock: () => BASE + 10 });
  await h.registry.recordTerminalClose({ inboxId, finalGeneration: close.finalGeneration, closedAtMs: close.closedAtMs });

  // A buggy/malicious claimant locally re-mints anyway and submits G+1.
  await h.store.remintGeneration({ inboxId, finalGeneration: 1, clock: () => BASE + 20 });
  const ctx = await claimThrough(h, h.store, inboxId, { nowMs: BASE + 30 });
  assert.equal(ctx._errors.length, 1);
  assert.equal(ctx._errors[0].code, "INBOX_CLOSED");
  assert.deepEqual(ctx._errors[0].detail, { closeReason: "terminal", finalGeneration: 1 },
    "typed detail says terminal — the client policy must never auto-re-mint on it");
});

test("M6 negatives: legacy tombstone (no reason) defaults terminal; old-generation replay stays dead after G+1; generation conflict refuses to guess", async () => {
  const h = await makeHarness();

  // Legacy tombstone without a reason field: treated as terminal (unknown
  // historical closure must never permit resurrection).
  const legacyInbox = await establishedGen1(h);
  await h.registry.recordTerminalClose({ inboxId: legacyInbox, finalGeneration: 1, closedAtMs: BASE + 5 });
  const legacyTombstone = h.registry.getTombstone(legacyInbox);
  assert.equal(legacyTombstone.reason, "terminal");

  // Fresh harness for the replay case.
  const h2 = await makeHarness();
  const inboxId = await establishedGen1(h2);
  // Capture the OLD generation-1 claim body before re-minting.
  const oldBody = await claimBodyFor(h2.store, inboxId, { nowMs: BASE + 2 });
  const sweeper = new InboxLifecycleSweeper({ registry: h2.registry, inboxStore: h2.inboxStore, now: () => RECLAIM_AT });
  await sweeper.sweepOnce();
  await h2.store.remintGeneration({ inboxId, finalGeneration: 1, clock: () => RECLAIM_AT + 5 });
  const okCtx = await claimThrough(h2, h2.store, inboxId, { nowMs: RECLAIM_AT + 10 });
  assert.equal(okCtx._errors.length, 0);

  // The old G=1 body replayed after G=2 is live: refused (tombstone ≤ 1) —
  // a stale lease of the dead lifetime can never re-activate it.
  const replayCtx = makeCtx({ registry: h2.registry, inboxStore: h2.inboxStore });
  await new InboxClaimHandler(replayCtx, { crypto: CRYPTO }).handleClaim("rq-replay", oldBody);
  assert.equal(replayCtx._errors.length, 1);
  assert.equal(replayCtx._errors[0].code, "INBOX_CLOSED");
  assert.deepEqual(replayCtx._errors[0].detail, { closeReason: "reclaimed", finalGeneration: 1 });

  // Generation conflict: the provider names a finalGeneration that does not
  // match the stored one — refuse loudly, never guess.
  await assert.rejects(
    h2.store.remintGeneration({ inboxId, finalGeneration: 7 }),
    (err) => err.code === "REMINT_GENERATION_CONFLICT",
  );
});

test("M6 one-semantic rule: the MailboxHandler bare-tombstone fallback (no lifecycleFor) applies the same reason+generation scoping", async () => {
  // Registry double with getTombstone/getClaim but NO lifecycleFor — the
  // legacy fallback branch under test.
  const tombstone = { finalGeneration: 1, closedAtMs: 5, reason: "reclaimed" };
  let claim = { generation: 2 };
  const registryDouble = {
    getTombstone: async () => tombstone,
    getClaim: async () => claim,
  };
  const inboxStore = makeInboxStore();
  const freshCtx = makeCtx({ registry: registryDouble, inboxStore });
  freshCtx.runtime.inboxClaimRegistry = registryDouble;
  await new MailboxHandler(freshCtx).handleDeposit("rq-1", { mailboxId: "inbox:" + "e".repeat(24), ciphertextB64: "bXNn" });
  assert.equal(freshCtx._errors.length, 0, "fresh lifetime (gen 2 > final 1, reclaimed) deposits proceed");

  claim = { generation: 1 };
  const deadCtx = makeCtx({ registry: registryDouble, inboxStore });
  deadCtx.runtime.inboxClaimRegistry = registryDouble;
  await new MailboxHandler(deadCtx).handleDeposit("rq-2", { mailboxId: "inbox:" + "e".repeat(24), ciphertextB64: "bXNn" });
  assert.equal(deadCtx._errors.length, 1, "dead generation refused");
  assert.deepEqual(deadCtx._errors[0].detail, { closeReason: "reclaimed", finalGeneration: 1 });

  tombstone.reason = "terminal";
  claim = { generation: 99 };
  const terminalCtx = makeCtx({ registry: registryDouble, inboxStore });
  terminalCtx.runtime.inboxClaimRegistry = registryDouble;
  await new MailboxHandler(terminalCtx).handleDeposit("rq-3", { mailboxId: "inbox:" + "e".repeat(24), ciphertextB64: "bXNn" });
  assert.equal(terminalCtx._errors.length, 1, "terminal governs the lineage regardless of claim generation");
});
