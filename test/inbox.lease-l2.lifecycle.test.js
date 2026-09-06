import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";

import { InboxClaimRegistry, INBOX_LIFECYCLE } from "../src/inbox/InboxClaimRegistry.js";
import { RetentionPolicy } from "../src/inbox/RetentionPolicy.js";
import { InboxLifecycleSweeper } from "../src/inbox/InboxLifecycleSweeper.js";
import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";

// Portable inbox lease L2 (plans/PORTABLE_INBOX_LEASE_SPEC.md §3, §5) — the
// PURE lifecycle verdict, exercised at EXACT ±1ms boundaries. The whole point
// of the design: the verdict is a function of durable state + now, with no
// timer anywhere, so these tests pass identically against a registry that
// "restarted" (rehydrated) at any moment.

const LEASE_GRACE = 1_000;
const TERMINAL_GRACE = 2_000;
const policy = new RetentionPolicy({
  standardLeaseGraceMs: LEASE_GRACE,
  standardTerminalGraceMs: TERMINAL_GRACE,
  transientTerminalGraceMs: TERMINAL_GRACE,
});

const IB = (c) => "inbox:" + String(c).repeat(24);

async function freshRegistry(storageProvider = new MemoryStorageProvider()) {
  const registry = new InboxClaimRegistry({ storageProvider, retentionPolicy: policy });
  await registry.hydrate();
  return { registry, storageProvider };
}

async function standardClaim(registry, { inboxId = IB("a"), expiresAtMs = 10_000 } = {}) {
  await registry.claim({
    inboxId,
    claimantPublicKeyB64: "K-" + inboxId,
    claimedAtMs: 1,
    closePublicKeyB64: "CK-" + inboxId,
    generation: 1,
    retentionClass: "standard",
    leaseExpiresAtMs: expiresAtMs,
  });
  return inboxId;
}

async function reattest(registry, inboxId, leaseExpiresAtMs, retentionClass = "standard", nowMs = 10_500) {
  const claim = registry.getClaim(inboxId);
  return registry.admit({
    inboxId,
    claimantPublicKeyB64: claim.claimantPublicKeyB64,
    claimedAtMs: claim.claimedAtMs,
    closePublicKeyB64: claim.closePublicKeyB64,
    generation: claim.generation,
    retentionClass,
    leaseExpiresAtMs,
  }, null, () => nowMs);
}

const state = (registry, id, now) => registry.lifecycleFor(id, now).state;

// ---- Expiry boundaries, to the millisecond ----

test("expiry boundaries: ACTIVE until expiresAt-1ms; CLOSED_EXPIRED at expiresAt through graceEnd-1ms; RECLAIMABLE at graceEnd", async () => {
  const { registry } = await freshRegistry();
  const id = await standardClaim(registry, { expiresAtMs: 10_000 });
  const graceEnd = 10_000 + LEASE_GRACE;

  assert.equal(state(registry, id, 9_999), INBOX_LIFECYCLE.ACTIVE, "expiresAt - 1ms");
  assert.equal(state(registry, id, 10_000), INBOX_LIFECYCLE.CLOSED_EXPIRED, "expiresAt exactly");
  assert.equal(state(registry, id, 10_001), INBOX_LIFECYCLE.CLOSED_EXPIRED, "expiresAt + 1ms");
  assert.equal(state(registry, id, graceEnd - 1), INBOX_LIFECYCLE.CLOSED_EXPIRED, "graceEnd - 1ms");
  assert.equal(state(registry, id, graceEnd), INBOX_LIFECYCLE.RECLAIMABLE, "graceEnd exactly");
  assert.equal(state(registry, id, graceEnd + 1), INBOX_LIFECYCLE.RECLAIMABLE, "graceEnd + 1ms");
  assert.equal(registry.lifecycleFor(id, graceEnd).reason, "expired");
});

test("terminal boundaries: CLOSED_TERMINAL from close through graceEnd-1ms; RECLAIMABLE at graceEnd — and terminal BEATS expiry state", async () => {
  const { registry } = await freshRegistry();
  const id = await standardClaim(registry, { expiresAtMs: 10_000 });
  await registry.recordTerminalClose({ inboxId: id, finalGeneration: 1, closedAtMs: 5_000 });
  const graceEnd = 5_000 + TERMINAL_GRACE;

  assert.equal(state(registry, id, 5_000), INBOX_LIFECYCLE.CLOSED_TERMINAL);
  assert.equal(state(registry, id, graceEnd - 1), INBOX_LIFECYCLE.CLOSED_TERMINAL, "graceEnd - 1ms");
  assert.equal(state(registry, id, graceEnd), INBOX_LIFECYCLE.RECLAIMABLE, "graceEnd exactly");
  assert.equal(state(registry, id, graceEnd + 1), INBOX_LIFECYCLE.RECLAIMABLE, "graceEnd + 1ms");
  // Even at a time where the LEASE would still be ACTIVE, terminal wins.
  assert.equal(registry.lifecycleFor(id, 6_000).reason, "terminal");
  // And long after lease expiry+grace, the reason stays terminal — the
  // tombstone shadows the expiry lifecycle entirely.
  assert.equal(registry.lifecycleFor(id, 50_000).reason, "terminal");
});

test("transient class and legacy claims are permanently ACTIVE (legacy-identical) — expiry drives no lifecycle for them", async () => {
  const { registry } = await freshRegistry();
  await registry.claim({
    inboxId: IB("b"),
    claimantPublicKeyB64: "K",
    claimedAtMs: 1,
    closePublicKeyB64: "CK",
    generation: 1,
    retentionClass: "transient",
    leaseExpiresAtMs: 100,
  });
  await registry.claim({ inboxId: IB("c"), claimantPublicKeyB64: "KL", claimedAtMs: 1 });

  assert.equal(state(registry, IB("b"), 1_000_000), INBOX_LIFECYCLE.ACTIVE, "transient: lease long expired, still ACTIVE");
  assert.equal(state(registry, IB("c"), 1_000_000), INBOX_LIFECYCLE.ACTIVE, "legacy: no lease at all, ACTIVE");
});

// ---- The restart attack: verdicts are durable-state functions ----

test("RESTART: a rehydrated registry gives IDENTICAL verdicts at every boundary — no timer ever existed to miss", async () => {
  const storageProvider = new MemoryStorageProvider();
  const { registry } = await freshRegistry(storageProvider);
  const id = await standardClaim(registry, { expiresAtMs: 10_000 });
  await registry.recordTerminalClose({ inboxId: IB("z"), finalGeneration: 2, closedAtMs: 4_000 });

  // "Restart": a brand-new instance over the same durable KV.
  const { registry: reborn } = await freshRegistry(storageProvider);
  for (const now of [9_999, 10_000, 10_999, 11_000, 11_001]) {
    assert.equal(state(reborn, id, now), state(registry, id, now), "verdict stable across restart at t=" + now);
  }
  assert.equal(reborn.getTombstone(IB("z")).finalGeneration, 2);
});

// ---- Renewal: restores ACTIVE during grace, monotonic, class-fixed ----

test("renewal during grace restores ACTIVE; expiry never moves backwards; retentionClass is fixed at claim time", async () => {
  const { registry } = await freshRegistry();
  const id = await standardClaim(registry, { expiresAtMs: 10_000 });

  assert.equal(state(registry, id, 10_500), INBOX_LIFECYCLE.CLOSED_EXPIRED, "in grace");
  await reattest(registry, id, 20_000);
  assert.equal(state(registry, id, 10_500), INBOX_LIFECYCLE.ACTIVE, "renewal restored ACTIVE");
  assert.equal(registry.getClaim(id).leaseExpiresAtMs, 20_000);

  // Monotonic: a stale (earlier-expiring) renewal never rewinds the lease.
  await reattest(registry, id, 15_000);
  assert.equal(registry.getClaim(id).leaseExpiresAtMs, 20_000, "expiry never moves backwards");

  await assert.rejects(
    () => reattest(registry, id, 30_000, "transient"),
    (err) => err.code === "CLAIM_RECORD_MISMATCH",
    "class is fixed at claim time",
  );
});

// ---- Reclamation ----

test("reclamation removes the claim, writes a tombstone, and kills the generation forever", async () => {
  const { registry } = await freshRegistry();
  const id = await standardClaim(registry, { expiresAtMs: 10_000 });
  const afterGrace = 10_000 + LEASE_GRACE + 5;

  assert.deepEqual(registry.reclaimDue(afterGrace), [id]);

  assert.deepEqual(await registry.markReclaimed(id, afterGrace), { inboxId: id, reclaimed: true });
  assert.equal(registry.getClaim(id), null, "claim record removed");
  const tombstone = registry.getTombstone(id);
  assert.equal(tombstone.reason, "reclaimed");
  assert.equal(tombstone.finalGeneration, 1);
  // A stale lease of the reclaimed lifetime can never re-activate it: the
  // claim path refuses generation ≤ tombstone.
  await assert.rejects(
    () => registry.claim({
      inboxId: id, claimantPublicKeyB64: "K2", claimedAtMs: 60_000,
      closePublicKeyB64: "CK2", generation: 1, retentionClass: "standard", leaseExpiresAtMs: 99_000,
    }),
    (err) => err.code === "INBOX_CLOSED",
  );
  // Idempotent.
  assert.deepEqual(await registry.markReclaimed(id, afterGrace + 10), { inboxId: id, reclaimed: false });
});

test("sweeper: purges stored ciphertext through the store's own surface, idempotent, and never touches non-reclaimable inboxes", async () => {
  const { registry } = await freshRegistry();
  const dueId = await standardClaim(registry, { inboxId: IB("d"), expiresAtMs: 10_000 });
  const liveId = await standardClaim(registry, { inboxId: IB("e"), expiresAtMs: 500_000 });

  const acked = [];
  const pages = { [dueId]: [{ eventId: "e1" }, { eventId: "e2" }], [liveId]: [{ eventId: "keep" }] };
  const inboxStore = {
    async list(mailboxId) { return { items: pages[mailboxId] || [] }; },
    async ack(mailboxId, eventId) {
      acked.push(mailboxId + "/" + eventId);
      pages[mailboxId] = (pages[mailboxId] || []).filter((i) => i.eventId !== eventId);
      return true;
    },
  };
  let now = 10_000 + LEASE_GRACE + 1;
  const sweeper = new InboxLifecycleSweeper({ registry, inboxStore, now: () => now });

  const first = await sweeper.sweepOnce();
  assert.deepEqual(first.reclaimed, [dueId]);
  assert.deepEqual(acked, [dueId + "/e1", dueId + "/e2"], "only the reclaimed inbox was purged");
  assert.deepEqual(pages[liveId], [{ eventId: "keep" }], "the live inbox is untouched");

  const second = await sweeper.sweepOnce();
  assert.deepEqual(second.reclaimed, [], "idempotent");
});

test("a failed reclamation purge is durable work and the next sweep retries it", async () => {
  const { registry } = await freshRegistry();
  const id = await standardClaim(registry, { inboxId: IB("p"), expiresAtMs: 10_000 });
  const items = [{ eventId: "e1" }];
  let failOnce = true;
  const inboxStore = {
    async list() { return { items: items.slice() }; },
    async ack(mailboxId, eventId) {
      assert.equal(mailboxId, id);
      if (failOnce) {
        failOnce = false;
        throw new Error("purge failed");
      }
      const at = items.findIndex((entry) => entry.eventId === eventId);
      if (at >= 0) items.splice(at, 1);
    },
  };
  const sweeper = new InboxLifecycleSweeper({ registry, inboxStore, now: () => 11_001 });

  await assert.rejects(() => sweeper.sweepOnce(), /purge failed/);
  assert.equal(registry.getClaim(id), null, "the dead claim remains revoked");
  assert.deepEqual(registry.pendingPurgeInboxIds(), [id], "cleanup intent survives the failed pass");

  await sweeper.sweepOnce();
  assert.deepEqual(items, []);
  assert.deepEqual(registry.pendingPurgeInboxIds(), []);
});

// ---- Admission vs drainability at the deposit gate ----

test("deposit gate: ACTIVE accepts; CLOSED_EXPIRED refuses with retryable LEASE_EXPIRED; terminal/reclaimed refuse with INBOX_CLOSED", async () => {
  const { registry } = await freshRegistry();
  const id = await standardClaim(registry, { expiresAtMs: 10_000 });

  function depositCtx() {
    const errors = [];
    const deposits = [];
    return {
      runtime: {
        inboxClaimRegistry: registry,
        gatewayLoop: { async sendToInbox(opts) { deposits.push(opts); return { packetId: "p" }; } },
      },
      principal: SessionPrincipal.claimant({ claimantPublicKeyB64: "K-dep" }),
      ownerPublicKeyB64: null,
      sendResponse() {},
      sendError(payload) { errors.push(payload); },
      _errors: errors,
      _deposits: deposits,
    };
  }

  // The gate consults Date.now(), so drive it through claims whose expiry sits
  // around the REAL clock instead of a fake one — the verdict function itself
  // is exercised at exact boundaries above.
  const realNow = Date.now();
  const liveId = await standardClaim(registry, { inboxId: IB("f"), expiresAtMs: realNow + 60_000 });
  const okCtx = depositCtx();
  await new MailboxHandler(okCtx).handleDeposit("d1", { mailboxId: liveId, ciphertextB64: "AQ==" });
  assert.equal(okCtx._errors.length, 0, JSON.stringify(okCtx._errors));
  assert.equal(okCtx._deposits.length, 1, "ACTIVE accepts");

  const expiredId = await standardClaim(registry, { inboxId: IB("g"), expiresAtMs: realNow - 10 });
  const expCtx = depositCtx();
  await new MailboxHandler(expCtx).handleDeposit("d2", { mailboxId: expiredId, ciphertextB64: "AQ==" });
  assert.equal(expCtx._errors.at(-1).code, "LEASE_EXPIRED");
  assert.equal(expCtx._errors.at(-1).retryable, true, "the recipient may renew — the sender can retry");

  await registry.recordTerminalClose({ inboxId: id, finalGeneration: 1, closedAtMs: realNow });
  const termCtx = depositCtx();
  await new MailboxHandler(termCtx).handleDeposit("d3", { mailboxId: id, ciphertextB64: "AQ==" });
  assert.equal(termCtx._errors.at(-1).code, "INBOX_CLOSED");
  assert.equal(termCtx._errors.at(-1).retryable, false, "terminal is forever");
});
