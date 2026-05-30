import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { DepositRateLimitStore } from "../src/inbox/DepositRateLimitStore.js";
import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";

/**
 * docs/SECURITY_AUDIT.md LOW-4 — the deposit rate limit was keyed only
 * on (depositor pubkey, inbox). A `session.hello` keypair rotation is
 * free, so an attacker could open many sessions with fresh pubkeys and
 * keep depositing at the per-pubkey cap repeatedly, effectively evading
 * BOTH the rate limit AND the per-inbox blocklist (which keys on the
 * same pubkey).
 *
 * The remediation adds a per-(source IP, inbox) cap alongside the
 * per-(pubkey, inbox) cap. Both must allow for a deposit to proceed.
 * The IP-keyed cap survives pubkey rotation, so the rotation-rate
 * attack is bounded to one inbox's worth of deposits per IP per
 * window — regardless of how many fresh keypairs Mallory burns.
 */

const INBOX = "inbox:low4-test";
const PK_A = "pk:alice";
const PK_B = "pk:bob";
const PK_C = "pk:charlie";

async function makeStore({ windowMs = 60_000, maxDeposits = 3 } = {}) {
  const storageProvider = new MemoryStorageProvider();
  const store = new DepositRateLimitStore({ storageProvider, windowMs, maxDeposits });
  await store.hydrate();
  return { store, storageProvider };
}

function makeMailboxCtx({ rateLimitStore, peerIp = "", ownerPublicKeyB64 = "" }) {
  const errors = [];
  const responses = [];
  const deposits = [];
  return {
    runtime: {
      depositPolicyStore: { get: () => null },
      depositRateLimitStore: rateLimitStore,
      gatewayLoop: {
        async sendToInbox(opts) { deposits.push(opts); return { packetId: "pkt" }; },
      },
    },
    ownerPublicKeyB64,
    peerIp,
    requireSession() { return true; },
    sendError(e) { errors.push(e); },
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    _errors: errors,
    _responses: responses,
    _deposits: deposits,
  };
}

test("LOW-4: per-pubkey cap still enforced (legacy HIGH-1 behavior preserved)", async () => {
  const { store } = await makeStore({ maxDeposits: 3 });
  const now = Date.now();
  for (let i = 0; i < 3; i++) {
    const ok = await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.0.0.1", mailboxId: INBOX, nowMs: now + i });
    assert.equal(ok, true);
  }
  const denied = await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.0.0.1", mailboxId: INBOX, nowMs: now + 3 });
  assert.equal(denied, false);
});

test("LOW-4: per-IP cap denies even when a different fresh pubkey is presented (the rotation-evasion attack)", async () => {
  const { store } = await makeStore({ maxDeposits: 3 });
  const now = Date.now();
  // Three different pubkeys, all from the same IP, hit the cap.
  for (const pk of [PK_A, PK_B, PK_C]) {
    const ok = await store.record({ depositorPubkeyB64: pk, sourceIp: "10.0.0.99", mailboxId: INBOX, nowMs: now });
    assert.equal(ok, true);
  }
  // A fourth attempt with yet another fresh pubkey from the same IP
  // must be denied — the IP cap survives the pubkey rotation.
  const fourth = await store.record({ depositorPubkeyB64: "pk:fresh", sourceIp: "10.0.0.99", mailboxId: INBOX, nowMs: now + 1 });
  assert.equal(fourth, false, "rotation must NOT escape the per-IP cap");
});

test("LOW-4: a deny on one cap does NOT consume budget on the other (atomic two-phase check)", async () => {
  const { store } = await makeStore({ maxDeposits: 2 });
  const now = Date.now();
  // Saturate PK_A's pubkey cap entirely from one IP.
  await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.1.1.1", mailboxId: INBOX, nowMs: now });
  await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.1.1.1", mailboxId: INBOX, nowMs: now + 1 });
  // PK_A from a DIFFERENT IP is now denied by the pubkey cap. The new
  // IP's bucket must NOT be charged for the denied attempt.
  const denied = await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.2.2.2", mailboxId: INBOX, nowMs: now + 2 });
  assert.equal(denied, false);
  // Verify the second IP wasn't charged: a different pubkey from the
  // same IP still has full budget (2 allowed).
  const ok1 = await store.record({ depositorPubkeyB64: PK_B, sourceIp: "10.2.2.2", mailboxId: INBOX, nowMs: now + 3 });
  const ok2 = await store.record({ depositorPubkeyB64: PK_C, sourceIp: "10.2.2.2", mailboxId: INBOX, nowMs: now + 4 });
  assert.equal(ok1, true, "IP 10.2.2.2 should not have been charged for the prior pubkey-cap deny");
  assert.equal(ok2, true);
});

test("LOW-4: pubkey budget is not charged when the IP cap denies", async () => {
  const { store } = await makeStore({ maxDeposits: 2 });
  const now = Date.now();
  // Saturate IP cap from one IP using two different pubkeys.
  await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.5.5.5", mailboxId: INBOX, nowMs: now });
  await store.record({ depositorPubkeyB64: PK_B, sourceIp: "10.5.5.5", mailboxId: INBOX, nowMs: now + 1 });
  // PK_C from the same IP is now denied by the IP cap. PK_C's own
  // budget must NOT have been charged.
  const denied = await store.record({ depositorPubkeyB64: PK_C, sourceIp: "10.5.5.5", mailboxId: INBOX, nowMs: now + 2 });
  assert.equal(denied, false);
  // PK_C from a DIFFERENT IP still has full budget.
  const ok = await store.record({ depositorPubkeyB64: PK_C, sourceIp: "10.6.6.6", mailboxId: INBOX, nowMs: now + 3 });
  assert.equal(ok, true, "PK_C should not have been charged for the prior IP-cap deny");
});

test("LOW-4: missing sourceIp still enforces the pubkey cap (no IP-keyed gate)", async () => {
  const { store } = await makeStore({ maxDeposits: 2 });
  const now = Date.now();
  await store.record({ depositorPubkeyB64: PK_A, mailboxId: INBOX, nowMs: now });
  await store.record({ depositorPubkeyB64: PK_A, mailboxId: INBOX, nowMs: now + 1 });
  const denied = await store.record({ depositorPubkeyB64: PK_A, mailboxId: INBOX, nowMs: now + 2 });
  assert.equal(denied, false);
});

test("LOW-4: missing depositor pubkey still enforces the IP cap", async () => {
  const { store } = await makeStore({ maxDeposits: 2 });
  const now = Date.now();
  await store.record({ sourceIp: "10.9.9.9", mailboxId: INBOX, nowMs: now });
  await store.record({ sourceIp: "10.9.9.9", mailboxId: INBOX, nowMs: now + 1 });
  const denied = await store.record({ sourceIp: "10.9.9.9", mailboxId: INBOX, nowMs: now + 2 });
  assert.equal(denied, false);
});

test("LOW-4: MailboxHandler.handleDeposit threads ctx.peerIp into the rate limit", async () => {
  const { store } = await makeStore({ maxDeposits: 2 });
  const handler = new MailboxHandler(makeMailboxCtx({
    rateLimitStore: store,
    peerIp: "10.4.4.4",
    ownerPublicKeyB64: PK_A,
  }));

  // First two deposits OK.
  const ctxA = makeMailboxCtx({ rateLimitStore: store, peerIp: "10.4.4.4", ownerPublicKeyB64: PK_A });
  await new MailboxHandler(ctxA).handleDeposit("r1", { mailboxId: INBOX, ciphertextB64: "AA==" });
  await new MailboxHandler(ctxA).handleDeposit("r2", { mailboxId: INBOX, ciphertextB64: "AA==" });
  assert.equal(ctxA._errors.length, 0);
  assert.equal(ctxA._deposits.length, 2);

  // Rotate to a fresh pubkey but stay on same IP. Must be rate-limited.
  const ctxB = makeMailboxCtx({ rateLimitStore: store, peerIp: "10.4.4.4", ownerPublicKeyB64: PK_B });
  await new MailboxHandler(ctxB).handleDeposit("r3", { mailboxId: INBOX, ciphertextB64: "AA==" });
  assert.equal(ctxB._errors.length, 1);
  assert.equal(ctxB._errors[0].code, "RATE_LIMITED",
    "rotation to fresh pubkey on same IP must still hit RATE_LIMITED");
  assert.equal(ctxB._deposits.length, 0);
});

test("LOW-4: per-IP counters persist across restart (rotation across a restart still bounded)", async () => {
  const { store, storageProvider } = await makeStore({ maxDeposits: 2 });
  const now = Date.now();
  await store.record({ depositorPubkeyB64: PK_A, sourceIp: "10.7.7.7", mailboxId: INBOX, nowMs: now });
  await store.record({ depositorPubkeyB64: PK_B, sourceIp: "10.7.7.7", mailboxId: INBOX, nowMs: now + 1 });

  // Simulate restart: new store instance over the same storage.
  const restarted = new DepositRateLimitStore({ storageProvider, windowMs: 60_000, maxDeposits: 2 });
  await restarted.hydrate();

  // Fresh pubkey on same IP after restart — still rate-limited.
  const denied = await restarted.record({ depositorPubkeyB64: "pk:fresh-after-restart", sourceIp: "10.7.7.7", mailboxId: INBOX, nowMs: now + 2 });
  assert.equal(denied, false, "per-IP counter must survive restart");
});
