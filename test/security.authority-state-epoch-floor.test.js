import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  AccountAuthorityStateV1,
  ACCOUNT_AUTHORITY_STATE_PURPOSE,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  DURABLE_RECORD_V2_VERSION,
  durableRecordV2Slot,
  durableRecordV2SignableBytes,
} from "@rezprotocol/core";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
import { DurableRecordStore } from "../src/routing/dht/DurableRecordStore.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { makeSignedAuthorityStateRecord } from "./support/durableRecord.js";

// AUDIT P0 follow-on (2026-07-26) — the ROLLBACK floor.
//
// Root-signed-only closed the FORGERY door: a revoked device can no longer author an authority
// state that un-revokes it. It does NOT close ROLLBACK. Every snapshot the account root ever signed
// stays valid-looking forever (until its own expiresAtMs), and slot replacement is ordered by the
// OUTER `issuedAtMs`, which only orders records that are both PRESENT. So:
//
//   1. The account revokes device C at epoch 6 and publishes it.
//   2. The epoch-6 record expires out of the slot (30-day TTL), or the node restarts.
//   3. Anyone who kept a copy of the epoch-5 record re-stores it into the now-empty slot.
//   4. Off-home peers read epoch 5, where C is not revoked.
//
// Nothing in the record is forged. The fix is that the HOLDER remembers the highest epoch it ever
// accepted for a slot and refuses anything below it — durably, so it outlives both the record's
// expiry and the process.
const CRYPTO = new NodeCryptoProvider();
const NOW = 1_700_000_000_000;
const HOUR = 3_600_000;
const DAY = 86_400_000;

function newKey() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey, keypair: kp };
}

// A root-signed authority state at `epoch`. Shares the ONE fixture builder with the rest of the
// suite (test/support/durableRecord.js) so a change to the record's shape cannot leave this test
// passing against a stale hand-rolled copy.
function buildAuthorityRecord({ account, epoch, revokedCertIds, issuedAtMs, ttlMs = DAY }) {
  return makeSignedAuthorityStateRecord({
    keypair: account.keypair,
    epoch,
    revokedCertIds,
    issuedAtMs,
    ttlMs,
  }).record;
}

function slotOf(account) {
  return durableRecordV2Slot({
    ownerPublicKeyB64: account.pubB64,
    recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
    recordId: "v1",
  });
}

const CERT_A = "rez:cap:" + "a".repeat(64);
const CERT_B = "rez:cap:" + "b".repeat(64);

describe("AUDIT P0 follow-on: the authority state's epoch floor is durable", () => {
  it("refuses an older epoch after the newer record EXPIRED out of the slot", () => {
    const account = newKey();
    const localId = slotOf(account);
    const store = new DurableRecordStore();

    // The stale snapshot (epoch 5, C not revoked) — kept by whoever wants to replay it. Note it is
    // issued LATER in wall-clock terms than the epoch-6 record, so `issuedAtMs` ordering would
    // actively PREFER it. Only the epoch catches this.
    const stale = buildAuthorityRecord({ account, epoch: 5, revokedCertIds: [], issuedAtMs: NOW + HOUR });
    // The current snapshot (epoch 6, C revoked), with a SHORT ttl so it expires first.
    const current = buildAuthorityRecord({ account, epoch: 6, revokedCertIds: [CERT_A], issuedAtMs: NOW, ttlMs: HOUR });

    assert.equal(store.store(localId, current, NOW).stored, true, "the epoch-6 revocation is stored");

    // Time passes: the epoch-6 record expires and the slot empties. There is now no incumbent for
    // issuedAtMs to order against — this is the exact moment the replay wins today.
    const later = NOW + HOUR + 1000;
    assert.equal(store.get(localId, later), null, "the slot is empty after expiry");

    const replay = store.store(localId, stale, later);
    assert.equal(replay.stored, false, "the empty slot must not accept the older epoch");
    assert.equal(replay.reason, "epoch-floor");
    assert.equal(store.get(localId, later), null, "and nothing was written");
  });

  it("refuses an older epoch after a RESTART that reloaded only the persisted floor", () => {
    const account = newKey();
    const localId = slotOf(account);
    const before = new DurableRecordStore();

    const current = buildAuthorityRecord({ account, epoch: 6, revokedCertIds: [CERT_A], issuedAtMs: NOW, ttlMs: HOUR });
    assert.equal(before.store(localId, current, NOW).stored, true);

    const floors = before.epochFloorEntries();
    assert.equal(floors.length, 1);
    assert.equal(floors[0].localId, localId);
    assert.equal(floors[0].epoch, 6);
    assert.equal(floors[0].ownerPublicKeyB64, account.pubB64);

    // Restart. The epoch-6 record is long expired, so no record snapshot survives — the floor file
    // is the ONLY thing carrying the memory of epoch 6.
    const after = new DurableRecordStore();
    const later = NOW + HOUR + 1000;
    assert.equal(after.loadEpochFloors(floors), 1);
    after.loadFromSnapshot([], later);

    const stale = buildAuthorityRecord({ account, epoch: 5, revokedCertIds: [], issuedAtMs: NOW + HOUR });
    const replay = after.store(localId, stale, later);
    assert.equal(replay.stored, false, "the reloaded floor still refuses the rollback");
    assert.equal(replay.reason, "epoch-floor");
  });

  it("re-derives the floor from a held record when the floor snapshot is LOST", () => {
    // Belt and braces: losing/corrupting the floor file degrades to "the floor is whatever we still
    // hold", not to no floor at all.
    const account = newKey();
    const localId = slotOf(account);
    const current = buildAuthorityRecord({ account, epoch: 6, revokedCertIds: [CERT_A], issuedAtMs: NOW });

    const restored = new DurableRecordStore();
    restored.loadEpochFloors([]); // the floor file is gone
    restored.loadFromSnapshot([{ localId, record: current, storedAtMs: NOW, ttlMs: DAY }], NOW + 1000);

    const floors = restored.epochFloorEntries();
    assert.equal(floors.length, 1);
    assert.equal(floors[0].epoch, 6, "the held record re-established its own floor");

    const stale = buildAuthorityRecord({ account, epoch: 5, revokedCertIds: [], issuedAtMs: NOW + HOUR });
    assert.equal(restored.store(localId, stale, NOW + HOUR + 1).reason, "epoch-floor");
  });

  it("loadEpochFloors and loadFromSnapshot only ever RAISE a floor", () => {
    const account = newKey();
    const localId = slotOf(account);
    const store = new DurableRecordStore();

    const high = buildAuthorityRecord({ account, epoch: 9, revokedCertIds: [CERT_A, CERT_B], issuedAtMs: NOW });
    assert.equal(store.store(localId, high, NOW).stored, true);

    // A stale floor file must not lower the running floor...
    store.loadEpochFloors([{ localId, epoch: 2, ownerPublicKeyB64: account.pubB64, observedAtMs: 0 }]);
    assert.equal(store.epochFloorEntry(localId).epoch, 9);

    // ...and neither may an older record loaded from a snapshot.
    const old = buildAuthorityRecord({ account, epoch: 3, revokedCertIds: [], issuedAtMs: NOW });
    store.loadFromSnapshot([{ localId, record: old, storedAtMs: NOW, ttlMs: DAY }], NOW + 1000);
    assert.equal(store.epochFloorEntry(localId).epoch, 9, "the floor survived a lower-epoch snapshot load");
  });

  it("admits the SAME epoch (re-replication) and any HIGHER epoch", () => {
    const account = newKey();
    const localId = slotOf(account);
    const store = new DurableRecordStore();

    const six = buildAuthorityRecord({ account, epoch: 6, revokedCertIds: [CERT_A], issuedAtMs: NOW });
    assert.equal(store.store(localId, six, NOW).stored, true);

    // Byte-identical re-store: this is what storer-side re-replication does every cycle. Refusing it
    // would break durability, so equal-to-the-floor must pass.
    const refresh = store.store(localId, six, NOW + 1000);
    assert.equal(refresh.stored, true);
    assert.equal(refresh.reason, "refreshed");

    // A same-epoch record with DIFFERENT content still falls through to the existing issuedAtMs /
    // sigB64 tie-break — the floor governs rollback, not convergence.
    const sixAgain = buildAuthorityRecord({ account, epoch: 6, revokedCertIds: [CERT_A, CERT_B], issuedAtMs: NOW + 2000 });
    assert.equal(store.store(localId, sixAgain, NOW + 2000).stored, true);

    const seven = buildAuthorityRecord({ account, epoch: 7, revokedCertIds: [CERT_A, CERT_B], issuedAtMs: NOW + 3000 });
    assert.equal(store.store(localId, seven, NOW + 3000).stored, true);
    assert.equal(store.epochFloorEntry(localId).epoch, 7);
  });

  it("does not pin a floor for kinds that carry no epoch", () => {
    const account = newKey();
    const store = new DurableRecordStore();
    const record = buildAuthorityRecord({ account, epoch: 4, revokedCertIds: [], issuedAtMs: NOW });
    const deviceSetish = { ...record, recordKind: "device-set" };
    deviceSetish.sigB64 = bytesToBase64(
      CRYPTO.sign({ privateKey: account.priv, msg: durableRecordV2SignableBytes(deviceSetish) }),
    );
    const localId = durableRecordV2Slot({
      ownerPublicKeyB64: account.pubB64,
      recordKind: "device-set",
      recordId: "v1",
    });

    assert.equal(store.store(localId, deviceSetish, NOW).stored, true);
    assert.equal(store.epochFloorEntry(localId), null, "device-set is not epoch-ordered");
  });

  it("REFUSES an authority state whose payload names a different account", async () => {
    // The floor is only meaningful if the epoch it pins belongs to the slot's owner. The envelope
    // signature covers the payload bytes, but without this check nothing ties the account NAMED IN
    // the payload to the owner key the slot is derived from. Enforced structurally in
    // verifyDurableRecordV2, so every verification site inherits it.
    const account = newKey();
    const victim = newKey();

    const stateBody = {
      v: 1,
      purpose: ACCOUNT_AUTHORITY_STATE_PURPOSE,
      accountIdentityPublicKeyB64: victim.pubB64, // speaks for the VICTIM...
      epoch: 99,
      revokedCertIds: [],
      minValidIssuedAtMs: 0,
      issuedAtMs: NOW,
      signerPublicKeyB64: victim.pubB64,
    };
    const stateSig = CRYPTO.sign({ privateKey: victim.priv, msg: AccountAuthorityStateV1.signableBytes(stateBody) });
    const state = new AccountAuthorityStateV1({ ...stateBody, sig: { alg: "ed25519", sigB64: bytesToBase64(stateSig) } });
    const envelope = {
      v: DURABLE_RECORD_V2_VERSION,
      recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
      recordId: "v1",
      ownerPublicKeyB64: account.pubB64, // ...inside the ATTACKER's own slot
      signerPublicKeyB64: account.pubB64,
      certChain: [],
      requiredCapability: null,
      issuedAtMs: NOW,
      expiresAtMs: NOW + DAY,
      payloadB64: bytesToBase64(new TextEncoder().encode(JSON.stringify(state.toJSON()))),
    };
    const mixed = {
      ...envelope,
      sigB64: bytesToBase64(CRYPTO.sign({ privateKey: account.priv, msg: durableRecordV2SignableBytes(envelope) })),
    };

    const verdict = await verifyDurableRecordDual(mixed, NOW + 1000);
    assert.equal(verdict.ok, false);
    assert.match(String(verdict.reason), /payload is not bound to the record owner/);
  });

  it("REFUSES an authority state whose payload has no readable epoch", async () => {
    // Absence is not zero. A payload with no epoch must be rejected, never treated as epoch 0 —
    // that would make every rollback look like an advance.
    const account = newKey();
    const envelope = {
      v: DURABLE_RECORD_V2_VERSION,
      recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
      recordId: "v1",
      ownerPublicKeyB64: account.pubB64,
      signerPublicKeyB64: account.pubB64,
      certChain: [],
      requiredCapability: null,
      issuedAtMs: NOW,
      expiresAtMs: NOW + DAY,
      payloadB64: bytesToBase64(new TextEncoder().encode(JSON.stringify({
        accountIdentityPublicKeyB64: account.pubB64,
      }))),
    };
    const record = {
      ...envelope,
      sigB64: bytesToBase64(CRYPTO.sign({ privateKey: account.priv, msg: durableRecordV2SignableBytes(envelope) })),
    };

    const verdict = await verifyDurableRecordDual(record, NOW + 1000);
    assert.equal(verdict.ok, false);
    assert.match(String(verdict.reason), /payload is unreadable/);

    // And the store refuses it independently, so a caller that skipped verification cannot slip it
    // past the floor either.
    const store = new DurableRecordStore();
    const result = store.store(slotOf(account), record, NOW + 1000);
    assert.equal(result.stored, false);
    assert.equal(result.reason, "epoch-unreadable");
  });
});
