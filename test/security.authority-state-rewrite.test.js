import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  base64ToBytes,
  AccountAuthorityStateV1,
  ACCOUNT_AUTHORITY_STATE_PURPOSE,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  AccountDeviceCapabilityV1,
  DeviceRegistrationV1,
  DURABLE_RECORD_V2_VERSION,
  durableRecordV2SignableBytes,
  verifyAccountAuthority,
} from "@rezprotocol/core";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
import { DurableRecordStore } from "../src/routing/dht/DurableRecordStore.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

// AUDIT P0 (2026-07-26) — a revoked delegated device must not be able to rewrite the very record
// that proves it is revoked.
//
// The published AccountAuthorityStateV1 is the ONLY way an OFF-HOME peer learns a cert was revoked
// (a durable slot cannot prove absence — only a positive statement can). The record is designed for
// a DELEGATED device to author, and the overlay's write path verifies a publication WITHOUT a
// revocation state, because historically this record WAS the revocation source. So the device a
// revocation names can still sign a newer snapshot that omits itself.
//
// The outbox completion path (PropagationOutboxHandler.handleComplete) checks a submission against
// the account's CURRENT revocation state and is NOT the hole. The generic record.put is: it
// deliberately does not bind a record to the session, and reaches the same slot.
//
// This test drives the OVERLAY write path directly — the layer both doors funnel into — so it pins
// the property regardless of which handler is calling.
const CRYPTO = new NodeCryptoProvider();
const NOW = 1_700_000_000_000; // a realistic epoch — NOW - HOUR must stay positive
const HOUR = 3_600_000;

function newKey() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}

// A B-signed leaf granting a device key the publish capability.
function mintLeaf({ account, granteePubB64, issuedAtMs }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePubB64),
    capabilities: ["deviceSet.publish"],
    maxDelegationDepth: 0,
    issuedAtMs,
    expiresAtMs: issuedAtMs + HOUR * 24,
    signerPublicKeyB64: account.pubB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = CRYPTO.sign({ privateKey: account.priv, msg: AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }) });
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig: { alg: "ed25519", sigB64: bytesToBase64(sig) } });
}

// An authority-state publication signed by `signer`, wrapped in a DurableRecordV2 owned by the
// account. `certChain` is empty for a direct (root-signed) publication.
function buildAuthorityPublication({ account, signer, certChain, epoch, revokedCertIds, issuedAtMs }) {
  const stateBody = {
    v: 1,
    purpose: ACCOUNT_AUTHORITY_STATE_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64,
    epoch,
    revokedCertIds,
    minValidIssuedAtMs: 0,
    issuedAtMs,
    signerPublicKeyB64: signer.pubB64,
  };
  const stateSig = CRYPTO.sign({ privateKey: signer.priv, msg: AccountAuthorityStateV1.signableBytes(stateBody) });
  const state = new AccountAuthorityStateV1({ ...stateBody, sig: { alg: "ed25519", sigB64: bytesToBase64(stateSig) } });
  const payloadB64 = bytesToBase64(new TextEncoder().encode(JSON.stringify(state.toJSON())));
  const envelope = {
    v: DURABLE_RECORD_V2_VERSION,
    recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
    recordId: "v1",
    ownerPublicKeyB64: account.pubB64,
    signerPublicKeyB64: signer.pubB64,
    certChain,
    requiredCapability: certChain.length > 0 ? "deviceSet.publish" : null,
    issuedAtMs,
    expiresAtMs: issuedAtMs + HOUR * 24,
    payloadB64,
  };
  const envSig = CRYPTO.sign({ privateKey: signer.priv, msg: durableRecordV2SignableBytes(envelope) });
  return { ...envelope, sigB64: bytesToBase64(envSig) };
}

function openPublishedState(record) {
  return new AccountAuthorityStateV1(JSON.parse(new TextDecoder().decode(base64ToBytes(record.payloadB64))));
}

describe("AUDIT P0: a revoked device must not rewrite the authority state that revokes it", () => {
  it("refuses an authority-state publication signed by a cert that state's predecessor revoked", async () => {
    const account = newKey();
    const attackerDevice = newKey();
    const leaf = mintLeaf({ account, granteePubB64: attackerDevice.pubB64, issuedAtMs: NOW - HOUR });

    // 1. The account (root) publishes the honest snapshot: the attacker's leaf IS revoked.
    const honest = buildAuthorityPublication({
      account,
      signer: account,
      certChain: [],
      epoch: 5,
      revokedCertIds: [leaf.certId],
      issuedAtMs: NOW,
    });
    const honestVerdict = await verifyDurableRecordDual(honest, NOW);
    assert.equal(honestVerdict.ok, true, "the root-signed snapshot is accepted");

    const store = new DurableRecordStore();
    assert.equal(store.store(honestVerdict.localId, honest, NOW).stored, true);

    // A peer reading the slot correctly rejects the revoked leaf.
    const publishedHonest = openPublishedState(store.get(honestVerdict.localId, NOW).record);
    const beforeAttack = await verifyAccountAuthority({
      expectedAccountIdentityPublicKeyB64: account.pubB64,
      opSignerPublicKeyB64: attackerDevice.pubB64,
      certChain: [leaf.toJSON()],
      crypto: CRYPTO,
      nowMs: NOW + 1000,
      revocationState: publishedHonest.toRevocationState(),
    });
    assert.equal(beforeAttack.ok, false, "off-home peers reject the revoked device");

    // 2. THE ATTACK. The revoked device still holds its key and its (now-revoked) leaf. It signs a
    // NEWER snapshot that simply omits its own certId, and pushes it at the same slot.
    const forged = buildAuthorityPublication({
      account,
      signer: attackerDevice,
      certChain: [leaf.toJSON()],
      epoch: 6,
      revokedCertIds: [],
      issuedAtMs: NOW + 60_000,
    });

    // The overlay must REFUSE it. The signer's authority is exactly what the record it is replacing
    // took away; accepting it lets the device un-revoke itself for every off-home peer.
    const forgedVerdict = await verifyDurableRecordDual(forged, NOW + 61_000, {
      revocationState: publishedHonest.toRevocationState(),
    });
    assert.equal(forgedVerdict.ok, false, "a revoked signer cannot author the account's authority state");

    // 3. And the honest snapshot must still be what a peer reads.
    const slotNow = store.get(honestVerdict.localId, NOW + 61_000);
    const stillPublished = openPublishedState(slotNow.record);
    assert.deepEqual(stillPublished.revokedCertIds, [leaf.certId], "the revocation survived the attack");

    const afterAttack = await verifyAccountAuthority({
      expectedAccountIdentityPublicKeyB64: account.pubB64,
      opSignerPublicKeyB64: attackerDevice.pubB64,
      certChain: [leaf.toJSON()],
      crypto: CRYPTO,
      nowMs: NOW + 61_000,
      revocationState: stillPublished.toRevocationState(),
    });
    assert.equal(afterAttack.ok, false, "the device is still revoked to off-home peers");
  });

  it("still accepts a publication from a NON-revoked delegated device", async () => {
    // The fix must not close off legitimate delegated publication — that is the whole point of the
    // outbox drain being client-owned. Only a signer the state itself revokes is refused.
    const account = newKey();
    const goodDevice = newKey();
    const revokedDevice = newKey();
    const goodLeaf = mintLeaf({ account, granteePubB64: goodDevice.pubB64, issuedAtMs: NOW - HOUR });
    const revokedLeaf = mintLeaf({ account, granteePubB64: revokedDevice.pubB64, issuedAtMs: NOW - HOUR });

    const publication = buildAuthorityPublication({
      account,
      signer: goodDevice,
      certChain: [goodLeaf.toJSON()],
      epoch: 7,
      revokedCertIds: [revokedLeaf.certId],
      issuedAtMs: NOW,
    });

    const verdict = await verifyDurableRecordDual(publication, NOW + 1000, {
      revocationState: { revokedCertIds: [revokedLeaf.certId], minValidIssuedAtMs: 0 },
    });
    assert.equal(verdict.ok, true, "an authorized, non-revoked device may still publish");
  });
});
