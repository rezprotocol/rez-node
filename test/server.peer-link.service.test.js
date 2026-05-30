import test from "node:test";
import assert from "node:assert/strict";
import { createPrivateKey, createPublicKey, generateKeyPairSync, sign as nodeSign, verify as nodeVerify } from "node:crypto";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { PeerLinkService } from "@rezprotocol/sdk/peer-link";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { createSessionIdentity, provisionPeerLinkBinding } from "./helpers/wsAuth.js";

function makePeerLinkService({ storageProvider, clock, ownerAccountId, getInviteAuthority, inviteBinding }) {
  return new PeerLinkService({
    storageProvider,
    clock,
    ownerAccountId,
    getInviteAuthority,
    inviteBinding,
    cryptoProvider: new NodeCryptoProvider(),
  });
}

function createTestInviteAuthority({ accountId }) {
  const keyId = "invite-ed25519-v1";
  const alg = "ed25519";
  const keyPair = generateKeyPairSync("ed25519", {
    publicKeyEncoding: { format: "der", type: "spki" },
    privateKeyEncoding: { format: "der", type: "pkcs8" },
  });
  const privateKeyBytes = new Uint8Array(keyPair.privateKey);
  const publicKeyBytes = new Uint8Array(keyPair.publicKey);
  const privateKeyObj = createPrivateKey({ key: privateKeyBytes, format: "der", type: "pkcs8" });
  const publicKeyObj = createPublicKey({ key: publicKeyBytes, format: "der", type: "spki" });
  return {
    signer: {
      getSignerRef() {
        return { accountId, keyId, alg };
      },
      async sign(bytes) {
        return new Uint8Array(nodeSign(null, bytes, privateKeyObj));
      },
    },
    verifier: {
      async verify({ signerRef, bytes, sigBytes } = {}) {
        if (!signerRef || typeof signerRef !== "object") {
          return false;
        }
        if (String(signerRef.alg || "") !== alg) {
          return false;
        }
        if (String(signerRef.keyId || "") !== keyId) {
          return false;
        }
        if (String(signerRef.accountId || "") !== accountId) {
          return false;
        }
        return nodeVerify(null, bytes, publicKeyObj, sigBytes);
      },
    },
  };
}

function createAuthorityProvider(accountIds = []) {
  const authorities = new Map();
  for (const accountId of accountIds) {
    authorities.set(accountId, createTestInviteAuthority({ accountId }));
  }
  return function getInviteAuthority(accountId) {
    const authority = authorities.get(String(accountId || "").trim());
    if (!authority) {
      throw new Error(`missing authority for ${accountId}`);
    }
    return authority;
  };
}

test("PeerLinkService create/accept/get/list persists direct peer links and idempotent accepts", async () => {
  const storageProvider = new MemoryStorageProvider();
  const clock = () => 1_770_001_000_000;
  const aliceIdentity = createSessionIdentity();
  const bobIdentity = createSessionIdentity();
  const aliceAccountId = aliceIdentity.accountId;
  const bobAccountId = bobIdentity.accountId;
  const getInviteAuthority = createAuthorityProvider([aliceAccountId, bobAccountId]);
  const inviterPeerLinks = makePeerLinkService({
    storageProvider,
    clock,
    ownerAccountId: aliceAccountId,
    getInviteAuthority,
    inviteBinding: { mailboxId: "inbox:alice", capabilityId: "cap:alice" },
  });
  const acceptorPeerLinks = makePeerLinkService({
    storageProvider,
    clock,
    ownerAccountId: bobAccountId,
    getInviteAuthority,
    inviteBinding: { mailboxId: "inbox:bob", capabilityId: "cap:bob" },
  });

  await provisionPeerLinkBinding({
    peerLinks: inviterPeerLinks,
    ownerAccountId: aliceAccountId,
    identity: aliceIdentity,
    issuedAtMs: clock(),
    expiresAtMs: clock() + 7 * 24 * 60 * 60 * 1000,
  });
  await provisionPeerLinkBinding({
    peerLinks: acceptorPeerLinks,
    ownerAccountId: bobAccountId,
    identity: bobIdentity,
    issuedAtMs: clock(),
    expiresAtMs: clock() + 7 * 24 * 60 * 60 * 1000,
  });

  const created = await inviterPeerLinks.createInvite({
    ownerAccountId: aliceAccountId,
    creatorDisplayName: "Alice",
    maxUses: 1,
    expiresAtMs: clock() + 60_000,
  });
  assert.equal(created.state, "invite_issued");
  assert.equal(created.peerLinkId, null);
  assert.ok(created.inviteId, "createInvite should return inviteId");

  const storedEnvelope = await inviterPeerLinks.getStoredInviteEnvelope(aliceAccountId, created.inviteId);
  assert.ok(storedEnvelope, "stored invite envelope should exist");
  assert.ok(storedEnvelope.envelope, "should have envelope");
  assert.ok(storedEnvelope.signatureB64, "should have signatureB64");

  const accepted = await acceptorPeerLinks.acceptInvite({
    envelope: storedEnvelope.envelope,
    signatureB64: storedEnvelope.signatureB64,
    acceptorAccountId: bobAccountId,
    acceptorDisplayName: "Bob",
  });
  assert.ok(accepted.snapshot.peerLinkId);
  assert.equal(accepted.snapshot.state, "accept_committed");
  assert.equal(accepted.snapshot.sessionState, "pending_remote_confirm");
  assert.equal(Object.hasOwn(accepted.snapshot, "threadId"), false);
  assert.equal(accepted.event.type, "handshake_pending");

  const secondAccept = await acceptorPeerLinks.acceptInvite({
    envelope: storedEnvelope.envelope,
    signatureB64: storedEnvelope.signatureB64,
    acceptorAccountId: bobAccountId,
  });
  assert.equal(secondAccept.snapshot.peerLinkId, accepted.snapshot.peerLinkId);
  assert.equal(secondAccept.event.type, "invite_accept_idempotent");

  const fetched = await acceptorPeerLinks.getPeerLink({
    ownerAccountId: bobAccountId,
    peerLinkId: accepted.snapshot.peerLinkId,
  });
  assert.equal(fetched.peerLinkId, accepted.snapshot.peerLinkId);
  assert.equal(fetched.events.length, 3);

  // The inviter's peer link is only created when the handshake confirmation
  // packet arrives — after `acceptInvite`, only the acceptor side exists.
  const inviterListed = await inviterPeerLinks.listPeerLinks({
    ownerAccountId: aliceAccountId,
  });
  assert.deepEqual(inviterListed.items.map((item) => item.peerLinkId), []);

  // The acceptor's peer link should exist with the accepted link id.
  const acceptorListed = await acceptorPeerLinks.listPeerLinks({
    ownerAccountId: bobAccountId,
  });
  assert.deepEqual(acceptorListed.items.map((item) => item.peerLinkId), [accepted.snapshot.peerLinkId]);

  // Thread/contact creation is now handled by ChatAppServer via SDK events,
  // not by PeerLinkService directly. Verify the peer link data is generic.
  assert.equal(Object.hasOwn(accepted.snapshot, "threadId"), false);
  assert.equal(accepted.snapshot.peerAccountId, aliceAccountId);
});
