import test from "node:test";
import assert from "node:assert/strict";
import { createPrivateKey, createPublicKey, generateKeyPairSync, sign as nodeSign, verify as nodeVerify } from "node:crypto";
import { MemoryStorageProvider, bytesToBase64 } from "@rezprotocol/core";
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

test("PeerLinkService sends and completes handshake server-side", async () => {
  const aliceStorageProvider = new MemoryStorageProvider();
  const bobStorageProvider = new MemoryStorageProvider();
  const clock = () => 1_770_002_000_000;
  const aliceIdentity = createSessionIdentity();
  const bobIdentity = createSessionIdentity();
  const aliceAccountId = aliceIdentity.accountId;
  const bobAccountId = bobIdentity.accountId;
  const getInviteAuthority = createAuthorityProvider([aliceAccountId, bobAccountId]);
  const alicePeerLinks = makePeerLinkService({
    storageProvider: aliceStorageProvider,
    clock,
    ownerAccountId: aliceAccountId,
    getInviteAuthority,
    inviteBinding: { mailboxId: "inbox:alice", capabilityId: "inbox:alice" },
  });
  const bobPeerLinks = makePeerLinkService({
    storageProvider: bobStorageProvider,
    clock,
    ownerAccountId: bobAccountId,
    getInviteAuthority,
    inviteBinding: { mailboxId: "inbox:bob", capabilityId: "inbox:bob" },
  });

  await provisionPeerLinkBinding({
    peerLinks: alicePeerLinks,
    ownerAccountId: aliceAccountId,
    identity: aliceIdentity,
    issuedAtMs: clock(),
    expiresAtMs: clock() + 7 * 24 * 60 * 60 * 1000,
  });
  await provisionPeerLinkBinding({
    peerLinks: bobPeerLinks,
    ownerAccountId: bobAccountId,
    identity: bobIdentity,
    issuedAtMs: clock(),
    expiresAtMs: clock() + 7 * 24 * 60 * 60 * 1000,
  });

  const created = await alicePeerLinks.createInvite({
    ownerAccountId: aliceAccountId,
    creatorDisplayName: "Alice",
    maxUses: 1,
    expiresAtMs: clock() + 60_000,
  });

  const storedEnvelope = await alicePeerLinks.getStoredInviteEnvelope(aliceAccountId, created.inviteId);

  let sentHandshake = null;
  const accepted = await bobPeerLinks.acceptInvite({
    envelope: storedEnvelope.envelope,
    signatureB64: storedEnvelope.signatureB64,
    acceptorAccountId: bobAccountId,
    sendHandshake(payload) {
      sentHandshake = payload;
      return { packetId: "gw:test:1" };
    },
  });

  assert.ok(sentHandshake);
  assert.equal(sentHandshake.deliverInboxId, "inbox:alice");
  assert.equal(accepted.snapshot.state, "handshake_sent");
  assert.equal(accepted.snapshot.sessionState, "pending_remote_confirm");
  assert.equal(accepted.event.type, "handshake_sent");

  const completed = await alicePeerLinks.handleIncomingHandshakePacket({
    ownerAccountId: aliceAccountId,
    packetBytes: sentHandshake.handshakePacket.toBytes(),
  });

  assert.ok(completed);
  assert.equal(completed.snapshot.state, "session_established");
  assert.equal(completed.snapshot.sessionState, "active");
  assert.equal(completed.event.type, "handshake_received");

  const fetched = await alicePeerLinks.getPeerLink({
    ownerAccountId: aliceAccountId,
    peerLinkId: completed.snapshot.peerLinkId,
  });
  assert.equal(fetched.state, "session_established");
  assert.equal(fetched.sessionState, "active");

  // S2.5 Slice 3: each side persists the PEER's stable account-level X3DH
  // identity-DH PUBLIC key on the peer-link record. This is the peer half of the
  // static-static agreement that derives the peer-scoped seal used to publish/
  // resolve encrypted device sets, so it must survive establishment on BOTH the
  // acceptor (learns it from the invite binding) and the responder (learns it
  // from the handshake packet).
  const aliceDhPubB64 = bytesToBase64(
    (await alicePeerLinks._requireBoundX3dhIdentity(aliceAccountId)).identityDhKeyPair.publicKey,
  );
  const bobDhPubB64 = bytesToBase64(
    (await bobPeerLinks._requireBoundX3dhIdentity(bobAccountId)).identityDhKeyPair.publicKey,
  );
  const aliceLinkRecord = await alicePeerLinks.peerLinkStorage.peerLinks.getByPair(aliceAccountId, bobAccountId);
  const bobLinkRecord = await bobPeerLinks.peerLinkStorage.peerLinks.getByPair(bobAccountId, aliceAccountId);
  assert.equal(bobLinkRecord.remoteIdentityDhPublicKeyB64, aliceDhPubB64, "acceptor persists the inviter's identity-DH pubkey");
  assert.equal(aliceLinkRecord.remoteIdentityDhPublicKeyB64, bobDhPubB64, "responder persists the acceptor's identity-DH pubkey");

  // Each side also persists the PEER's ACCOUNT identity (B) public key — the
  // durable-record publisher key needed to fetch the peer's sealed device set.
  const aliceAccountPubB64 = bytesToBase64(aliceIdentity.publicKey);
  const bobAccountPubB64 = bytesToBase64(bobIdentity.publicKey);
  assert.equal(bobLinkRecord.remoteAccountIdentityPublicKeyB64, aliceAccountPubB64, "acceptor persists the inviter's account-identity pubkey");
  assert.equal(aliceLinkRecord.remoteAccountIdentityPublicKeyB64, bobAccountPubB64, "responder persists the acceptor's account-identity pubkey");

  const plaintextBytes = new TextEncoder().encode(JSON.stringify({
    contentType: "application/json;charset=utf-8",
    payload: {
      kind: "text",
      text: "hello from alice",
    },
  }));
  const encrypted = await bobPeerLinks.encryptDirectMessage({
    ownerAccountId: bobAccountId,
    peerAccountId: aliceAccountId,
    plaintextBytes,
  });
  assert.ok(encrypted.encryptedPacket, "encryptedPacket record returned");
  assert.ok(encrypted.encryptedPacket.toBytes() instanceof Uint8Array, "record serializes to bytes");

  const decrypted = await alicePeerLinks.decryptDirectMessage({
    ownerAccountId: aliceAccountId,
    peerAccountId: bobAccountId,
    packetBytes: encrypted.encryptedPacket.toBytes(),
  });
  assert.deepEqual(decrypted.plaintextBytes, plaintextBytes);
});
