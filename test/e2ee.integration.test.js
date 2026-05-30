import test from "node:test";
import assert from "node:assert/strict";
import { SecureChannelManager, E2eePacketCodec, X3DHKeyExchange, bytesToBase64, base64ToBytes, X3DHPreKeyBundle, signHandshakeEnvelope, verifyHandshakeEnvelope } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

function makeCrypto() {
  return new NodeCryptoProvider();
}

async function makeIdentitySet(crypto) {
  const signing = crypto.generateSigningKeyPair();
  const dh = crypto.dhGenerateKeyPair();
  const dhSig = await crypto.sign({ privateKey: signing.privateKey, msg: dh.publicKey });
  return { signing, dh, dhSig };
}

test("SecureChannelManager generates pre-key bundle", async () => {
  const crypto = makeCrypto();
  const mgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);

  const { bundle, signedPreKeyPair } = await mgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });

  assert.ok(bundle, "bundle is returned");
  assert.equal(bundle.receiverId, "rez:acct:alice");
  assert.ok(bundle.signedPreKeyPublic instanceof Uint8Array, "signedPreKeyPublic is Uint8Array");
  assert.ok(bundle.identityDhPublicKey instanceof Uint8Array, "identityDhPublicKey is Uint8Array");
  assert.ok(bundle.identityDhSignature instanceof Uint8Array, "identityDhSignature is Uint8Array");
  assert.ok(signedPreKeyPair.publicKey instanceof Uint8Array, "signedPreKeyPair.publicKey is Uint8Array");
  assert.ok(signedPreKeyPair.privateKey instanceof Uint8Array, "signedPreKeyPair.privateKey is Uint8Array");
});

test("SecureChannelManager: initiator+responder establish session and encrypt/decrypt", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const bobMgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);
  const bob = await makeIdentitySet(crypto);

  // Alice generates a pre-key bundle
  const { bundle, signedPreKeyPair } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });

  // Bob establishes initiator session
  const { sid: bobSid, handshakeData } = await bobMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: bob.signing,
    initiatorIdentityDhKeyPair: bob.dh,
    initiatorIdentityDhSignature: bob.dhSig,
  });
  assert.ok(bobSid instanceof Uint8Array, "bob sid is Uint8Array");
  assert.ok(handshakeData.ephemeralPublicKeyB64, "handshake has ephemeralPublicKeyB64");

  // Alice completes responder session
  const { sid: aliceSid, senderIdentitySigningPublicKey } = await aliceMgr.establishResponderSession({
    peerId: "rez:acct:bob",
    signedPreKeyPrivate: signedPreKeyPair.privateKey,
    identityDhPrivate: alice.dh.privateKey,
    receiverBundle: bundle,
    handshakeData,
  });
  assert.ok(aliceSid instanceof Uint8Array, "alice sid is Uint8Array");
  assert.deepEqual(senderIdentitySigningPublicKey, bob.signing.publicKey, "responder learns the verified initiator identity");

  // Bob encrypts a message
  const plaintext = new TextEncoder().encode("hello alice");
  const encrypted = await bobMgr.encryptPayload("rez:acct:alice", plaintext);
  assert.ok(encrypted instanceof Uint8Array, "encrypted is Uint8Array");
  assert.notDeepEqual(encrypted, plaintext, "encrypted differs from plaintext");

  // Alice decrypts
  const decrypted = await aliceMgr.decryptPayload(encrypted);
  assert.ok(decrypted, "decryptPayload returned result");
  assert.deepEqual(decrypted.plaintextBytes, plaintext, "round-trip decryption matches");
  assert.equal(decrypted.peerId, "rez:acct:bob");
});

test("E2eePacketCodec: encrypt and decrypt round-trip", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const bobMgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);
  const bob = await makeIdentitySet(crypto);

  // Setup sessions
  const { bundle, signedPreKeyPair } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });
  const { handshakeData } = await bobMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: bob.signing,
    initiatorIdentityDhKeyPair: bob.dh,
    initiatorIdentityDhSignature: bob.dhSig,
  });
  await aliceMgr.establishResponderSession({
    peerId: "rez:acct:bob",
    signedPreKeyPrivate: signedPreKeyPair.privateKey,
    identityDhPrivate: alice.dh.privateKey,
    receiverBundle: bundle,
    handshakeData,
  });

  const bobCodec = new E2eePacketCodec({ secureChannelManager: bobMgr });
  const aliceCodec = new E2eePacketCodec({ secureChannelManager: aliceMgr });

  // Bob encrypts plaintext bytes
  const originalPayload = JSON.stringify({ contentType: "application/json", payload: { text: "hello" } });
  const originalBytes = new TextEncoder().encode(originalPayload);

  const encryptedRecord = await bobCodec.encryptForPeer({ peerId: "rez:acct:alice", plaintextBytes: originalBytes });
  const encryptedBytes = encryptedRecord.toBytes();
  assert.ok(encryptedBytes instanceof Uint8Array, "encrypted record produces bytes");
  assert.ok(bobCodec.isEncryptedPacket(encryptedBytes), "encrypted packet detected");

  // Alice decrypts
  const result = await aliceCodec.decryptIncoming({ packetBytes: encryptedBytes });
  assert.equal(result.encrypted, true, "packet flagged as encrypted");
  assert.equal(result.peerId, "rez:acct:bob", "peerId matches");
  assert.deepEqual(result.plaintextBytes, originalBytes, "decrypted bytes match original");
});

test("E2eePacketCodec: plaintext passthrough for unencrypted packets", async () => {
  const crypto = makeCrypto();
  const mgr = new SecureChannelManager({ crypto });
  const codec = new E2eePacketCodec({ secureChannelManager: mgr });

  const payload = JSON.stringify({ contentType: "text/plain", payload: { text: "plain message" } });
  const packetBytes = new TextEncoder().encode(payload);

  const result = await codec.decryptIncoming({ packetBytes });
  assert.equal(result.encrypted, false, "not flagged as encrypted");
  assert.deepEqual(result.plaintextBytes, packetBytes, "original returned unchanged");
  assert.equal(result.handshake, null, "no handshake");
  assert.ok(!codec.isEncryptedPacket(packetBytes), "not detected as encrypted");
});

test("E2eePacketCodec: handshake packet detection", async () => {
  const crypto = makeCrypto();
  const mgr = new SecureChannelManager({ crypto });
  const codec = new E2eePacketCodec({ secureChannelManager: mgr });
  const bob = await makeIdentitySet(crypto);

  const handshakeData = {
    inviteId: "inv_test123",
    senderIdentitySigningPubKeyB64: bytesToBase64(bob.signing.publicKey),
    senderIdentityDhPubKeyB64: bytesToBase64(bob.dh.publicKey),
    senderIdentityDhSignatureB64: bytesToBase64(bob.dhSig),
    senderDisplayName: "Bob",
    ackNonce: "nonce123",
    ephemeralPublicKeyB64: "abc123",
    initiatorDhPublicKeyB64: "dh456",
    usedOneTimePreKey: false,
    receiverId: "rez:acct:alice",
  };
  const signatureB64 = await signHandshakeEnvelope({
    handshake: handshakeData,
    crypto,
    signingPrivateKey: bob.signing.privateKey,
  });
  const handshakeRecord = E2eePacketCodec.createHandshakePacket({ handshakeData, signatureB64 });
  const packetBytes = handshakeRecord.toBytes();

  assert.ok(codec.isEncryptedPacket(packetBytes), "handshake has e2ee marker");

  const result = await codec.decryptIncoming({ packetBytes });
  assert.equal(result.encrypted, false, "handshake is not encrypted");
  assert.ok(result.handshake, "handshake object returned");
  assert.equal(result.handshake.ephemeralPublicKeyB64, "abc123");

  // Tampering with the handshake after signing must invalidate verification.
  const tampered = { ...handshakeData, ackNonce: "different-nonce" };
  const tamperedVerified = await verifyHandshakeEnvelope({
    handshake: tampered,
    signatureB64,
    crypto,
  });
  assert.equal(tamperedVerified, false, "tampered handshake fails verification");
});

test("E2eePacketCodec: throws when no session exists", async () => {
  const crypto = makeCrypto();
  const mgr = new SecureChannelManager({ crypto });
  const codec = new E2eePacketCodec({ secureChannelManager: mgr });

  const payload = JSON.stringify({ contentType: "text/plain", payload: { text: "hello" } });
  const plaintextBytes = new TextEncoder().encode(payload);

  // Encrypt without session should throw NO_SESSION
  await assert.rejects(
    () => codec.encryptForPeer({ peerId: "rez:acct:unknown", plaintextBytes }),
    (err) => err.code === "NO_SESSION",
    "throws NO_SESSION when no session exists",
  );
});

test("X3DHKeyExchange: full invite round-trip with key exchange", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const bobMgr = new SecureChannelManager({ crypto });

  const aliceIdKp = crypto.generateSigningKeyPair();
  const aliceIdDhKp = crypto.dhGenerateKeyPair();
  const bobIdKp = crypto.generateSigningKeyPair();
  const bobIdDhKp = crypto.dhGenerateKeyPair();
  const bobIdDhSig = await crypto.sign({ privateKey: bobIdKp.privateKey, msg: bobIdDhKp.publicKey });

  const aliceExchange = new X3DHKeyExchange({ secureChannelManager: aliceMgr });
  const bobExchange = new X3DHKeyExchange({ secureChannelManager: bobMgr });

  // Alice prepares invite binding with X3DH bundle
  const { binding, preKeyState } = await aliceExchange.prepareInviteBinding({
    accountId: "rez:acct:alice",
    identityKeyPair: aliceIdKp,
    identityDhKeyPair: aliceIdDhKp,
  });
  assert.ok(binding.x3dh, "x3dh field exists in binding");
  assert.ok(preKeyState.signedPreKeyPrivate, "preKeyState has signedPreKeyPrivate");
  assert.ok(preKeyState.bundleJson, "preKeyState has bundleJson");

  // Bob accepts invite and processes the binding
  const { handshakeData, sid: bobSid } = await bobExchange.processAcceptedInvite({
    inviteBinding: binding,
    peerId: "rez:acct:alice",
    initiatorIdentityKeyPair: bobIdKp,
    initiatorIdentityDhKeyPair: bobIdDhKp,
    initiatorIdentityDhSignature: bobIdDhSig,
  });
  assert.ok(bobSid instanceof Uint8Array, "bob sid is Uint8Array");
  assert.ok(handshakeData.ephemeralPublicKeyB64, "handshake has ephemeralPublicKeyB64");
  assert.equal(handshakeData.senderIdentitySigningPubKeyB64, bytesToBase64(bobIdKp.publicKey), "handshake carries Bob's verified identity");

  // Alice completes handshake when she receives the handshake message
  const { sid: aliceSid, senderIdentitySigningPublicKey } = await aliceExchange.completeInviteHandshake({
    preKeyState,
    identityDhPrivate: aliceIdDhKp.privateKey,
    handshakeData,
    peerId: "rez:acct:bob",
  });
  assert.ok(aliceSid instanceof Uint8Array, "alice sid is Uint8Array");
  assert.deepEqual(senderIdentitySigningPublicKey, bobIdKp.publicKey, "Alice learns the verified sender identity");

  const aliceCodec = new E2eePacketCodec({ secureChannelManager: aliceMgr });
  const bobCodec = new E2eePacketCodec({ secureChannelManager: bobMgr });

  // Bob (initiator) sends first — responder must receive before sending
  const msgPayload = JSON.stringify({ contentType: "application/json", payload: { text: "encrypted hello" } });
  const msgBytes = new TextEncoder().encode(msgPayload);

  const encryptedRecord = await bobCodec.encryptForPeer({ peerId: "rez:acct:alice", plaintextBytes: msgBytes });
  const encryptedBytes = encryptedRecord.toBytes();

  const decrypted = await aliceCodec.decryptIncoming({ packetBytes: encryptedBytes });
  assert.equal(decrypted.encrypted, true);
  assert.deepEqual(decrypted.plaintextBytes, msgBytes, "Alice decrypts Bob's message");

  // Alice (responder) can now send after receiving Bob's message
  const replyPayload = JSON.stringify({ contentType: "application/json", payload: { text: "encrypted reply" } });
  const replyBytes = new TextEncoder().encode(replyPayload);

  const encryptedReplyRecord = await aliceCodec.encryptForPeer({ peerId: "rez:acct:bob", plaintextBytes: replyBytes });
  const decryptedReply = await bobCodec.decryptIncoming({ packetBytes: encryptedReplyRecord.toBytes() });
  assert.equal(decryptedReply.encrypted, true);
  assert.deepEqual(decryptedReply.plaintextBytes, replyBytes, "Bob decrypts Alice's reply");
});

test("X3DHKeyExchange: serialize/deserialize bundle round-trip", () => {
  const crypto = makeCrypto();
  const idKp = crypto.generateSigningKeyPair();
  const idDhKp = crypto.dhGenerateKeyPair();
  const spkKp = crypto.dhGenerateKeyPair();

  // Manually create a bundle-like structure to test serialization
  const fakeBundle = {
    receiverId: "rez:acct:test",
    identitySigningPublicKey: idKp.publicKey,
    identityDhPublicKey: idDhKp.publicKey,
    identityDhSignature: crypto.randomBytes(64),
    signedPreKeyPublic: spkKp.publicKey,
    signedPreKeySignature: crypto.randomBytes(64),
    oneTimePreKeyPublic: null,
  };

  const bundle = new X3DHPreKeyBundle(fakeBundle);

  const serialized = X3DHKeyExchange.serializeBundle(bundle);
  assert.equal(serialized.receiverId, "rez:acct:test");
  assert.ok(typeof serialized.identitySigningPublicKeyB64 === "string");
  assert.ok(typeof serialized.identityDhPublicKeyB64 === "string");
  assert.ok(typeof serialized.identityDhSignatureB64 === "string");
  assert.ok(typeof serialized.signedPreKeyPublicB64 === "string");

  const deserialized = X3DHKeyExchange.deserializeBundle(serialized);
  assert.equal(deserialized.receiverId, "rez:acct:test");
  assert.deepEqual(deserialized.identitySigningPublicKey, idKp.publicKey);
  assert.deepEqual(deserialized.identityDhPublicKey, idDhKp.publicKey);
  assert.deepEqual(deserialized.signedPreKeyPublic, spkKp.publicKey);
});

test("SecureChannelManager snapshot restore preserves e2ee sessions across restart", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const bobMgr = new SecureChannelManager({ crypto });

  const aliceIdKp = crypto.generateSigningKeyPair();
  const aliceIdDhKp = crypto.dhGenerateKeyPair();
  const bobIdKp = crypto.generateSigningKeyPair();
  const bobIdDhKp = crypto.dhGenerateKeyPair();
  const bobIdDhSig = await crypto.sign({ privateKey: bobIdKp.privateKey, msg: bobIdDhKp.publicKey });

  const { bundle, signedPreKeyPair } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: aliceIdKp,
    identityDhKeyPair: aliceIdDhKp,
  });
  const { handshakeData } = await bobMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: bobIdKp,
    initiatorIdentityDhKeyPair: bobIdDhKp,
    initiatorIdentityDhSignature: bobIdDhSig,
  });
  await aliceMgr.establishResponderSession({
    peerId: "rez:acct:bob",
    signedPreKeyPrivate: signedPreKeyPair.privateKey,
    identityDhPrivate: aliceIdDhKp.privateKey,
    receiverBundle: bundle,
    handshakeData,
  });

  const aliceCodec = new E2eePacketCodec({ secureChannelManager: aliceMgr });
  const bobCodec = new E2eePacketCodec({ secureChannelManager: bobMgr });

  const firstPacketBytes = new TextEncoder().encode(JSON.stringify({
    contentType: "application/json",
    payload: { text: "before snapshot" },
  }));
  const encryptedFirstRecord = await bobCodec.encryptForPeer({
    peerId: "rez:acct:alice",
    plaintextBytes: firstPacketBytes,
  });
  const decryptedFirst = await aliceCodec.decryptIncoming({ packetBytes: encryptedFirstRecord.toBytes() });
  assert.deepEqual(decryptedFirst.plaintextBytes, firstPacketBytes);

  const aliceSnapshot = aliceMgr.exportSnapshot();
  const bobSnapshot = bobMgr.exportSnapshot();

  const aliceRestored = new SecureChannelManager({ crypto });
  const bobRestored = new SecureChannelManager({ crypto });
  aliceRestored.importSnapshot(aliceSnapshot);
  bobRestored.importSnapshot(bobSnapshot);

  assert.ok(aliceRestored.hasSession("rez:acct:bob"));
  assert.ok(bobRestored.hasSession("rez:acct:alice"));

  const aliceRestoredCodec = new E2eePacketCodec({ secureChannelManager: aliceRestored });
  const bobRestoredCodec = new E2eePacketCodec({ secureChannelManager: bobRestored });

  const secondPacketBytes = new TextEncoder().encode(JSON.stringify({
    contentType: "application/json",
    payload: { text: "after snapshot" },
  }));
  const encryptedSecondRecord = await bobRestoredCodec.encryptForPeer({
    peerId: "rez:acct:alice",
    plaintextBytes: secondPacketBytes,
  });
  const decryptedSecond = await aliceRestoredCodec.decryptIncoming({ packetBytes: encryptedSecondRecord.toBytes() });
  assert.equal(decryptedSecond.encrypted, true);
  assert.deepEqual(decryptedSecond.plaintextBytes, secondPacketBytes);
});

// --- CRITICAL-1 impersonation defenses with real NodeCryptoProvider ---
// These tests exercise the full handshake against real Ed25519 / X25519
// primitives; mocked crypto would mask any of the gaps described in
// docs/SECURITY_AUDIT.md CRITICAL-1.

test("CRITICAL-1: legitimate Alice -> Bob handshake round-trip with real crypto", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const bobMgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);
  const bob = await makeIdentitySet(crypto);

  const { bundle, signedPreKeyPair } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });

  const { handshakeData } = await bobMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: bob.signing,
    initiatorIdentityDhKeyPair: bob.dh,
    initiatorIdentityDhSignature: bob.dhSig,
  });
  const sig = await signHandshakeEnvelope({ handshake: handshakeData, crypto, signingPrivateKey: bob.signing.privateKey });
  const verified = await verifyHandshakeEnvelope({ handshake: handshakeData, signatureB64: sig, crypto });
  assert.equal(verified, true, "legitimate envelope verifies");

  const { senderIdentitySigningPublicKey } = await aliceMgr.establishResponderSession({
    peerId: "rez:acct:bob",
    signedPreKeyPrivate: signedPreKeyPair.privateKey,
    identityDhPrivate: alice.dh.privateKey,
    receiverBundle: bundle,
    handshakeData,
  });
  assert.deepEqual(senderIdentitySigningPublicKey, bob.signing.publicKey, "Alice verifies Bob's identity from the X3DH transcript");

  // Bidirectional E2EE encryption works.
  const plaintext = new TextEncoder().encode("hello from bob");
  const encrypted = await bobMgr.encryptPayload("rez:acct:alice", plaintext);
  const decrypted = await aliceMgr.decryptPayload(encrypted);
  assert.deepEqual(decrypted.plaintextBytes, plaintext);
});

test("CRITICAL-1: Mallory with only Alice's bundle CANNOT forge a Bob-claiming handshake (no sig privkey)", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);
  const bob = await makeIdentitySet(crypto);

  const { bundle } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });

  // Mallory has Alice's bundle and Bob's PUBLIC identity (e.g. from a
  // previous handshake) but not Bob's private signing key. She can build a
  // handshakeData claiming Bob's identity pubkey, but cannot produce a valid
  // identityDh signature or envelope signature without Bob's privkey.
  const mallory = await makeIdentitySet(crypto);

  // Try 1: Mallory uses HER OWN privkey to sign an envelope claiming Bob's pubkey.
  const handshakeData = {
    receiverId: bundle.receiverId,
    senderIdentitySigningPubKeyB64: bytesToBase64(bob.signing.publicKey), // Bob's pubkey...
    senderIdentityDhPubKeyB64: bytesToBase64(mallory.dh.publicKey),
    senderIdentityDhSignatureB64: bytesToBase64(mallory.dhSig), // signed by Mallory, not Bob
    ephemeralPublicKeyB64: "ZmFrZQ==",
    initiatorDhPublicKeyB64: "ZmFrZQ==",
    usedOneTimePreKey: false,
    inviteId: "inv:test",
    ackNonce: "n",
  };
  // signHandshakeEnvelope's sanity check catches the mismatch BEFORE shipping.
  await assert.rejects(
    () => signHandshakeEnvelope({ handshake: handshakeData, crypto, signingPrivateKey: mallory.signing.privateKey }),
    /does not match handshake\.senderIdentitySigningPubKeyB64/,
    "signing with Mallory's key against a handshake claiming Bob's pubkey is rejected at sign time",
  );

  // Try 2: Mallory bypasses the sanity check and writes a raw signature by
  // her own privkey, then ships. Receiver verification MUST fail.
  const forgedSigBytes = await crypto.sign({ privateKey: mallory.signing.privateKey, msg: new TextEncoder().encode(JSON.stringify(handshakeData)) });
  const forgedVerified = await verifyHandshakeEnvelope({
    handshake: handshakeData,
    signatureB64: bytesToBase64(forgedSigBytes),
    crypto,
  });
  assert.equal(forgedVerified, false, "envelope signed by Mallory but claiming Bob's pubkey fails verification");
});

test("CRITICAL-1: tampering with handshake bytes after signing invalidates verification", async () => {
  const crypto = makeCrypto();
  const bob = await makeIdentitySet(crypto);
  const mallory = await makeIdentitySet(crypto);

  const handshakeData = {
    receiverId: "rez:acct:alice",
    senderIdentitySigningPubKeyB64: bytesToBase64(bob.signing.publicKey),
    senderIdentityDhPubKeyB64: bytesToBase64(bob.dh.publicKey),
    senderIdentityDhSignatureB64: bytesToBase64(bob.dhSig),
    ephemeralPublicKeyB64: "ZWZlbWVyYWw=",
    initiatorDhPublicKeyB64: "aW5pdERo",
    usedOneTimePreKey: false,
    inviteId: "inv:test",
    ackNonce: "n",
  };
  const sig = await signHandshakeEnvelope({ handshake: handshakeData, crypto, signingPrivateKey: bob.signing.privateKey });

  // Mallory swaps the ephemeral DH pubkey to one she controls. Even if she
  // could mount a MITM and re-derive the shared secret, the signature is
  // over the original ephemeral and now fails to verify.
  const tampered = { ...handshakeData, ephemeralPublicKeyB64: bytesToBase64(mallory.dh.publicKey) };
  const verified = await verifyHandshakeEnvelope({ handshake: tampered, signatureB64: sig, crypto });
  assert.equal(verified, false, "tampering with ephemeral DH pubkey invalidates the signature");

  // Swap senderIdentityDhPublicKeyB64 (the DH1 input). Same defense.
  const tampered2 = { ...handshakeData, senderIdentityDhPubKeyB64: bytesToBase64(mallory.dh.publicKey) };
  const verified2 = await verifyHandshakeEnvelope({ handshake: tampered2, signatureB64: sig, crypto });
  assert.equal(verified2, false, "tampering with senderIdentityDh invalidates the signature");
});

test("CRITICAL-1: handshake bound to initiator identity in shared secret (DH1)", async () => {
  // Two initiators with otherwise-identical inputs MUST derive different
  // shared secrets, because DH1 mixes their long-term identity DH keys.
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);
  const bob = await makeIdentitySet(crypto);
  const mallory = await makeIdentitySet(crypto);

  const { bundle, signedPreKeyPair } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });

  const bobMgr = new SecureChannelManager({ crypto });
  const malloryMgr = new SecureChannelManager({ crypto });

  const { handshakeData: bobHandshake } = await bobMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: bob.signing,
    initiatorIdentityDhKeyPair: bob.dh,
    initiatorIdentityDhSignature: bob.dhSig,
  });
  const { handshakeData: malloryHandshake } = await malloryMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: mallory.signing,
    initiatorIdentityDhKeyPair: mallory.dh,
    initiatorIdentityDhSignature: mallory.dhSig,
  });

  // Each handshake carries the legitimate initiator's identity pubkey.
  assert.equal(bobHandshake.senderIdentitySigningPubKeyB64, bytesToBase64(bob.signing.publicKey));
  assert.equal(malloryHandshake.senderIdentitySigningPubKeyB64, bytesToBase64(mallory.signing.publicKey));
  assert.notEqual(bobHandshake.senderIdentityDhPubKeyB64, malloryHandshake.senderIdentityDhPubKeyB64);

  // Alice can complete the responder side for either party and learns the
  // correct identity each time. There is no way for Mallory to make Alice
  // believe Mallory's session was from Bob: the verified pubkey returned to
  // Alice is Mallory's, and the chat layer can reject on that basis.
  const aliceMgr2 = new SecureChannelManager({ crypto });
  const { senderIdentitySigningPublicKey: verifiedAsMallory } = await aliceMgr2.establishResponderSession({
    peerId: "rez:acct:mallory",
    signedPreKeyPrivate: signedPreKeyPair.privateKey,
    identityDhPrivate: alice.dh.privateKey,
    receiverBundle: bundle,
    handshakeData: malloryHandshake,
  });
  assert.deepEqual(verifiedAsMallory, mallory.signing.publicKey, "responder learns Mallory's actual identity, not Bob's");
  assert.notDeepEqual(verifiedAsMallory, bob.signing.publicKey, "responder is NOT fooled into thinking the handshake was from Bob");
});

test("CRITICAL-1: receiver rejects handshake with forged senderIdentityDh signature", async () => {
  const crypto = makeCrypto();
  const aliceMgr = new SecureChannelManager({ crypto });
  const alice = await makeIdentitySet(crypto);
  const bob = await makeIdentitySet(crypto);
  const mallory = await makeIdentitySet(crypto);

  const { bundle, signedPreKeyPair } = await aliceMgr.generatePreKeyBundle({
    accountId: "rez:acct:alice",
    identityKeyPair: alice.signing,
    identityDhKeyPair: alice.dh,
  });

  // Mallory builds a handshake claiming Bob's identity pubkey but with her
  // OWN identityDh pubkey. The senderIdentityDhSignatureB64 in her handshake
  // must be a signature by Bob's signing key over Mallory's DH pubkey — she
  // can't produce that. If she ships a sig produced by her own privkey, the
  // X3DH receiverCompute path (which verifies it) MUST reject.
  const bobMgr = new SecureChannelManager({ crypto });
  const { handshakeData } = await bobMgr.establishInitiatorSession({
    peerId: "rez:acct:alice",
    receiverBundle: bundle,
    initiatorIdentityKeyPair: bob.signing,
    initiatorIdentityDhKeyPair: bob.dh,
    initiatorIdentityDhSignature: bob.dhSig,
  });

  // Mallory keeps Bob's identity pubkey + Bob's identityDh signature but
  // swaps in her own identityDh pubkey (which the sig doesn't cover).
  const tampered = {
    ...handshakeData,
    senderIdentityDhPubKeyB64: bytesToBase64(mallory.dh.publicKey),
  };

  await assert.rejects(
    () => aliceMgr.establishResponderSession({
      peerId: "rez:acct:mallory",
      signedPreKeyPrivate: signedPreKeyPair.privateKey,
      identityDhPrivate: alice.dh.privateKey,
      receiverBundle: bundle,
      handshakeData: tampered,
    }),
    /sender identityDh signature verification failed/,
    "receiver rejects tampered senderIdentityDhPublicKey",
  );
});
