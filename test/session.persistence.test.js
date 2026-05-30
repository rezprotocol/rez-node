import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  RatchetService,
  RatchetKeyPair,
  MemorySessionManager,
  Header,
  Envelope,
  CodecChain,
  CanonicalizeCodec,
  JsonCodec,
  EncryptEnvelopeCodec,
  DecryptEnvelopeCodec,
} from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { X25519_SUPPORTED } from "../src/crypto/dh/index.js";
import { FsSessionStore } from "../src/storage/sessions/FsSessionStore.js";
import { PersistentSessionManager } from "../src/services/sessions/PersistentSessionManager.js";

function makeProviderOrSkip(t) {
  if (!X25519_SUPPORTED) {
    t.skip("X25519 not supported in this Node runtime");
    return null;
  }
  return new NodeCryptoProvider();
}

async function withTempDir(fn) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-node-"));
  try {
    return await fn(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

function buildRatchet(ratchet, crypto, sharedSecret) {
  const senderDh = crypto.dhGenerateKeyPair();
  const receiverDh = crypto.dhGenerateKeyPair();
  const sendState = ratchet.initializeAsInitiator({
    sharedSecret,
    selfDhKeyPair: new RatchetKeyPair({ publicKey: senderDh.publicKey, privateKey: senderDh.privateKey }),
    remoteDhPublicKey: receiverDh.publicKey,
  });
  const recvState = ratchet.initializeAsResponder({
    sharedSecret,
    selfDhKeyPair: new RatchetKeyPair({ publicKey: receiverDh.publicKey, privateKey: receiverDh.privateKey }),
    remoteDhPublicKey: senderDh.publicKey,
  });
  return { sendState, recvState, senderDh, receiverDh };
}

test("Persists and reloads session state", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const ratchet = new RatchetService({ crypto });
  const sharedSecret = new Uint8Array(32);

  await withTempDir(async (dir) => {
    const store = new FsSessionStore({ rootDir: dir });
    const inner = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr = new PersistentSessionManager({ inner, store });

    const selfDh = crypto.dhGenerateKeyPair();
    const remoteDh = crypto.dhGenerateKeyPair();

    const sid = await mgr.createInitiatorSession({
      peerId: "peer-a",
      sharedSecret,
      selfDhKeyPair: new RatchetKeyPair({ publicKey: selfDh.publicKey, privateKey: selfDh.privateKey }),
      remoteDhPublicKey: remoteDh.publicKey,
    });

    const ctx = mgr.getSendContext("peer-a");
    const { newState } = await ratchet.nextSendingMessageKey(ctx.ratchetState);
    await ctx.commit(newState);

    const inner2 = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr2 = new PersistentSessionManager({ inner: inner2, store });
    await mgr2.loadAll();

    const ctx2 = mgr2.getSendContext("peer-a");
    assert.deepEqual(ctx2.sid, sid);
    assert.equal(ctx2.ratchetState.sendingChain.messageIndex, newState.sendingChain.messageIndex);
  });
});

test("Encrypt/decrypt across restart boundary", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const ratchet = new RatchetService({ crypto });
  const sharedSecret = new Uint8Array(32);

  await withTempDir(async (dir) => {
    const store = new FsSessionStore({ rootDir: dir });
    const innerRecv = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const recvMgr = new PersistentSessionManager({ inner: innerRecv, store });

    const { sendState, recvState, senderDh } = buildRatchet(ratchet, crypto, sharedSecret);
    const sid = await recvMgr.createResponderSession({
      peerId: "peer-b",
      sharedSecret,
      selfDhKeyPair: recvState.selfDhKeyPair,
      remoteDhPublicKey: senderDh.publicKey,
    });

    const innerRecv2 = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const recvMgr2 = new PersistentSessionManager({ inner: innerRecv2, store });
    await recvMgr2.loadAll();

    const chain = new CodecChain([new CanonicalizeCodec(), new JsonCodec()]);
    const enc = new EncryptEnvelopeCodec({ innerCodecChain: chain, ratchetService: ratchet });
    const dec = new DecryptEnvelopeCodec({ innerCodecChain: chain, ratchetService: ratchet });

    const header = new Header({ id: "p-1", type: "test.object", createdAt: 1 });
    const inner = new Envelope({ header, body: { ok: true } });

    const encCtx = await enc.encode({ envelope: inner, meta: { secureChannel: { sid, ratchetState: sendState } } });

    const recvCtx = recvMgr2.getRecvContext(sid);
    const decCtx = await dec.decode({
      envelope: encCtx.envelope,
      meta: { secureChannel: { sid, ratchetState: recvCtx.ratchetState } },
    });

    assert.deepEqual(decCtx.envelope.toJSON(), inner.toJSON());
  });
});

test("Encrypted session store round-trips through persist and reload", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const ratchet = new RatchetService({ crypto });
  const sharedSecret = new Uint8Array(32);
  const encryptionKey = crypto.randomBytes(32);

  await withTempDir(async (dir) => {
    const store = new FsSessionStore({ rootDir: dir, crypto, encryptionKey });
    assert.equal(store.encryptionEnabled, true);

    const inner = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr = new PersistentSessionManager({ inner, store });

    const selfDh = crypto.dhGenerateKeyPair();
    const remoteDh = crypto.dhGenerateKeyPair();

    const sid = await mgr.createInitiatorSession({
      peerId: "encrypted-peer",
      sharedSecret,
      selfDhKeyPair: new RatchetKeyPair({ publicKey: selfDh.publicKey, privateKey: selfDh.privateKey }),
      remoteDhPublicKey: remoteDh.publicKey,
    });

    // Advance the ratchet
    const ctx = mgr.getSendContext("encrypted-peer");
    const { newState } = await ratchet.nextSendingMessageKey(ctx.ratchetState);
    await ctx.commit(newState);

    // Verify the file on disk is encrypted (not plaintext JSON)
    const files = await fs.readdir(dir);
    const jsonFile = files.find((f) => f.endsWith(".json"));
    assert.ok(jsonFile, "session file should exist");
    const raw = await fs.readFile(path.join(dir, jsonFile), "utf8");
    const diskJson = JSON.parse(raw);
    assert.equal(diskJson.encrypted, 1, "disk file should be encrypted envelope");
    assert.ok(diskJson.envelope, "disk file should contain envelope");
    assert.ok(diskJson.envelope.ciphertextB64, "envelope should have ciphertext");
    // Verify plaintext fields are NOT visible
    assert.equal(diskJson.peerId, undefined, "peerId should not be visible in encrypted file");
    assert.equal(diskJson.ratchetState, undefined, "ratchetState should not be visible in encrypted file");

    // Reload from encrypted store
    const inner2 = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr2 = new PersistentSessionManager({ inner: inner2, store });
    await mgr2.loadAll();

    const ctx2 = mgr2.getSendContext("encrypted-peer");
    assert.deepEqual(ctx2.sid, sid);
    assert.equal(ctx2.ratchetState.sendingChain.messageIndex, newState.sendingChain.messageIndex);
  });
});

test("Encrypted store rejects tampered session files", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const ratchet = new RatchetService({ crypto });
  const sharedSecret = new Uint8Array(32);
  const encryptionKey = crypto.randomBytes(32);

  await withTempDir(async (dir) => {
    const store = new FsSessionStore({ rootDir: dir, crypto, encryptionKey });
    const inner = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr = new PersistentSessionManager({ inner, store });

    const selfDh = crypto.dhGenerateKeyPair();
    const remoteDh = crypto.dhGenerateKeyPair();

    await mgr.createInitiatorSession({
      peerId: "tamper-peer",
      sharedSecret,
      selfDhKeyPair: new RatchetKeyPair({ publicKey: selfDh.publicKey, privateKey: selfDh.privateKey }),
      remoteDhPublicKey: remoteDh.publicKey,
    });

    // Tamper with the ciphertext on disk
    const files = await fs.readdir(dir);
    const jsonFile = files.find((f) => f.endsWith(".json"));
    const raw = await fs.readFile(path.join(dir, jsonFile), "utf8");
    const diskJson = JSON.parse(raw);
    diskJson.envelope.ciphertextB64 = diskJson.envelope.ciphertextB64.slice(0, -4) + "XXXX";
    await fs.writeFile(path.join(dir, jsonFile), JSON.stringify(diskJson), "utf8");

    // Reload should fail due to integrity check
    const inner2 = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr2 = new PersistentSessionManager({ inner: inner2, store });
    await assert.rejects(() => mgr2.loadAll(), /integrity|decrypt|tag|authenticate/i);
  });
});

test("Encrypted store rejects wrong key", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const ratchet = new RatchetService({ crypto });
  const sharedSecret = new Uint8Array(32);
  const key1 = crypto.randomBytes(32);
  const key2 = crypto.randomBytes(32);

  await withTempDir(async (dir) => {
    // Write with key1
    const store1 = new FsSessionStore({ rootDir: dir, crypto, encryptionKey: key1 });
    const inner1 = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr1 = new PersistentSessionManager({ inner: inner1, store: store1 });

    const selfDh = crypto.dhGenerateKeyPair();
    const remoteDh = crypto.dhGenerateKeyPair();

    await mgr1.createInitiatorSession({
      peerId: "key-peer",
      sharedSecret,
      selfDhKeyPair: new RatchetKeyPair({ publicKey: selfDh.publicKey, privateKey: selfDh.privateKey }),
      remoteDhPublicKey: remoteDh.publicKey,
    });

    // Try to read with key2 — should fail
    const store2 = new FsSessionStore({ rootDir: dir, crypto, encryptionKey: key2 });
    const inner2 = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr2 = new PersistentSessionManager({ inner: inner2, store: store2 });
    await assert.rejects(() => mgr2.loadAll(), /integrity|decrypt|tag|authenticate/i);
  });
});

test("Corrupt file behavior is deterministic", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const ratchet = new RatchetService({ crypto });

  await withTempDir(async (dir) => {
    const store = new FsSessionStore({ rootDir: dir });
    const inner = new MemorySessionManager({ ratchetService: ratchet, crypto });
    const mgr = new PersistentSessionManager({ inner, store });

    const badPath = path.join(dir, "bad.json");
    await fs.writeFile(badPath, "{ not json", "utf8");

    await assert.rejects(() => mgr.loadAll(), /JSON|Unexpected token|FsSessionStore/);
  });
});
