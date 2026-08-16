import test from "node:test";
import assert from "node:assert/strict";
import {
  OnionLayerAeadV2,
  OnionPacketV2,
  JsonCodec,
  Header,
  Envelope,
} from "@rezprotocol/core";
import { X25519_SUPPORTED } from "../src/crypto/dh/index.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { RelayRuntime } from "../src/relay/RelayRuntime.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { TcpRelayTransport } from "../src/relay/TcpRelayTransport.js";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RMailbox, MemoryDataStore, createDefaultRegistry, encodeOuterPacket, newRoutingKey } from "@rezprotocol/core";
import { OnionKeyringV1 } from "@rezprotocol/core";
import { makeRelayIdentity } from "./support/relayIdentity.js";

function makeProviderOrSkip(t) {
  if (!X25519_SUPPORTED) {
    t.skip("X25519 not supported in this Node runtime");
    return null;
  }
  return new NodeCryptoProvider();
}

function encodeJsonBytes(obj) {
  const encoder = new TextEncoder();
  return encoder.encode(canonicalStringify(obj));
}

function bytesToBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function onionKeyIdFor(crypto, pubKeyBytes) {
  return Buffer.from(crypto.hashSha256(pubKeyBytes)).toString("hex");
}

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    const out = {};
    const keys = Object.keys(value).sort();
    for (const key of keys) {
      out[key] = canonicalize(value[key]);
    }
    return out;
  }
  return value;
}

function canonicalStringify(value) {
  return JSON.stringify(canonicalize(value));
}

function buildFixedPacketV2(blobBytes) {
  const sizeClasses = [4096, 8192, 16384, 32768];
  const sizeClass = sizeClasses.find((s) => s >= blobBytes.length);
  if (!sizeClass) {
    throw new Error("buildFixedPacketV2 blob exceeds max size class");
  }
  const payload = new Uint8Array(sizeClass);
  payload.set(blobBytes, 0);
  return new OnionPacketV2({ v: 2, sizeClass, payload });
}

async function buildOnionV2WithDeliver({ crypto, path, finalRelayKeyId, deliverInboxId, innerBytes, ttl = path.length }) {
  const layer = new OnionLayerAeadV2({ crypto });
  let blob = innerBytes;

  for (let i = path.length - 1; i >= 0; i -= 1) {
    const hop = path[i];
    const hopTtl = Math.max(0, ttl - i);
    const next = (i + 1 < path.length)
      ? { relayKeyId: path[i + 1].relayKeyId }
      : { relayKeyId: finalRelayKeyId };
    const layerPlain = {
      v: 2,
      ttl: hopTtl,
      next,
      flags: { dropOnFail: true },
      inner: bytesToBase64(blob),
      ...(i === path.length - 1 ? { deliver: { inboxId: deliverInboxId } } : {}),
    };

    const plaintextBytes = encodeJsonBytes(layerPlain);
    const encrypted = await layer.encryptLayerV2({
      relayPubKeyBytes: hop.onionPubKeyBytes,
      plaintextBytes,
      hopIndex: i,
      ttl: hopTtl,
      onionKeyId: hop.onionKeyId,
    });

    const cipherObj = {
      v: 2,
      hopIndex: i,
      onionKeyId: encrypted.onionKeyId,
      ttl: hopTtl,
      ephPub: bytesToBase64(encrypted.ephPub),
      ct: bytesToBase64(encrypted.ct),
    };

    blob = encodeJsonBytes(cipherObj);
  }

  return buildFixedPacketV2(blob);
}

function makeRelay({ crypto, keypair, transport, inboxStore, nowMs, relayDirectory = null }) {
  const keyring = new OnionKeyringV1();
  const fixedNow = Number.isFinite(nowMs) ? nowMs : Date.now();
  const onionKeyId = onionKeyIdFor(crypto, keypair.publicKey);
  keyring.addKey({
    onionKeyId,
    privateKeyBytes: keypair.privateKey,
    notBefore: fixedNow - 1000,
    notAfter: fixedNow + 1000,
    status: "active",
  });

  return new RelayRuntime({
    transport,
    inboxStore,
    relayDirectory: relayDirectory ?? undefined,
    onion: {
      crypto,
      v1: { privateKeyBytes: keypair.privateKey },
      v2: { keyring },
    },
    nowMs: () => fixedNow,
  });
}

async function encodeEnvelopeBytes(packet) {
  const header = new Header({ id: `onion-${Date.now()}`, type: "rez.onion.v2", createdAt: Date.now() });
  const envelope = new Envelope({ header, body: packet.toJSON() });
  const codec = new JsonCodec();
  const ctx = await codec.encode({ envelope });
  return ctx.bytes;
}

async function waitFor(fn, { attempts = 40, delayMs = 10 } = {}) {
  for (let i = 0; i < attempts; i += 1) {
    const result = await fn();
    if (result) return result;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  return null;
}

async function waitForListenAddress(transport, { timeoutMs = 3000, intervalMs = 20 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const addr = transport.getListenAddress();
    if (addr && Number.isInteger(addr.port) && addr.port > 0) return addr;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error("TcpRelayTransport did not bind to a port in time");
}

test("RelayRuntime forwards to deliver inbox on second hop", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const keyA = crypto.dhGenerateKeyPair();
  const keyB = crypto.dhGenerateKeyPair();
  const nowMs = Date.now();
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayKeyIdA = idA.relayKeyId;
  const relayKeyIdB = idB.relayKeyId;

  const transportA = new TcpRelayTransport({ endpointId: relayKeyIdA, listenPort: 0 });
  const transportB = new TcpRelayTransport({ endpointId: relayKeyIdB, listenPort: 0 });

  const inboxA = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const inboxB = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });

  const directoryA = new RelayPeerDirectory();
  const relayA = makeRelay({ crypto, keypair: keyA, transport: transportA, inboxStore: inboxA, nowMs, relayDirectory: directoryA });
  const relayB = makeRelay({ crypto, keypair: keyB, transport: transportB, inboxStore: inboxB, nowMs });
  const client = new TcpRelayTransport({ endpointId: "client", listenPort: 0 });
  t.after(async () => {
    await relayA.stop();
    await relayB.stop();
    await client.stop();
  });

  try {
    await relayA.start();
    await relayB.start();

    const addrA = await waitForListenAddress(transportA);
    const addrB = await waitForListenAddress(transportB);

    const net = await import("node:net");
    const socketAtoB = net.createConnection({ host: addrB.host, port: addrB.port });
    await new Promise((res, rej) => socketAtoB.once("connect", res).once("error", rej));
    directoryA.authenticate(socketAtoB, {
      relayKeyId: idB.relayKeyId,
      nodeKeyId: idB.nodeKeyId,
      nodePublicKeyB64: idB.nodePublicKeyB64,
      authLevel: "relay-verified",
    });
    t.after(() => socketAtoB.destroy());

    const path = [
      { relayKeyId: relayKeyIdA, onionPubKeyBytes: keyA.publicKey, onionKeyId: onionKeyIdFor(crypto, keyA.publicKey) },
      { relayKeyId: relayKeyIdB, onionPubKeyBytes: keyB.publicKey, onionKeyId: onionKeyIdFor(crypto, keyB.publicKey) },
    ];

    const deliverInboxId = "inboxB";
    const innerBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([1, 2, 3, 4]) });
    const packet = await buildOnionV2WithDeliver({
      crypto,
      path,
      finalRelayKeyId: relayKeyIdB,
      deliverInboxId,
      innerBytes,
    });

    const bytes = await encodeEnvelopeBytes(packet);
    await client.sendBytes(addrA, bytes);

    const deposited = await waitFor(async () => {
      const list = await inboxB.list(deliverInboxId, { limit: 10 });
      return list.items.length > 0 ? list.items[0] : null;
    });

    assert.ok(deposited);
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    // cleanup handled by t.after
  }
});

test("RelayRuntime deposits on deliver at first hop", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const keyA = crypto.dhGenerateKeyPair();
  const nowMs = Date.now();
  const relayKeyIdA = "relayA";

  const transportA = new TcpRelayTransport({ endpointId: relayKeyIdA, listenPort: 0 });
  const inboxA = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayA = makeRelay({ crypto, keypair: keyA, transport: transportA, inboxStore: inboxA, nowMs });
  const client = new TcpRelayTransport({ endpointId: "client", listenPort: 0 });
  t.after(async () => {
    await relayA.stop();
    await client.stop();
  });

  try {
    await relayA.start();
    const addrA = await waitForListenAddress(transportA);

    const path = [
      { relayKeyId: relayKeyIdA, onionPubKeyBytes: keyA.publicKey, onionKeyId: onionKeyIdFor(crypto, keyA.publicKey) },
    ];

    const deliverInboxId = "inboxA";
    const innerBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([9, 9, 9]) });
    const packet = await buildOnionV2WithDeliver({
      crypto,
      path,
      finalRelayKeyId: relayKeyIdA,
      deliverInboxId,
      innerBytes,
    });

    const bytes = await encodeEnvelopeBytes(packet);
    await client.sendBytes(addrA, bytes);

    const deposited = await waitFor(async () => {
      const list = await inboxA.list(deliverInboxId, { limit: 10 });
      return list.items.length > 0 ? list.items[0] : null;
    });

    assert.ok(deposited);
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    // cleanup handled by t.after
  }
});

test("RelayRuntime drops replayed packet", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const keyA = crypto.dhGenerateKeyPair();
  const nowMs = Date.now();
  const relayKeyIdA = "relayA";

  const transportA = new TcpRelayTransport({ endpointId: relayKeyIdA, listenPort: 0 });
  const inboxA = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const relayA = makeRelay({ crypto, keypair: keyA, transport: transportA, inboxStore: inboxA, nowMs });
  const client = new TcpRelayTransport({ endpointId: "client", listenPort: 0 });
  t.after(async () => {
    await relayA.stop();
    await client.stop();
  });

  try {
    await relayA.start();
    const addrA = await waitForListenAddress(transportA);

    const path = [
      { relayKeyId: relayKeyIdA, onionPubKeyBytes: keyA.publicKey, onionKeyId: onionKeyIdFor(crypto, keyA.publicKey) },
    ];

    const deliverInboxId = "inboxA";
    const innerBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([5, 5, 5]) });
    const packet = await buildOnionV2WithDeliver({
      crypto,
      path,
      finalRelayKeyId: relayKeyIdA,
      deliverInboxId,
      innerBytes,
    });

    const bytes = await encodeEnvelopeBytes(packet);
    await client.sendBytes(addrA, bytes);
    await client.sendBytes(addrA, bytes);

    const list = await waitFor(async () => {
      const current = await inboxA.list(deliverInboxId, { limit: 10 });
      return current.items.length >= 1 ? current : null;
    });
    assert.equal(list.items.length, 1, "replayed packet must be dropped (only first deposit accepted)");
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    // cleanup handled by t.after
  }
});

test("RelayRuntime drops when ttl expires before delivery", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const keyA = crypto.dhGenerateKeyPair();
  const keyB = crypto.dhGenerateKeyPair();
  const nowMs = Date.now();
  const idA = makeRelayIdentity();
  const idB = makeRelayIdentity();
  const relayKeyIdA = idA.relayKeyId;
  const relayKeyIdB = idB.relayKeyId;

  const transportA = new TcpRelayTransport({ endpointId: relayKeyIdA, listenPort: 0 });
  const transportB = new TcpRelayTransport({ endpointId: relayKeyIdB, listenPort: 0 });

  const inboxA = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const inboxB = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });

  const directoryA = new RelayPeerDirectory();
  const relayA = makeRelay({ crypto, keypair: keyA, transport: transportA, inboxStore: inboxA, nowMs, relayDirectory: directoryA });
  const relayB = makeRelay({ crypto, keypair: keyB, transport: transportB, inboxStore: inboxB, nowMs });
  const client = new TcpRelayTransport({ endpointId: "client", listenPort: 0 });
  t.after(async () => {
    await relayA.stop();
    await relayB.stop();
    await client.stop();
  });

  try {
    await relayA.start();
    await relayB.start();

    const addrA = await waitForListenAddress(transportA);
    const addrB = await waitForListenAddress(transportB);

    const net = await import("node:net");
    const socketAtoB = net.createConnection({ host: addrB.host, port: addrB.port });
    await new Promise((res, rej) => socketAtoB.once("connect", res).once("error", rej));
    directoryA.authenticate(socketAtoB, {
      relayKeyId: idB.relayKeyId,
      nodeKeyId: idB.nodeKeyId,
      nodePublicKeyB64: idB.nodePublicKeyB64,
      authLevel: "relay-verified",
    });
    t.after(() => socketAtoB.destroy());

    const path = [
      { relayKeyId: relayKeyIdA, onionPubKeyBytes: keyA.publicKey, onionKeyId: onionKeyIdFor(crypto, keyA.publicKey) },
      { relayKeyId: relayKeyIdB, onionPubKeyBytes: keyB.publicKey, onionKeyId: onionKeyIdFor(crypto, keyB.publicKey) },
    ];

    const deliverInboxId = "inboxB";
    const innerBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([7, 7, 7]) });
    const packet = await buildOnionV2WithDeliver({
      crypto,
      path,
      finalRelayKeyId: relayKeyIdB,
      deliverInboxId,
      innerBytes,
      ttl: 1,
    });

    const bytes = await encodeEnvelopeBytes(packet);
    await client.sendBytes(addrA, bytes);

    const list = await waitFor(async () => inboxB.list(deliverInboxId, { limit: 10 }));
    assert.equal(list.items.length, 0);
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    // cleanup handled by t.after
  }
});

test("mesh routing: A→R1→R2→R3→B and B→R3→R2→R1→A", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const keyR1 = crypto.dhGenerateKeyPair();
  const keyR2 = crypto.dhGenerateKeyPair();
  const keyR3 = crypto.dhGenerateKeyPair();
  const nowMs = Date.now();
  const idR1 = makeRelayIdentity();
  const idR2 = makeRelayIdentity();
  const idR3 = makeRelayIdentity();
  const relayKeyIdR1 = idR1.relayKeyId;
  const relayKeyIdR2 = idR2.relayKeyId;
  const relayKeyIdR3 = idR3.relayKeyId;

  const transportR1 = new TcpRelayTransport({ endpointId: relayKeyIdR1, listenPort: 0 });
  const transportR2 = new TcpRelayTransport({ endpointId: relayKeyIdR2, listenPort: 0 });
  const transportR3 = new TcpRelayTransport({ endpointId: relayKeyIdR3, listenPort: 0 });

  const inboxStoreR1 = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const inboxStoreR2 = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const inboxStoreR3 = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });

  const dirR1 = new RelayPeerDirectory();
  const dirR2 = new RelayPeerDirectory();
  const dirR3 = new RelayPeerDirectory();

  const relayR1 = makeRelay({ crypto, keypair: keyR1, transport: transportR1, inboxStore: inboxStoreR1, nowMs, relayDirectory: dirR1 });
  const relayR2 = makeRelay({ crypto, keypair: keyR2, transport: transportR2, inboxStore: inboxStoreR2, nowMs, relayDirectory: dirR2 });
  const relayR3 = makeRelay({ crypto, keypair: keyR3, transport: transportR3, inboxStore: inboxStoreR3, nowMs, relayDirectory: dirR3 });

  const clientA = new TcpRelayTransport({ endpointId: "clientA", listenPort: 0 });
  const clientB = new TcpRelayTransport({ endpointId: "clientB", listenPort: 0 });

  t.after(async () => {
    await relayR1.stop();
    await relayR2.stop();
    await relayR3.stop();
    await clientA.stop();
    await clientB.stop();
  });

  try {
    await relayR1.start();
    await relayR2.start();
    await relayR3.start();

    const addrR1 = await waitForListenAddress(transportR1);
    const addrR2 = await waitForListenAddress(transportR2);
    const addrR3 = await waitForListenAddress(transportR3);

    const net = await import("node:net");
    // Each socket authenticates as the SPECIFIC relay it dials — the relay's
    // self-certifying triple must bind (ADR-RELAY-IDENTITY).
    const authTripleFor = (identity) => ({
      relayKeyId: identity.relayKeyId,
      nodeKeyId: identity.nodeKeyId,
      nodePublicKeyB64: identity.nodePublicKeyB64,
      authLevel: "relay-verified",
    });

    // --- Wire write-counting on inter-relay sockets to prove pathway ---
    const writeCounts = { R1toR2: 0, R2toR1: 0, R2toR3: 0, R3toR2: 0 };
    function instrumentWrite(socket, counterKey) {
      const origWrite = socket.write.bind(socket);
      socket.write = function (...args) {
        writeCounts[counterKey] += 1;
        return origWrite(...args);
      };
    }

    // R1→R2: socket from R1 to R2's listener, registered in dirR1
    const socketR1toR2 = net.createConnection({ host: addrR2.host, port: addrR2.port });
    await new Promise((res, rej) => socketR1toR2.once("connect", res).once("error", rej));
    dirR1.authenticate(socketR1toR2, authTripleFor(idR2));
    instrumentWrite(socketR1toR2, "R1toR2");
    t.after(() => socketR1toR2.destroy());

    // R2→R1: socket from R2 to R1's listener, registered in dirR2
    const socketR2toR1 = net.createConnection({ host: addrR1.host, port: addrR1.port });
    await new Promise((res, rej) => socketR2toR1.once("connect", res).once("error", rej));
    dirR2.authenticate(socketR2toR1, authTripleFor(idR1));
    instrumentWrite(socketR2toR1, "R2toR1");
    t.after(() => socketR2toR1.destroy());

    // R2→R3: socket from R2 to R3's listener, registered in dirR2
    const socketR2toR3 = net.createConnection({ host: addrR3.host, port: addrR3.port });
    await new Promise((res, rej) => socketR2toR3.once("connect", res).once("error", rej));
    dirR2.authenticate(socketR2toR3, authTripleFor(idR3));
    instrumentWrite(socketR2toR3, "R2toR3");
    t.after(() => socketR2toR3.destroy());

    // R3→R2: socket from R3 to R2's listener, registered in dirR3
    const socketR3toR2 = net.createConnection({ host: addrR2.host, port: addrR2.port });
    await new Promise((res, rej) => socketR3toR2.once("connect", res).once("error", rej));
    dirR3.authenticate(socketR3toR2, authTripleFor(idR2));
    instrumentWrite(socketR3toR2, "R3toR2");
    t.after(() => socketR3toR2.destroy());

    // --- Assert topology: NO direct R1↔R3 link ---
    assert.equal(dirR1.getSocket(relayKeyIdR3), null, "R1 must NOT have a direct socket to R3");
    assert.equal(dirR3.getSocket(relayKeyIdR1), null, "R3 must NOT have a direct socket to R1");

    // Snapshot write counts before sending (should be 0)
    const beforeAtoB = { ...writeCounts };

    // === Direction A→R1→R2→R3→B ===
    const inboxB = "inboxB";
    const innerA = encodeOuterPacket({ bodyBytes: new Uint8Array([10, 20, 30, 40]) });
    const pathAtoB = [
      { relayKeyId: relayKeyIdR1, onionPubKeyBytes: keyR1.publicKey, onionKeyId: onionKeyIdFor(crypto, keyR1.publicKey) },
      { relayKeyId: relayKeyIdR2, onionPubKeyBytes: keyR2.publicKey, onionKeyId: onionKeyIdFor(crypto, keyR2.publicKey) },
      { relayKeyId: relayKeyIdR3, onionPubKeyBytes: keyR3.publicKey, onionKeyId: onionKeyIdFor(crypto, keyR3.publicKey) },
    ];
    const packetAtoB = await buildOnionV2WithDeliver({
      crypto,
      path: pathAtoB,
      finalRelayKeyId: relayKeyIdR3,
      deliverInboxId: inboxB,
      innerBytes: innerA,
    });
    const bytesAtoB = await encodeEnvelopeBytes(packetAtoB);
    await clientA.sendBytes(addrR1, bytesAtoB);

    const depositedB = await waitFor(async () => {
      const list = await inboxStoreR3.list(inboxB, { limit: 10 });
      return list.items.length > 0 ? list.items[0] : null;
    }, { attempts: 80, delayMs: 25 });
    assert.ok(depositedB, "message A→B should be deposited in R3 inbox");

    // Verify payload is readable and matches original bytes
    const fetchedB = await inboxStoreR3.fetch(inboxB, depositedB.eventId);
    assert.ok(fetchedB, "fetched event must exist");
    assert.ok(fetchedB.bytes instanceof Uint8Array, "deposited payload must be Uint8Array");
    assert.deepEqual(
      Array.from(fetchedB.bytes),
      Array.from(innerA),
      "deposited payload must match original outer packet bytes (A→B)"
    );

    // Verify pathway: R1 forwarded to R2, R2 forwarded to R3
    assert.ok(writeCounts.R1toR2 > beforeAtoB.R1toR2, "R1→R2 socket must have been written to (hop 1→2)");
    assert.ok(writeCounts.R2toR3 > beforeAtoB.R2toR3, "R2→R3 socket must have been written to (hop 2→3)");

    // R2 must NOT have deposited anything — it's a pass-through
    const r2ListB = await inboxStoreR2.list(inboxB, { limit: 10 });
    assert.equal(r2ListB.items.length, 0, "R2 must not deposit A→B message (pass-through only)");

    // Snapshot write counts before B→A
    const beforeBtoA = { ...writeCounts };

    // === Direction B→R3→R2→R1→A ===
    const inboxA = "inboxA";
    const innerB = encodeOuterPacket({ bodyBytes: new Uint8Array([50, 60, 70, 80]) });
    const pathBtoA = [
      { relayKeyId: relayKeyIdR3, onionPubKeyBytes: keyR3.publicKey, onionKeyId: onionKeyIdFor(crypto, keyR3.publicKey) },
      { relayKeyId: relayKeyIdR2, onionPubKeyBytes: keyR2.publicKey, onionKeyId: onionKeyIdFor(crypto, keyR2.publicKey) },
      { relayKeyId: relayKeyIdR1, onionPubKeyBytes: keyR1.publicKey, onionKeyId: onionKeyIdFor(crypto, keyR1.publicKey) },
    ];
    const packetBtoA = await buildOnionV2WithDeliver({
      crypto,
      path: pathBtoA,
      finalRelayKeyId: relayKeyIdR1,
      deliverInboxId: inboxA,
      innerBytes: innerB,
    });
    const bytesBtoA = await encodeEnvelopeBytes(packetBtoA);
    await clientB.sendBytes(addrR3, bytesBtoA);

    const depositedA = await waitFor(async () => {
      const list = await inboxStoreR1.list(inboxA, { limit: 10 });
      return list.items.length > 0 ? list.items[0] : null;
    }, { attempts: 80, delayMs: 25 });
    assert.ok(depositedA, "message B→A should be deposited in R1 inbox");

    // Verify payload is readable and matches original bytes
    const fetchedA = await inboxStoreR1.fetch(inboxA, depositedA.eventId);
    assert.ok(fetchedA, "fetched event must exist");
    assert.ok(fetchedA.bytes instanceof Uint8Array, "deposited payload must be Uint8Array");
    assert.deepEqual(
      Array.from(fetchedA.bytes),
      Array.from(innerB),
      "deposited payload must match original outer packet bytes (B→A)"
    );

    // Verify reverse pathway: R3 forwarded to R2, R2 forwarded to R1
    assert.ok(writeCounts.R3toR2 > beforeBtoA.R3toR2, "R3→R2 socket must have been written to (hop 1→2)");
    assert.ok(writeCounts.R2toR1 > beforeBtoA.R2toR1, "R2→R1 socket must have been written to (hop 2→3)");

    // R2 must NOT have deposited anything for B→A either
    const r2ListA = await inboxStoreR2.list(inboxA, { limit: 10 });
    assert.equal(r2ListA.items.length, 0, "R2 must not deposit B→A message (pass-through only)");
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});

test("RelayRuntime withdraws route when client disconnects (Gap 3)", async (t) => {
  const crypto = makeProviderOrSkip(t);
  if (!crypto) return;

  const keyA = crypto.dhGenerateKeyPair();
  const nowMs = Date.now();

  const transportA = new TcpRelayTransport({ endpointId: "relayA", listenPort: 0 });
  const inboxStoreA = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const inboxRouter = new InboxRouter({ transport: transportA, inboxStore: inboxStoreA });

  const relayA = new RelayRuntime({
    transport: transportA,
    inboxStore: inboxStoreA,
    inboxRouter,
    onion: {
      crypto: crypto,
      v1: { privateKeyBytes: keyA.privateKey },
      v2: (() => {
        const keyring = new OnionKeyringV1();
        keyring.addKey({
          onionKeyId: onionKeyIdFor(crypto, keyA.publicKey),
          privateKeyBytes: keyA.privateKey,
          notBefore: nowMs - 1000,
          notAfter: nowMs + 1000,
          status: "active",
        });
        return { keyring };
      })(),
    },
    nowMs: () => nowMs,
  });

  t.after(async () => {
    await relayA.stop();
  });

  try {
    await relayA.start();
    const addr = await waitForListenAddress(transportA);

    const net = await import("node:net");
    const clientSocket = await new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: addr.host, port: addr.port });
      socket.once("connect", () => resolve(socket));
      socket.once("error", reject);
    });
    t.after(() => {
      try {
        clientSocket.destroy();
      } catch {}
    });

    const routeSocket = await waitFor(
      () => {
        const sockets = transportA.transport?.sockets;
        if (!(sockets instanceof Set)) return null;
        const match = Array.from(sockets).find((socket) => socket !== clientSocket && socket.destroyed !== true);
        return match || null;
      },
      { attempts: 60, delayMs: 20 },
    );
    assert.ok(routeSocket, "accepted relay socket should exist");

    const inboxId = "inbox:disconnect-test";
    inboxRouter.registerLocal([inboxId], routeSocket);
    assert.ok(inboxRouter.getRouteTo(inboxId), "route should exist after client registers");

    clientSocket.destroy();

    const routeGone = await waitFor(
      () => (inboxRouter.getRouteTo(inboxId) ? null : true),
      { attempts: 50, delayMs: 20 }
    );
    assert.ok(routeGone, "route should be withdrawn after client disconnects");
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});
