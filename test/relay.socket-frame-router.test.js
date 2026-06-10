import test from "node:test";
import assert from "node:assert/strict";
import { bytesToBase64, OnionKeyRecordV1 } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { SocketFrameRouter } from "../src/relay/SocketFrameRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import {
  PEER_AUTH_PROTOCOL_VERSION,
  buildSignedRelayDescriptorJson,
  meshPeerAuthPayload,
  signedPayloadBytes,
} from "../src/relay/PeerAuthShared.js";

test("SocketFrameRouter peer auth requires peer.bind before unknown relay is authoritative", async () => {
  const crypto = new NodeCryptoProvider();
  const localKeyPair = crypto.generateSigningKeyPair();
  const remoteKeyPair = crypto.generateSigningKeyPair();
  const dir = new RelayPeerDirectory();
  const writes = [];
  const socket = {
    destroyed: false,
    write(frame) {
      writes.push(frame);
    },
  };
  const onionKey = new OnionKeyRecordV1({
    onionKeyId: "onion-test",
    publicKeyBytes: new Uint8Array([1, 2, 3]),
    format: "raw",
    createdAt: Date.now() - 1_000,
    notBefore: Date.now() - 1_000,
    notAfter: Date.now() + 60_000,
    status: "active",
  });
  const storedDescriptors = new Map();
  const relayStore = {
    getDescriptor(relayKeyId) {
      return storedDescriptors.get(relayKeyId) || null;
    },
    upsertDescriptor(descriptor) {
      storedDescriptors.set(descriptor.relayKeyId, descriptor);
      return { accepted: true };
    },
  };
  const router = new SocketFrameRouter({
    relayPeerDirectory: dir,
    relayStore,
    getSelfDescriptor: () => buildSignedRelayDescriptorJson({
      relayKeyId: "relay-local",
      advertisedHost: "127.0.0.1",
      relayPort: 2222,
      keyRecords: [onionKey],
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    }),
    selfPeerAuth: {
      relayKeyId: "relay-local",
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    },
  });

  const helloBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.hello",
    protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
    relayKeyId: "relay-remote",
    nodeKeyId: "remote-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    clientNonceB64: bytesToBase64(new Uint8Array(32).fill(9)),
  }));
  assert.equal(await router.dispatch(helloBytes, socket), true);
  assert.equal(writes.length, 1);

  const challenge = JSON.parse(Buffer.from(writes[0]).subarray(4).toString("utf8"));
  assert.equal(challenge._ctl, "peer.challenge");
  assert.equal(challenge.protocolVersion, PEER_AUTH_PROTOCOL_VERSION);

  const identifySig = bytesToBase64(crypto.sign({
    privateKey: remoteKeyPair.privateKey,
    msg: signedPayloadBytes(meshPeerAuthPayload({
      challengeId: challenge.challengeId,
      nonceB64: challenge.nonceB64,
      relayKeyId: "relay-remote",
      nodeKeyId: "remote-key",
    })),
  }));
  const identifyBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.identify",
    protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
    relayKeyId: "relay-remote",
    nodeKeyId: "remote-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    challengeId: challenge.challengeId,
    signatureB64: identifySig,
  }));
  assert.equal(await router.dispatch(identifyBytes, socket), true);
  assert.equal(dir.getSocket("relay-remote"), null, "unknown relay should remain provisional until peer.bind");
  assert.equal(writes.length, 3, "identify should emit peer.accept plus local peer.bind");

  const remoteDescriptor = buildSignedRelayDescriptorJson({
    relayKeyId: "relay-remote",
    advertisedHost: "127.0.0.1",
    relayPort: 3333,
    keyRecords: [onionKey],
    nodeKeyId: "remote-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    nodePrivateKeyB64: bytesToBase64(remoteKeyPair.privateKey),
  });
  const bindBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.bind",
    descriptor: remoteDescriptor,
  }));
  assert.equal(await router.dispatch(bindBytes, socket), true);
  // Inbound TOFU peer.bind stores the descriptor but does NOT promote the relay.
  // TOFU peers stay provisional — promotion only via outbound connections we
  // initiate or descriptor gossip from already-verified relays.
  assert.equal(dir.getSocket("relay-remote"), null, "inbound TOFU bind must not promote relay");
  const auth = dir.getAuth(socket);
  assert.ok(auth, "socket should still be authenticated");
  assert.equal(auth.authLevel, "relay-provisional", "auth level must remain relay-provisional after TOFU bind");
});

test("SocketFrameRouter outbound verified peer.bind promotes relay immediately", async () => {
  const crypto = new NodeCryptoProvider();
  const localKeyPair = crypto.generateSigningKeyPair();
  const remoteKeyPair = crypto.generateSigningKeyPair();
  const dir = new RelayPeerDirectory();
  const inboxRouter = new InboxRouter({ selfRelayKeyId: "relay-local" });
  const writes = [];
  const socket = {
    destroyed: false,
    write(frame) {
      writes.push(frame);
    },
  };
  const onionKey = new OnionKeyRecordV1({
    onionKeyId: "onion-test-outbound",
    publicKeyBytes: new Uint8Array([1, 2, 3]),
    format: "raw",
    createdAt: Date.now() - 1_000,
    notBefore: Date.now() - 1_000,
    notAfter: Date.now() + 60_000,
    status: "active",
  });
  const remoteDescriptor = buildSignedRelayDescriptorJson({
    relayKeyId: "relay-remote",
    advertisedHost: "127.0.0.1",
    relayPort: 3333,
    keyRecords: [onionKey],
    nodeKeyId: "remote-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    nodePrivateKeyB64: bytesToBase64(remoteKeyPair.privateKey),
  });
  const storedDescriptors = new Map();
  storedDescriptors.set("relay-remote", remoteDescriptor);
  const relayStore = {
    getDescriptor(relayKeyId) {
      return storedDescriptors.get(relayKeyId) || null;
    },
    upsertDescriptor(descriptor) {
      storedDescriptors.set(descriptor.relayKeyId, descriptor);
      return { accepted: true };
    },
  };
  const router = new SocketFrameRouter({
    relayPeerDirectory: dir,
    relayStore,
    inboxRouter,
    getSelfDescriptor: () => null,
    selfPeerAuth: {
      relayKeyId: "relay-local",
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    },
  });

  // Simulate outbound-authenticated peer (as if we dialled it)
  dir.authenticate(socket, {
    relayKeyId: "relay-remote",
    nodeKeyId: "remote-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    source: "outbound",
    authLevel: "relay-verified",
  });

  const bindBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.bind",
    descriptor: remoteDescriptor,
  }));
  assert.equal(await router.dispatch(bindBytes, socket), true);
  assert.equal(dir.getSocket("relay-remote"), socket, "outbound verified bind must promote relay");
  assert.ok(inboxRouter._peerSockets.has(socket), "outbound verified relay must be in route gossip peers");
});

test("relay-provisional peer is excluded from route gossip after identify", async () => {
  const crypto = new NodeCryptoProvider();
  const localKeyPair = crypto.generateSigningKeyPair();
  const remoteKeyPair = crypto.generateSigningKeyPair();
  const dir = new RelayPeerDirectory();
  const inboxRouter = new InboxRouter({ selfRelayKeyId: "relay-local" });
  const writes = [];
  const socket = {
    destroyed: false,
    write(frame) {
      writes.push(frame);
    },
  };
  const onionKey = new OnionKeyRecordV1({
    onionKeyId: "onion-test-prov",
    publicKeyBytes: new Uint8Array([1, 2, 3]),
    format: "raw",
    createdAt: Date.now() - 1_000,
    notBefore: Date.now() - 1_000,
    notAfter: Date.now() + 60_000,
    status: "active",
  });
  const router = new SocketFrameRouter({
    relayPeerDirectory: dir,
    relayStore: { getDescriptor() { return null; }, upsertDescriptor() { return { accepted: true }; } },
    inboxRouter,
    getSelfDescriptor: () => buildSignedRelayDescriptorJson({
      relayKeyId: "relay-local",
      advertisedHost: "127.0.0.1",
      relayPort: 2222,
      keyRecords: [onionKey],
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    }),
    selfPeerAuth: {
      relayKeyId: "relay-local",
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    },
  });

  // Peer connects claiming a relayKeyId not in our store → relay-provisional
  const helloBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.hello",
    protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
    relayKeyId: "relay-unknown",
    nodeKeyId: "unknown-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    clientNonceB64: bytesToBase64(new Uint8Array(32).fill(9)),
  }));
  assert.equal(await router.dispatch(helloBytes, socket), true);
  const challenge = JSON.parse(Buffer.from(writes[0]).subarray(4).toString("utf8"));

  const identifySig = bytesToBase64(crypto.sign({
    privateKey: remoteKeyPair.privateKey,
    msg: signedPayloadBytes(meshPeerAuthPayload({
      challengeId: challenge.challengeId,
      nonceB64: challenge.nonceB64,
      relayKeyId: "relay-unknown",
      nodeKeyId: "unknown-key",
    })),
  }));
  const identifyBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.identify",
    protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
    relayKeyId: "relay-unknown",
    nodeKeyId: "unknown-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    challengeId: challenge.challengeId,
    signatureB64: identifySig,
  }));
  assert.equal(await router.dispatch(identifyBytes, socket), true);

  const auth = dir.getAuth(socket);
  assert.equal(auth.authLevel, "relay-provisional");
  assert.ok(!inboxRouter._peerSockets.has(socket), "relay-provisional must NOT be in route gossip peers");
});

test("SocketFrameRouter dispatch route.failed calls onRouteFailed only for authenticated relays", async () => {
  let captured = null;
  const dir = new RelayPeerDirectory();
  const socket = {};
  dir.authenticate(socket, {
    relayKeyId: "relay-auth",
    nodeKeyId: "node-key",
    nodePublicKeyB64: "node-pub",
    authLevel: "relay-verified",
  });
  const router = new SocketFrameRouter({
    relayPeerDirectory: dir,
    onRouteFailed: (obj, sock) => {
      captured = { obj, sock };
    },
  });
  const bytes = new TextEncoder().encode(
    JSON.stringify({ _ctl: "route.failed", packetId: "p1", relayKeyId: "r1", reason: "no_peer" }),
  );
  const ok = await router.dispatch(bytes, socket);
  assert.equal(ok, true);
  assert.equal(captured?.obj?.packetId, "p1");
  assert.equal(captured?.sock, socket);
});

test("SocketFrameRouter dispatch non-JSON returns false", async () => {
  const router = new SocketFrameRouter({});
  const socket = {};
  const bytes = new Uint8Array([1, 2, 3]);
  const ok = await router.dispatch(bytes, socket);
  assert.equal(ok, false);
});

test("leaf node (node auth level) is excluded from route gossip to prevent topology leak", async () => {
  const crypto = new NodeCryptoProvider();
  const localKeyPair = crypto.generateSigningKeyPair();
  const remoteKeyPair = crypto.generateSigningKeyPair();
  const dir = new RelayPeerDirectory();
  const inboxRouter = new InboxRouter({ selfRelayKeyId: "relay-local" });
  const writes = [];
  const socket = {
    destroyed: false,
    write(frame) {
      writes.push(frame);
    },
  };
  const onionKey = new OnionKeyRecordV1({
    onionKeyId: "onion-test-leaf",
    publicKeyBytes: new Uint8Array([1, 2, 3]),
    format: "raw",
    createdAt: Date.now() - 1_000,
    notBefore: Date.now() - 1_000,
    notAfter: Date.now() + 60_000,
    status: "active",
  });
  const router = new SocketFrameRouter({
    relayPeerDirectory: dir,
    relayStore: { getDescriptor() { return null; }, upsertDescriptor() { return { accepted: true }; } },
    inboxRouter,
    getSelfDescriptor: () => buildSignedRelayDescriptorJson({
      relayKeyId: "relay-local",
      advertisedHost: "127.0.0.1",
      relayPort: 2222,
      keyRecords: [onionKey],
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    }),
    selfPeerAuth: {
      relayKeyId: "relay-local",
      nodeKeyId: "local-key",
      nodePublicKeyB64: bytesToBase64(localKeyPair.publicKey),
      nodePrivateKeyB64: bytesToBase64(localKeyPair.privateKey),
    },
  });

  // Leaf node connects with NO relayKeyId → "node" auth level
  const helloBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.hello",
    protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
    nodeKeyId: "leaf-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    clientNonceB64: bytesToBase64(new Uint8Array(32).fill(9)),
  }));
  assert.equal(await router.dispatch(helloBytes, socket), true);

  const challenge = JSON.parse(Buffer.from(writes[0]).subarray(4).toString("utf8"));
  assert.equal(challenge._ctl, "peer.challenge");

  const identifySig = bytesToBase64(crypto.sign({
    privateKey: remoteKeyPair.privateKey,
    msg: signedPayloadBytes(meshPeerAuthPayload({
      challengeId: challenge.challengeId,
      nonceB64: challenge.nonceB64,
      nodeKeyId: "leaf-key",
    })),
  }));
  const identifyBytes = new TextEncoder().encode(JSON.stringify({
    _ctl: "peer.identify",
    protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
    nodeKeyId: "leaf-key",
    nodePublicKeyB64: bytesToBase64(remoteKeyPair.publicKey),
    challengeId: challenge.challengeId,
    signatureB64: identifySig,
  }));
  assert.equal(await router.dispatch(identifyBytes, socket), true);

  // Leaf nodes must NOT be in route gossip peers — exposing the full route
  // table leaks inbox topology (active inbox IDs + delivery relay relationships).
  assert.ok(!inboxRouter._peerSockets.has(socket), "leaf node must NOT be in route gossip peers");

  const auth = dir.getAuth(socket);
  assert.equal(auth.authLevel, "node", "leaf node should authenticate as 'node' auth level");
  assert.ok(auth.authenticated, "leaf node should be authenticated for register/deposit");
});
