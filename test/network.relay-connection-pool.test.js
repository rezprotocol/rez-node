import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { base64ToBytes } from "@rezprotocol/core";
import { RelayConnectionPool } from "../src/network/RelayConnectionPool.js";
import { RelayStore } from "../src/network/RelayStore.js";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { SocketFrameRouter } from "../src/relay/SocketFrameRouter.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { encodeFrame, createFrameDecoder } from "../src/network/tcp/TcpFraming.js";
import {
  PEER_AUTH_PROTOCOL_VERSION,
  meshPeerAcceptPayload,
  meshPeerChallengePayload,
  signRelayDescriptorJson,
  signedPayloadBytes,
} from "../src/relay/PeerAuthShared.js";
import { listenLoopbackEphemeral } from "./_harness/listenLoopbackEphemeral.js";
import { createClaimantNodeDelegation, createNodeTestIdentity, createSessionIdentity } from "./helpers/wsAuth.js";

const PEER_AUTH_CRYPTO = new NodeCryptoProvider();

async function withServer(fn) {
  const server = net.createServer();
  const bound = await listenLoopbackEphemeral(server);
  const addr = server.address();
  try {
    return await fn(server, addr);
  } finally {
    await bound.close();
  }
}

async function waitForCondition(check, { timeoutMs = 2_000, intervalMs = 10 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

function createPeerAuthServer(server, { endpoint, relayKeyId = null, sendPeerBind = false } = {}) {
  const remoteIdentity = createNodeTestIdentity();
  const received = [];
  const sockets = new Set();
  let connections = 0;

  function createDescriptor() {
    if (!relayKeyId || !endpoint) return null;
    return signRelayDescriptorJson({
      v: 1,
      relayKeyId,
      endpoints: [{ host: endpoint.host, port: endpoint.port }],
      onionKeys: [],
      capabilities: {},
      expiresAt: Date.now() + 60_000,
      meta: {
        v: 1,
        node: {
          keyId: remoteIdentity.nodeKeyId,
          publicKeyB64: remoteIdentity.nodePublicKeyB64,
          protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
        },
      },
    }, {
      nodeKeyId: remoteIdentity.nodeKeyId,
      nodePrivateKey: base64ToBytes(remoteIdentity.nodePrivateKeyB64),
    });
  }

  server.on("connection", (socket) => {
    sockets.add(socket);
    connections += 1;
    let challengeId = null;
    let nonceB64 = null;
    const decoder = createFrameDecoder((bytes) => {
      let obj = null;
      try {
        obj = JSON.parse(new TextDecoder().decode(bytes));
      } catch {
        received.push({ bytes, obj: null, socket });
        return;
      }
      if (obj?._ctl === "peer.hello") {
        challengeId = `peer_challenge:test:${Date.now()}`;
        nonceB64 = Buffer.from(new Uint8Array(32).fill(7)).toString("base64");
        const signature = PEER_AUTH_CRYPTO.sign({
          privateKey: base64ToBytes(remoteIdentity.nodePrivateKeyB64),
          msg: signedPayloadBytes(meshPeerChallengePayload({
            challengeId,
            nonceB64,
            relayKeyId,
            nodeKeyId: remoteIdentity.nodeKeyId,
          })),
        });
        socket.write(encodeFrame(new TextEncoder().encode(JSON.stringify({
          _ctl: "peer.challenge",
          protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
          challengeId,
          nonceB64,
          issuedAtMs: Date.now(),
          expiresAtMs: Date.now() + 60_000,
          relayKeyId: relayKeyId || undefined,
          nodeKeyId: remoteIdentity.nodeKeyId,
          nodePublicKeyB64: remoteIdentity.nodePublicKeyB64,
          signatureB64: Buffer.from(signature).toString("base64"),
        }))));
        return;
      }
      if (obj?._ctl === "peer.identify") {
        const acceptedAs = relayKeyId ? "relay-provisional" : "leaf";
        const trustLevel = relayKeyId ? "tofu" : "verified";
        const signature = PEER_AUTH_CRYPTO.sign({
          privateKey: base64ToBytes(remoteIdentity.nodePrivateKeyB64),
          msg: signedPayloadBytes(meshPeerAcceptPayload({
            challengeId,
            acceptedAs,
            relayKeyId,
            nodeKeyId: remoteIdentity.nodeKeyId,
            trustLevel,
          })),
        });
        socket.write(encodeFrame(new TextEncoder().encode(JSON.stringify({
          _ctl: "peer.accept",
          protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
          challengeId,
          acceptedAs,
          relayKeyId: relayKeyId || undefined,
          nodeKeyId: remoteIdentity.nodeKeyId,
          nodePublicKeyB64: remoteIdentity.nodePublicKeyB64,
          trustLevel,
          signatureB64: Buffer.from(signature).toString("base64"),
        }))));
        if (relayKeyId && sendPeerBind) {
          const descriptor = createDescriptor();
          socket.write(encodeFrame(new TextEncoder().encode(JSON.stringify({
            _ctl: "peer.bind",
            descriptor,
          }))));
        }
        return;
      }
      received.push({ bytes, obj, socket });
    });
    socket.on("data", (chunk) => decoder.push(chunk));
  });

  return {
    remoteIdentity,
    received,
    get sockets() {
      return Array.from(sockets);
    },
    get connectionCount() {
      return connections;
    },
    sendCtl(ctl) {
      const bytes = encodeFrame(new TextEncoder().encode(JSON.stringify(ctl)));
      for (const socket of sockets) {
        socket.write(bytes);
      }
    },
  };
}

function createRegistrations(nodeIdentity, inboxIds) {
  const identity = createSessionIdentity();
  return inboxIds.map((inboxId) => createClaimantNodeDelegation({
    claimantIdentity: identity,
    inboxId,
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    relayKeyId: nodeIdentity.relayKeyId,
  }));
}

function createPool({
  nodeIdentity = createNodeTestIdentity(),
  registrations = [],
  getInboxIds = null,
  onInboundFrame = null,
  inboxStore = null,
  inboxRouter = null,
  relayPeerDirectory = null,
  relayStore = null,
  frameRouter = null,
} = {}) {
  const pool = new RelayConnectionPool({
    maxConnections: 8,
    getRegistrations: () => registrations,
    getInboxIds,
    onInboundFrame,
    inboxStore,
    inboxRouter,
    relayPeerDirectory,
    relayStore,
    frameRouter,
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    nodePrivateKeyB64: nodeIdentity.nodePrivateKeyB64,
  });
  return { pool, nodeIdentity };
}

function findControlFrame(received, ctl) {
  return received.find((entry) => entry.obj?._ctl === ctl)?.obj || null;
}

function findRawFrame(received, predicate) {
  return received.find((entry) => entry.obj == null && predicate(entry.bytes)) || null;
}

test("RelayConnectionPool sends bytes via an authenticated pooled connection", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const { pool } = createPool();
      try {
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([1, 2, 3]));
        const delivered = await waitForCondition(() => !!findRawFrame(peer.received, (bytes) =>
          bytes.length === 3 && bytes[0] === 1 && bytes[1] === 2 && bytes[2] === 3,
        ));
        assert.equal(delivered, true);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool reuses a single authenticated connection per endpoint", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const { pool } = createPool();
      try {
        const endpoint = { host: "127.0.0.1", port: addr.port };
        await pool.sendBytes(endpoint, new Uint8Array([1]));
        await pool.sendBytes(endpoint, new Uint8Array([2]));
        await pool.sendBytes(endpoint, new Uint8Array([3]));
        const delivered = await waitForCondition(() => {
          const rawFrames = peer.received.filter((entry) => entry.obj == null);
          return rawFrames.length >= 3;
        });
        assert.equal(delivered, true);
        assert.equal(peer.connectionCount, 1);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool sends signed inbox.register exactly once per endpoint", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const nodeIdentity = createNodeTestIdentity();
      const registrations = createRegistrations(nodeIdentity, ["inbox:test-a", "inbox:test-b"]);
      const { pool } = createPool({
        nodeIdentity,
        registrations,
      });
      try {
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([42]));
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([43]));
        const seenRegister = await waitForCondition(() => !!findControlFrame(peer.received, "inbox.register"));
        assert.equal(seenRegister, true);
        const registerFrames = peer.received.filter((entry) => entry.obj?._ctl === "inbox.register");
        assert.equal(registerFrames.length, 1);
        assert.equal(registerFrames[0].obj.registrations.length, 2);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool demuxes inbox.deposit to hosted inboxes", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const deposited = [];
      const inboxStore = {
        depositFromWire(inboxId, wireBytes) {
          deposited.push({ inboxId, bytes: Array.from(wireBytes) });
          return Promise.resolve();
        },
      };
      const nodeIdentity = createNodeTestIdentity();
      const registrations = createRegistrations(nodeIdentity, ["inbox:demux-a", "inbox:demux-b"]);
      const { pool } = createPool({
        registrations,
        getInboxIds: () => ["inbox:demux-a", "inbox:demux-b"],
        inboxStore,
      });
      try {
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([9]));
        const registerReady = await waitForCondition(() => !!findControlFrame(peer.received, "inbox.register"));
        assert.equal(registerReady, true);
        peer.sendCtl({
          _ctl: "inbox.deposit",
          inboxId: "inbox:demux-a",
          inner: Buffer.from(new Uint8Array([1, 2, 3])).toString("base64"),
        });
        peer.sendCtl({
          _ctl: "inbox.deposit",
          inboxId: "inbox:not-hosted",
          inner: Buffer.from(new Uint8Array([4, 5, 6])).toString("base64"),
        });
        const delivered = await waitForCondition(() => deposited.length >= 1);
        assert.equal(delivered, true);
        assert.deepEqual(deposited, [{ inboxId: "inbox:demux-a", bytes: [1, 2, 3] }]);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool forwards non-control inbound frames to callback", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const inbound = [];
      const { pool } = createPool({
        onInboundFrame: (bytes) => inbound.push(bytes),
      });
      try {
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([99]));
        const registerDone = await waitForCondition(() => peer.sockets.length === 1);
        assert.equal(registerDone, true);
        const frame = encodeFrame(new Uint8Array([7, 8, 9]));
        peer.sockets[0].write(frame);
        const delivered = await waitForCondition(() => inbound.length >= 1);
        assert.equal(delivered, true);
        assert.deepEqual(inbound[0], new Uint8Array([7, 8, 9]));
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool.connectToKnownRelays deduplicates endpoints", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const { pool } = createPool();
      try {
        const endpoint = { host: "127.0.0.1", port: addr.port };
        await pool.connectToKnownRelays([
          { endpoint },
          { endpoint },
        ]);
        const connected = await waitForCondition(() => pool.connectionCount === 1);
        assert.equal(connected, true);
        assert.equal(peer.connectionCount, 1);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool routes inbound relay control through SocketFrameRouter after peer.bind", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const relayPeerDirectory = new RelayPeerDirectory();
      const relayStore = new RelayStore();
      const inboxRouter = new InboxRouter({
        relayPeerDirectory,
        selfRelayKeyId: "relay-local",
      });
      const frameRouter = new SocketFrameRouter({
        relayPeerDirectory,
        relayStore,
        inboxRouter,
      });
      const peer = createPeerAuthServer(server, {
        endpoint: { host: "127.0.0.1", port: addr.port },
        relayKeyId: "relay-peer",
        sendPeerBind: true,
      });
      const { pool } = createPool({
        relayPeerDirectory,
        relayStore,
        inboxRouter,
        frameRouter,
      });
      try {
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([1]));
        const bound = await waitForCondition(() => relayPeerDirectory.getSocket("relay-peer") != null);
        assert.equal(bound, true);

        // Post-MED-8: gossip carries only hops=0 entries with a valid
        // claimant-signed registration. Build one so the wire path is
        // still exercised (the test's load-bearing claim is that control
        // messages flow through to InboxRouter after peer.bind). The
        // registration's nodeKey must match the peer's authenticated
        // identity for verifyHostedInboxRegistration to accept it.
        const identity = createSessionIdentity();
        const registration = createClaimantNodeDelegation({
          claimantIdentity: identity,
          inboxId: "inbox:remote-via-peer",
          nodeKeyId: peer.remoteIdentity.nodeKeyId,
          nodePublicKeyB64: peer.remoteIdentity.nodePublicKeyB64,
          relayKeyId: "relay-peer",
        });
        peer.sendCtl({
          _ctl: "inbox.route",
          entries: [{
            inboxId: "inbox:remote-via-peer",
            hops: 0,
            nextHopRelayKeyId: "relay-peer",
            deliveryRelayKeyId: "relay-peer",
            registration,
          }],
        });
        const routed = await waitForCondition(() => inboxRouter.getRouteTo("inbox:remote-via-peer") != null);
        assert.equal(routed, true);
        assert.equal(inboxRouter.getRouteTo("inbox:remote-via-peer")?.nextHopRelayKeyId, "relay-peer");

        peer.sendCtl({
          _ctl: "inbox.withdraw",
          inboxIds: ["inbox:remote-via-peer"],
        });
        const withdrawn = await waitForCondition(() => inboxRouter.getRouteTo("inbox:remote-via-peer") == null);
        assert.equal(withdrawn, true);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool resolves sendByRelayId after peer.bind promotes a relay", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const relayPeerDirectory = new RelayPeerDirectory();
      const relayStore = new RelayStore();
      const frameRouter = new SocketFrameRouter({
        relayPeerDirectory,
        relayStore,
      });
      const peer = createPeerAuthServer(server, {
        endpoint: { host: "127.0.0.1", port: addr.port },
        relayKeyId: "relay-peer",
        sendPeerBind: true,
      });
      const { pool } = createPool({
        relayPeerDirectory,
        relayStore,
        frameRouter,
      });
      try {
        await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([1]));
        const bound = await waitForCondition(() => relayPeerDirectory.getSocket("relay-peer") != null);
        assert.equal(bound, true);
        await pool.sendByRelayId("relay-peer", new Uint8Array([9, 9]));
        const delivered = await waitForCondition(() => !!findRawFrame(peer.received, (bytes) =>
          bytes.length === 2 && bytes[0] === 9 && bytes[1] === 9,
        ));
        assert.equal(delivered, true);
      } finally {
        await pool.close();
      }
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});

test("RelayConnectionPool.close tears down connections and rejects future sends", async (t) => {
  try {
    await withServer(async (server, addr) => {
      let closed = 0;
      server.on("connection", (socket) => {
        socket.on("close", () => { closed += 1; });
      });
      const peer = createPeerAuthServer(server, { endpoint: { host: "127.0.0.1", port: addr.port } });
      const { pool } = createPool();
      await pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([1]));
      const connected = await waitForCondition(() => peer.connectionCount === 1);
      assert.equal(connected, true);
      await pool.close();
      const observedClose = await waitForCondition(() => closed >= 1);
      assert.equal(observedClose, true);
      await assert.rejects(
        () => pool.sendBytes({ host: "127.0.0.1", port: addr.port }, new Uint8Array([2])),
        /closed/,
      );
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted");
      return;
    }
    throw err;
  }
});
