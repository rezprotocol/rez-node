import test from "node:test";
import assert from "node:assert/strict";
import { WebSocket } from "ws";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { RMailbox, MemoryDataStore, MemoryStorageProvider, createDefaultRegistry } from "@rezprotocol/core";
import { createServerServices, createPerAccountServices, createProtocolFactory, createDepositHandler } from "./helpers/nodeTestServices.js";
import { authenticateSession, createNodeTestIdentity } from "./helpers/wsAuth.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

function waitForMessage(ws, predicate, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for WS message"));
    }, timeoutMs);
    function cleanup() {
      clearTimeout(timer);
      ws.off("message", onMessage);
      ws.off("error", onError);
    }
    function onError(err) {
      cleanup();
      reject(err);
    }
    function onMessage(data) {
      let frame;
      try {
        frame = JSON.parse(data.toString("utf8"));
      } catch {
        return;
      }
      if (!predicate(frame)) return;
      cleanup();
      resolve(frame);
    }
    ws.on("message", onMessage);
    ws.on("error", onError);
  });
}

test("ws raw node.status returns mesh and peers snapshot", async (t) => {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:test",
    deviceId: "dev:test",
    localInboxId: "inbox:test",
  });
  const runtime = {
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    relayStore: null,
    metrics: null,
    serverServices: createServerServices({
      storageProvider,
      clock: () => Date.now(),
      ownerAccountId: identity.accountId,
    }),
    serviceCache: new PerAccountServiceCache({
      storageProvider,
      clock: () => Date.now(),
      createServices: createPerAccountServices,
    }),
    getIdentity() {
      return { ...identity };
    },
    getMeshStatus() {
      return {
        enabled: true,
        mode: "seeded-gossip",
        participateInRouting: true,
        peerCount: 1,
        seedReachable: { "http://127.0.0.1:9999": true },
        lastDiscoveryAtMs: 123,
        routeStats: { evicted: 1 },
        policy: { rateLimit: 120, payloadMaxBytes: 1048576, failureThreshold: 8 },
        peers: [{ nodeId: "relay-a", transport: "tcp", lastSeenAtMs: null, health: "healthy", source: "seed" }],
      };
    },
    async stop() {},
  };

  const server = new WsGatewayServer({ runtime, port: 0, protocolFactory: createProtocolFactory(), onInboundDeposit: createDepositHandler({ crypto: new NodeCryptoProvider() }) });
  try {
    await server.start();
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err?.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => {
    await server.stop();
  });

  const address = server.address();
  const ws = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  t.after(() => ws.close());

  // Authenticate first — all operations require a session.
  await authenticateSession({ ws, waitForMessage, id: "hello", deviceId: "dev:mesh-test" });

  ws.send(JSON.stringify({ id: "mesh-1", t: "node.status", body: {} }));
  const frame = await waitForMessage(ws, (msg) => msg.id === "mesh-1" && msg.t === "node.status.res");
  // Node identifies itself by nodeKeyId, not accountId (multi-tenant).
  assert.equal(typeof frame.body.node.nodeKeyId, "string");
  assert.ok(frame.body.node.nodeKeyId.length > 0);
  assert.equal(typeof frame.body.node.nodePublicKeyB64, "string");
  // No accountId / deviceId / localInboxId fields on the node object.
  assert.equal(frame.body.node.accountId, undefined);
  assert.equal(frame.body.node.deviceId, undefined);
  assert.equal(frame.body.node.localInboxId, undefined);
  assert.equal(frame.body.mesh.peerCount, 1);
  assert.equal(Array.isArray(frame.body.peers), true);
  assert.equal(frame.body.peers[0].nodeId, "relay-a");
});
