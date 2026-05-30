import test from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import {
  RMailbox,
  MemoryDataStore,
  MemoryStorageProvider,
  createDefaultRegistry,
  CONTRACT_VERSION,
  REZ_CONTRACT_TYPES,
} from "@rezprotocol/core";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import {
  createServerServices,
  createPerAccountServices,
  createProtocolFactory,
  createDepositHandler,
} from "./helpers/nodeTestServices.js";
import {
  createNodeTestIdentity,
  createSessionIdentity,
} from "./helpers/wsAuth.js";
import { SESSION_HELLO_RATE_LIMITER } from "../src/protocol/GatewaySession.js";

/**
 * docs/SECURITY_AUDIT.md pass-1 LOW observation: "`session.hello` accepts
 * any well-formed pubkey and the node has no rate-limit on
 * `session.hello` per-IP." LOW-4 mitigated rotation-evasion at the
 * deposit layer; this is the upstream defense-in-depth cap.
 *
 * Fix: a process-wide per-IP `SlidingWindowRateLimiter` on
 * `session.hello`. Beyond the cap, the node emits `RATE_LIMITED` and
 * closes the WS with 1013.
 */

const CRYPTO = new NodeCryptoProvider();

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

async function startNode(t) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:hello-rl-test:" + randomBytes(4).toString("hex"),
    deviceId: "dev:test",
    localInboxId: "inbox:test",
  });
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider });
  await inboxClaimRegistry.hydrate();
  const runtime = {
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    relayStore: null,
    metrics: null,
    inboxClaimRegistry,
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
      return { enabled: true, mode: "seeded-gossip", participateInRouting: true, peerCount: 0 };
    },
    async stop() {},
  };
  const server = new WsGatewayServer({
    runtime,
    port: 0,
    protocolFactory: createProtocolFactory(),
    onInboundDeposit: createDepositHandler({ crypto: CRYPTO }),
  });
  await server.start();
  t.after(() => server.stop());
  return { server };
}

function sendSessionHello(ws, claimant, deviceId = "dev:test") {
  ws.send(JSON.stringify({
    id: "hello",
    t: REZ_CONTRACT_TYPES.SESSION_HELLO,
    type: REZ_CONTRACT_TYPES.SESSION_HELLO,
    v: CONTRACT_VERSION,
    body: {
      contractVersion: CONTRACT_VERSION,
      clientName: "hello-rl-test",
      clientVersion: "1.0",
      deviceId,
      accountIdentityPublicKeyB64: claimant.accountIdentityPublicKeyB64,
    },
  }));
}

async function openWs(server) {
  const address = server.address();
  const ws = new WebSocket("ws://127.0.0.1:" + address.port + "/ws");
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  return ws;
}

// Loopback peer IP — every test in this file shares the same bucket. We
// drain it at setup and clean up after each test to keep the state
// scoped. The limiter is process-wide so we have to be a good citizen.
const LOOPBACK_PEER_IPS = ["::ffff:127.0.0.1", "127.0.0.1", "::1"];

function resetLoopbackBucket() {
  for (const ip of LOOPBACK_PEER_IPS) {
    SESSION_HELLO_RATE_LIMITER.forget(ip);
  }
}

test("session.hello LOW: legitimate hello under the cap is accepted", async (t) => {
  resetLoopbackBucket();
  t.after(resetLoopbackBucket);
  const { server } = await startNode(t);
  const claimant = createSessionIdentity();

  const ws = await openWs(server);
  t.after(() => ws.close());
  sendSessionHello(ws, claimant);
  const challenge = await waitForMessage(ws, (m) => m.id === "hello" && m.t === REZ_CONTRACT_TYPES.SESSION_CHALLENGE);
  assert.ok(challenge.body.challengeId, "first session.hello got a challenge");
});

test("session.hello LOW: per-IP cap throttles a flood from the same source", async (t) => {
  resetLoopbackBucket();
  t.after(resetLoopbackBucket);
  const { server } = await startNode(t);
  const claimant = createSessionIdentity();

  // Pre-fill the limiter directly up to the cap. This is faster and
  // deterministic versus opening 60 WS connections and racing on the
  // response. The point of the e2e portion below is to verify the WS
  // path consults the limiter and emits RATE_LIMITED — once the bucket
  // is full, the very next hello should be rejected.
  const now = Date.now();
  for (let i = 0; i < SESSION_HELLO_RATE_LIMITER.maxAttempts; i += 1) {
    SESSION_HELLO_RATE_LIMITER.record("::ffff:127.0.0.1", now);
    SESSION_HELLO_RATE_LIMITER.record("127.0.0.1", now);
    SESSION_HELLO_RATE_LIMITER.record("::1", now);
  }

  const ws = await openWs(server);
  t.after(() => ws.close());
  sendSessionHello(ws, claimant);

  const errFrame = await waitForMessage(ws, (m) => m.t === REZ_CONTRACT_TYPES.ERROR || m.type === REZ_CONTRACT_TYPES.ERROR);
  assert.equal(errFrame.body.code, "RATE_LIMITED", "hello past the cap returns RATE_LIMITED");
  assert.equal(errFrame.body.detail && errFrame.body.detail.retryable, true, "rate-limit errors are retryable");
});

test("session.hello LOW: sliding window restores budget", () => {
  // Unit-level: confirm the limiter's sliding-window contract for the
  // session.hello key. (Generic sliding-window behavior is covered in
  // dht.store.ratelimit.low6.test.js; this asserts session.hello
  // specifically resets within the natural window.)
  resetLoopbackBucket();
  const t0 = 10_000;
  // Fill up to the cap at t0.
  for (let i = 0; i < SESSION_HELLO_RATE_LIMITER.maxAttempts; i += 1) {
    SESSION_HELLO_RATE_LIMITER.record("203.0.113.42", t0);
  }
  assert.equal(SESSION_HELLO_RATE_LIMITER.record("203.0.113.42", t0), false, "at cap, next attempt rejected");
  // Slide past the window.
  const tPast = t0 + SESSION_HELLO_RATE_LIMITER.windowMs + 1;
  assert.equal(SESSION_HELLO_RATE_LIMITER.record("203.0.113.42", tPast), true, "after window slides, budget restored");
  SESSION_HELLO_RATE_LIMITER.forget("203.0.113.42");
});
