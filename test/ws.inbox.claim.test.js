import test from "node:test";
import assert from "node:assert/strict";
import { WebSocket } from "ws";
import {
  RMailbox,
  MemoryDataStore,
  MemoryStorageProvider,
  createDefaultRegistry,
  CONTRACT_VERSION,
  REZ_CONTRACT_TYPES,
  bytesToBase64,
  canonicalJSONStringify,
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
  authenticateSession,
  createNodeTestIdentity,
  createClaimantNodeDelegation,
} from "./helpers/wsAuth.js";

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

function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

function signClaim({ inboxId, claimantPublicKeyB64, claimedAtMs, privateKey }) {
  const sig = CRYPTO.sign({
    privateKey,
    msg: signedPayloadBytes({ inboxId, claimantPublicKeyB64, claimedAtMs }),
  });
  return bytesToBase64(sig);
}

function freshClaimantIdentity() {
  const keyPair = CRYPTO.generateSigningKeyPair();
  return {
    publicKey: keyPair.publicKey,
    privateKey: keyPair.privateKey,
    accountIdentityPublicKeyB64: bytesToBase64(keyPair.publicKey),
    accountIdentityPrivateKeyB64: bytesToBase64(keyPair.privateKey),
  };
}

function freshInboxId() {
  return "inbox:" + Buffer.from(CRYPTO.randomBytes(12)).toString("hex");
}

async function startNodeForClaimTests() {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:claim-test",
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
    onInboundDeposit: createDepositHandler({ crypto: new NodeCryptoProvider() }),
  });
  await server.start();
  return { server, runtime, inboxClaimRegistry, nodeIdentity: identity };
}

async function openAuthenticatedSocket(t, server, claimantIdentity) {
  const address = server.address();
  const ws = new WebSocket("ws://127.0.0.1:" + address.port + "/ws");
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  t.after(() => ws.close());
  await authenticateSession({
    ws,
    waitForMessage,
    id: "hello",
    deviceId: "dev:claim-test",
    identity: claimantIdentity,
  });
  return ws;
}

function buildClaimBody({ claimantIdentity, inboxId, claimedAtMs, nodeIdentity }) {
  const claimantPublicKeyB64 = claimantIdentity.accountIdentityPublicKeyB64;
  const signatureB64 = signClaim({
    inboxId,
    claimantPublicKeyB64,
    claimedAtMs,
    privateKey: claimantIdentity.privateKey,
  });
  const delegation = createClaimantNodeDelegation({
    claimantIdentity,
    inboxId,
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    relayKeyId: nodeIdentity.relayKeyId,
  });
  return {
    inboxId,
    claimantPublicKeyB64,
    claimedAtMs,
    signatureB64,
    nodeDelegation: {
      nodeKeyId: delegation.nodeKeyId,
      nodePublicKeyB64: delegation.nodePublicKeyB64,
      relayKeyId: delegation.relayKeyId,
      issuedAtMs: delegation.issuedAtMs,
      expiresAtMs: delegation.expiresAtMs,
      delegationSigB64: delegation.delegationSigB64,
    },
  };
}

test("inbox.claim succeeds with a valid signature, persists in the registry", async (t) => {
  let server;
  let inboxClaimRegistry;
  let nodeIdentity;
  try {
    ({ server, inboxClaimRegistry, nodeIdentity } = await startNodeForClaimTests());
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err && err.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await server.stop(); });

  const claimantIdentity = freshClaimantIdentity();
  const ws = await openAuthenticatedSocket(t, server, claimantIdentity);

  const inboxId = freshInboxId();
  const claimedAtMs = Date.now();

  ws.send(JSON.stringify({
    id: "claim-1",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: buildClaimBody({ claimantIdentity, inboxId, claimedAtMs, nodeIdentity }),
  }));

  const frame = await waitForMessage(ws, (msg) => msg.id === "claim-1");
  assert.equal(frame.t, REZ_CONTRACT_TYPES.INBOX_CLAIM_RES, "expected inbox.claim.res");
  assert.equal(frame.body.inboxId, inboxId);
  assert.equal(frame.body.claimedAtMs, claimedAtMs);

  assert.equal(inboxClaimRegistry.hasInbox(inboxId), true);
  assert.equal(inboxClaimRegistry.getClaimantPublicKey(inboxId), claimantIdentity.accountIdentityPublicKeyB64);
});

test("inbox.claim rejects a tampered claim signature", async (t) => {
  let server;
  let nodeIdentity;
  try {
    ({ server, nodeIdentity } = await startNodeForClaimTests());
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err && err.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await server.stop(); });

  const claimantIdentity = freshClaimantIdentity();
  const ws = await openAuthenticatedSocket(t, server, claimantIdentity);

  const inboxId = freshInboxId();
  const claimedAtMs = Date.now();

  const body = buildClaimBody({ claimantIdentity, inboxId, claimedAtMs, nodeIdentity });
  // Sign over a DIFFERENT inboxId than the one we send — claim sig should fail.
  body.signatureB64 = signClaim({
    inboxId: freshInboxId(),
    claimantPublicKeyB64: claimantIdentity.accountIdentityPublicKeyB64,
    claimedAtMs,
    privateKey: claimantIdentity.privateKey,
  });

  ws.send(JSON.stringify({
    id: "claim-bad",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body,
  }));

  const frame = await waitForMessage(ws, (msg) => msg.id === "claim-bad");
  assert.equal(frame.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(frame.body.code, "INVALID_SIGNATURE");
});

test("inbox.claim rejects a missing node-delegation", async (t) => {
  let server;
  let nodeIdentity;
  try {
    ({ server, nodeIdentity } = await startNodeForClaimTests());
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err && err.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await server.stop(); });

  const claimantIdentity = freshClaimantIdentity();
  const ws = await openAuthenticatedSocket(t, server, claimantIdentity);

  const inboxId = freshInboxId();
  const claimedAtMs = Date.now();
  const body = buildClaimBody({ claimantIdentity, inboxId, claimedAtMs, nodeIdentity });
  delete body.nodeDelegation;

  ws.send(JSON.stringify({
    id: "claim-nodel",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body,
  }));

  const frame = await waitForMessage(ws, (msg) => msg.id === "claim-nodel");
  assert.equal(frame.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(frame.body.code, "INVALID_SIGNATURE");
});

test("inbox.claim rejects a duplicate claim with INBOX_ALREADY_CLAIMED", async (t) => {
  let server;
  let inboxClaimRegistry;
  let nodeIdentity;
  try {
    ({ server, inboxClaimRegistry, nodeIdentity } = await startNodeForClaimTests());
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err && err.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await server.stop(); });

  const inboxId = freshInboxId();

  // First claim: legit owner authenticates with their key.
  const owner = freshClaimantIdentity();
  const ws1 = await openAuthenticatedSocket(t, server, owner);
  const ts1 = Date.now();
  ws1.send(JSON.stringify({
    id: "claim-first",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: ts1, nodeIdentity }),
  }));
  const first = await waitForMessage(ws1, (msg) => msg.id === "claim-first");
  assert.equal(first.t, REZ_CONTRACT_TYPES.INBOX_CLAIM_RES);

  // Second claim: attacker authenticates with their key and tries to take over.
  const attacker = freshClaimantIdentity();
  const ws2 = await openAuthenticatedSocket(t, server, attacker);
  const ts2 = ts1 + 1000;
  ws2.send(JSON.stringify({
    id: "claim-collide",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: buildClaimBody({ claimantIdentity: attacker, inboxId, claimedAtMs: ts2, nodeIdentity }),
  }));
  const second = await waitForMessage(ws2, (msg) => msg.id === "claim-collide");
  assert.equal(second.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(second.body.code, "INBOX_ALREADY_CLAIMED");

  assert.equal(
    inboxClaimRegistry.getClaimantPublicKey(inboxId),
    owner.accountIdentityPublicKeyB64,
  );
});

test("inbox.claim is idempotent for the same claimant pubkey — re-claim binds to the new session", async (t) => {
  let server;
  let inboxClaimRegistry;
  let nodeIdentity;
  try {
    ({ server, inboxClaimRegistry, nodeIdentity } = await startNodeForClaimTests());
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err && err.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await server.stop(); });

  const claimantIdentity = freshClaimantIdentity();
  const inboxId = freshInboxId();

  // First session.
  const ws1 = await openAuthenticatedSocket(t, server, claimantIdentity);
  const ts1 = Date.now();
  ws1.send(JSON.stringify({
    id: "claim-1",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: buildClaimBody({ claimantIdentity, inboxId, claimedAtMs: ts1, nodeIdentity }),
  }));
  const first = await waitForMessage(ws1, (msg) => msg.id === "claim-1");
  assert.equal(first.t, REZ_CONTRACT_TYPES.INBOX_CLAIM_RES);

  // Second session, same identity, re-claims the same inbox.
  const ws2 = await openAuthenticatedSocket(t, server, claimantIdentity);
  const ts2 = ts1 + 5000;
  ws2.send(JSON.stringify({
    id: "claim-rebind",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: buildClaimBody({ claimantIdentity, inboxId, claimedAtMs: ts2, nodeIdentity }),
  }));
  const second = await waitForMessage(ws2, (msg) => msg.id === "claim-rebind");
  assert.equal(second.t, REZ_CONTRACT_TYPES.INBOX_CLAIM_RES, "re-claim by same pubkey should succeed");
  assert.equal(second.body.inboxId, inboxId);

  assert.equal(
    inboxClaimRegistry.getClaimantPublicKey(inboxId),
    claimantIdentity.accountIdentityPublicKeyB64,
  );
});

test("inbox.claim rejects malformed bodies with BAD_REQUEST", async (t) => {
  let server;
  try {
    ({ server } = await startNodeForClaimTests());
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err && err.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await server.stop(); });

  const claimantIdentity = freshClaimantIdentity();
  const ws = await openAuthenticatedSocket(t, server, claimantIdentity);

  // Empty inboxId
  ws.send(JSON.stringify({
    id: "claim-bad-1",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: { inboxId: "", claimantPublicKeyB64: "AAAA", claimedAtMs: 1, signatureB64: "BBBB" },
  }));
  const f1 = await waitForMessage(ws, (msg) => msg.id === "claim-bad-1");
  assert.equal(f1.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(f1.body.code, "BAD_REQUEST");

  // Missing claimedAtMs
  ws.send(JSON.stringify({
    id: "claim-bad-2",
    type: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    t: REZ_CONTRACT_TYPES.INBOX_CLAIM,
    v: CONTRACT_VERSION,
    body: { inboxId: "inbox:x", claimantPublicKeyB64: "AAAA", claimedAtMs: 0, signatureB64: "BBBB" },
  }));
  const f2 = await waitForMessage(ws, (msg) => msg.id === "claim-bad-2");
  assert.equal(f2.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(f2.body.code, "BAD_REQUEST");
});
