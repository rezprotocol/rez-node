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
  bytesToBase64,
  base64ToBytes,
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
  createNodeTestIdentity,
  createSessionIdentity,
} from "./helpers/wsAuth.js";

/**
 * Regression tests for docs/SECURITY_AUDIT.md CRITICAL-2.
 *
 * The session-auth signature used to omit the node identity, making it
 * portable: a MITM could relay a victim's signature from node A to node B.
 * Fix: server signs the challenge with its node identity, and the SDK signs
 * an auth payload that binds (nodeKeyId, nodePublicKeyB64). A signature
 * produced for one node MUST NOT verify against another node.
 */

const CRYPTO = new NodeCryptoProvider();

function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

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
    accountId: "rez:node:crossnode-test:" + randomBytes(4).toString("hex"),
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
  return { server, identity };
}

async function helloAndReceiveChallenge({ server, claimant, deviceId = "dev:test" }) {
  const address = server.address();
  const ws = new WebSocket("ws://127.0.0.1:" + address.port + "/ws");
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  ws.send(JSON.stringify({
    id: "hello",
    t: REZ_CONTRACT_TYPES.SESSION_HELLO,
    type: REZ_CONTRACT_TYPES.SESSION_HELLO,
    v: CONTRACT_VERSION,
    body: {
      contractVersion: CONTRACT_VERSION,
      clientName: "crossnode-test",
      clientVersion: "1.0",
      deviceId,
      accountIdentityPublicKeyB64: claimant.accountIdentityPublicKeyB64,
    },
  }));
  const challengeFrame = await waitForMessage(
    ws,
    (msg) => msg.id === "hello" && msg.t === REZ_CONTRACT_TYPES.SESSION_CHALLENGE,
  );
  return { ws, challenge: challengeFrame.body };
}

function buildAuthSignatureForChallenge({ challenge, claimant, deviceId, overrideNode = null }) {
  // Build the canonical signed-auth payload the way the SDK does. If
  // overrideNode is set, substitute its (nodeKeyId, nodePublicKeyB64,
  // relayKeyId) — that models a cross-node attempt by an adversary.
  const nodeKeyId = overrideNode ? overrideNode.nodeKeyId : challenge.nodeKeyId;
  const nodePublicKeyB64 = overrideNode ? overrideNode.nodePublicKeyB64 : challenge.nodePublicKeyB64;
  const relayKeyId = overrideNode
    ? (overrideNode.relayKeyId || ("node-" + (overrideNode.deviceId || "")))
    : challenge.relayKeyId;
  const payload = {
    kind: "session-auth",
    challengeId: challenge.challengeId,
    nonceB64: challenge.nonceB64,
    nodeKeyId,
    nodePublicKeyB64,
    relayKeyId,
    publicKeyB64: claimant.accountIdentityPublicKeyB64,
    deviceId,
    wsPath: challenge.wsPath,
  };
  const sig = CRYPTO.sign({
    privateKey: claimant.privateKey,
    msg: signedPayloadBytes(payload),
  });
  return bytesToBase64(sig);
}

test("CRITICAL-2: challenge carries a node-identity signature the SDK can verify", async (t) => {
  const { server, identity: nodeIdentity } = await startNode(t);
  const claimant = createSessionIdentity();
  const deviceId = "dev:test";

  const { ws, challenge } = await helloAndReceiveChallenge({ server, claimant, deviceId });
  t.after(() => ws.close());

  assert.ok(challenge.signatureB64, "challenge includes signatureB64");
  assert.equal(challenge.nodeKeyId, nodeIdentity.nodeKeyId);
  assert.equal(challenge.nodePublicKeyB64, nodeIdentity.nodePublicKeyB64);
  assert.equal(typeof challenge.relayKeyId, "string");
  assert.ok(challenge.relayKeyId.length > 0, "challenge includes relayKeyId");

  const verified = await CRYPTO.verify({
    publicKey: base64ToBytes(challenge.nodePublicKeyB64),
    msg: signedPayloadBytes({
      kind: "session-challenge",
      challengeId: challenge.challengeId,
      nonceB64: challenge.nonceB64,
      issuedAtMs: challenge.issuedAtMs,
      expiresAtMs: challenge.expiresAtMs,
      nodeKeyId: challenge.nodeKeyId,
      nodePublicKeyB64: challenge.nodePublicKeyB64,
      relayKeyId: challenge.relayKeyId,
      accountIdentityPublicKeyB64: claimant.accountIdentityPublicKeyB64,
      sessionDeviceId: deviceId,
      wsPath: challenge.wsPath,
    }),
    sig: base64ToBytes(challenge.signatureB64),
  });
  assert.equal(verified, true, "challenge signature verifies against the node's pubkey");
});

test("CRITICAL-2: legitimate session-auth signed with node binding is accepted", async (t) => {
  const { server } = await startNode(t);
  const claimant = createSessionIdentity();
  const deviceId = "dev:test";

  const { ws, challenge } = await helloAndReceiveChallenge({ server, claimant, deviceId });
  t.after(() => ws.close());

  const signatureB64 = buildAuthSignatureForChallenge({ challenge, claimant, deviceId });
  ws.send(JSON.stringify({
    id: "hello",
    type: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    t: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    v: CONTRACT_VERSION,
    body: { challengeId: challenge.challengeId, signatureB64 },
  }));
  const ready = await waitForMessage(
    ws,
    (msg) => msg.id === "hello"
      && (msg.t === REZ_CONTRACT_TYPES.SESSION_READY || msg.t === REZ_CONTRACT_TYPES.ERROR),
  );
  assert.equal(ready.t, REZ_CONTRACT_TYPES.SESSION_READY, "legit auth accepted");
});

test("CRITICAL-2: signature bound to node A is rejected when relayed at node B", async (t) => {
  // Two independent nodes. Victim claimant. Attacker captures the victim's
  // signature against node A and tries to authenticate at node B with it.
  const nodeA = await startNode(t);
  const nodeB = await startNode(t);
  const claimant = createSessionIdentity();
  const deviceId = "dev:test";

  // Phase 1: victim opens a session against node A and produces a signature.
  const sessionA = await helloAndReceiveChallenge({ server: nodeA.server, claimant, deviceId });
  t.after(() => sessionA.ws.close());
  const signatureAgainstA = buildAuthSignatureForChallenge({ challenge: sessionA.challenge, claimant, deviceId });

  // Phase 2: attacker opens a session against node B with the same claimant
  // pubkey (think of it as the attacker proxying the claimant's hello) and
  // tries to reuse the signature.
  const sessionB = await helloAndReceiveChallenge({ server: nodeB.server, claimant, deviceId });
  t.after(() => sessionB.ws.close());

  // Each node issues a different challengeId; relaying signatureAgainstA's
  // bytes verbatim is trivially rejected on challengeId mismatch. The real
  // attack is when the attacker repackages with B's challengeId. So instead,
  // sign a payload that uses B's challengeId/nonce but claims A's node
  // identity — which is what a MITM forwarder would let the client produce.
  const crossNodeSig = buildAuthSignatureForChallenge({
    challenge: sessionB.challenge,
    claimant,
    deviceId,
    overrideNode: { nodeKeyId: nodeA.identity.nodeKeyId, nodePublicKeyB64: nodeA.identity.nodePublicKeyB64 },
  });
  sessionB.ws.send(JSON.stringify({
    id: "hello",
    type: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    t: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    v: CONTRACT_VERSION,
    body: { challengeId: sessionB.challenge.challengeId, signatureB64: crossNodeSig },
  }));
  const result = await waitForMessage(
    sessionB.ws,
    (msg) => msg.id === "hello"
      && (msg.t === REZ_CONTRACT_TYPES.SESSION_READY || msg.t === REZ_CONTRACT_TYPES.ERROR),
  );
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR, "cross-node-bound signature rejected at node B");
  assert.equal(result.body.code, "UNAUTHORIZED");

  // Also: the raw signature from A's challenge, used unchanged at B, fails on
  // challengeId mismatch — earlier reject path. Either way, the gap is closed.
  // Keep the assertion above as the load-bearing one; consume the helper so
  // the lint doesn't complain.
  void signatureAgainstA;
});

test("CRITICAL-2: server-side signed payload also rejects mismatched nodePublicKeyB64", async (t) => {
  // Even within the same connection, if the attacker can intercept and
  // rewrite the nodePublicKeyB64 in the signed payload to a different value
  // (impossible without the claimant's privkey, but defense-in-depth check):
  // the server-side verification fails because the nodePublicKeyB64 in the
  // signed payload doesn't match the pending challenge's nodePublicKeyB64.
  const { server } = await startNode(t);
  const claimant = createSessionIdentity();
  const deviceId = "dev:test";

  const { ws, challenge } = await helloAndReceiveChallenge({ server, claimant, deviceId });
  t.after(() => ws.close());

  // Build an unrelated node identity to bind into the signed payload.
  const otherNode = createNodeTestIdentity();
  const signatureB64 = buildAuthSignatureForChallenge({
    challenge,
    claimant,
    deviceId,
    overrideNode: { nodeKeyId: otherNode.nodeKeyId, nodePublicKeyB64: otherNode.nodePublicKeyB64 },
  });
  ws.send(JSON.stringify({
    id: "hello",
    type: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    t: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    v: CONTRACT_VERSION,
    body: { challengeId: challenge.challengeId, signatureB64 },
  }));
  const result = await waitForMessage(
    ws,
    (msg) => msg.id === "hello"
      && (msg.t === REZ_CONTRACT_TYPES.SESSION_READY || msg.t === REZ_CONTRACT_TYPES.ERROR),
  );
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR, "signature over wrong-node payload rejected");
  assert.equal(result.body.code, "UNAUTHORIZED");
});
