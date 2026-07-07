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
  canonicalJSONStringify,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  DeviceRegistrationV1,
} from "@rezprotocol/core";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { AccountAuthorityRevocationCache } from "../src/protocol/AccountAuthorityRevocationCache.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import {
  createServerServices,
  createPerAccountServices,
  createProtocolFactory,
  createDepositHandler,
} from "./helpers/nodeTestServices.js";
import { createNodeTestIdentity, createSessionIdentity } from "./helpers/wsAuth.js";

// S2.5 S7 / audit F1: cert-backed C session authentication. A DELEGATED device
// holds only its per-device key C plus a capability chain C←…←B; it cannot sign
// with the account root key (B-sign). It authenticates by signing the session-
// auth payload with C and presenting the chain, which the node anchors to the
// CLAIMED account via verifyAccountAuthority. Real Ed25519, through the live
// _handleSessionAuthenticate path. The PRIMARY (B-sign) path stays unchanged —
// proven by ws.session.auth.crossnode.test.js.

const CRYPTO = new NodeCryptoProvider();

function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

// Build a signed AccountDeviceCapabilityV1 (B is the anchor + the root signer).
function buildLeafCert({ accountPubB64, signerKeyPair, granteePubB64, capabilities, maxDelegationDepth = 0, issuedAtMs, expiresAtMs }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: accountPubB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePubB64),
    capabilities,
    maxDelegationDepth,
    issuedAtMs,
    expiresAtMs,
    signerPublicKeyB64: bytesToBase64(signerKeyPair.publicKey),
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = CRYPTO.sign({ privateKey: signerKeyPair.privateKey, msg: AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }) });
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig: { alg: "ed25519", sigB64: bytesToBase64(sig) } });
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

async function startNode(t, { accountAuthorityRevocationCache = null } = {}) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:delegated-test:" + randomBytes(4).toString("hex"),
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
    accountAuthorityRevocationCache,
    serverServices: createServerServices({ storageProvider, clock: () => Date.now(), ownerAccountId: identity.accountId }),
    serviceCache: new PerAccountServiceCache({ storageProvider, clock: () => Date.now(), createServices: createPerAccountServices }),
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

async function helloAndReceiveChallenge({ server, accountPubB64, deviceId }) {
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
      clientName: "delegated-test",
      clientVersion: "1.0",
      deviceId,
      accountIdentityPublicKeyB64: accountPubB64,
    },
  }));
  const challengeFrame = await waitForMessage(ws, (msg) => msg.id === "hello" && msg.t === REZ_CONTRACT_TYPES.SESSION_CHALLENGE);
  return { ws, challenge: challengeFrame.body };
}

// Sign the session-auth payload exactly as the SDK does — but with the delegated
// device key (signerKeyPair), not the account root.
function signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair }) {
  const payload = {
    kind: "session-auth",
    challengeId: challenge.challengeId,
    nonceB64: challenge.nonceB64,
    nodeKeyId: challenge.nodeKeyId,
    nodePublicKeyB64: challenge.nodePublicKeyB64,
    relayKeyId: challenge.relayKeyId,
    publicKeyB64: accountPubB64,
    deviceId,
    wsPath: challenge.wsPath,
  };
  return bytesToBase64(CRYPTO.sign({ privateKey: signerKeyPair.privateKey, msg: signedPayloadBytes(payload) }));
}

function sendAuthenticate(ws, body) {
  ws.send(JSON.stringify({
    id: "hello",
    type: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    t: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    v: CONTRACT_VERSION,
    body,
  }));
}

function awaitAuthResult(ws) {
  return waitForMessage(
    ws,
    (msg) => msg.id === "hello" && (msg.t === REZ_CONTRACT_TYPES.SESSION_READY || msg.t === REZ_CONTRACT_TYPES.ERROR),
  );
}

// B = account root (B-sign); C = delegated device key.
function makeAccountAndDevice() {
  const B = createSessionIdentity(); // has publicKey/privateKey bytes + b64
  const C = CRYPTO.generateSigningKeyPair();
  const accountPubB64 = B.accountIdentityPublicKeyB64;
  const devicePubB64 = bytesToBase64(C.publicKey);
  const deviceId = DeviceRegistrationV1.deviceIdFor(devicePubB64);
  const now = Date.now();
  return { B, C, accountPubB64, devicePubB64, deviceId, now };
}

test("delegated device authenticates by signing with C + presenting a B→C capability chain", async (t) => {
  const { server } = await startNode(t);
  const { B, C, accountPubB64, devicePubB64, deviceId, now } = makeAccountAndDevice();
  const cert = buildLeafCert({
    accountPubB64,
    signerKeyPair: { publicKey: B.publicKey, privateKey: B.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create", "deviceSet.publish"],
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
  });

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.SESSION_READY, "delegated auth accepted");
});

test("delegated rejected: C signs but presents NO cert chain (direct mode verifies against B)", async (t) => {
  const { server } = await startNode(t);
  const { C, accountPubB64, deviceId } = makeAccountAndDevice();

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  // No certChain/signerPublicKeyB64 ⇒ direct mode, verified against B — a C
  // signature can never verify against B.
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64 });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "UNAUTHORIZED");
});

test("delegated rejected: the cert chain anchors to a DIFFERENT account than the hello claims", async (t) => {
  const { server } = await startNode(t);
  const { C, accountPubB64, devicePubB64, deviceId, now } = makeAccountAndDevice();
  // Cert is internally self-consistent but for a DIFFERENT account (BOther).
  const BOther = createSessionIdentity();
  const cert = buildLeafCert({
    accountPubB64: BOther.accountIdentityPublicKeyB64,
    signerKeyPair: { publicKey: BOther.publicKey, privateKey: BOther.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create"],
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
  });

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "UNAUTHORIZED");
});

test("delegated rejected: the claimed session deviceId is not C's self-certifying id", async (t) => {
  const { server } = await startNode(t);
  const { B, C, accountPubB64, devicePubB64, now } = makeAccountAndDevice();
  const cert = buildLeafCert({
    accountPubB64,
    signerKeyPair: { publicKey: B.publicKey, privateKey: B.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create"],
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
  });

  const bogusDeviceId = "rez:dev:bogus-not-c";
  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId: bogusDeviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId: bogusDeviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "UNAUTHORIZED");
});

test("delegated rejected: the leaf cert is REVOKED in the home authority-state (S11)", async (t) => {
  const { B, C, accountPubB64, devicePubB64, deviceId, now } = makeAccountAndDevice();
  const cert = buildLeafCert({
    accountPubB64,
    signerKeyPair: { publicKey: B.publicKey, privateKey: B.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create", "deviceSet.publish"],
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
  });
  // A home whose authority-state has revoked exactly this leaf cert. The real
  // cache projects it to a non-null revocationState, which verifyAccountAuthority
  // consults to reject the chain.
  const serializer = {
    async getAuthorityState() {
      return { epoch: 1, revokedCertIds: [cert.certId], minValidIssuedAtMs: 0 };
    },
  };
  const accountAuthorityRevocationCache = new AccountAuthorityRevocationCache({ serializer });
  const { server } = await startNode(t, { accountAuthorityRevocationCache });

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR, "a revoked device can no longer authenticate");
  assert.equal(result.body.code, "UNAUTHORIZED");
});

test("delegated ACCEPTED: a DIFFERENT cert revoked leaves this chain valid (null-when-empty is precise)", async (t) => {
  const { B, C, accountPubB64, devicePubB64, deviceId, now } = makeAccountAndDevice();
  const cert = buildLeafCert({
    accountPubB64,
    signerKeyPair: { publicKey: B.publicKey, privateKey: B.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create", "deviceSet.publish"],
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
  });
  const serializer = {
    async getAuthorityState() {
      return { epoch: 1, revokedCertIds: ["rez:cap:some-other-cert"], minValidIssuedAtMs: 0 };
    },
  };
  const accountAuthorityRevocationCache = new AccountAuthorityRevocationCache({ serializer });
  const { server } = await startNode(t, { accountAuthorityRevocationCache });

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.SESSION_READY, "an unrevoked chain still authenticates");
});

test("delegated rejected: a tampered cert signature fails the chain", async (t) => {
  const { server } = await startNode(t);
  const { B, C, accountPubB64, devicePubB64, deviceId, now } = makeAccountAndDevice();
  const cert = buildLeafCert({
    accountPubB64,
    signerKeyPair: { publicKey: B.publicKey, privateKey: B.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create"],
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
  });
  const tampered = cert.toJSON();
  // Flip the signature to a valid-shaped but wrong value.
  tampered.sig = { alg: "ed25519", sigB64: bytesToBase64(new Uint8Array(64)) };

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [tampered] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "UNAUTHORIZED");
});
