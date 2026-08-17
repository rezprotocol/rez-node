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

async function startNode(t, {
  accountAuthorityRevocationCache = null,
  // L5 review-4 finding 1 + P1: delegated auth requires exactly ONE authority source — the coherent
  // resolver (accountAuthorityRevocationCache), whose snapshot carries BOTH revocation dimensions
  // (the revoked-cert state AND the terminal device status, the latter resolved through the
  // serializer's own canonical registry). Tests that need delegated auth to SUCCEED supply a clean
  // resolver; tests that assert rejection supply a revoked cert via that resolver. The runtime
  // registry below is NO LONGER consulted by the delegated-auth gate (terminal lives inside the
  // snapshot); it is retained only as inert runtime wiring.
  accountDeviceRegistry = {
    async isTerminallyRevoked() { return false; },
    async isTerminallyRevokedInTx() { return false; },
  },
} = {}) {
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
    accountDeviceRegistry,
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
  // A real authority home can resolve revocation (clean here — nothing revoked). Round-6
  // finding 3: a node that can resolve NEITHER the revoked-cert state NOR the terminal device
  // status fails delegated auth closed, so this test supplies a (clean) revocation cache.
  const { server } = await startNode(t, {
    accountAuthorityRevocationCache: {
      async currentEpoch() { return 0; },
      async resolveDelegatedSnapshot() { return { state: null, epoch: 0, terminal: false }; },
    },
  });
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

test("L5 review-4 finding P1: an INCOMPLETE authority snapshot at admission → SERVICE_UNAVAILABLE, NOT admitted (no fail-open)", async (t) => {
  // The runtime resolver is a public injection point. A snapshot missing `terminal` must NOT coerce
  // to "not terminal" and admit an otherwise-valid delegated chain — it is an AVAILABILITY failure.
  const { server } = await startNode(t, {
    accountAuthorityRevocationCache: {
      async currentEpoch() { return 1; },
      async resolveDelegatedSnapshot() { return { state: null, epoch: 1 }; }, // no `terminal`
    },
  });
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
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR, "not admitted — no SESSION_READY on a partial snapshot");
  assert.equal(result.body.code, "SERVICE_UNAVAILABLE", "availability failure, never a false authorize");
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
    async getCurrentEpoch() { return 1; },
    async getDelegatedAuthoritySnapshot() {
      return { epoch: 1, revokedCertIds: [cert.certId], minValidIssuedAtMs: 0, terminal: false };
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
    async getCurrentEpoch() { return 1; },
    async getDelegatedAuthoritySnapshot() {
      return { epoch: 1, revokedCertIds: ["rez:cap:some-other-cert"], minValidIssuedAtMs: 0, terminal: false };
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

// ---------------------------------------------------------------------------
// rez-node#2 — a home that CANNOT do delegated devices must say so
// ---------------------------------------------------------------------------
// An fs/desktop home wires no authority resolver, so it can never admit a
// delegated device. It used to refuse with the generic UNAUTHORIZED; the client
// then failed every uplink and reported `UNREACHABLE` — a retryable,
// network-shaped code for a node that was running and answering. Testers went
// to debug their connection.
//
// Precedence matters as much as the code: a bad chain is a credential failure
// wherever it is presented, so only a caller whose chain would otherwise have
// verified learns the home's posture.

test("rez-node#2: a valid chain against a home with no authority resolver → DELEGATED_DEVICES_UNSUPPORTED", async (t) => {
  const { server } = await startNode(t); // no accountAuthorityRevocationCache — the fs/desktop shape
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
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "DELEGATED_DEVICES_UNSUPPORTED",
    "UNAUTHORIZED here reads as 'your credentials are wrong' for a home that would refuse anyone");
  // Retryability rides in detail, not at the body root (WsErrorDetail).
  assert.equal(result.body.detail.retryable, false,
    "retrying cannot make a filesystem-backed home multi-device");
  assert.match(result.body.message, /single-device/i);
  assert.match(result.body.message, /Postgres/i, "the message must say what would fix it");
});

test("rez-node#2: the session is still REFUSED — a clearer error is not a softer one", async (t) => {
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

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.notEqual(result.t, REZ_CONTRACT_TYPES.SESSION_READY,
    "a home with no authority resolver must never admit a delegated device");
});

test("rez-node#2: a FORGED chain gets UNAUTHORIZED, not the capability hint", async (t) => {
  // Precedence check. Answering a forged cert with "this home is single-device"
  // would be inaccurate and would hand an unauthenticated caller a free read of
  // the home's posture.
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
  tampered.sig = { alg: "ed25519", sigB64: bytesToBase64(new Uint8Array(64)) };

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [tampered] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "UNAUTHORIZED",
    "a bad credential is a bad credential wherever it is presented");
});

test("rez-node#2: an EXPIRED chain gets UNAUTHORIZED, not the capability hint", async (t) => {
  const { server } = await startNode(t);
  const { B, C, accountPubB64, devicePubB64, deviceId, now } = makeAccountAndDevice();
  const cert = buildLeafCert({
    accountPubB64,
    signerKeyPair: { publicKey: B.publicKey, privateKey: B.privateKey },
    granteePubB64: devicePubB64,
    capabilities: ["peerLink.create"],
    issuedAtMs: now - 7_200_000,
    expiresAtMs: now - 3_600_000, // lapsed an hour ago
  });

  const { ws, challenge } = await helloAndReceiveChallenge({ server, accountPubB64, deviceId });
  t.after(() => ws.close());
  const signatureB64 = signAuthWith({ challenge, accountPubB64, deviceId, signerKeyPair: C });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64, signerPublicKeyB64: devicePubB64, certChain: [cert.toJSON()] });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.ERROR);
  assert.equal(result.body.code, "UNAUTHORIZED");
});

test("rez-node#2: a PRIMARY device is unaffected — no chain, no capability gate", async (t) => {
  // The direct (B-sign) path is what every desktop install actually uses. It must
  // stay working on exactly the home shape that refuses delegated devices.
  const { server } = await startNode(t);
  const { B, accountPubB64, deviceId } = makeAccountAndDevice();

  const { ws, challenge } = await helloAndReceiveChallenge({
    server, accountPubB64, deviceId: DeviceRegistrationV1.deviceIdFor(accountPubB64),
  });
  t.after(() => ws.close());
  const primaryDeviceId = DeviceRegistrationV1.deviceIdFor(accountPubB64);
  const signatureB64 = signAuthWith({
    challenge, accountPubB64, deviceId: primaryDeviceId, signerKeyPair: B,
  });
  sendAuthenticate(ws, { challengeId: challenge.challengeId, signatureB64 });

  const result = await awaitAuthResult(ws);
  assert.equal(result.t, REZ_CONTRACT_TYPES.SESSION_READY,
    "single-device homes must still serve their one device");
});
