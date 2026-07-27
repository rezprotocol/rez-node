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
  DeviceRegistrationV1,
  DEVICE_REGISTRATION_PURPOSE,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  AccountDeviceMutationV2,
  ACCOUNT_DEVICE_MUTATION_V2_PURPOSE,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
} from "@rezprotocol/core";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { AccountAuthorityRevocationCache } from "../src/protocol/AccountAuthorityRevocationCache.js";
import { DurableHomeInboxStore } from "../src/storage/DurableHomeInboxStore.js";
import { RevokedDeviceError } from "../src/storage/DurableInbox.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
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

// S2.5 S11 GATE — the serialized device-mutation authority proven END TO END over
// real WebSocket sockets against a REAL Postgres home. It ties together the three
// home-side leaves: L6 (the mutation/authority-state wire ops → serializer),
// L7 (device.bind enroll + account-wide revoke fail-close), and L8 (the live
// authority-cache consult that fails a revoked delegated cert at session-auth).
//
// The overlay-propagation half (republish at the new revision → peer re-ingests →
// a revoked device-set signer is rejected) is proven with REAL crypto at the
// rez-sdk (peer-link.device-set) + rez-chat (account-mutation.service) layers; this
// gate exercises the authority/home half on live Pg + sockets.
const PG_URL = process.env.REZ_PG_TEST_URL || "";
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();

function waitForMessage(ws, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error("Timed out waiting for WS message")); }, timeoutMs);
    function cleanup() { clearTimeout(timer); ws.off("message", onMessage); ws.off("error", onError); }
    function onError(err) { cleanup(); reject(err); }
    function onMessage(data) {
      let frame;
      try { frame = JSON.parse(data.toString("utf8")); } catch { return; }
      if (!predicate(frame)) return;
      cleanup();
      resolve(frame);
    }
    ws.on("message", onMessage);
    ws.on("error", onError);
  });
}

function signClaim({ inboxId, claimantPublicKeyB64, claimedAtMs, privateKey }) {
  return bytesToBase64(CRYPTO.sign({
    privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify({ inboxId, claimantPublicKeyB64, claimedAtMs })),
  }));
}

function freshOwner() {
  const kp = CRYPTO.generateSigningKeyPair();
  return {
    publicKey: kp.publicKey,
    privateKey: kp.privateKey,
    accountIdentityPublicKeyB64: bytesToBase64(kp.publicKey),
  };
}

function buildClaimBody({ owner, inboxId, claimedAtMs, nodeIdentity }) {
  const claimantPublicKeyB64 = owner.accountIdentityPublicKeyB64;
  const signatureB64 = signClaim({ inboxId, claimantPublicKeyB64, claimedAtMs, privateKey: owner.privateKey });
  const d = createClaimantNodeDelegation({
    claimantIdentity: { accountIdentityPublicKeyB64: claimantPublicKeyB64, privateKey: owner.privateKey, inboxId },
    inboxId,
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    relayKeyId: nodeIdentity.relayKeyId,
  });
  return {
    inboxId, claimantPublicKeyB64, claimedAtMs, signatureB64,
    nodeDelegation: {
      nodeKeyId: d.nodeKeyId, nodePublicKeyB64: d.nodePublicKeyB64, relayKeyId: d.relayKeyId,
      issuedAtMs: d.issuedAtMs, expiresAtMs: d.expiresAtMs, delegationSigB64: d.delegationSigB64,
    },
  };
}

async function startS11PgNode(conn) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({ accountId: "rez:node:s11-test", deviceId: "dev:node", localInboxId: "inbox:test" });
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider });
  await inboxClaimRegistry.hydrate();

  const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
  const accountDeviceRegistry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
  const accountMutationSerializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });
  const accountAuthorityRevocationCache = new AccountAuthorityRevocationCache({ serializer: accountMutationSerializer });
  const isHostedHere = (id) => inboxClaimRegistry.hasInbox(id);
  const inboxStore = new DurableHomeInboxStore({
    rmailbox: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    durableInbox,
    isHostedHere,
  });

  const runtime = {
    inboxStore,
    durableInbox,
    accountDeviceRegistry,
    accountMutationSerializer,
    accountAuthorityRevocationCache,
    isHostedHere,
    relayStore: null,
    metrics: null,
    inboxClaimRegistry,
    serverServices: createServerServices({ storageProvider, clock: () => Date.now(), ownerAccountId: identity.accountId }),
    serviceCache: new PerAccountServiceCache({ storageProvider, clock: () => Date.now(), createServices: createPerAccountServices }),
    getIdentity() { return { ...identity }; },
    getOwnerPublicKeysForInbox() { return new Set(); },
    getMeshStatus() { return { enabled: true, mode: "seeded-gossip", participateInRouting: true, peerCount: 0 }; },
    async stop() {},
  };
  const server = new WsGatewayServer({
    runtime, port: 0,
    protocolFactory: createProtocolFactory(),
    onInboundDeposit: createDepositHandler({ crypto: new NodeCryptoProvider() }),
  });
  await server.start();
  return { server, runtime, nodeIdentity: identity };
}

async function edSig(privateKey, msgBytes) {
  return { alg: "ed25519", sigB64: bytesToBase64(await CRYPTO.sign({ privateKey, msg: msgBytes })) };
}

async function buildDeviceProofs({ owner, inboxId }) {
  const devKp = CRYPTO.generateSigningKeyPair();
  const devicePublicKeyB64 = bytesToBase64(devKp.publicKey);
  const deviceId = DeviceRegistrationV1.deviceIdFor(devicePublicKeyB64);
  const now = Date.now();
  const regBody = {
    v: 1, purpose: DEVICE_REGISTRATION_PURPOSE,
    accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64, devicePublicKeyB64,
    deviceId, issuedAtMs: now - 1000, expiresAtMs: now + 3_600_000,
  };
  const registration = { ...regBody, sig: await edSig(owner.privateKey, DeviceRegistrationV1.signableBytes(regBody)) };
  const bindBody = {
    v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64, deviceId, inboxId, issuedAtMs: now - 1000, expiresAtMs: now + 3_600_000,
  };
  const binding = { ...bindBody, sig: await edSig(devKp.privateKey, DeviceInboxBindingV1.signableBytes(bindBody)) };
  return { devKp, devicePublicKeyB64, deviceId, registration, binding };
}

// A device-signed inbox binding for a NEW sibling (the device.add target).
async function buildSiblingBinding(inboxId) {
  const devKp = CRYPTO.generateSigningKeyPair();
  const devicePublicKeyB64 = bytesToBase64(devKp.publicKey);
  const deviceId = DeviceRegistrationV1.deviceIdFor(devicePublicKeyB64);
  const now = Date.now();
  const bindBody = {
    v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64, deviceId, inboxId, issuedAtMs: now - 1000, expiresAtMs: now + 3_600_000,
  };
  const binding = { ...bindBody, sig: await edSig(devKp.privateKey, DeviceInboxBindingV1.signableBytes(bindBody)) };
  return { deviceId, devicePublicKeyB64, inboxId, binding };
}

// An account-signed AccountDeviceMutationV2 (primary path — signer == owner B).
async function buildAccountMutation({ owner, opId, expectedRevision, action, target }) {
  const now = Date.now();
  const body = {
    v: 2, purpose: ACCOUNT_DEVICE_MUTATION_V2_PURPOSE,
    opId, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64,
    expectedRevision, action, target,
    signerPublicKeyB64: owner.accountIdentityPublicKeyB64,
    issuedAtMs: now - 1000, expiresAtMs: now + 300_000,
  };
  const sig = await edSig(owner.privateKey, AccountDeviceMutationV2.signableBytes(body));
  return { ...body, sig };
}

// A B-signed leaf capability cert granting a device key C.
function buildLeafCert({ owner, granteePubB64, capabilities }) {
  const now = Date.now();
  const fields = {
    v: 1, purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePubB64),
    capabilities,
    maxDelegationDepth: 0,
    issuedAtMs: now - 1000,
    expiresAtMs: now + 3_600_000,
    signerPublicKeyB64: owner.accountIdentityPublicKeyB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = CRYPTO.sign({ privateKey: owner.privateKey, msg: AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }) });
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig: { alg: "ed25519", sigB64: bytesToBase64(sig) } });
}

async function openAuthed(t, server, owner, deviceId) {
  const ws = new WebSocket("ws://127.0.0.1:" + server.address().port + "/ws");
  await new Promise((resolve, reject) => { ws.once("open", resolve); ws.once("error", reject); });
  t.after(() => ws.close());
  await authenticateSession({ ws, waitForMessage, id: "hello", deviceId, identity: owner });
  return ws;
}

function send(ws, id, type, body) {
  ws.send(JSON.stringify({ id, type, t: type, v: CONTRACT_VERSION, body }));
  return waitForMessage(ws, (m) => m.id === id);
}

const signedPayloadBytes = (payload) => new TextEncoder().encode(canonicalJSONStringify(payload));

test(
  "S11 gate (real Pg): device.add + device.revoke over the wire → serializer epoch + account-wide fail-close + revoked-cert session-auth reject",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_s11_gate";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();

    let node;
    try {
      node = await startS11PgNode(conn);
    } catch (err) {
      if (["EACCES", "EPERM"].includes(err && err.code)) { t.skip("WebSocket bind not permitted"); return; }
      throw err;
    }
    t.after(async () => { await node.server.stop(); });

    const owner = freshOwner();
    const inboxId = "inbox:" + Buffer.from(CRYPTO.randomBytes(10)).toString("hex");

    // The PRIMARY device P authenticates as its OWN self-cert deviceId (so
    // device.bind's session-device check passes), claims the inbox, then binds —
    // registering its cursor + enrolling it into the account registry (L7).
    const primary = await buildDeviceProofs({ owner, inboxId });
    const wsP = await openAuthed(t, node.server, owner, primary.deviceId);
    const claimRes = await send(wsP, "claim", T.INBOX_CLAIM, buildClaimBody({ owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: node.nodeIdentity }));
    assert.equal(claimRes.t, T.INBOX_CLAIM_RES, "claim ok");
    const bindRes = await send(wsP, "bind", T.DEVICE_BIND, { deviceRegistration: primary.registration, deviceInboxBinding: primary.binding });
    assert.equal(bindRes.t, T.DEVICE_BIND_RES, "device.bind ok: " + JSON.stringify(bindRes.body));

    const enrolled = await node.runtime.accountDeviceRegistry.getDevice(owner.accountIdentityPublicKeyB64, primary.deviceId);
    assert.ok(enrolled && enrolled.inboxId === inboxId, "primary device enrolled into the account registry (L7)");

    // --- device.add a sibling S over the wire (L6 → serializer epoch 0 → 1) ---
    const sibling = await buildSiblingBinding("inbox:sibling-" + Buffer.from(CRYPTO.randomBytes(6)).toString("hex"));
    // Audit R4 completeness: device.add carries the sibling's leaf capability cert (C←B).
    const siblingCert = buildLeafCert({ owner, granteePubB64: sibling.devicePublicKeyB64, capabilities: ["deviceSet.publish"] });
    const addMut = await buildAccountMutation({
      owner, opId: "add-1", expectedRevision: 0, action: "device.add",
      target: { deviceInboxBinding: sibling.binding, deviceCapability: siblingCert.toJSON() },
    });
    const addRes = await send(wsP, "add", T.ACCOUNT_DEVICE_MUTATION_SUBMIT, { mutation: addMut });
    assert.equal(addRes.t, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, "device.add ok: " + JSON.stringify(addRes.body));
    assert.equal(addRes.body.revision, 1, "serializer bumped the epoch to 1");
    assert.ok(addRes.body.devices.some((d) => d.deviceId === sibling.deviceId), "sibling in the folded set");

    // --- getAuthorityState over the wire → epoch 1 ---
    const auth1 = await send(wsP, "as1", T.ACCOUNT_AUTHORITY_STATE_GET, { accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 });
    assert.equal(auth1.t, T.ACCOUNT_AUTHORITY_STATE_GET_RES);
    assert.equal(auth1.body.epoch, 1);

    // Give the primary device some mail so its post-revoke read has something to fail on.
    await node.runtime.durableInbox.append(inboxId, new Uint8Array([1, 2, 3]));
    const before = await node.runtime.durableInbox.readAfterCursor(inboxId, primary.deviceId, 10);
    assert.equal(before.length, 1, "bound primary reads its mail before revoke");

    // Build the delegated device C + its leaf cert first (its certId is later presented
    // at session-auth). Under Option A a cert is revoked by revoking the DEVICE it is
    // bound to, so C is enrolled with this leaf cert as its registry cert below.
    const cKp = CRYPTO.generateSigningKeyPair();
    const cPubB64 = bytesToBase64(cKp.publicKey);
    const cDeviceId = DeviceRegistrationV1.deviceIdFor(cPubB64);
    const leafCert = buildLeafCert({ owner, granteePubB64: cPubB64, capabilities: ["deviceSet.publish"] });

    // --- device.revoke the PRIMARY (L6/L7). Option A auto-revokes the target's OWN bound
    // cert; the point here is the account-wide cursor fail-close + the epoch bump. ---
    const revPrimaryMut = await buildAccountMutation({
      owner, opId: "rev-primary", expectedRevision: 1, action: "device.revoke",
      target: { revokedDeviceId: primary.deviceId },
    });
    const revPrimaryRes = await send(wsP, "revp", T.ACCOUNT_DEVICE_MUTATION_SUBMIT, { mutation: revPrimaryMut });
    assert.equal(revPrimaryRes.t, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, "device.revoke primary ok: " + JSON.stringify(revPrimaryRes.body));
    assert.equal(revPrimaryRes.body.revision, 2, "epoch bumped to 2");

    // Account-wide fail-close (L7): the primary's durable cursor is now closed.
    await assert.rejects(
      () => node.runtime.durableInbox.readAfterCursor(inboxId, primary.deviceId, 10),
      (err) => err instanceof RevokedDeviceError,
      "the revoked primary can no longer read — account-wide home fail-close",
    );

    // --- Enroll delegated device C bound to the leaf cert, then device.revoke C over the
    // wire → Option A AUTO-revokes C's OWN bound leaf cert (completeness — the supported
    // way to get a cert into the revoked set is to revoke the device it authorizes). ---
    const cInboxId = "inbox:c-" + Buffer.from(CRYPTO.randomBytes(6)).toString("hex");
    await node.runtime.accountDeviceRegistry.enrollWithCursor({
      accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64,
      deviceId: cDeviceId, inboxId: cInboxId, certId: leafCert.certId, authorityEpoch: 2, devicePublicKeyB64: cPubB64,
    });
    const revCMut = await buildAccountMutation({
      owner, opId: "rev-c", expectedRevision: 2, action: "device.revoke",
      target: { revokedDeviceId: cDeviceId },
    });
    const revRes = await send(wsP, "revc", T.ACCOUNT_DEVICE_MUTATION_SUBMIT, { mutation: revCMut });
    assert.equal(revRes.t, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, "device.revoke C ok: " + JSON.stringify(revRes.body));
    assert.equal(revRes.body.revision, 3, "epoch bumped to 3");
    assert.ok(revRes.body.authorityState.revokedCertIds.includes(leafCert.certId), "C's OWN leaf cert auto-revoked into the revoked set");

    // getAuthorityState → epoch 3 with the revocation.
    const auth2 = await send(wsP, "as2", T.ACCOUNT_AUTHORITY_STATE_GET, { accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 });
    assert.equal(auth2.body.epoch, 3);
    assert.ok(auth2.body.revokedCertIds.includes(leafCert.certId));

    // --- L8 LIVE consult: a delegated session presenting the now-REVOKED leaf
    // cert is rejected at session-auth (the authority cache reads the live epoch-2
    // revocation from the serializer). ---
    const wsC = new WebSocket("ws://127.0.0.1:" + node.server.address().port + "/ws");
    await new Promise((resolve, reject) => { wsC.once("open", resolve); wsC.once("error", reject); });
    t.after(() => wsC.close());
    wsC.send(JSON.stringify({
      id: "hello", t: T.SESSION_HELLO, type: T.SESSION_HELLO, v: CONTRACT_VERSION,
      body: { contractVersion: CONTRACT_VERSION, clientName: "c", clientVersion: "1", deviceId: cDeviceId, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 },
    }));
    const challenge = (await waitForMessage(wsC, (m) => m.id === "hello" && m.t === T.SESSION_CHALLENGE)).body;
    const authPayload = {
      kind: "session-auth", challengeId: challenge.challengeId, nonceB64: challenge.nonceB64,
      nodeKeyId: challenge.nodeKeyId, nodePublicKeyB64: challenge.nodePublicKeyB64, relayKeyId: challenge.relayKeyId,
      publicKeyB64: owner.accountIdentityPublicKeyB64, deviceId: cDeviceId, wsPath: challenge.wsPath,
    };
    const sigB64 = bytesToBase64(CRYPTO.sign({ privateKey: cKp.privateKey, msg: signedPayloadBytes(authPayload) }));
    wsC.send(JSON.stringify({
      id: "hello", type: T.SESSION_AUTHENTICATE, t: T.SESSION_AUTHENTICATE, v: CONTRACT_VERSION,
      body: { challengeId: challenge.challengeId, signatureB64: sigB64, signerPublicKeyB64: cPubB64, certChain: [leafCert.toJSON()] },
    }));
    const authResult = await waitForMessage(wsC, (m) => m.id === "hello" && (m.t === T.SESSION_READY || m.t === T.ERROR));
    assert.equal(authResult.t, T.ERROR, "a delegated session with a revoked leaf cert must be rejected");
    assert.equal(authResult.body.code, "UNAUTHORIZED");

    // --- Round-4 finding 1: a delegated device revoked by DEVICE ID that NEVER bound its
    // cert (so NOTHING landed in the revoked-cert set) must STILL be barred from
    // authenticating — the terminal tombstone is the authoritative device status. Without
    // the consumption-boundary check, its unrevoked leaf cert would pass verifyAccountAuthority. ---
    const dKp = CRYPTO.generateSigningKeyPair();
    const dPubB64 = bytesToBase64(dKp.publicKey);
    const dDeviceId = DeviceRegistrationV1.deviceIdFor(dPubB64);
    const dLeaf = buildLeafCert({ owner, granteePubB64: dPubB64, capabilities: ["deviceSet.publish"] });
    // Revoke D by device id WITHOUT ever binding it → tombstone only, NO cert revoked.
    const revDMut = await buildAccountMutation({
      owner, opId: "rev-d", expectedRevision: 3, action: "device.revoke", target: { revokedDeviceId: dDeviceId },
    });
    const revDRes = await send(wsP, "revd", T.ACCOUNT_DEVICE_MUTATION_SUBMIT, { mutation: revDMut });
    assert.equal(revDRes.t, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, "device.revoke D (never-enrolled) ok");
    assert.equal(revDRes.body.revision, 4, "epoch bumped to 4");
    assert.ok(!revDRes.body.authorityState.revokedCertIds.includes(dLeaf.certId), "D's leaf cert is NOT in the revoked set (never bound)");

    // D presents its VALID, un-revoked leaf cert at session-auth — rejected by the tombstone.
    const wsD = new WebSocket("ws://127.0.0.1:" + node.server.address().port + "/ws");
    await new Promise((resolve, reject) => { wsD.once("open", resolve); wsD.once("error", reject); });
    t.after(() => wsD.close());
    wsD.send(JSON.stringify({
      id: "hello", t: T.SESSION_HELLO, type: T.SESSION_HELLO, v: CONTRACT_VERSION,
      body: { contractVersion: CONTRACT_VERSION, clientName: "d", clientVersion: "1", deviceId: dDeviceId, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 },
    }));
    const dChallenge = (await waitForMessage(wsD, (m) => m.id === "hello" && m.t === T.SESSION_CHALLENGE)).body;
    const dAuthPayload = {
      kind: "session-auth", challengeId: dChallenge.challengeId, nonceB64: dChallenge.nonceB64,
      nodeKeyId: dChallenge.nodeKeyId, nodePublicKeyB64: dChallenge.nodePublicKeyB64, relayKeyId: dChallenge.relayKeyId,
      publicKeyB64: owner.accountIdentityPublicKeyB64, deviceId: dDeviceId, wsPath: dChallenge.wsPath,
    };
    const dSigB64 = bytesToBase64(CRYPTO.sign({ privateKey: dKp.privateKey, msg: signedPayloadBytes(dAuthPayload) }));
    wsD.send(JSON.stringify({
      id: "hello", type: T.SESSION_AUTHENTICATE, t: T.SESSION_AUTHENTICATE, v: CONTRACT_VERSION,
      body: { challengeId: dChallenge.challengeId, signatureB64: dSigB64, signerPublicKeyB64: dPubB64, certChain: [dLeaf.toJSON()] },
    }));
    const dAuthResult = await waitForMessage(wsD, (m) => m.id === "hello" && (m.t === T.SESSION_READY || m.t === T.ERROR));
    assert.equal(dAuthResult.t, T.ERROR, "a tombstoned-before-bind device is rejected despite an unrevoked leaf cert (finding 1)");
    assert.equal(dAuthResult.body.code, "UNAUTHORIZED");
  },
);
