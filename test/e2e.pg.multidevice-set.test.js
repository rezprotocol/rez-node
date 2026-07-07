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
  DevicePrekeyBundleV1,
  DEVICE_PREKEY_BUNDLE_PURPOSE,
  AccountDeviceMutationV1,
  ACCOUNT_DEVICE_MUTATION_PURPOSE,
} from "@rezprotocol/core";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceBundleStore } from "../src/storage/pg/PgAccountDeviceBundleStore.js";
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

// S2.5 S12 GATE (home half) — the multi-device home-aggregated device set proven
// over REAL WebSocket sockets against a REAL Postgres home. Two devices of one
// account each device.bind their own inbox + self-publish their prekey bundle;
// account.deviceSet.get serves BOTH bundles; an account-wide revoke drops a
// device from the served set AND fail-closes its cursor. (The per-device fan-out
// CRYPTO — a peer establishing + delivering to both devices — is proven un-mocked
// at the rez-sdk layer, peer-link.multidevice-fanout; this ties the home pieces
// together on live Pg + sockets.)
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

function freshOwner() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { publicKey: kp.publicKey, privateKey: kp.privateKey, accountIdentityPublicKeyB64: bytesToBase64(kp.publicKey) };
}

function buildClaimBody({ owner, inboxId, claimedAtMs, nodeIdentity }) {
  const claimantPublicKeyB64 = owner.accountIdentityPublicKeyB64;
  const signatureB64 = bytesToBase64(CRYPTO.sign({
    privateKey: owner.privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify({ inboxId, claimantPublicKeyB64, claimedAtMs })),
  }));
  const d = createClaimantNodeDelegation({
    claimantIdentity: { accountIdentityPublicKeyB64: claimantPublicKeyB64, privateKey: owner.privateKey, inboxId },
    inboxId, nodeKeyId: nodeIdentity.nodeKeyId, nodePublicKeyB64: nodeIdentity.nodePublicKeyB64, relayKeyId: nodeIdentity.relayKeyId,
  });
  return {
    inboxId, claimantPublicKeyB64, claimedAtMs, signatureB64,
    nodeDelegation: { nodeKeyId: d.nodeKeyId, nodePublicKeyB64: d.nodePublicKeyB64, relayKeyId: d.relayKeyId, issuedAtMs: d.issuedAtMs, expiresAtMs: d.expiresAtMs, delegationSigB64: d.delegationSigB64 },
  };
}

async function startNode(conn) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({ accountId: "rez:node:s12-test", deviceId: "dev:node", localInboxId: "inbox:test" });
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider });
  await inboxClaimRegistry.hydrate();
  const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
  const accountDeviceRegistry = new PgAccountDeviceRegistry({ connection: conn });
  const accountMutationSerializer = new PgAccountMutationSerializer({ connection: conn });
  const accountAuthorityRevocationCache = new AccountAuthorityRevocationCache({ serializer: accountMutationSerializer });
  const accountDeviceBundleStore = new PgAccountDeviceBundleStore({ connection: conn });
  const isHostedHere = (id) => inboxClaimRegistry.hasInbox(id);
  const inboxStore = new DurableHomeInboxStore({ rmailbox: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }), durableInbox, isHostedHere });
  const runtime = {
    inboxStore, durableInbox, accountDeviceRegistry, accountMutationSerializer, accountAuthorityRevocationCache, accountDeviceBundleStore, isHostedHere,
    relayStore: null, metrics: null, inboxClaimRegistry,
    serverServices: createServerServices({ storageProvider, clock: () => Date.now(), ownerAccountId: identity.accountId }),
    serviceCache: new PerAccountServiceCache({ storageProvider, clock: () => Date.now(), createServices: createPerAccountServices }),
    getIdentity() { return { ...identity }; },
    getOwnerPublicKeysForInbox() { return new Set(); },
    getMeshStatus() { return { enabled: true, mode: "seeded-gossip", participateInRouting: true, peerCount: 0 }; },
    async stop() {},
  };
  const server = new WsGatewayServer({ runtime, port: 0, protocolFactory: createProtocolFactory(), onInboundDeposit: createDepositHandler({ crypto: new NodeCryptoProvider() }) });
  await server.start();
  return { server, runtime, nodeIdentity: identity };
}

async function edSig(privateKey, msgBytes) {
  return { alg: "ed25519", sigB64: bytesToBase64(await CRYPTO.sign({ privateKey, msg: msgBytes })) };
}
function rand(n) { return bytesToBase64(CRYPTO.randomBytes(n)); }

// A device of `owner`: its key, self-cert deviceId, account-signed registration,
// device-signed inbox binding, and a device-signed prekey bundle for `inboxId`.
async function makeDevice({ owner, inboxId }) {
  const devKp = CRYPTO.generateSigningKeyPair();
  const devicePublicKeyB64 = bytesToBase64(devKp.publicKey);
  const deviceId = DeviceRegistrationV1.deviceIdFor(devicePublicKeyB64);
  const now = Date.now();
  const regBody = { v: 1, purpose: DEVICE_REGISTRATION_PURPOSE, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64, devicePublicKeyB64, deviceId, issuedAtMs: now - 1000, expiresAtMs: now + 3_600_000 };
  const registration = { ...regBody, sig: await edSig(owner.privateKey, DeviceRegistrationV1.signableBytes(regBody)) };
  const bindBody = { v: 1, purpose: DEVICE_INBOX_BINDING_PURPOSE, devicePublicKeyB64, deviceId, inboxId, issuedAtMs: now - 1000, expiresAtMs: now + 3_600_000 };
  const binding = { ...bindBody, sig: await edSig(devKp.privateKey, DeviceInboxBindingV1.signableBytes(bindBody)) };
  const bundleJson = {
    receiverId: deviceId, identitySigningPublicKeyB64: devicePublicKeyB64,
    identityDhPublicKeyB64: rand(32), identityDhSignatureB64: rand(64),
    signedPreKeyPublicB64: rand(32), signedPreKeySignatureB64: rand(64), oneTimePreKeyPublicB64: rand(32),
  };
  const pbBody = { v: 1, purpose: DEVICE_PREKEY_BUNDLE_PURPOSE, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64, devicePublicKeyB64, deviceId, inboxId, prekeyVersion: 1, bundleJson, issuedAtMs: now - 1000, expiresAtMs: now + 3_600_000 };
  const bundle = new DevicePrekeyBundleV1({ ...pbBody, sig: await edSig(devKp.privateKey, DevicePrekeyBundleV1.signableBytes(pbBody)) });
  return { devKp, devicePublicKeyB64, deviceId, inboxId, registration, binding, bundle };
}

async function buildAccountMutation({ owner, opId, expectedRevision, action, target }) {
  const now = Date.now();
  const body = { v: 1, purpose: ACCOUNT_DEVICE_MUTATION_PURPOSE, opId, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64, expectedRevision, action, target, signerPublicKeyB64: owner.accountIdentityPublicKeyB64, issuedAtMs: now - 1000, expiresAtMs: now + 300_000 };
  return { ...body, sig: await edSig(owner.privateKey, AccountDeviceMutationV1.signableBytes(body)) };
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

// Bring a device online: authenticate as its deviceId, claim its inbox, bind, publish its bundle.
async function bringOnline(t, node, owner, device) {
  const ws = await openAuthed(t, node.server, owner, device.deviceId);
  await send(ws, "claim", T.INBOX_CLAIM, buildClaimBody({ owner, inboxId: device.inboxId, claimedAtMs: Date.now(), nodeIdentity: node.nodeIdentity }));
  const bindRes = await send(ws, "bind", T.DEVICE_BIND, { deviceRegistration: device.registration, deviceInboxBinding: device.binding });
  assert.equal(bindRes.t, T.DEVICE_BIND_RES, "device.bind ok: " + JSON.stringify(bindRes.body));
  const pubRes = await send(ws, "pub", T.ACCOUNT_DEVICE_BUNDLE_PUBLISH, { bundle: device.bundle.toJSON() });
  assert.equal(pubRes.t, T.ACCOUNT_DEVICE_BUNDLE_PUBLISH_RES, "bundle publish ok: " + JSON.stringify(pubRes.body));
  return ws;
}

test(
  "S12 gate (real Pg): two devices publish bundles → account.deviceSet.get serves both; account-wide revoke drops one + fail-closes",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_s12_multidevice";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();

    let node;
    try { node = await startNode(conn); }
    catch (err) { if (["EACCES", "EPERM"].includes(err && err.code)) { t.skip("WebSocket bind not permitted"); return; } throw err; }
    t.after(async () => { await node.server.stop(); });

    const owner = freshOwner();
    const d1 = await makeDevice({ owner, inboxId: "inbox:d1-" + rand(6) });
    const d2 = await makeDevice({ owner, inboxId: "inbox:d2-" + rand(6) });

    // Both devices of the ONE account come online (each its own inbox + bundle).
    const ws1 = await bringOnline(t, node, owner, d1);
    await bringOnline(t, node, owner, d2);

    // account.deviceSet.get serves BOTH active devices' bundles (home aggregation).
    const set1 = await send(ws1, "gs1", T.ACCOUNT_DEVICE_SET_GET, { accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 });
    assert.equal(set1.t, T.ACCOUNT_DEVICE_SET_GET_RES);
    const ids = set1.body.devices.map((x) => x.deviceId).sort();
    assert.deepEqual(ids, [d1.deviceId, d2.deviceId].sort(), "the aggregated set enumerates BOTH devices");
    for (const dev of set1.body.devices) {
      assert.ok(dev.bundle && dev.bundle.devicePublicKeyB64, "each entry carries the full device-signed bundle");
    }

    // Give d2 some durable mail so its post-revoke read has something to fail on.
    await node.runtime.durableInbox.append(d2.inboxId, new Uint8Array([9, 9, 9]));
    assert.equal((await node.runtime.durableInbox.readAfterCursor(d2.inboxId, d2.deviceId, 10)).length, 1);

    // Account-wide revoke of d2 (account-signed mutation, from d1's session).
    const rev = await buildAccountMutation({ owner, opId: "rev-d2", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: d2.deviceId } });
    const revRes = await send(ws1, "rev", T.ACCOUNT_DEVICE_MUTATION_SUBMIT, { mutation: rev });
    assert.equal(revRes.t, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, "revoke ok: " + JSON.stringify(revRes.body));

    // d2 is fail-closed at the home (account-wide revoke, L7).
    await assert.rejects(
      () => node.runtime.durableInbox.readAfterCursor(d2.inboxId, d2.deviceId, 10),
      (err) => err instanceof RevokedDeviceError,
      "the revoked device can no longer read",
    );

    // account.deviceSet.get now serves ONLY the surviving device (revoked dropped).
    const set2 = await send(ws1, "gs2", T.ACCOUNT_DEVICE_SET_GET, { accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 });
    assert.deepEqual(set2.body.devices.map((x) => x.deviceId), [d1.deviceId], "the revoked device drops out of the served set");
  },
);

test(
  "S12 gate (real Pg): a device cannot publish a bundle for a device it is not, and getDeviceSet is own-account-only",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_s12_multidevice_authz";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    let node;
    try { node = await startNode(conn); }
    catch (err) { if (["EACCES", "EPERM"].includes(err && err.code)) { t.skip("WebSocket bind not permitted"); return; } throw err; }
    t.after(async () => { await node.server.stop(); });

    const owner = freshOwner();
    const d1 = await makeDevice({ owner, inboxId: "inbox:d1-" + rand(6) });
    const ws1 = await bringOnline(t, node, owner, d1);

    // A bundle for a DIFFERENT device than the session is rejected.
    const other = await makeDevice({ owner, inboxId: "inbox:other-" + rand(6) });
    const bad = await send(ws1, "badpub", T.ACCOUNT_DEVICE_BUNDLE_PUBLISH, { bundle: other.bundle.toJSON() });
    assert.equal(bad.t, T.ERROR);
    assert.equal(bad.body.code, "FORBIDDEN");

    // getDeviceSet for a DIFFERENT account is forbidden.
    const foreign = freshOwner();
    const gs = await send(ws1, "gsf", T.ACCOUNT_DEVICE_SET_GET, { accountIdentityPublicKeyB64: foreign.accountIdentityPublicKeyB64 });
    assert.equal(gs.t, T.ERROR);
    assert.equal(gs.body.code, "FORBIDDEN");
  },
);
