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
  encodeOuterPacket,
  DeviceRegistrationV1,
  DEVICE_REGISTRATION_PURPOSE,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  DevicePrekeyBundleV1,
  DEVICE_PREKEY_BUNDLE_PURPOSE,
  AccountDeviceMutationV2,
  ACCOUNT_DEVICE_MUTATION_V2_PURPOSE,
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
import { pgTestUrl } from "./support/integrationBackends.js";

// FIRST true 2-device fan-out proof with the gate OPEN. Every earlier multi-device
// test ran the durable inbox at maxDevices=1 (S12 home-aggregation, durable delivery);
// this is the first that constructs PgDurableInbox with maxDevices>1 (the E6 fan-out
// gate OPEN, exactly what an operator setting node.device.multiDeviceFanout=true gets)
// and drives it over REAL WebSocket sockets against REAL Postgres. It proves, un-mocked:
//   1. the gate is genuinely OPEN (an unproven claim-path cursor is refused — F2/P1);
//   2. two proven-bound devices of ONE account each drain their OWN inbox (fan-out
//      delivery reaches both);
//   3. a CONCURRENT account-signed device.revoke of one device, racing that device's
//      own live read, terminally fail-closes it (DEVICE_REVOKED on list + the home read
//      rejects) — linearizable, never a partial;
//   4. the SURVIVING device keeps receiving new fan-out mail;
//   5. the home stops accepting deposits for the revoked device's inbox (no live device)
//      and account.deviceSet.get serves only the survivor.
const PG_URL = pgTestUrl();
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();
const OPEN_MAX_DEVICES = 8; // DEVICE_FANOUT_MAX — the gate-OPEN device cap.

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
  const identity = createNodeTestIdentity({ accountId: "rez:node:fanout-test", deviceId: "dev:node", localInboxId: "inbox:test" });
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider });
  await inboxClaimRegistry.hydrate();
  // THE distinguishing line: gate OPEN (maxDevices > 1).
  const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: OPEN_MAX_DEVICES });
  const accountDeviceRegistry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
  const accountMutationSerializer = new PgAccountMutationSerializer({ connection: conn, durableInbox });
  const accountAuthorityRevocationCache = new AccountAuthorityRevocationCache({ serializer: accountMutationSerializer });
  const accountDeviceBundleStore = new PgAccountDeviceBundleStore({ connection: conn });
  const isHostedHere = (id) => inboxClaimRegistry.hasInbox(id);
  const inboxStore = new DurableHomeInboxStore({ rmailbox: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }), durableInbox, isHostedHere });
  const runtime = {
    inboxStore, durableInbox, accountDeviceRegistry, accountMutationSerializer, accountAuthorityRevocationCache, accountDeviceBundleStore, isHostedHere,
    // The STORAGE-layer open gate is PgDurableInbox({ maxDevices: OPEN_MAX_DEVICES }) above —
    // that is the knob this test exercises (per-device cursor create/read semantics). The
    // advertised-capability flag is orthogonal and stays false: MULTI_DEVICE_FANOUT_READY is
    // currently false (completeness reverted by the No-Go audit), so a runtime advertising
    // multiDeviceFanout:true would fail the readiness interlock during session auth. This test
    // proves the durable storage fan-out + revocation semantics directly, independent of the
    // (still-closed) advertised gate.
    relayStore: null, metrics: null, inboxClaimRegistry, multiDeviceFanout: false,
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
const wire = (...b) => encodeOuterPacket({ bodyBytes: new Uint8Array(b) });
const b64 = (...b) => Buffer.from(new Uint8Array(b)).toString("base64");

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
  const body = { v: 2, purpose: ACCOUNT_DEVICE_MUTATION_V2_PURPOSE, opId, accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64, expectedRevision, action, target, signerPublicKeyB64: owner.accountIdentityPublicKeyB64, issuedAtMs: now - 1000, expiresAtMs: now + 300_000 };
  return { ...body, sig: await edSig(owner.privateKey, AccountDeviceMutationV2.signableBytes(body)) };
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
function listMailbox(ws, id, mailboxId) {
  return send(ws, id, T.MAILBOX_LIST, { mailboxId, limit: 50 });
}
function cursorAck(ws, id, mailboxId, throughSeq) {
  return send(ws, id, T.MAILBOX_CURSOR_ACK, { mailboxId, throughSeq });
}
function fetchMailbox(ws, id, mailboxId, eventId) {
  return send(ws, id, T.MAILBOX_FETCH, { mailboxId, eventId });
}

async function bringOnline(t, node, owner, device) {
  const ws = await openAuthed(t, node.server, owner, device.deviceId);
  await send(ws, "claim-" + device.deviceId, T.INBOX_CLAIM, buildClaimBody({ owner, inboxId: device.inboxId, claimedAtMs: Date.now(), nodeIdentity: node.nodeIdentity }));
  const bindRes = await send(ws, "bind-" + device.deviceId, T.DEVICE_BIND, { deviceRegistration: device.registration, deviceInboxBinding: device.binding });
  assert.equal(bindRes.t, T.DEVICE_BIND_RES, "device.bind ok: " + JSON.stringify(bindRes.body));
  const pubRes = await send(ws, "pub-" + device.deviceId, T.ACCOUNT_DEVICE_BUNDLE_PUBLISH, { bundle: device.bundle.toJSON() });
  assert.equal(pubRes.t, T.ACCOUNT_DEVICE_BUNDLE_PUBLISH_RES, "bundle publish ok: " + JSON.stringify(pubRes.body));
  return ws;
}

test(
  "fan-out (gate OPEN, real Pg + WS): two devices each drain their inbox; a concurrent revoke fail-closes one, the survivor keeps receiving",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_multidevice_fanout_revoke";
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

    // (1) Prove the gate is ACTUALLY OPEN: an unproven claim (no device.bind) creates NO
    // cursor under the open gate (F2/P1 — only a proven device.bind may create one). If the
    // gate were closed this claim WOULD create a legacy null-key cursor, so this assertion
    // is what distinguishes an open-gate run from a closed-gate one.
    const probe = await makeDevice({ owner, inboxId: "inbox:probe-" + rand(6) });
    const probeWs = await openAuthed(t, node.server, owner, probe.deviceId);
    await send(probeWs, "claim-probe", T.INBOX_CLAIM, buildClaimBody({ owner, inboxId: probe.inboxId, claimedAtMs: Date.now(), nodeIdentity: node.nodeIdentity }));
    assert.equal(await node.runtime.durableInbox.getDevice(probe.inboxId, probe.deviceId), null,
      "gate OPEN: an unproven claim-path cursor is refused (no cursor until device.bind)");

    // Both real devices come online (each its own inbox + proven bind + bundle).
    const ws1 = await bringOnline(t, node, owner, d1);
    const ws2 = await bringOnline(t, node, owner, d2);

    // (2) Fan-out: a message is deposited to BOTH device-inboxes; each device drains its OWN
    // over its socket, concurrently. Both receive — per-device fan-out delivery under an open gate.
    await node.runtime.inboxStore.depositFromWire(d1.inboxId, wire(11, 11));
    await node.runtime.inboxStore.depositFromWire(d2.inboxId, wire(22, 22));
    const [list1, list2] = await Promise.all([
      listMailbox(ws1, "l1", d1.inboxId),
      listMailbox(ws2, "l2", d2.inboxId),
    ]);
    assert.deepEqual(list1.body.items, [{ seq: 1, ciphertextB64: b64(11, 11) }], "d1 drains its own inbox");
    assert.deepEqual(list2.body.items, [{ seq: 1, ciphertextB64: b64(22, 22) }], "d2 drains its own inbox");
    await Promise.all([cursorAck(ws1, "a1", d1.inboxId, 1), cursorAck(ws2, "a2", d2.inboxId, 1)]);

    // (3) CONCURRENT revoke racing d2's own read: fire the account-signed device.revoke(d2)
    // from d1's session AT THE SAME TIME as another d2.list. The race outcome of d2's list is
    // non-deterministic (it may win before the revoke commits or lose after) — we do NOT assert
    // it, only that neither call crashes and the revoke commits. The linearizable guarantee is
    // the TERMINAL state, asserted next.
    const rev = await buildAccountMutation({ owner, opId: "rev-d2", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: d2.deviceId } });
    const [revSettled] = await Promise.allSettled([
      send(ws1, "rev", T.ACCOUNT_DEVICE_MUTATION_SUBMIT, { mutation: rev }),
      listMailbox(ws2, "l2-race", d2.inboxId).catch((err) => ({ raced: true, err })),
    ]);
    assert.equal(revSettled.status, "fulfilled");
    assert.equal(revSettled.value.t, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, "revoke committed: " + JSON.stringify(revSettled.value.body));

    // (4) TERMINAL: d2 is fail-closed. Over the socket, mailbox.list now errors DEVICE_REVOKED;
    // at the home, the durable read rejects. A revoked device cannot read regardless of timing.
    const d2after = await listMailbox(ws2, "l2-after", d2.inboxId);
    assert.equal(d2after.t, T.ERROR, "revoked d2 list is an error frame");
    assert.equal(d2after.body.code, "DEVICE_REVOKED", "revoked d2 is fail-closed on read");
    await assert.rejects(
      () => node.runtime.durableInbox.readAfterCursor(d2.inboxId, d2.deviceId, 10),
      (err) => err instanceof RevokedDeviceError,
      "the home read for the revoked device rejects",
    );
    // The home refuses NEW deposits to the revoked device's inbox (no live device remains).
    await assert.rejects(
      () => node.runtime.inboxStore.depositFromWire(d2.inboxId, wire(23, 23)),
      (err) => err instanceof RevokedDeviceError,
      "deposits to an all-revoked inbox are refused at the home",
    );

    // (4b) The random-access mailbox.fetch surface is ALSO device-gated (No-Go P1#1): a revoked
    // device cannot fetch stored ciphertext by seq, even though the row still exists. A proven,
    // non-revoked device fetches its own stored event normally.
    const d2fetch = await fetchMailbox(ws2, "f2", d2.inboxId, "1");
    assert.equal(d2fetch.t, T.ERROR, "revoked d2 fetch is an error frame");
    assert.equal(d2fetch.body.code, "DEVICE_REVOKED", "revoked d2 is fail-closed on fetch, not just list");
    const d1fetch = await fetchMailbox(ws1, "f1", d1.inboxId, "1");
    assert.equal(d1fetch.t, T.MAILBOX_FETCH_RES, "proven d1 fetch succeeds");
    assert.equal(d1fetch.body.ciphertextB64, b64(11, 11), "d1 fetch returns its stored ciphertext by seq");

    // (5) The SURVIVOR keeps receiving fan-out mail, unaffected by the concurrent revoke.
    await node.runtime.inboxStore.depositFromWire(d1.inboxId, wire(33, 33));
    const d1after = await listMailbox(ws1, "l1-after", d1.inboxId);
    assert.deepEqual(d1after.body.items, [{ seq: 2, ciphertextB64: b64(33, 33) }], "d1 still receives new mail after the revoke");

    // account.deviceSet.get now serves ONLY the surviving device.
    const set = await send(ws1, "gs", T.ACCOUNT_DEVICE_SET_GET, { accountIdentityPublicKeyB64: owner.accountIdentityPublicKeyB64 });
    assert.equal(set.t, T.ACCOUNT_DEVICE_SET_GET_RES);
    assert.deepEqual(set.body.devices.map((x) => x.deviceId), [d1.deviceId], "the revoked device drops out of the served set");
  },
);
