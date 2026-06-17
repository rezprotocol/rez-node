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
} from "@rezprotocol/core";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { DurableHomeInboxStore } from "../src/storage/DurableHomeInboxStore.js";
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

const PG_URL = process.env.REZ_PG_TEST_URL || "";
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();

function waitForMessage(ws, predicate, timeoutMs = 2000) {
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

function freshClaimantIdentity() {
  const kp = CRYPTO.generateSigningKeyPair();
  return {
    publicKey: kp.publicKey,
    privateKey: kp.privateKey,
    accountIdentityPublicKeyB64: bytesToBase64(kp.publicKey),
    accountIdentityPrivateKeyB64: bytesToBase64(kp.privateKey),
  };
}

function freshInboxId() { return "inbox:" + Buffer.from(CRYPTO.randomBytes(12)).toString("hex"); }

function buildClaimBody({ claimantIdentity, inboxId, claimedAtMs, nodeIdentity }) {
  const claimantPublicKeyB64 = claimantIdentity.accountIdentityPublicKeyB64;
  const signatureB64 = signClaim({ inboxId, claimantPublicKeyB64, claimedAtMs, privateKey: claimantIdentity.privateKey });
  const d = createClaimantNodeDelegation({
    claimantIdentity, inboxId,
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

async function startDurablePgNode(conn) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({ accountId: "rez:node:durable-test", deviceId: "dev:node", localInboxId: "inbox:test" });
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider });
  await inboxClaimRegistry.hydrate();

  const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
  const isHostedHere = (id) => inboxClaimRegistry.hasInbox(id);
  const inboxStore = new DurableHomeInboxStore({
    rmailbox: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    durableInbox,
    isHostedHere,
  });

  const runtime = {
    inboxStore,
    durableInbox,
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
  return { server, runtime, inboxClaimRegistry, durableInbox, nodeIdentity: identity };
}

async function openAuthed(t, server, identity, deviceId) {
  const ws = new WebSocket("ws://127.0.0.1:" + server.address().port + "/ws");
  await new Promise((resolve, reject) => { ws.once("open", resolve); ws.once("error", reject); });
  t.after(() => ws.close());
  await authenticateSession({ ws, waitForMessage, id: "hello", deviceId, identity });
  return ws;
}

async function claim(ws, id, body) {
  ws.send(JSON.stringify({ id, type: T.INBOX_CLAIM, t: T.INBOX_CLAIM, v: CONTRACT_VERSION, body }));
  return waitForMessage(ws, (m) => m.id === id);
}

async function listMailbox(ws, id, mailboxId) {
  ws.send(JSON.stringify({ id, type: T.MAILBOX_LIST, t: T.MAILBOX_LIST, v: CONTRACT_VERSION, body: { mailboxId, limit: 50 } }));
  return waitForMessage(ws, (m) => m.id === id);
}

async function cursorAck(ws, id, mailboxId, throughSeq) {
  ws.send(JSON.stringify({ id, type: T.MAILBOX_CURSOR_ACK, t: T.MAILBOX_CURSOR_ACK, v: CONTRACT_VERSION, body: { mailboxId, throughSeq } }));
  return waitForMessage(ws, (m) => m.id === id);
}

const wire = (...b) => encodeOuterPacket({ bodyBytes: new Uint8Array(b) });
const b64 = (...b) => Buffer.from(new Uint8Array(b)).toString("base64");

test(
  "1-pg-node durable delivery: claim registers device, drain-by-cursor, no redeliver after ack, redeliver without ack, single-device gate",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_e2e_durable_delivery";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    let started;
    try {
      started = await startDurablePgNode(conn);
    } catch (err) {
      if (["EACCES", "EPERM"].includes(err && err.code)) { t.skip("WebSocket bind not permitted"); return; }
      throw err;
    }
    const { server, runtime, durableInbox, nodeIdentity } = started;
    t.after(async () => { await server.stop(); });

    const owner = freshClaimantIdentity();
    const inboxId = freshInboxId();
    const DEVICE = "dev:owner-a";

    // --- Claim registers the durable device cursor (register-on-bind) ---
    const ws1 = await openAuthed(t, server, owner, DEVICE);
    const claimRes = await claim(ws1, "c1", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity }));
    assert.equal(claimRes.t, T.INBOX_CLAIM_RES, "claim should succeed");
    const dev = await conn.query("SELECT last_seq FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", [inboxId, DEVICE]);
    assert.equal(dev.rowCount, 1, "claim must register the session device cursor");

    // --- Two deposits land durably (simulating the ingress convergence point) ---
    await runtime.inboxStore.depositFromWire(inboxId, wire(10, 10));
    await runtime.inboxStore.depositFromWire(inboxId, wire(20, 20));

    // --- Catch-up drain returns inline {seq, ciphertextB64} ---
    const list1 = await listMailbox(ws1, "l1", inboxId);
    assert.equal(list1.t, T.MAILBOX_LIST_RES, "list should return (authorize passes for owner)");
    assert.deepEqual(list1.body.items, [
      { seq: 1, ciphertextB64: b64(10, 10) },
      { seq: 2, ciphertextB64: b64(20, 20) },
    ]);

    // --- Consume + cursorAck through 2; reconnect drains nothing (no redeliver) ---
    const ack = await cursorAck(ws1, "a1", inboxId, 2);
    assert.equal(ack.t, T.MAILBOX_CURSOR_ACK_RES);
    assert.equal(ack.body.lastSeq, 2);

    const ws2 = await openAuthed(t, server, owner, DEVICE); // reconnect, SAME device
    await claim(ws2, "c2", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity }));
    const list2 = await listMailbox(ws2, "l2", inboxId);
    assert.deepEqual(list2.body.items, [], "acked messages are not redelivered after reconnect");

    // --- A 3rd deposit; list it but DON'T ack; reconnect still redelivers it ---
    await runtime.inboxStore.depositFromWire(inboxId, wire(30, 30));
    const list3 = await listMailbox(ws2, "l3", inboxId);
    assert.deepEqual(list3.body.items, [{ seq: 3, ciphertextB64: b64(30, 30) }]);

    const ws3 = await openAuthed(t, server, owner, DEVICE); // reconnect without acking seq 3
    await claim(ws3, "c3", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity }));
    const list4 = await listMailbox(ws3, "l4", inboxId);
    assert.deepEqual(list4.body.items, [{ seq: 3, ciphertextB64: b64(30, 30) }], "un-acked message redelivered (cursor not advanced)");

    // --- Single-device gate: a SECOND distinct device claiming the same inbox is refused ---
    const ws4 = await openAuthed(t, server, owner, "dev:owner-b");
    const claim2 = await claim(ws4, "c4", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity }));
    assert.equal(claim2.t, T.ERROR);
    assert.equal(claim2.body.code, "DEVICE_LIMIT", "2nd device refused until S2.5");

    void durableInbox;
  },
);

test(
  "P1: a SAME-NODE live deposit advances last_delivered, so cursorAck works WITHOUT an intervening list",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_e2e_same_node_live_ack";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => { await conn.close(); await dropSchema(PG_URL, SCHEMA); });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq, inbox_claims");

    let started;
    try {
      started = await startDurablePgNode(conn);
    } catch (err) {
      if (["EACCES", "EPERM"].includes(err && err.code)) { t.skip("WebSocket bind not permitted"); return; }
      throw err;
    }
    const { server, runtime, nodeIdentity } = started;
    t.after(async () => { await server.stop(); });

    const owner = freshClaimantIdentity();
    const inboxId = freshInboxId();
    const DEVICE = "dev:owner";
    const ws = await openAuthed(t, server, owner, DEVICE);
    assert.equal((await claim(ws, "c1", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity }))).t, T.INBOX_CLAIM_RES);

    // Arm the live EVT listener, then deposit on THIS node while the socket is
    // live here. Pre-fix this took the broadcast branch and never advanced
    // last_delivered; now it drains in-process (advances last_delivered).
    const evtPromise = waitForMessage(ws, (m) => m.t === T.EVT_MAILBOX_DEPOSITED && m.body && m.body.mailboxId === inboxId);
    await runtime.inboxStore.depositFromWire(inboxId, wire(5, 5));
    const evt = await evtPromise;
    assert.equal(evt.body.seq, 1, "same-node live push delivered the event");

    // cursorAck WITHOUT ever calling mailbox.list. Pre-fix last_delivered was 0
    // so this clamped to 0 (lastSeq:0) and reconnect redelivered forever.
    const ack = await cursorAck(ws, "a1", inboxId, 1);
    assert.equal(ack.body.lastSeq, 1, "cursorAck advances past a live-pushed event (NOT clamped to 0)");

    // Reconnect (same device) and list: the acked event is NOT redelivered.
    const ws2 = await openAuthed(t, server, owner, DEVICE);
    await claim(ws2, "c2", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity }));
    assert.deepEqual((await listMailbox(ws2, "l1", inboxId)).body.items, [],
      "no infinite redelivery after a live-pushed event is consumed + acked");
  },
);
