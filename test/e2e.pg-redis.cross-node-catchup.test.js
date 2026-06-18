// S2 headline (Slice 4): the multi-user, multi-node proof. The cross-node LIVE
// push is proven in e2e.pg-redis.cross-node.test.js; the durable-delivery test
// proves catch-up + cursorAck against ONE node. This file proves the case those
// two don't: a deposit lands while the client is OFFLINE, the client reconnects
// to a DIFFERENT node, catches up from the shared durable log, and — because the
// device cursor is SHARED cluster state — an ack on one node is honored on every
// node (no redeliver, no dup). Plus per-inbox isolation across nodes (real
// multi-user: two owners on two nodes never see each other's ciphertext).
//
// Topology mirrors the hosted cluster: N WsGatewayServer nodes, each with its own
// PgDurableInbox instance + LivenessBus, all over ONE shared Pg connection + ONE
// shared PgInboxClaimRegistry + ONE Redis. Un-mocked: real WS, real Pg, real Redis.

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
import { PgInboxClaimRegistry } from "../src/storage/pg/PgInboxClaimRegistry.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { DurableHomeInboxStore } from "../src/storage/DurableHomeInboxStore.js";
import { createLivenessBus } from "../src/relay/createLivenessBus.js";
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
const REDIS_URL = process.env.REZ_REDIS_TEST_URL || "";
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();
const SKIP = (PG_URL && REDIS_URL) ? false : "set REZ_PG_TEST_URL and REZ_REDIS_TEST_URL to run";

function waitForMessage(ws, predicate, timeoutMs = 4000) {
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

function freshClaimantIdentity() {
  const kp = CRYPTO.generateSigningKeyPair();
  return {
    publicKey: kp.publicKey, privateKey: kp.privateKey,
    accountIdentityPublicKeyB64: bytesToBase64(kp.publicKey),
    accountIdentityPrivateKeyB64: bytesToBase64(kp.privateKey),
  };
}
const freshInboxId = () => "inbox:" + Buffer.from(CRYPTO.randomBytes(12)).toString("hex");
const wire = (...b) => encodeOuterPacket({ bodyBytes: new Uint8Array(b) });
const b64 = (...b) => Buffer.from(new Uint8Array(b)).toString("base64");

function buildClaimBody({ claimantIdentity, inboxId, claimedAtMs, nodeIdentity }) {
  const claimantPublicKeyB64 = claimantIdentity.accountIdentityPublicKeyB64;
  const signatureB64 = bytesToBase64(CRYPTO.sign({
    privateKey: claimantIdentity.privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify({ inboxId, claimantPublicKeyB64, claimedAtMs })),
  }));
  const d = createClaimantNodeDelegation({
    claimantIdentity, inboxId,
    nodeKeyId: nodeIdentity.nodeKeyId, nodePublicKeyB64: nodeIdentity.nodePublicKeyB64, relayKeyId: nodeIdentity.relayKeyId,
  });
  return {
    inboxId, claimantPublicKeyB64, claimedAtMs, signatureB64,
    nodeDelegation: {
      nodeKeyId: d.nodeKeyId, nodePublicKeyB64: d.nodePublicKeyB64, relayKeyId: d.relayKeyId,
      issuedAtMs: d.issuedAtMs, expiresAtMs: d.expiresAtMs, delegationSigB64: d.delegationSigB64,
    },
  };
}

// One cluster node over the SHARED Pg connection + claim registry + Redis.
async function startClusterNode(conn, claimRegistry, suffix) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:" + suffix, deviceId: "dev:" + suffix, localInboxId: "inbox:node",
  });
  const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
  const isHostedHere = (id) => claimRegistry.hasInbox(id);
  const inboxStore = new DurableHomeInboxStore({
    rmailbox: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    durableInbox, isHostedHere,
  });
  const { bus, close: closeBus } = createLivenessBus({ url: REDIS_URL });
  await bus.start();

  const runtime = {
    inboxStore, durableInbox, isHostedHere, livenessBus: bus,
    relayStore: null, metrics: null, inboxClaimRegistry: claimRegistry,
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
  return { server, runtime, durableInbox, nodeIdentity: identity, closeBus };
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

async function startClusterN(t, schema, count) {
  const conn = await createIsolatedPgConnection(PG_URL, schema);
  await new MigrationRunner({ connection: conn }).migrate();
  await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq, inbox_claims");
  const claimRegistry = new PgInboxClaimRegistry({ connection: conn });
  const nodes = [];
  for (let i = 0; i < count; i++) {
    nodes.push(await startClusterNode(conn, claimRegistry, "N" + i));
  }
  t.after(async () => {
    for (const n of nodes) { await n.server.stop(); await n.closeBus(); }
    await conn.close(); await dropSchema(PG_URL, schema);
  });
  return { conn, claimRegistry, nodes };
}

async function startTwoNodeCluster(t, schema) {
  const { conn, claimRegistry, nodes } = await startClusterN(t, schema, 2);
  return { conn, claimRegistry, nodeA: nodes[0], nodeB: nodes[1] };
}

test(
  "offline deposit → reconnect to a DIFFERENT node → catch-up drains it; the device cursor is SHARED cluster state (ack on B honored on A)",
  { skip: SKIP },
  async (t) => {
    const { nodeA, nodeB } = await startTwoNodeCluster(t, "test_e2e_cross_node_catchup");
    const owner = freshClaimantIdentity();
    const inboxId = freshInboxId();
    const DEVICE = "dev:owner";

    // 1) Client claims the inbox on node A (registers the device cursor in shared Pg),
    //    then goes OFFLINE.
    const wsA1 = await openAuthed(t, nodeA.server, owner, DEVICE);
    assert.equal(
      (await claim(wsA1, "cA1", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodeA.nodeIdentity }))).t,
      T.INBOX_CLAIM_RES, "claim on node A succeeds");
    wsA1.close();

    // 2) A deposit lands on node B while the client is offline (shared durable log).
    await nodeB.runtime.inboxStore.depositFromWire(inboxId, wire(10, 10));

    // 3) Client reconnects to node B (a DIFFERENT node), re-claims (register-on-bind),
    //    and catches up from the shared cursor — the offline deposit is delivered inline.
    const wsB1 = await openAuthed(t, nodeB.server, owner, DEVICE);
    assert.equal(
      (await claim(wsB1, "cB1", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodeB.nodeIdentity }))).t,
      T.INBOX_CLAIM_RES, "re-claim on node B (same device) succeeds");
    assert.deepEqual(
      (await listMailbox(wsB1, "lB1", inboxId)).body.items,
      [{ seq: 1, ciphertextB64: b64(10, 10) }],
      "offline deposit on node B is caught up after reconnecting to node B");

    // 4) Consume + cursorAck on node B.
    assert.equal((await cursorAck(wsB1, "aB1", inboxId, 1)).body.lastSeq, 1, "cursorAck on node B advances the shared cursor");
    wsB1.close();

    // 5) Reconnect to node A — the ack made on node B must be visible here: NO redeliver.
    //    This is the cluster invariant: the cursor is shared state, not node-local.
    const wsA2 = await openAuthed(t, nodeA.server, owner, DEVICE);
    await claim(wsA2, "cA2", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodeA.nodeIdentity }));
    assert.deepEqual(
      (await listMailbox(wsA2, "lA2", inboxId)).body.items, [],
      "seq 1 acked on node B is NOT redelivered after reconnecting to node A (shared cursor)");

    // 6) A second offline deposit (this time on node A) is caught up — and only it,
    //    never the already-acked seq 1: the cursor advanced exactly once, cluster-wide.
    wsA2.close();
    await nodeA.runtime.inboxStore.depositFromWire(inboxId, wire(20, 20));
    const wsB2 = await openAuthed(t, nodeB.server, owner, DEVICE);
    await claim(wsB2, "cB2", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodeB.nodeIdentity }));
    assert.deepEqual(
      (await listMailbox(wsB2, "lB2", inboxId)).body.items,
      [{ seq: 2, ciphertextB64: b64(20, 20) }],
      "only the new deposit (seq 2) drains — seq 1 stays acked across the whole cluster");
  },
);

test(
  "real multi-user: two owners claim two inboxes on two different nodes; each catches up only its OWN ciphertext across nodes (per-inbox isolation in the shared backend)",
  { skip: SKIP },
  async (t) => {
    const { nodeA, nodeB } = await startTwoNodeCluster(t, "test_e2e_cross_node_multiuser");
    const userX = freshClaimantIdentity();
    const userY = freshClaimantIdentity();
    const inboxX = freshInboxId();
    const inboxY = freshInboxId();

    // userX claims inboxX on node A; userY claims inboxY on node B.
    const wsX = await openAuthed(t, nodeA.server, userX, "dev:x");
    assert.equal(
      (await claim(wsX, "cx", buildClaimBody({ claimantIdentity: userX, inboxId: inboxX, claimedAtMs: Date.now(), nodeIdentity: nodeA.nodeIdentity }))).t,
      T.INBOX_CLAIM_RES, "userX claims inboxX on node A");
    const wsY = await openAuthed(t, nodeB.server, userY, "dev:y");
    assert.equal(
      (await claim(wsY, "cy", buildClaimBody({ claimantIdentity: userY, inboxId: inboxY, claimedAtMs: Date.now(), nodeIdentity: nodeB.nodeIdentity }))).t,
      T.INBOX_CLAIM_RES, "userY claims inboxY on node B");
    wsX.close(); wsY.close();

    // Cross-node deposits: inboxX gets mail via node B, inboxY via node A.
    await nodeB.runtime.inboxStore.depositFromWire(inboxX, wire(1, 1, 1));
    await nodeA.runtime.inboxStore.depositFromWire(inboxY, wire(2, 2, 2));

    // userX reconnects to node B, userY reconnects to node A — each catches up its own.
    const wsX2 = await openAuthed(t, nodeB.server, userX, "dev:x");
    await claim(wsX2, "cx2", buildClaimBody({ claimantIdentity: userX, inboxId: inboxX, claimedAtMs: Date.now(), nodeIdentity: nodeB.nodeIdentity }));
    const xItems = (await listMailbox(wsX2, "lx", inboxX)).body.items;
    assert.deepEqual(xItems, [{ seq: 1, ciphertextB64: b64(1, 1, 1) }], "userX drains only inboxX's ciphertext");

    const wsY2 = await openAuthed(t, nodeA.server, userY, "dev:y");
    await claim(wsY2, "cy2", buildClaimBody({ claimantIdentity: userY, inboxId: inboxY, claimedAtMs: Date.now(), nodeIdentity: nodeA.nodeIdentity }));
    const yItems = (await listMailbox(wsY2, "ly", inboxY)).body.items;
    assert.deepEqual(yItems, [{ seq: 1, ciphertextB64: b64(2, 2, 2) }], "userY drains only inboxY's ciphertext");

    // Isolation: neither owner's ciphertext appears in the other's drain.
    assert.ok(!xItems.some((i) => i.ciphertextB64 === b64(2, 2, 2)), "userX never sees inboxY's ciphertext");
    assert.ok(!yItems.some((i) => i.ciphertextB64 === b64(1, 1, 1)), "userY never sees inboxX's ciphertext");

    // And userY cannot even authorize a read of inboxX (not its claimant).
    const denied = await listMailbox(wsY2, "lxy", inboxX);
    assert.equal(denied.t, T.ERROR, "a non-claimant is denied listing another owner's inbox");
  },
);

test(
  "cluster soak: one device reconnects to RANDOM nodes under continuous deposits — zero loss, zero dup below the acked watermark",
  { skip: SKIP },
  async (t) => {
    const { nodes } = await startClusterN(t, "test_e2e_cluster_soak", 3);
    const owner = freshClaimantIdentity();
    const inboxId = freshInboxId();
    const DEVICE = "dev:soak";
    const pick = () => nodes[Math.floor(Math.random() * nodes.length)];

    // Claim once up front so isHostedHere is true for every subsequent deposit
    // (an unclaimed inbox would route deposits to the transient buffer, not the
    // durable log, and the cursor model would never see them).
    const wsInit = await openAuthed(t, nodes[0].server, owner, DEVICE);
    assert.equal(
      (await claim(wsInit, "c0", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodes[0].nodeIdentity }))).t,
      T.INBOX_CLAIM_RES, "initial claim succeeds");
    wsInit.close();

    let deposited = 0;
    let ackedThrough = 0;
    const deliveries = new Map(); // seq -> times delivered (>1 = an allowed at-least-once redeliver)

    // Each deposit lands on a RANDOM node (the non-sticky-LB ingress); its body
    // encodes the deposit ordinal for traceability.
    const depositSome = async (n) => {
      for (let i = 0; i < n; i++) {
        deposited += 1;
        await pick().runtime.inboxStore.depositFromWire(inboxId, wire((deposited >> 8) & 0xff, deposited & 0xff));
      }
    };

    // One reconnect cycle: connect to a RANDOM node, re-claim (register-on-bind),
    // drain via mailbox.list, and PROBABILISTICALLY consume+cursorAck. A cycle
    // that lists but does NOT ack models a disconnect mid-consume: those messages
    // must be redelivered next time (at-least-once) but NEVER reappear at or below
    // the acked watermark (the zero-dup-after-cursor-advance invariant).
    const cycle = async (ackIt) => {
      const node = pick();
      const ws = await openAuthed(t, node.server, owner, DEVICE);
      await claim(ws, "c", buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: node.nodeIdentity }));
      const items = (await listMailbox(ws, "l", inboxId)).body.items;
      let maxSeq = ackedThrough;
      for (const it of items) {
        assert.ok(it.seq > ackedThrough, "seq " + it.seq + " re-delivered at/below the acked watermark " + ackedThrough);
        deliveries.set(it.seq, (deliveries.get(it.seq) || 0) + 1);
        if (it.seq > maxSeq) maxSeq = it.seq;
      }
      if (ackIt && maxSeq > ackedThrough) {
        const ack = await cursorAck(ws, "a", inboxId, maxSeq);
        assert.equal(ack.body.lastSeq, maxSeq, "cursorAck advances to the listed high-water");
        ackedThrough = maxSeq;
      }
      ws.close();
    };

    // Interleave continuous deposits with random reconnect-and-(maybe)-consume.
    const CYCLES = 30;
    for (let i = 0; i < CYCLES; i++) {
      await depositSome(1 + Math.floor(Math.random() * 3));
      await cycle(Math.random() < 0.7); // ~70% of cycles actually ack
    }

    // Settle: keep draining + acking (still on random nodes) until fully caught up.
    for (let guard = 0; guard < 60 && ackedThrough < deposited; guard++) {
      await cycle(true);
    }

    assert.equal(ackedThrough, deposited, "every deposit was eventually consumed and acked (zero loss)");
    for (let s = 1; s <= deposited; s++) {
      assert.ok((deliveries.get(s) || 0) >= 1, "seq " + s + " was delivered at least once");
    }
  },
);
