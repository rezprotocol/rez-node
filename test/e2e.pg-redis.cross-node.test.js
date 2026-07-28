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
import { pgTestUrl, redisTestUrl } from "./support/integrationBackends.js";

const PG_URL = pgTestUrl();
const REDIS_URL = redisTestUrl();
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();

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

// One cluster node: its own WS server + LivenessBus + PgDurableInbox instance
// (per-node setOnDeposit hook), all over the SHARED Pg connection + claim
// registry + Redis — exactly the hosted-cluster topology.
async function startClusterNode(conn, claimRegistry, relayKeyIdSuffix) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:" + relayKeyIdSuffix, deviceId: "dev:" + relayKeyIdSuffix, localInboxId: "inbox:node",
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

test(
  "cross-node: a deposit on node B is pushed in real time to the client's socket on node A (LivenessBus)",
  { skip: (PG_URL && REDIS_URL) ? false : "set REZ_PG_TEST_URL and REZ_REDIS_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_e2e_cross_node";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq, inbox_claims");

    // Shared cluster state: one Pg connection + one claim registry, two nodes.
    const claimRegistry = new PgInboxClaimRegistry({ connection: conn });
    const nodeA = await startClusterNode(conn, claimRegistry, "A");
    const nodeB = await startClusterNode(conn, claimRegistry, "B");
    t.after(async () => {
      await nodeA.server.stop(); await nodeB.server.stop();
      await nodeA.closeBus(); await nodeB.closeBus();
      await conn.close(); await dropSchema(PG_URL, SCHEMA);
    });

    const owner = freshClaimantIdentity();
    const inboxId = freshInboxId();
    const DEVICE = "dev:owner";

    // Client connects to node A and claims the inbox there (→ device cursor in
    // shared Pg, and node A subscribes to the bus for this inbox).
    const wsA = await openAuthed(t, nodeA.server, owner, DEVICE);
    wsA.send(JSON.stringify({
      id: "claim", type: T.INBOX_CLAIM, t: T.INBOX_CLAIM, v: CONTRACT_VERSION,
      body: buildClaimBody({ claimantIdentity: owner, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodeA.nodeIdentity }),
    }));
    const claimRes = await waitForMessage(wsA, (m) => m.id === "claim");
    assert.equal(claimRes.t, T.INBOX_CLAIM_RES, "claim on node A succeeds");

    // Arm the EVT listener BEFORE depositing so we can't miss the push.
    const evtPromise = waitForMessage(wsA, (m) => m.t === T.EVT_MAILBOX_DEPOSITED && m.body && m.body.mailboxId === inboxId);

    // Deposit lands on node B (which holds NO socket for this inbox). B persists
    // to the shared durable log and — per the Option-Y gate — pings the bus.
    const ciphertext = new Uint8Array([42, 43, 44]);
    await nodeB.runtime.inboxStore.depositFromWire(inboxId, encodeOuterPacket({ bodyBytes: ciphertext }));

    // Node A receives the ping, drains the durable log from the device cursor,
    // and pushes the deposit to the client's socket — no reconnect.
    const evt = await evtPromise;
    assert.equal(evt.body.seq, 1, "carries the durable seq for cursor-model dedupe");
    assert.equal(evt.body.ciphertextB64, Buffer.from(ciphertext).toString("base64"),
      "delivers the decoded ciphertext across nodes");

    // Track every subsequent EVT to prove push-once: a 2nd deposit (no cursorAck
    // in between, so the consumed cursor is still 0) must deliver ONLY seq 2 —
    // the readUndelivered watermark prevents re-draining seq 1 (the P1 fix).
    const seqsAfter = [];
    wsA.on("message", (data) => {
      let f; try { f = JSON.parse(data.toString("utf8")); } catch { return; }
      if (f && f.t === T.EVT_MAILBOX_DEPOSITED && f.body && f.body.mailboxId === inboxId) seqsAfter.push(f.body.seq);
    });
    const evt2Promise = waitForMessage(wsA, (m) => m.t === T.EVT_MAILBOX_DEPOSITED && m.body && m.body.seq === 2);
    await nodeB.runtime.inboxStore.depositFromWire(inboxId, encodeOuterPacket({ bodyBytes: new Uint8Array([99]) }));
    await evt2Promise;
    assert.ok(!seqsAfter.includes(1), "seq 1 is NOT re-pushed by the 2nd deposit's ping (no re-drain)");
  },
);

test(
  "cross-node drain direct-sends to the claiming socket, NOT other sessions under the same auth owner (P3 privacy path)",
  { skip: (PG_URL && REDIS_URL) ? false : "set REZ_PG_TEST_URL and REZ_REDIS_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_e2e_cross_node_privacy";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq, inbox_claims");
    const claimRegistry = new PgInboxClaimRegistry({ connection: conn });
    const nodeA = await startClusterNode(conn, claimRegistry, "A");
    const nodeB = await startClusterNode(conn, claimRegistry, "B");
    t.after(async () => {
      await nodeA.server.stop(); await nodeB.server.stop();
      await nodeA.closeBus(); await nodeB.closeBus();
      await conn.close(); await dropSchema(PG_URL, SCHEMA);
    });

    // Privacy multi-key path: the session authenticates as ACCOUNT but claims the
    // inbox under a DISTINCT claimant key (cap model §8). A SECOND session under
    // the same ACCOUNT does NOT claim it.
    const account = freshClaimantIdentity();
    const claimant = freshClaimantIdentity(); // distinct from the auth identity
    const inboxId = freshInboxId();

    // wsClaim: auth=ACCOUNT, claims inboxId under `claimant`.
    const wsClaim = await openAuthed(t, nodeA.server, account, "dev:claim");
    wsClaim.send(JSON.stringify({
      id: "claim", type: T.INBOX_CLAIM, t: T.INBOX_CLAIM, v: CONTRACT_VERSION,
      body: buildClaimBody({ claimantIdentity: claimant, inboxId, claimedAtMs: Date.now(), nodeIdentity: nodeA.nodeIdentity }),
    }));
    assert.equal((await waitForMessage(wsClaim, (m) => m.id === "claim")).t, T.INBOX_CLAIM_RES);

    // wsOther: same ACCOUNT auth, on node A, but never claims this inbox. Under
    // the old owner-bucket broadcast it would receive the claimed inbox's
    // ciphertext; the direct-send fix must exclude it.
    const wsOther = await openAuthed(t, nodeA.server, account, "dev:other");
    const otherLeak = [];
    wsOther.on("message", (data) => {
      let f; try { f = JSON.parse(data.toString("utf8")); } catch { return; }
      if (f && f.t === T.EVT_MAILBOX_DEPOSITED && f.body && f.body.mailboxId === inboxId) otherLeak.push(f.body.seq);
    });

    const claimEvt = waitForMessage(wsClaim, (m) => m.t === T.EVT_MAILBOX_DEPOSITED && m.body && m.body.mailboxId === inboxId);
    await nodeB.runtime.inboxStore.depositFromWire(inboxId, encodeOuterPacket({ bodyBytes: new Uint8Array([7, 7]) }));
    const evt = await claimEvt;
    assert.equal(evt.body.seq, 1, "the claiming socket receives the cross-node push");

    // Give any (erroneous) broadcast to the other same-owner socket time to land,
    // then assert it received nothing for this claimed inbox.
    await new Promise((r) => setTimeout(r, 250));
    assert.deepEqual(otherLeak, [], "a same-auth-owner session that did NOT claim the inbox gets no ciphertext");
  },
);
