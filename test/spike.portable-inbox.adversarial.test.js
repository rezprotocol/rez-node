import test from "node:test";
import assert from "node:assert/strict";
import nodeCrypto, { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import {
  RMailbox,
  MemoryDataStore,
  MemoryStorageProvider,
  createDefaultRegistry,
  REZ_CONTRACT_TYPES,
  relayKeyIdForNodePublicKeyB64,
  nodeKeyIdForNodePublicKeyB64,
  bytesToBase64,
} from "@rezprotocol/core";
import { UplinkPoolClient, InboxClaimStore } from "@rezprotocol/sdk";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry, INBOX_LIFECYCLE } from "../src/inbox/InboxClaimRegistry.js";
import { RetentionPolicy } from "../src/inbox/RetentionPolicy.js";
import { InboxLifecycleSweeper } from "../src/inbox/InboxLifecycleSweeper.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import {
  createServerServices,
  createPerAccountServices,
  createProtocolFactory,
  createDepositHandler,
} from "./helpers/nodeTestServices.js";
import { createNodeTestIdentity } from "./helpers/wsAuth.js";

// THE ADVERSARIAL PORTABLE-INBOX SPIKE (plans/PORTABLE_INBOX_LEASE_SPEC.md §9;
// PORTABLE_HOME.md Phase 1). Root OFFLINE throughout — structurally: no
// account root key is ever GENERATED in this file. Every session is claimant-
// mode; every lifecycle verdict must be reconstructible from durable state +
// now (the provider "restarts" by discarding every in-memory object and
// rehydrating from the shared storage), and the frame log of each scenario
// ends with the mechanical zero-identity assertion.
//
// Time: SHORT REAL DURATIONS with generous margins stand in for the
// 1h/1d/…/30d matrix (the spec's sanctioned alternative to wall-clock).
// EXACT ±1ms boundary semantics are pinned deterministically in
// inbox.lease-l2.lifecycle.test.js against the same pure verdict function
// this spike exercises end-to-end.

const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Short-real-time retention policy: lease grace 1.5s, terminal grace 1.5s.
const POLICY = new RetentionPolicy({
  standardLeaseGraceMs: 1_500,
  standardTerminalGraceMs: 1_500,
  transientTerminalGraceMs: 1_500,
});

function getFreePort() {
  return new Promise((resolve, reject) => {
    import("node:net").then(({ default: net }) => {
      const server = net.createServer();
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const addr = server.address();
        const port = addr && typeof addr === "object" ? addr.port : 0;
        server.close((err) => (err ? reject(err) : resolve(port)));
      });
    });
  });
}

/**
 * "Durable disk" shared across provider incarnations: the claim-registry KV
 * and the ciphertext store survive; every in-memory object (registry
 * instance, server, sessions) is discarded on restart.
 */
function makeDurableDisk() {
  return {
    storageProvider: new MemoryStorageProvider(),
    mailData: new MemoryDataStore(),
  };
}

async function bootProvider({ disk, identity, wsPort }) {
  const inboxClaimRegistry = new InboxClaimRegistry({
    storageProvider: disk.storageProvider,
    retentionPolicy: POLICY,
  });
  await inboxClaimRegistry.hydrate();
  const inboxStore = new RMailbox({ store: disk.mailData, registry: createDefaultRegistry() });
  const runtime = {
    inboxStore,
    // Local-delivery gateway stub: every spike inbox is hosted on THIS
    // provider, so routing collapses to a local RMailbox deposit (the same
    // convergence point production's GatewayLoop reaches for a local inbox).
    gatewayLoop: {
      async sendToInbox({ deliverInboxId, innerBytes }) {
        const deposited = await inboxStore.depositFromWire(deliverInboxId, innerBytes);
        return { packetId: deposited && deposited.eventId ? deposited.eventId : "evt" };
      },
    },
    relayStore: null,
    metrics: null,
    inboxClaimRegistry,
    accountAuthorityRevocationCache: null,
    accountDeviceRegistry: { async isTerminallyRevoked() { return false; }, async isTerminallyRevokedInTx() { return false; } },
    serverServices: createServerServices({ storageProvider: disk.storageProvider, clock: () => Date.now(), ownerAccountId: identity.accountId }),
    serviceCache: new PerAccountServiceCache({ storageProvider: disk.storageProvider, clock: () => Date.now(), createServices: createPerAccountServices }),
    getIdentity() { return { ...identity }; },
    getMeshStatus() { return { enabled: true, mode: "seeded-gossip", participateInRouting: true, peerCount: 0 }; },
    async stop() {},
  };
  const server = new WsGatewayServer({
    runtime,
    port: wsPort,
    protocolFactory: createProtocolFactory(),
    onInboundDeposit: createDepositHandler({ crypto: CRYPTO }),
  });
  await server.start();
  const sweeper = new InboxLifecycleSweeper({ registry: inboxClaimRegistry, inboxStore, now: Date.now });
  return { server, registry: inboxClaimRegistry, inboxStore, sweeper };
}

function providerIdentity() {
  const base = createNodeTestIdentity({
    accountId: "rez:node:spike:" + randomBytes(4).toString("hex"),
    deviceId: "dev:spike",
    localInboxId: "inbox:spike",
  });
  base.nodeKeyId = nodeKeyIdForNodePublicKeyB64(base.nodePublicKeyB64);
  base.relayKeyId = relayKeyIdForNodePublicKeyB64(base.nodePublicKeyB64);
  return base;
}

/** SDK-format claimant key (SPKI/PKCS8 DER b64 — what a real client holds). */
function sdkKey() {
  const { publicKey, privateKey } = nodeCrypto.generateKeyPairSync("ed25519");
  return {
    publicKeyB64: Buffer.from(publicKey.export({ format: "der", type: "spki" })).toString("base64"),
    privateKeyB64: Buffer.from(privateKey.export({ format: "der", type: "pkcs8" })).toString("base64"),
  };
}

/** Frame-logging claimant client. Every frame from every socket lands in log.frames. */
function makeClient({ wsUrl, claimant, log }) {
  const wsFactory = (url) => {
    const ws = new WebSocket(url);
    const realSend = ws.send.bind(ws);
    ws.send = (data, ...rest) => {
      try { log.frames.push(JSON.parse(String(data))); } catch { /* non-JSON */ }
      return realSend(data, ...rest);
    };
    return ws;
  };
  return new UplinkPoolClient({
    uplinks: [wsUrl],
    claimantIdentity: { claimantPublicKeyB64: claimant.publicKeyB64, privateKeyB64: claimant.privateKeyB64 },
    wsFactory,
    warmSpareCount: 0,
  });
}

/** Claim/renew through the real wire, exactly as the chat server does. */
async function claimOrRenew({ client, store, inboxId, identity, ttlMs, retentionClass = "standard" }) {
  const attestation = await store.createReattestation(inboxId);
  const delegation = await store.createNodeDelegation({
    inboxId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    relayKeyId: identity.relayKeyId,
    ttlMs,
    retentionClass,
  });
  const body = {
    inboxId: attestation.inboxId,
    claimantPublicKeyB64: attestation.claimantPublicKeyB64,
    closePublicKeyB64: attestation.closePublicKeyB64,
    generation: attestation.generation,
    claimedAtMs: attestation.claimedAtMs,
    signatureB64: attestation.claimSignatureB64,
    nodeDelegation: {
      nodeKeyId: delegation.nodeKeyId,
      nodePublicKeyB64: delegation.nodePublicKeyB64,
      relayKeyId: delegation.relayKeyId,
      issuedAtMs: delegation.issuedAtMs,
      expiresAtMs: delegation.expiresAtMs,
      delegationSigB64: delegation.delegationSigB64,
      generation: delegation.generation,
      retentionClass: delegation.retentionClass,
    },
  };
  return client.request(T.INBOX_CLAIM, body);
}

/** The mechanical zero-identity proof, run at the end of EVERY scenario. */
function assertZeroIdentity(log) {
  const hellos = log.frames.filter((f) => String(f.t || f.type || "") === T.SESSION_HELLO);
  assert.ok(hellos.length >= 1, "the scenario authenticated at least once");
  for (const hello of hellos) {
    assert.equal(hello.body.authMode, "claimant", "every hello on every socket is claimant-mode");
  }
  const allBytes = JSON.stringify(log.frames);
  assert.ok(!allBytes.includes("\"accountIdentityPublicKeyB64\""), "no account identity field in ANY frame");
  assert.ok(!allBytes.includes("\"deviceId\""), "no device identifier field in ANY frame");
  assert.ok(!allBytes.includes("\"authMode\":\"account\""), "no account-mode handshake ever occurred");
}

async function newClaimStore() {
  const store = new InboxClaimStore({ storageProvider: new MemoryStorageProvider(), cryptoProvider: CRYPTO });
  await store.hydrate();
  return store;
}

// ─────────────────────────────────────────────────────────────────────────────

test("SPIKE 1 — the heart case: suspend, expire DURING provider downtime, restart, wake in grace, renew — mail intact, cursor state intact, zero identity", async (t) => {
  const identity = providerIdentity();
  const disk = makeDurableDisk();
  const wsPort = await getFreePort();
  const log = { frames: [] };

  let provider = await bootProvider({ disk, identity, wsPort });
  t.after(() => provider.server.stop().catch(() => {}));

  // Recipient device claims its per-device inbox: standard class, short lease.
  const recipientStore = await newClaimStore();
  const claim = await recipientStore.persist(await recipientStore.createClaim());
  const recipient = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: claim.claimantPublicKeyB64, privateKeyB64: recipientStore.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => recipient.close().catch(() => {}));
  await recipient.connect();
  await claimOrRenew({ client: recipient, store: recipientStore, inboxId: claim.inboxId, identity, ttlMs: 1_200 });
  const leaseIssuedAt = Date.now();

  // Route-survival guardrail (the HostedInboxRegistry lesson, end-to-end):
  // the signed lease fields survived the ENTIRE wire route into durable state.
  const stored = provider.registry.getClaim(claim.inboxId);
  assert.equal(stored.generation, 1);
  assert.equal(stored.retentionClass, "standard");
  assert.ok(stored.leaseExpiresAtMs > leaseIssuedAt, "lease expiry persisted from the signed delegation");

  // A sender (its own claimant session — senders are anonymous too) deposits m1.
  const sender = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: sdkKey(), log });
  t.after(() => sender.close().catch(() => {}));
  await sender.connect();
  await sender.request(T.MAILBOX_DEPOSIT, { mailboxId: claim.inboxId, ciphertextB64: "bTE=" });
  const before = await recipient.request(T.MAILBOX_LIST, { mailboxId: claim.inboxId });
  assert.equal(before.items.length, 1, "m1 retained while ACTIVE");

  // SUSPEND the device and KILL the provider. The lease expires while
  // NOTHING is running — no process exists to own a timer.
  await recipient.close();
  await sender.close();
  await provider.server.stop();
  await sleep(1_500); // past expiry (1.2s), inside grace (ends at 2.7s)

  // RESTART: every in-memory object is new; only the disk survived.
  provider = await bootProvider({ disk, identity, wsPort });
  t.after(() => provider.server.stop().catch(() => {}));
  assert.equal(provider.registry.lifecycleFor(claim.inboxId, Date.now()).state, INBOX_LIFECYCLE.CLOSED_EXPIRED,
    "the restarted provider derives CLOSED_EXPIRED from disk + now — no timer ever fired");

  // A sender's deposit during grace is refused RETRYABLY.
  const sender2 = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: sdkKey(), log });
  t.after(() => sender2.close().catch(() => {}));
  await sender2.connect();
  await assert.rejects(
    () => sender2.request(T.MAILBOX_DEPOSIT, { mailboxId: claim.inboxId, ciphertextB64: "bTI=" }),
    (err) => err && err.code === "LEASE_EXPIRED" && err.retryable === true,
    "expired-grace refusals invite retry — the recipient may renew",
  );

  // The device WAKES late (the five-minutes-late phone): same persisted claim
  // store, fresh session, renewal during grace.
  const woken = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: claim.claimantPublicKeyB64, privateKeyB64: recipientStore.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => woken.close().catch(() => {}));
  await woken.connect();
  await claimOrRenew({ client: woken, store: recipientStore, inboxId: claim.inboxId, identity, ttlMs: 60_000 });
  assert.equal(provider.registry.lifecycleFor(claim.inboxId, Date.now()).state, INBOX_LIFECYCLE.ACTIVE,
    "renewal during grace restored ACTIVE");

  // Admission is back; the retried deposit lands; ALL mail is intact.
  await sender2.request(T.MAILBOX_DEPOSIT, { mailboxId: claim.inboxId, ciphertextB64: "bTI=" });
  const after = await woken.request(T.MAILBOX_LIST, { mailboxId: claim.inboxId });
  assert.equal(after.items.length, 2, "m1 survived suspend + expiry + provider restart; m2 arrived after renewal");

  assertZeroIdentity(log);
});

test("SPIKE 2 — grace lapses: renewal refused, sweep reclaims storage, the generation is dead forever", async (t) => {
  const identity = providerIdentity();
  const disk = makeDurableDisk();
  const wsPort = await getFreePort();
  const log = { frames: [] };
  const provider = await bootProvider({ disk, identity, wsPort });
  t.after(() => provider.server.stop().catch(() => {}));

  const store = await newClaimStore();
  const claim = await store.persist(await store.createClaim());
  const device = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: claim.claimantPublicKeyB64, privateKeyB64: store.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => device.close().catch(() => {}));
  await device.connect();
  await claimOrRenew({ client: device, store, inboxId: claim.inboxId, identity, ttlMs: 600 });
  await device.request(T.MAILBOX_DEPOSIT, { mailboxId: claim.inboxId, ciphertextB64: "bTE=" });
  await device.close();

  await sleep(2_400); // past expiry (0.6s) AND past grace (ends 2.1s)

  // Renewal after grace is refused — the verdict says RECLAIMABLE whether or
  // not the sweep has run yet.
  const late = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: claim.claimantPublicKeyB64, privateKeyB64: store.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => late.close().catch(() => {}));
  await late.connect();
  await assert.rejects(
    () => claimOrRenew({ client: late, store, inboxId: claim.inboxId, identity, ttlMs: 60_000 }),
    (err) => err && err.code === "LEASE_EXPIRED",
    "past grace, renewal is refused",
  );

  // The sweep reclaims: claim gone, ciphertext gone, tombstone remains.
  const swept = await provider.sweeper.sweepOnce();
  assert.deepEqual(swept.reclaimed, [claim.inboxId]);
  assert.equal(provider.registry.getClaim(claim.inboxId), null);
  const remaining = await provider.inboxStore.list(claim.inboxId, { limit: 10 });
  assert.equal(remaining.items.length, 0, "RECLAIMED: bytes gone");
  assert.equal(provider.registry.getTombstone(claim.inboxId).reason, "reclaimed");

  // The lifetime is dead: a fresh claim of the same inboxId at generation 1
  // (a stale-lifetime replay) can never re-activate it.
  const replayStore = await newClaimStore();
  const replay = await replayStore.persist(await replayStore.createClaim({ inboxId: claim.inboxId }));
  const replayClient = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: replay.claimantPublicKeyB64, privateKeyB64: replayStore.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => replayClient.close().catch(() => {}));
  await replayClient.connect();
  await assert.rejects(
    () => claimOrRenew({ client: replayClient, store: replayStore, inboxId: claim.inboxId, identity, ttlMs: 60_000 }),
    (err) => err && err.code === "INBOX_CLOSED",
    "want the address back? mint a new inboxId — this lifetime is over",
  );

  assertZeroIdentity(log);
});

test("SPIKE 3 — terminal close survives the nastiest ordering: close → newer lease → provider restart → reconnect; drain works through grace; then reclamation", async (t) => {
  const identity = providerIdentity();
  const disk = makeDurableDisk();
  const wsPort = await getFreePort();
  const log = { frames: [] };

  let provider = await bootProvider({ disk, identity, wsPort });
  t.after(() => provider.server.stop().catch(() => {}));

  const store = await newClaimStore();
  const claim = await store.persist(await store.createClaim());
  const device = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: claim.claimantPublicKeyB64, privateKeyB64: store.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => device.close().catch(() => {}));
  await device.connect();
  await claimOrRenew({ client: device, store, inboxId: claim.inboxId, identity, ttlMs: 60_000 }); // lease NOT the limiting factor
  await device.request(T.MAILBOX_DEPOSIT, { mailboxId: claim.inboxId, ciphertextB64: "bTM=" });

  // TERMINAL CLOSE — the record authorizes itself, carried over this same
  // claimant session (the account never touches the wire, even to kill).
  const close = await store.createTerminalClose(claim.inboxId);
  const closed = await device.request(T.INBOX_CLOSE, close.toJSON());
  assert.equal(closed.closed, true);

  // Admission is dead immediately; drain still works (drain-then-die).
  await assert.rejects(
    () => device.request(T.MAILBOX_DEPOSIT, { mailboxId: claim.inboxId, ciphertextB64: "eA==" }),
    (err) => err && err.code === "INBOX_CLOSED" && err.retryable === false,
  );
  const drained = await device.request(T.MAILBOX_LIST, { mailboxId: claim.inboxId });
  assert.equal(drained.items.length, 1, "retained ciphertext stays drainable through the terminal grace window");

  // HOSTILE: a newer-looking lease arrives — refused.
  await assert.rejects(
    () => claimOrRenew({ client: device, store, inboxId: claim.inboxId, identity, ttlMs: 120_000 }),
    (err) => err && err.code === "INBOX_CLOSED",
    "no lease resurrects a terminal close",
  );

  // PROVIDER RESTART, then the claimant reconnects and tries again.
  await device.close();
  await provider.server.stop();
  provider = await bootProvider({ disk, identity, wsPort });
  t.after(() => provider.server.stop().catch(() => {}));
  const reconnected = makeClient({ wsUrl: "ws://127.0.0.1:" + wsPort + "/ws", claimant: { publicKeyB64: claim.claimantPublicKeyB64, privateKeyB64: store.get(claim.inboxId).claimantPrivateKeyB64 }, log });
  t.after(() => reconnected.close().catch(() => {}));
  await reconnected.connect();
  await assert.rejects(
    () => claimOrRenew({ client: reconnected, store, inboxId: claim.inboxId, identity, ttlMs: 120_000 }),
    (err) => err && err.code === "INBOX_CLOSED",
    "the tombstone wins forever, across restarts",
  );

  // Terminal grace lapses → the sweep reclaims; the tombstone REMAINS.
  await sleep(1_700);
  const swept = await provider.sweeper.sweepOnce();
  assert.deepEqual(swept.reclaimed, [claim.inboxId]);
  assert.equal((await provider.inboxStore.list(claim.inboxId, { limit: 10 })).items.length, 0, "RECLAIMED: bytes gone");
  assert.equal(provider.registry.getClaim(claim.inboxId), null);
  assert.equal(provider.registry.getTombstone(claim.inboxId).finalGeneration, 1, "the tombstone outlives the storage");

  assertZeroIdentity(log);
});
