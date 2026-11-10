import test from "node:test";
import assert from "node:assert/strict";
import { WebSocket } from "ws";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import fs from "node:fs/promises";
import path from "node:path";
import {
  CONTRACT_VERSION,
  REZ_CONTRACT_TYPES,
  bytesToBase64,
  canonicalJSONStringify,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  DeviceRegistrationV1,
  DEVICE_REGISTRATION_PURPOSE,
} from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import {
  authenticateSession,
  createClaimantNodeDelegation,
  createSessionIdentity,
} from "./helpers/wsAuth.js";

const URL = String(process.env.REZ_HOSTED_E2E_URL || "").trim();
const RUN = URL !== "";
const EXPECT_LIVE = process.env.REZ_HOSTED_E2E_EXPECT_LIVE !== "0";
const INSECURE_TLS = process.env.REZ_HOSTED_E2E_INSECURE_TLS === "1";
const RECOVERY_ROOT = String(process.env.REZ_HOSTED_RECOVERY_ROOT || "");
const RECOVERY = RECOVERY_ROOT.length > 0;
const SOAK_MS = Number(process.env.REZ_HOSTED_SOAK_MS || 0);
assert.ok(Number.isSafeInteger(SOAK_MS) && SOAK_MS >= 0 && SOAK_MS <= 21_600_000,
  "REZ_HOSTED_SOAK_MS must be an integer between zero and six hours");
if (SOAK_MS > 0) assert.match(URL, /^wss:\/\//, "live soak requires TLS");
assert.ok(!RECOVERY || SOAK_MS === 0, "run destructive recovery and the live soak separately");
const exec = promisify(execFile);
const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();

function waitForMessage(ws, predicate, timeoutMs = 10_000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for hosted-cluster frame"));
    }, timeoutMs);
    function cleanup() {
      clearTimeout(timer);
      ws.off("message", onMessage);
      ws.off("error", onError);
      ws.off("close", onClose);
    }
    function onError(err) { cleanup(); reject(err); }
    function onClose() { cleanup(); reject(new Error("hosted-cluster socket closed before expected frame")); }
    function onMessage(data) {
      let frame;
      try { frame = JSON.parse(data.toString("utf8")); } catch { return; }
      if (!predicate(frame)) return;
      cleanup();
      resolve(frame);
    }
    ws.on("message", onMessage);
    ws.on("error", onError);
    ws.on("close", onClose);
  });
}

function send(ws, id, type, body) {
  ws.send(JSON.stringify({ id, type, t: type, v: CONTRACT_VERSION, body }));
}

async function openAuthenticated(identity, deviceId) {
  const ws = new WebSocket(URL, { rejectUnauthorized: !INSECURE_TLS });
  try {
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  const auth = await authenticateSession({ ws, waitForMessage, identity, deviceId, timeoutMs: 10_000 });
  return { ws, node: auth.challenge.body };
  } catch (err) {
    ws.terminate();
    throw err;
  }
}

async function openOnDifferentNode(identity, deviceId, excludedNodeKeyId) {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const opened = await openAuthenticated(identity, deviceId);
    if (opened.node.nodeKeyId !== excludedNodeKeyId) return opened;
    opened.ws.close();
  }
  throw new Error("load balancer never reached a second hosted node identity");
}

function claimBody(identity, inboxId, node) {
  const claimedAtMs = Date.now();
  const claimantPublicKeyB64 = identity.accountIdentityPublicKeyB64;
  const signatureB64 = bytesToBase64(CRYPTO.sign({
    privateKey: identity.privateKey,
    msg: new TextEncoder().encode(canonicalJSONStringify({ inboxId, claimantPublicKeyB64, claimedAtMs })),
  }));
  const delegation = createClaimantNodeDelegation({
    claimantIdentity: identity,
    inboxId,
    nodeKeyId: node.nodeKeyId,
    nodePublicKeyB64: node.nodePublicKeyB64,
    relayKeyId: node.relayKeyId,
  });
  return {
    inboxId,
    claimantPublicKeyB64,
    claimedAtMs,
    signatureB64,
    nodeDelegation: {
      nodeKeyId: delegation.nodeKeyId,
      nodePublicKeyB64: delegation.nodePublicKeyB64,
      relayKeyId: delegation.relayKeyId,
      issuedAtMs: delegation.issuedAtMs,
      expiresAtMs: delegation.expiresAtMs,
      delegationSigB64: delegation.delegationSigB64,
    },
  };
}

async function claim(opened, identity, inboxId, id) {
  send(opened.ws, id, T.INBOX_CLAIM, claimBody(identity, inboxId, opened.node));
  const response = await waitForMessage(opened.ws, (frame) => frame.id === id);
  assert.equal(response.t, T.INBOX_CLAIM_RES, "inbox claim must succeed on either node");
}

function rootDeviceProofs(identity, inboxId) {
  const devicePublicKeyB64 = identity.accountIdentityPublicKeyB64;
  const deviceId = DeviceRegistrationV1.deviceIdFor(devicePublicKeyB64);
  const issuedAtMs = Date.now() - 1_000;
  const expiresAtMs = issuedAtMs + 3_600_000;
  const registrationBody = {
    v: 1,
    purpose: DEVICE_REGISTRATION_PURPOSE,
    accountIdentityPublicKeyB64: identity.accountIdentityPublicKeyB64,
    devicePublicKeyB64,
    deviceId,
    issuedAtMs,
    expiresAtMs,
  };
  const bindingBody = {
    v: 1,
    purpose: DEVICE_INBOX_BINDING_PURPOSE,
    devicePublicKeyB64,
    deviceId,
    inboxId,
    issuedAtMs,
    expiresAtMs,
  };
  const sign = (bytes) => ({
    alg: "ed25519",
    sigB64: bytesToBase64(CRYPTO.sign({ privateKey: identity.privateKey, msg: bytes })),
  });
  return {
    deviceId,
    registration: {
      ...registrationBody,
      sig: sign(DeviceRegistrationV1.signableBytes(registrationBody)),
    },
    binding: {
      ...bindingBody,
      sig: sign(DeviceInboxBindingV1.signableBytes(bindingBody)),
    },
  };
}

async function bindDevice(opened, proofs, id) {
  send(opened.ws, id, T.DEVICE_BIND, {
    deviceRegistration: proofs.registration,
    deviceInboxBinding: proofs.binding,
  });
  const response = await waitForMessage(opened.ws, (frame) => frame.id === id);
  assert.equal(response.t, T.DEVICE_BIND_RES, "proven device bind must succeed in fan-out mode");
}

async function list(opened, inboxId, id) {
  send(opened.ws, id, T.MAILBOX_LIST, { mailboxId: inboxId, limit: 50 });
  return waitForMessage(opened.ws, (frame) => frame.id === id);
}

async function ack(opened, inboxId, throughSeq, id) {
  send(opened.ws, id, T.MAILBOX_CURSOR_ACK, { mailboxId: inboxId, throughSeq });
  return waitForMessage(opened.ws, (frame) => frame.id === id);
}

async function compose(...args) {
  assert.match(RECOVERY_ROOT, /^\/tmp\/rez-network-proof-[0-9]+$/);
  assert.match(URL, /^wss:\/\/localhost:/, "destructive recovery probes require the isolated local fixture");
  return exec("docker", ["compose", "--project-name", path.basename(RECOVERY_ROOT),
    "--env-file", path.join(RECOVERY_ROOT, "rez-node/deploy/.env"),
    "--file", path.join(RECOVERY_ROOT, "rez-node/deploy/docker-compose.yml"),
    "--file", path.join(RECOVERY_ROOT, "compose.override.json"), ...args], { timeout: 90_000, maxBuffer: 2 * 1024 * 1024 });
}

test("shipping hosted topology: TLS + non-sticky two-node delivery + shared cursor + tenant isolation", { skip: !RUN, timeout: Math.max(RECOVERY ? 300_000 : 120_000, SOAK_MS + 120_000) }, async (t) => {
  const recipient = createSessionIdentity();
  const sender = createSessionIdentity();
  const otherTenant = createSessionIdentity();
  const inbox = "inbox:hosted:" + Buffer.from(CRYPTO.randomBytes(12)).toString("hex");
  const otherInbox = "inbox:hosted:" + Buffer.from(CRYPTO.randomBytes(12)).toString("hex");
  const recipientDevice = rootDeviceProofs(recipient, inbox);
  const sockets = [];
  t.after(() => sockets.forEach((ws) => ws.close()));

  const onA = await openAuthenticated(recipient, recipientDevice.deviceId);
  sockets.push(onA.ws);
  await claim(onA, recipient, inbox, "claim-a");
  await bindDevice(onA, recipientDevice, "bind-a");

  const onB = await openOnDifferentNode(sender, "dev:sender", onA.node.nodeKeyId);
  sockets.push(onB.ws);
  assert.notEqual(onA.node.nodeKeyId, onB.node.nodeKeyId, "TLS load balancer reaches two distinct node identities");

  const liveEvent = EXPECT_LIVE
    ? waitForMessage(onA.ws, (frame) => frame.t === T.EVT_MAILBOX_DEPOSITED && frame.body && frame.body.mailboxId === inbox)
    : null;
  const ciphertextB64 = Buffer.from(new Uint8Array([11, 22, 33, 44])).toString("base64");
  send(onB.ws, "deposit-b", T.MAILBOX_DEPOSIT, {
    mailboxId: inbox,
    objectId: "hosted-smoke-object",
    ciphertextB64,
    metadata: {},
  });
  const deposit = await waitForMessage(onB.ws, (frame) => frame.id === "deposit-b");
  assert.equal(deposit.t, T.MAILBOX_DEPOSIT_RES, "deposit through the other node succeeds");
  if (liveEvent) {
    const pushed = await liveEvent;
    assert.equal(pushed.body.ciphertextB64, ciphertextB64, "Redis wakes the socket held by the other node");
    assert.equal(pushed.body.seq, 1);
  }

  onA.ws.close();
  const recipientOnB = await openOnDifferentNode(recipient, recipientDevice.deviceId, onA.node.nodeKeyId);
  sockets.push(recipientOnB.ws);
  await claim(recipientOnB, recipient, inbox, "claim-b");
  const caughtUp = await list(recipientOnB, inbox, "list-b");
  assert.deepEqual(caughtUp.body.items, [{ seq: 1, ciphertextB64 }], "reconnect on another node drains the durable log");
  const acked = await ack(recipientOnB, inbox, 1, "ack-b");
  assert.equal(acked.body.lastSeq, 1);
  recipientOnB.ws.close();

  const recipientAgain = await openAuthenticated(recipient, recipientDevice.deviceId);
  sockets.push(recipientAgain.ws);
  await claim(recipientAgain, recipient, inbox, "claim-again");
  const empty = await list(recipientAgain, inbox, "list-again");
  assert.deepEqual(empty.body.items, [], "shared cursor prevents redelivery after another-node ack");

  const other = await openAuthenticated(otherTenant, "dev:other");
  sockets.push(other.ws);
  await claim(other, otherTenant, otherInbox, "claim-other");
  const denied = await list(other, inbox, "cross-tenant-read");
  assert.equal(denied.t, T.ERROR, "another tenant cannot read the recipient inbox");

  if (SOAK_MS > 0) await t.test("monitored live soak preserves exact bytes, shared cursors and tenant isolation", { timeout: SOAK_MS + 60_000 }, async () => {
    const started = Date.now();
    const deadline = started + SOAK_MS;
    const readyUrl = URL.replace("wss:", "https:").replace(/\/ws$/, "/ready");
    const nodeIds = new Set([recipientAgain.node.nodeKeyId]);
    let current = recipientAgain;
    let sent = 0;
    let reconnects = 0;
    let tenantChecks = 0;
    let worstRoundTripMs = 0;
    let nextReport = started;
    while (Date.now() < deadline) {
      if (sent % 5 === 0) {
        current.ws.close();
        current = await openAuthenticated(recipient, recipientDevice.deviceId);
        sockets.push(current.ws);
        nodeIds.add(current.node.nodeKeyId);
        await claim(current, recipient, inbox, "soak-claim-" + sent);
        assert.deepEqual((await list(current, inbox, "soak-cursor-" + sent)).body.items, [], "acknowledged data must not reappear on reconnect");
        reconnects += 1;
      }
      const sequence = sent + 2;
      const value = Buffer.from(CRYPTO.randomBytes(64)).toString("base64");
      const live = waitForMessage(current.ws, (frame) => frame.t === T.EVT_MAILBOX_DEPOSITED
        && frame.body && frame.body.mailboxId === inbox && frame.body.seq === sequence);
      // A failed deposit must not leave an unobserved waiter rejection.
      live.catch(() => {});
      const roundTripStarted = Date.now();
      send(onB.ws, "soak-deposit-" + sent, T.MAILBOX_DEPOSIT, {
        mailboxId: inbox, objectId: "soak-object-" + sent, ciphertextB64: value, metadata: {},
      });
      assert.equal((await waitForMessage(onB.ws, (frame) => frame.id === "soak-deposit-" + sent)).t, T.MAILBOX_DEPOSIT_RES);
      assert.equal((await live).body.ciphertextB64, value, "live delivery must retain the exact deposited bytes");
      assert.equal((await ack(current, inbox, sequence, "soak-ack-" + sent)).body.lastSeq, sequence);
      worstRoundTripMs = Math.max(worstRoundTripMs, Date.now() - roundTripStarted);
      sent += 1;
      if (sent % 10 === 0) {
        const response = await fetch(readyUrl, { signal: AbortSignal.timeout(6000) });
        assert.equal(response.status, 200);
        const readiness = await response.json();
        assert.equal(readiness.ok, true);
        assert.equal(readiness.degraded, false);
        assert.equal((await list(other, inbox, "soak-tenant-" + sent)).t, T.ERROR);
        tenantChecks += 1;
      }
      if (Date.now() >= nextReport) {
        t.diagnostic(JSON.stringify({ elapsedSeconds: Math.floor((Date.now() - started) / 1000), sent, acknowledged: sent, reconnects, tenantChecks, nodeCount: nodeIds.size, worstRoundTripMs }));
        nextReport = Date.now() + 60_000;
      }
      await new Promise((resolve) => setTimeout(resolve, Math.min(2000, Math.max(0, deadline - Date.now()))));
    }
    assert.ok(sent >= Math.floor(SOAK_MS / 5000), "soak must sustain traffic, not merely wait with idle sockets");
    assert.equal(nodeIds.size, 2);
    assert.deepEqual((await list(current, inbox, "soak-final-cursor")).body.items, []);
    t.diagnostic(JSON.stringify({ elapsedSeconds: Math.floor((Date.now() - started) / 1000), sent, acknowledged: sent, reconnects, tenantChecks, nodeCount: nodeIds.size, worstRoundTripMs, complete: true }));
  });

  if (!RECOVERY) return;
  t.after(async () => { await compose("unpause", "postgres").catch(() => {}); });
  let recovered = recipientAgain;
  async function depositPacket(opened, id, value) {
    send(opened.ws, id, T.MAILBOX_DEPOSIT, { mailboxId: inbox, objectId: id, ciphertextB64: value, metadata: {} });
    const response = await waitForMessage(opened.ws, (frame) => frame.id === id, 20_000);
    assert.equal(response.t, T.MAILBOX_DEPOSIT_RES);
    return response;
  }
  async function reconnect(id) {
    let opened;
    const deadline = Date.now() + 20_000;
    while (!opened) {
      try {
        opened = await openAuthenticated(recipient, recipientDevice.deviceId);
      } catch (err) {
        if (Date.now() >= deadline) throw err;
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    }
    sockets.push(opened.ws);
    await claim(opened, recipient, inbox, id);
    return opened;
  }
  async function waitReadiness(predicate) {
    const deadline = Date.now() + 12_000;
    while (Date.now() < deadline) {
      const response = await fetch(URL.replace("wss:", "https:").replace("/ws", "/ready"), { signal: AbortSignal.timeout(6000) });
      const body = response.status === 200 ? await response.json() : null;
      if (body === null) await response.text();
      if (predicate(response.status, body)) return;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error("readiness failed to converge within its recovery budget");
  }

  await t.test("SIGKILL both node processes: retained ciphertext and acknowledged cursor survive", async () => {
    recovered.ws.close();
    const value = Buffer.from("retained across node process death").toString("base64");
    await depositPacket(onB, "before-kill", value);
    await compose("kill", "--signal", "SIGKILL", "node1", "node2");
    await compose("up", "--detach", "--no-build", "--wait", "node1", "node2");
    recovered = await reconnect("claim-after-kill");
    const rows = await list(recovered, inbox, "after-kill");
    assert.deepEqual(rows.body.items, [{ seq: 2, ciphertextB64: value }]);
    assert.equal((await ack(recovered, inbox, 2, "ack-after-kill")).body.lastSeq, 2);
  });

  await t.test("Redis loss preserves durable delivery; reconnect replays missed wakeups without reconnecting the client", async () => {
    const depositing = await openOnDifferentNode(sender, "dev:redis-recovery-sender", recovered.node.nodeKeyId);
    sockets.push(depositing.ws);
    await compose("stop", "redis");
    await waitReadiness((status, body) => status === 200 && body.degraded === true);
    const value = Buffer.from("deposited while Redis was unavailable").toString("base64");
    await depositPacket(depositing, "during-redis-outage", value);
    const resumed = waitForMessage(recovered.ws, (frame) => frame.t === T.EVT_MAILBOX_DEPOSITED
      && frame.body && frame.body.mailboxId === inbox && frame.body.seq === 3, 25_000);
    await compose("up", "--detach", "--no-build", "--wait", "redis");
    assert.equal((await resumed).body.ciphertextB64, value);
    assert.equal((await ack(recovered, inbox, 3, "ack-redis-recovery")).body.lastSeq, 3);
  });

  await t.test("Postgres blackhole fails readiness within a bound; restart recovers committed data", async () => {
    await compose("pause", "postgres");
    try {
      const started = Date.now();
      await waitReadiness((status) => status === 503 || status === 502);
      assert.ok(Date.now() - started < 10_000);
      await assert.rejects(() => openAuthenticated(sender, "dev:unready"), /50[23]/);
      send(recovered.ws, "list-during-pg-blackhole", T.MAILBOX_LIST, { mailboxId: inbox, limit: 50 });
      const refused = await waitForMessage(recovered.ws, (frame) => frame.id === "list-during-pg-blackhole", 20_000);
      assert.equal(refused.t, T.ERROR);
      assert.equal(refused.body.code, "SERVICE_UNAVAILABLE");
      assert.equal(refused.body.detail.retryable, true);
    } finally {
      await compose("unpause", "postgres");
    }
    await compose("restart", "--timeout", "0", "postgres");
    await compose("up", "--detach", "--no-build", "--wait", "postgres");
    await waitReadiness((status) => status === 200);
    // Existing node processes and sockets must recover without a node restart.
    assert.equal(recovered.ws.readyState, WebSocket.OPEN);
    assert.deepEqual((await list(recovered, inbox, "after-postgres-restart")).body.items, []);
  });

  await t.test("restore a Postgres backup into another database: retained packet, cursor and tenant isolation survive", async () => {
    recovered.ws.close();
    const depositing = await openAuthenticated(sender, "dev:backup-sender");
    sockets.push(depositing.ws);
    const value = Buffer.from("retained across database backup and restore").toString("base64");
    await depositPacket(depositing, "before-backup", value);
    await compose("exec", "--no-TTY", "postgres", "pg_dump", "--username=rez", "--format=custom", "--file=/tmp/rez-proof.dump", "rez");
    await compose("exec", "--no-TTY", "postgres", "createdb", "--username=rez", "rez_restore_proof");
    await compose("exec", "--no-TTY", "postgres", "pg_restore", "--exit-on-error", "--username=rez", "--dbname=rez_restore_proof", "/tmp/rez-proof.dump");
    const overrideFile = path.join(RECOVERY_ROOT, "compose.override.json");
    const override = JSON.parse(await fs.readFile(overrideFile, "utf8"));
    for (const name of ["node1", "node2"]) {
      override.services[name].environment.REZ_PG_URL = "postgres://rez:rez-proof-only@postgres:5432/rez_restore_proof";
    }
    await fs.writeFile(overrideFile, JSON.stringify(override));
    await compose("up", "--detach", "--no-build", "--force-recreate", "--no-deps", "--wait", "node1", "node2");
    recovered = await reconnect("claim-after-restore");
    assert.deepEqual((await list(recovered, inbox, "after-restore")).body.items, [{ seq: 4, ciphertextB64: value }]);
    assert.equal((await ack(recovered, inbox, 4, "ack-after-restore")).body.lastSeq, 4);
    const foreign = await openAuthenticated(otherTenant, "dev:other");
    sockets.push(foreign.ws);
    await claim(foreign, otherTenant, otherInbox, "other-after-restore");
    assert.equal((await list(foreign, inbox, "foreign-after-restore")).t, T.ERROR);
  });
});
