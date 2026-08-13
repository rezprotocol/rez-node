import test from "node:test";
import assert from "node:assert/strict";
import { WebSocket } from "ws";
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
  const ws = new WebSocket(URL, { rejectUnauthorized: false });
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  const auth = await authenticateSession({ ws, waitForMessage, identity, deviceId, timeoutMs: 10_000 });
  return { ws, node: auth.challenge.body };
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

test("shipping hosted topology: TLS + non-sticky two-node delivery + shared cursor + tenant isolation", { skip: !RUN, timeout: 120_000 }, async (t) => {
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
});
