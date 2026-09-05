import test from "node:test";
import assert from "node:assert/strict";
import { bytesToBase64, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { handleSessionHello } from "../src/protocol/sessionBootstrap.js";
import { signedPayloadBytes } from "../src/relay/PeerAuthShared.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";

const T = REZ_CONTRACT_TYPES;
const crypto = new NodeCryptoProvider();

function fakeWs() {
  const closes = [];
  return {
    closes,
    OPEN: 1, readyState: 1,
    send() {}, on() {}, once() {}, off() {}, removeListener() {},
    close(code, reason) { closes.push({ code, reason }); },
  };
}

async function nodeIdentity() {
  const kp = await crypto.generateSigningKeyPair();
  return {
    nodeKeyId: "nk-test",
    nodePublicKeyB64: bytesToBase64(kp.publicKey),
    nodePrivateKeyB64: bytesToBase64(kp.privateKey),
    relayKeyId: "rk-test",
  };
}

function harnessSession(identity) {
  const session = new GatewaySession({ runtime: { getIdentity: () => identity }, ws: fakeWs() });
  const errors = [];
  const raws = [];
  const records = [];
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = (type, opts) => raws.push({ type, opts });
  session._safeSendRecord = (record, id) => records.push({ record, id });
  return { session, errors, raws, records };
}

function lastChallenge(raws) {
  const frame = [...raws].reverse().find((r) => r.type === T.SESSION_CHALLENGE);
  return frame ? frame.opts.body : null;
}

async function signClaimantAuth({ challenge, claimant }) {
  const payload = signedPayloadBytes({
    kind: "session-auth-claimant",
    challengeId: challenge.challengeId,
    nonceB64: challenge.nonceB64,
    nodeKeyId: challenge.nodeKeyId,
    nodePublicKeyB64: challenge.nodePublicKeyB64,
    relayKeyId: challenge.relayKeyId,
    claimantPublicKeyB64: claimant.pubB64,
    wsPath: challenge.wsPath,
  });
  return bytesToBase64(await crypto.sign({ privateKey: claimant.priv, msg: payload }));
}

async function completeClaimantHandshake({ session, raws, claimant }) {
  const hello = handleSessionHello({
    body: { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: claimant.pubB64 },
  });
  assert.equal(hello.error, undefined, "claimant hello accepted");
  await session._beginSessionAuthentication(hello.pendingAuthentication, "r-hello");
  const challenge = lastChallenge(raws);
  assert.ok(challenge, "a challenge was issued");
  const signatureB64 = await signClaimantAuth({ challenge, claimant });
  await session._handleSessionAuthenticate("r-auth", { challengeId: challenge.challengeId, signatureB64 });
  return challenge;
}

async function genClaimant() {
  const kp = await crypto.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}

// ---- Hello shape matrix (2A.2): v5 is explicit and fail-closed; v4 rejects v5 concepts ----

import { CapabilitySigner, MemoryStorageProvider } from "@rezprotocol/core";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";

test("claimant and account sessions reject a capability granted to someone else", async () => {
  const victim = crypto.generateSigningKeyPair();
  const grantee = crypto.generateSigningKeyPair();
  const attacker = await genClaimant();
  const registry = new InboxClaimRegistry({ storageProvider: new MemoryStorageProvider() });
  await registry.hydrate();
  const inboxId = "inbox:audit-victim";
  await registry.claim({ inboxId, claimantPublicKeyB64: bytesToBase64(victim.publicKey), claimedAtMs: Date.now() });
  const cap = await new CapabilitySigner({ crypto }).createRootCapability({
    resource: "mailbox:" + inboxId, actions: ["read"], signerPublicKeyB64: bytesToBase64(victim.publicKey),
    granteePublicKeyB64: bytesToBase64(grantee.publicKey), privateKeyBytes: victim.privateKey,
  });
  const h = harnessSession(await nodeIdentity());
  let reads = 0;
  h.session.runtime.inboxClaimRegistry = registry;
  h.session.runtime.inboxStore = { async list() { reads += 1; return { items: [{ eventId: "victim-event" }], nextCursor: null }; } };
  h.session._safeSendRawFrame = () => {};
  await completeClaimantHandshake({ ...h, claimant: attacker });
  assert.equal(h.session.principal.claimantPublicKeyB64, attacker.pubB64);
  await h.session._handleSocketMessage(Buffer.from(JSON.stringify({ id: "cap-theft", t: T.MAILBOX_LIST,
    v: 5, body: { mailboxId: inboxId, capChain: [cap.toJSON()] } })));
  assert.equal(h.errors[0].code, "FORBIDDEN");
  assert.equal(reads, 0, "foreign grantee cannot read");
  // Control: same cap on an unrelated ACCOUNT principal is refused.
  const control = harnessSession(await nodeIdentity());
  control.session.runtime.inboxClaimRegistry = registry;
  control.session.runtime.inboxStore = h.session.runtime.inboxStore;
  control.session._commitPrincipal(SessionPrincipal.accountDirect({ accountPublicKeyB64: attacker.pubB64,
    sessionDeviceId: "dev-attacker", authority: { mode: "direct", accountIdentityPublicKeyB64: attacker.pubB64, signerPublicKeyB64: attacker.pubB64 } }));
  control.session._installSessionServices();
  await control.session._handleSocketMessage(Buffer.from(JSON.stringify({ id: "cap-control", t: T.MAILBOX_LIST,
    v: 4, body: { mailboxId: inboxId, capChain: [cap.toJSON()] } })));
  assert.equal(control.errors[0].code, "FORBIDDEN");
  assert.equal(reads, 0);
});

test("a matching claimant grantee remains authorized", async () => {
  const claimant = await genClaimant();
  const registry = new InboxClaimRegistry({ storageProvider: new MemoryStorageProvider() });
  await registry.hydrate();
  const root = crypto.generateSigningKeyPair();
  await registry.claim({ inboxId: "inbox:grantee", claimantPublicKeyB64: bytesToBase64(root.publicKey), claimedAtMs: Date.now() });
  const cap = await new CapabilitySigner({ crypto }).createRootCapability({ resource: "mailbox:inbox:grantee", actions: ["read"], signerPublicKeyB64: bytesToBase64(root.publicKey), granteePublicKeyB64: claimant.pubB64, privateKeyBytes: root.privateKey });
  const h = harnessSession(await nodeIdentity());
  h.session.runtime.inboxClaimRegistry = registry;
  let reads = 0;
  h.session.runtime.inboxStore = { async list() { reads++; return { items: [], nextCursor: null }; } };
  h.session._safeSendRawFrame = () => {};
  await completeClaimantHandshake({ ...h, claimant });
  await h.session._handleSocketMessage(Buffer.from(JSON.stringify({ id: "read", t: T.MAILBOX_LIST, v: 5, body: { mailboxId: "inbox:grantee", capChain: [cap.toJSON()] } })));
  assert.equal(h.errors.length, 0);
  assert.equal(reads, 1);
});
