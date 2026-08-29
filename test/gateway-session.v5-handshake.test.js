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

test("hello shape matrix — every ambiguous or mixed shape is refused, never inferred around", () => {
  const ACCOUNT_KEY = "A".repeat(43) + "=";
  const CLAIMANT_KEY = "B".repeat(43) + "=";
  const DEVICE = "rez:dev:" + "a".repeat(64);

  const accepted = [
    ["v4 legacy", { contractVersion: 4, deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY }],
    ["v5 account", { contractVersion: 5, authMode: "account", deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY }],
    ["v5 claimant", { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: CLAIMANT_KEY }],
  ];
  for (const [label, body] of accepted) {
    const result = handleSessionHello({ body });
    assert.equal(result.error, undefined, label + " must be accepted");
    assert.ok(result.pendingAuthentication, label + " yields a pending authentication");
  }

  const rejected = [
    ["v5 without authMode", { contractVersion: 5, deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY }],
    ["v5 unknown mode", { contractVersion: 5, authMode: "root", deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY }],
    ["v5 claimant + deviceId (correlation metadata)", { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: CLAIMANT_KEY, deviceId: DEVICE }],
    ["v5 claimant + account identity (both identity forms)", { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: CLAIMANT_KEY, accountIdentityPublicKeyB64: ACCOUNT_KEY }],
    ["v5 claimant with NO identity", { contractVersion: 5, authMode: "claimant" }],
    ["v5 account + claimant key", { contractVersion: 5, authMode: "account", deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY, claimantPublicKeyB64: CLAIMANT_KEY }],
    ["v5 account with NO identity", { contractVersion: 5, authMode: "account" }],
    ["v4 carrying authMode (v4 has no modes)", { contractVersion: 4, authMode: "account", deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY }],
    ["v4 carrying a claimant key", { contractVersion: 4, deviceId: DEVICE, accountIdentityPublicKeyB64: ACCOUNT_KEY, claimantPublicKeyB64: CLAIMANT_KEY }],
    ["unsupported contract 6", { contractVersion: 6, authMode: "claimant", claimantPublicKeyB64: CLAIMANT_KEY }],
    ["v5 claimant key not base64", { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: "@@not-base64@@" }],
  ];
  for (const [label, body] of rejected) {
    const result = handleSessionHello({ body });
    assert.ok(result.error, label + " must be refused");
    assert.equal(result.pendingAuthentication, undefined, label + " yields no pending authentication");
  }
});

// ---- Full claimant handshake: hello → challenge → authenticate → CLAIMANT principal ----

test("claimant handshake commits a CLAIMANT principal at contract 5 with a domain-separated proof", async () => {
  const identity = await nodeIdentity();
  const { session, errors, raws, records } = harnessSession(identity);
  const claimant = await genClaimant();

  const challenge = await completeClaimantHandshake({ session, raws, claimant });

  assert.deepEqual(errors, [], "no errors on the happy path");
  assert.equal(session.authenticated, true);
  assert.equal(session.sessionContractVersion, 5);
  assert.ok(session.principal.isClaimant());
  assert.equal(session.principal.claimantPublicKeyB64, claimant.pubB64);
  assert.equal(session.ownerPublicKeyB64, null, "a claimant session exposes NO account identity");
  assert.equal(session.sessionDeviceId, null, "a claimant session carries NO device identity");
  assert.equal(session.sessionAuthority, null, "no dual-mode account authority object");

  const ready = records.find((r) => r.record && r.record.constructor && r.record.constructor.type === T.SESSION_READY);
  assert.ok(ready, "session.ready was sent");
  assert.equal(ready.record.capabilities.authMode, "claimant");
  assert.equal(ready.record.capabilities.deviceId, "");
  assert.equal(ready.record.capabilities.contractVersion, 5);

  // Domain separation: the ACCOUNT-kind payload signed with the same key and
  // challenge must NOT authenticate a claimant pending — replay across modes
  // is structurally impossible, not just unlikely.
  const { session: s2, raws: raws2, errors: errors2 } = harnessSession(identity);
  const hello2 = handleSessionHello({ body: { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: claimant.pubB64 } });
  await s2._beginSessionAuthentication(hello2.pendingAuthentication, "r1");
  const ch2 = lastChallenge(raws2);
  const wrongKindPayload = signedPayloadBytes({
    kind: "session-auth",
    challengeId: ch2.challengeId,
    nonceB64: ch2.nonceB64,
    nodeKeyId: ch2.nodeKeyId,
    nodePublicKeyB64: ch2.nodePublicKeyB64,
    relayKeyId: ch2.relayKeyId,
    publicKeyB64: claimant.pubB64,
    deviceId: "",
    wsPath: ch2.wsPath,
  });
  const wrongSig = bytesToBase64(await crypto.sign({ privateKey: claimant.priv, msg: wrongKindPayload }));
  await s2._handleSessionAuthenticate("r2", { challengeId: ch2.challengeId, signatureB64: wrongSig });
  assert.equal(s2.authenticated, false, "an account-kind signature never authenticates a claimant pending");
  assert.ok(errors2.some((e) => e.code === "UNAUTHORIZED"));
  void challenge;
});

test("claimant authenticate presenting a cert chain (delegation) is refused before verification", async () => {
  const identity = await nodeIdentity();
  const { session, errors, raws } = harnessSession(identity);
  const claimant = await genClaimant();
  const hello = handleSessionHello({ body: { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: claimant.pubB64 } });
  await session._beginSessionAuthentication(hello.pendingAuthentication, "r1");
  const challenge = lastChallenge(raws);
  const signatureB64 = await signClaimantAuth({ challenge, claimant });
  await session._handleSessionAuthenticate("r2", {
    challengeId: challenge.challengeId,
    signatureB64,
    certChain: [{ certId: "x" }],
  });
  assert.equal(session.authenticated, false, "claimant mode has no delegation");
  assert.ok(errors.some((e) => e.code === "UNAUTHORIZED"));
});

// ---- v5 re-auth rejection: ALREADY_AUTHENTICATED, principal unchanged, close 1008 ----

test("a committed v5 CLAIMANT session refuses hello AND authenticate with ALREADY_AUTHENTICATED (close 1008), principal unchanged", async () => {
  const identity = await nodeIdentity();
  const { session, errors, raws } = harnessSession(identity);
  const claimant = await genClaimant();
  await completeClaimantHandshake({ session, raws, claimant });
  const principalBefore = session.principal;

  for (const frame of [
    { id: "x1", type: T.SESSION_HELLO, body: { contractVersion: 5, authMode: "claimant", claimantPublicKeyB64: claimant.pubB64 } },
    { id: "x2", type: "session.authenticate", body: { challengeId: "c", signatureB64: "s" } },
  ]) {
    session._frameCodec = { decodeFrame: () => frame };
    await session._handleSocketMessage(Buffer.from("{}"));
    const err = errors.at(-1);
    assert.equal(err.code, "ALREADY_AUTHENTICATED", frame.type + " is a protocol-state violation");
  }
  assert.ok(session.ws.closes.some((c) => c.code === 1008 && c.reason === "already_authenticated"));
  assert.equal(session.principal, principalBefore, "the committed principal remains unchanged until close");
  assert.equal(session.authenticated, true);
});

test("a committed v5 ACCOUNT session also refuses re-auth; a v4 session keeps shipped replacement semantics", async () => {
  const identity = await nodeIdentity();
  const account = await genClaimant(); // an ed25519 pair; used as account root here
  const DEVICE = "rez:dev:" + "c".repeat(64);

  async function completeAccountHandshake(session, raws, contractVersion) {
    const body = contractVersion === 5
      ? { contractVersion: 5, authMode: "account", deviceId: DEVICE, accountIdentityPublicKeyB64: account.pubB64 }
      : { contractVersion: 4, deviceId: DEVICE, accountIdentityPublicKeyB64: account.pubB64 };
    const hello = handleSessionHello({ body });
    assert.equal(hello.error, undefined);
    await session._beginSessionAuthentication(hello.pendingAuthentication, "r1");
    const challenge = lastChallenge(raws);
    const payload = signedPayloadBytes({
      kind: "session-auth",
      challengeId: challenge.challengeId,
      nonceB64: challenge.nonceB64,
      nodeKeyId: challenge.nodeKeyId,
      nodePublicKeyB64: challenge.nodePublicKeyB64,
      relayKeyId: challenge.relayKeyId,
      publicKeyB64: account.pubB64,
      deviceId: DEVICE,
      wsPath: challenge.wsPath,
    });
    const signatureB64 = bytesToBase64(await crypto.sign({ privateKey: account.priv, msg: payload }));
    await session._handleSessionAuthenticate("r2", { challengeId: challenge.challengeId, signatureB64 });
  }

  // v5 account: committed, then a re-hello is ALREADY_AUTHENTICATED.
  const v5 = harnessSession(identity);
  await completeAccountHandshake(v5.session, v5.raws, 5);
  assert.equal(v5.session.sessionContractVersion, 5);
  assert.ok(v5.session.principal instanceof SessionPrincipal);
  v5.session._frameCodec = { decodeFrame: () => ({ id: "x", type: T.SESSION_HELLO, body: {} }) };
  await v5.session._handleSocketMessage(Buffer.from("{}"));
  assert.equal(v5.errors.at(-1).code, "ALREADY_AUTHENTICATED");

  // v4: committed, then a re-hello is ACCEPTED (challenge issued — the shipped
  // completed-replacement path stays reachable, frozen commit-point rule).
  const v4 = harnessSession(identity);
  await completeAccountHandshake(v4.session, v4.raws, 4);
  assert.equal(v4.session.sessionContractVersion, 4);
  const challengesBefore = v4.raws.filter((r) => r.type === T.SESSION_CHALLENGE).length;
  v4.session._frameCodec = {
    decodeFrame: () => ({
      id: "x",
      type: T.SESSION_HELLO,
      body: { contractVersion: 4, deviceId: DEVICE, accountIdentityPublicKeyB64: account.pubB64 },
    }),
  };
  await v4.session._handleSocketMessage(Buffer.from("{}"));
  const challengesAfter = v4.raws.filter((r) => r.type === T.SESSION_CHALLENGE).length;
  assert.equal(challengesAfter, challengesBefore + 1, "v4 re-hello still issues a challenge");
  assert.ok(!v4.errors.some((e) => e.code === "ALREADY_AUTHENTICATED"));
});

// ---- Envelope gate: enumerated {4, 5}, everything else BAD_VERSION ----

test("frame envelope accepts v 4 and 5 and refuses others (enumerated, no negotiation)", async () => {
  const identity = await nodeIdentity();
  for (const [version, ok] of [[4, true], [5, true], [3, false], [6, false]]) {
    const { session, errors } = harnessSession(identity);
    await session._handleSocketMessage(Buffer.from(JSON.stringify({ id: "p1", t: "ping", v: version, body: {} })));
    if (ok) {
      assert.ok(!errors.some((e) => e.code === "BAD_VERSION"), "v" + version + " accepted");
    } else {
      assert.ok(errors.some((e) => e.code === "BAD_VERSION"), "v" + version + " refused");
    }
  }
});
