import test from "node:test";
import assert from "node:assert/strict";
import {
  REZ_CONTRACT_TYPES,
  bytesToBase64,
  AccountDeviceMutationV1,
  ACCOUNT_DEVICE_MUTATION_VERSION,
  ACCOUNT_DEVICE_MUTATION_PURPOSE,
  AccountDeviceMutationV2,
  ACCOUNT_DEVICE_MUTATION_V2_VERSION,
  ACCOUNT_DEVICE_MUTATION_V2_PURPOSE,
  DeviceInboxBindingV1,
  DEVICE_INBOX_BINDING_PURPOSE,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  DeviceRegistrationV1,
  CONTRACT_VERSION,
} from "@rezprotocol/core";
import { AccountMutationHandler } from "../src/protocol/handlers/AccountMutationHandler.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

// AUDIT #5 — THE ROLLING-VERSION MATRIX, stated as executable cases.
//
// Two signed schemas were changed in place, so "v1" meant two different bodies. V2 fixes that, but
// a version split only helps if the behaviour at every mixed pairing is DECIDED rather than
// discovered in production. The three cases:
//
//   1. NEW CLIENT → OLD NODE. Not reachable: CONTRACT_VERSION is an equality check at
//      session.hello, and this change bumps it. A v2 mutation can never arrive at a node that only
//      understands v1, because the session is refused first. That is the answer to "record-level
//      v: 2 alone only detects incompatibility after submission" — the handshake detects it before.
//   2. OLD CLIENT → NEW NODE. Also unreachable over a live session for the same reason. The
//      handler still dispatches on `v` (defence in depth, and the path a replayed/stored record
//      would take): a v1 device.revoke is HONOURED, a v1 device.add is refused UPGRADE_REQUIRED.
//   3. STORED V1 → NEW READER. A v1 record that already exists must still parse and verify against
//      v1 bytes. It does — the class is frozen — and its signature is checked with the class it was
//      parsed as, never with v2's.
const CRYPTO = new NodeCryptoProvider();
const T = REZ_CONTRACT_TYPES;
const NOW = 1_700_000_000_000;
const FAR = NOW + 3_600_000;

function key() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { pubB64: bytesToBase64(kp.publicKey), priv: kp.privateKey };
}

function signJson(priv, bytes) {
  return { alg: "ed25519", sigB64: bytesToBase64(CRYPTO.sign({ privateKey: priv, msg: bytes })) };
}

function makeBinding(device, inboxId) {
  const fields = {
    v: 1,
    purpose: DEVICE_INBOX_BINDING_PURPOSE,
    deviceId: DeviceRegistrationV1.deviceIdFor(device.pubB64),
    devicePublicKeyB64: device.pubB64,
    inboxId,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
  };
  return new DeviceInboxBindingV1({ ...fields, sig: signJson(device.priv, DeviceInboxBindingV1.signableBytes(fields)) });
}

function makeCapability(account, device) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account.pubB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: device.pubB64,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(device.pubB64),
    capabilities: ["device.add"],
    maxDelegationDepth: 0,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
    signerPublicKeyB64: account.pubB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  return new AccountDeviceCapabilityV1({
    ...fields,
    certId,
    sig: signJson(account.priv, AccountDeviceCapabilityV1.signableBytes({ ...fields, certId })),
  });
}

function makeMutation({ Klass, v, purpose, account, action, target, opId = "op-1" }) {
  const body = {
    v,
    purpose,
    opId,
    accountIdentityPublicKeyB64: account.pubB64,
    expectedRevision: 0,
    action,
    target,
    signerPublicKeyB64: account.pubB64,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
  };
  return new Klass({ ...body, sig: signJson(account.priv, Klass.signableBytes(body)) }).toJSON();
}

function makeCtx({ account }) {
  const sent = [];
  const serializer = {
    async getAuthorityState() { return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 }; },
    async submitMutation() { return { applied: true, revision: 1, devices: [], stale: false }; },
  };
  const ctx = {
    runtime: { accountMutationSerializer: serializer },
    ownerPublicKeyB64: account.pubB64,
    sessionAuthority: { mode: "direct", accountIdentityPublicKeyB64: account.pubB64, signerPublicKeyB64: account.pubB64 },
    now: () => NOW + 1000,
    requireSession() { return true; },
    sendError(opts) { sent.push({ kind: "error", ...opts }); },
    sendResponse(requestId, type, body) { sent.push({ kind: "response", requestId, type, body }); },
  };
  return { ctx, sent };
}

const last = (sent) => sent.at(-1);

test("MATRIX 1 — new client vs old node is prevented at the HANDSHAKE, not after submission", () => {
  // The decision the audit asked for: a global contract-version bump, not per-record negotiation.
  // session.hello asserts EQUALITY against CONTRACT_VERSION (see rez-node session.bootstrap.auth),
  // so a client speaking v2 mutations cannot establish a session with a node that predates them.
  // Record-level `v` alone would only surface the mismatch after a mutation was already sent and
  // signed — which for device.add means after a ceremony had begun.
  assert.equal(CONTRACT_VERSION >= 4, true,
    "the schema split must be accompanied by a contract-version bump, or mixed pairs can connect");
});

test("MATRIX 2 — old client, new node: a v1 device.revoke is still HONOURED", async () => {
  // v1 revoke survives because its target shape and meaning never changed — only its validation
  // tightened. Refusing it would break a schema that is still perfectly safe.
  const account = key();
  const victim = key();
  const { ctx, sent } = makeCtx({ account });
  const mutation = makeMutation({
    Klass: AccountDeviceMutationV1,
    v: ACCOUNT_DEVICE_MUTATION_VERSION,
    purpose: ACCOUNT_DEVICE_MUTATION_PURPOSE,
    account,
    action: "device.revoke",
    target: { revokedDeviceId: DeviceRegistrationV1.deviceIdFor(victim.pubB64) },
  });

  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });

  const res = last(sent);
  assert.equal(res.kind, "response", res.kind === "error" ? res.message : "");
  assert.equal(res.type, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES);
});

test("MATRIX 2 — old client, new node: a v1 device.add is refused UPGRADE_REQUIRED", async () => {
  // The unsafe one. A v1 device.add carries no leaf cert, so the home has no certId to bind and a
  // later revoke cannot kill that device's authority for off-home peers.
  const account = key();
  const newDevice = key();
  const { ctx, sent } = makeCtx({ account });
  const mutation = makeMutation({
    Klass: AccountDeviceMutationV1,
    v: ACCOUNT_DEVICE_MUTATION_VERSION,
    purpose: ACCOUNT_DEVICE_MUTATION_PURPOSE,
    account,
    action: "device.add",
    target: { deviceInboxBinding: makeBinding(newDevice, "rez:inbox:new").toJSON() },
  });

  await new AccountMutationHandler(ctx).handleSubmit("r1", { mutation });

  const err = last(sent);
  assert.equal(err.kind, "error");
  assert.equal(err.code, "UPGRADE_REQUIRED");
  assert.match(err.message, /leaf capability certificate/);
  assert.equal(err.retryable, false, "retrying the same v1 record can never succeed");
});

test("MATRIX 3 — a stored v1 record still parses and verifies against V1 bytes", () => {
  // Freezing the class is what makes this true. If v1 had kept drifting, an existing signature
  // would verify against bytes it was never made over — or, worse, stop verifying silently.
  const account = key();
  const victim = key();
  const json = makeMutation({
    Klass: AccountDeviceMutationV1,
    v: ACCOUNT_DEVICE_MUTATION_VERSION,
    purpose: ACCOUNT_DEVICE_MUTATION_PURPOSE,
    account,
    action: "device.revoke",
    target: { revokedDeviceId: DeviceRegistrationV1.deviceIdFor(victim.pubB64) },
  });

  const reparsed = new AccountDeviceMutationV1(json);
  const ok = CRYPTO.verify({
    publicKey: Buffer.from(account.pubB64, "base64"),
    msg: AccountDeviceMutationV1.signableBytes(reparsed),
    sig: Buffer.from(reparsed.sig.sigB64, "base64"),
  });
  assert.equal(ok, true, "the frozen v1 bytes still verify the original signature");

  // And the cross-version protection, stated as it actually works: `signableBytes` is a pure
  // function of the body it is handed, so calling V2's static on a v1 body yields v1's bytes. The
  // binding is not which class computes the bytes — it is that `v` and `purpose` are INSIDE them.
  // A v1 record therefore cannot be re-presented as v2: V2's validate() rejects it outright, and
  // rewriting v/purpose to pass would change the signed bytes and break the signature.
  assert.throws(() => new AccountDeviceMutationV2(json), /must be 2/,
    "a v1 body is not a valid v2 record");
  const forged = { ...json, v: ACCOUNT_DEVICE_MUTATION_V2_VERSION, purpose: ACCOUNT_DEVICE_MUTATION_V2_PURPOSE };
  const forgedOk = CRYPTO.verify({
    publicKey: Buffer.from(account.pubB64, "base64"),
    msg: AccountDeviceMutationV2.signableBytes(forged),
    sig: Buffer.from(reparsed.sig.sigB64, "base64"),
  });
  assert.equal(forgedOk, false, "relabelling a v1 body as v2 breaks its signature");
});

test("a v2 device.add is accepted, and an unknown version is refused outright", async () => {
  const account = key();
  const newDevice = key();
  const { ctx, sent } = makeCtx({ account });
  const handler = new AccountMutationHandler(ctx);

  await handler.handleSubmit("r1", {
    mutation: makeMutation({
      Klass: AccountDeviceMutationV2,
      v: ACCOUNT_DEVICE_MUTATION_V2_VERSION,
      purpose: ACCOUNT_DEVICE_MUTATION_V2_PURPOSE,
      account,
      action: "device.add",
      target: {
        deviceInboxBinding: makeBinding(newDevice, "rez:inbox:new").toJSON(),
        deviceCapability: makeCapability(account, newDevice).toJSON(),
      },
    }),
  });
  assert.equal(last(sent).kind, "response", "v2 device.add is the produced, accepted path");

  await handler.handleSubmit("r2", { mutation: { v: 99, action: "device.add" } });
  assert.equal(last(sent).code, "BAD_REQUEST");
  assert.match(last(sent).message, /unsupported mutation version/);
});
