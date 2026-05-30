import { canonicalJSONStringify, bytesToBase64, deriveAccountIdFromPublicKey, CONTRACT_VERSION, REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { SessionHello } from "../../src/contracts/records/SessionHello.js";
import { NodeCryptoProvider } from "../../src/crypto/NodeCryptoProvider.js";

const CRYPTO = new NodeCryptoProvider();

function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

function sendRecord(ws, id, record) {
  ws.send(JSON.stringify({
    id,
    type: record.constructor.type,
    t: record.constructor.type,
    v: CONTRACT_VERSION,
    body: record,
  }));
}

function sendRawFrame(ws, { id, type, body }) {
  ws.send(JSON.stringify({
    id,
    type,
    t: type,
    v: CONTRACT_VERSION,
    body,
  }));
}

export function createSessionIdentity() {
  const keyPair = CRYPTO.generateSigningKeyPair();
  return {
    accountId: deriveAccountIdFromPublicKey(keyPair.publicKey),
    publicKey: keyPair.publicKey,
    privateKey: keyPair.privateKey,
    accountIdentityPublicKeyB64: bytesToBase64(keyPair.publicKey),
    accountIdentityPrivateKeyB64: bytesToBase64(keyPair.privateKey),
  };
}

export function createNodeTestIdentity({
  accountId = "rez:test:node",
  deviceId = "dev:test:node",
  localInboxId = "inbox:test:node",
  relayKeyId = null,
} = {}) {
  const keyPair = CRYPTO.generateSigningKeyPair();
  const nodeKeyId = Buffer.from(CRYPTO.hashSha256(keyPair.publicKey)).toString("hex");
  return {
    accountId,
    deviceId,
    localInboxId,
    nodeKeyId,
    nodePublicKeyB64: bytesToBase64(keyPair.publicKey),
    nodePrivateKeyB64: bytesToBase64(keyPair.privateKey),
    relayKeyId: relayKeyId || ("node-" + deviceId),
  };
}

/**
 * Build a claimant→node delegation record signed by the given claimant key.
 * The wire shape and signed-payload kind must match
 * `claimantNodeDelegationPayload` in rez-node/src/relay/InboxRouter.js.
 */
export function createClaimantNodeDelegation({
  claimantIdentity,
  inboxId,
  nodeKeyId,
  nodePublicKeyB64,
  relayKeyId,
  issuedAtMs = Date.now(),
  expiresAtMs = issuedAtMs + (7 * 24 * 60 * 60 * 1000),
} = {}) {
  if (!claimantIdentity || !claimantIdentity.privateKey || !claimantIdentity.accountIdentityPublicKeyB64) {
    throw new Error("createClaimantNodeDelegation requires claimantIdentity");
  }
  const normalizedInboxId = String(inboxId || "").trim();
  const normalizedNodeKeyId = String(nodeKeyId || "").trim();
  const normalizedNodePublicKeyB64 = String(nodePublicKeyB64 || "").trim();
  const normalizedRelayKeyId = String(relayKeyId || "").trim();
  if (!normalizedInboxId || !normalizedNodeKeyId || !normalizedNodePublicKeyB64 || !normalizedRelayKeyId) {
    throw new Error("createClaimantNodeDelegation requires inboxId, nodeKeyId, nodePublicKeyB64, relayKeyId");
  }
  const payload = {
    kind: "inbox-node-delegation",
    inboxId: normalizedInboxId,
    claimantPublicKeyB64: claimantIdentity.accountIdentityPublicKeyB64,
    nodeKeyId: normalizedNodeKeyId,
    nodePublicKeyB64: normalizedNodePublicKeyB64,
    relayKeyId: normalizedRelayKeyId,
    issuedAtMs,
    expiresAtMs,
  };
  const sig = CRYPTO.sign({
    privateKey: claimantIdentity.privateKey,
    msg: signedPayloadBytes(payload),
  });
  return {
    inboxId: normalizedInboxId,
    claimantPublicKeyB64: claimantIdentity.accountIdentityPublicKeyB64,
    nodeKeyId: normalizedNodeKeyId,
    nodePublicKeyB64: normalizedNodePublicKeyB64,
    relayKeyId: normalizedRelayKeyId,
    issuedAtMs,
    expiresAtMs,
    delegationSigB64: bytesToBase64(sig),
  };
}

export async function provisionPeerLinkBinding({
  peerLinks,
  ownerAccountId,
  identity,
  issuedAtMs = Date.now(),
  expiresAtMs = issuedAtMs + (7 * 24 * 60 * 60 * 1000),
} = {}) {
  if (!peerLinks || typeof peerLinks.getOrCreateAccountBindingChallenge !== "function") {
    throw new Error("provisionPeerLinkBinding requires peerLinks");
  }
  if (!identity || identity.accountId !== ownerAccountId) {
    throw new Error("provisionPeerLinkBinding identity must match ownerAccountId");
  }
  const challenge = await peerLinks.getOrCreateAccountBindingChallenge({ ownerAccountId });
  const x3dhIdentityPublicKeyB64 = String(challenge?.x3dhIdentityPublicKeyB64 || "").trim();
  if (!x3dhIdentityPublicKeyB64) {
    throw new Error("peer-link binding challenge missing x3dhIdentityPublicKeyB64");
  }
  const payload = {
    kind: "x3dh-subkey-binding",
    accountId: ownerAccountId,
    x3dhIdentityPublicKeyB64,
    issuedAtMs,
    expiresAtMs,
  };
  const sig = CRYPTO.sign({
    privateKey: identity.privateKey,
    msg: signedPayloadBytes(payload),
  });
  return peerLinks.upsertAccountBinding({
    ownerAccountId,
    accountBinding: {
      accountId: ownerAccountId,
      accountIdentityPublicKeyB64: identity.accountIdentityPublicKeyB64,
      x3dhIdentityPublicKeyB64,
      issuedAtMs,
      expiresAtMs,
      accountBindingSigB64: bytesToBase64(sig),
    },
  });
}

/**
 * Drive session.hello → session.challenge → session.authenticate and return
 * the session.ready frame. The session is identified solely by the SDK's
 * public key; inboxes are bound separately via inbox.claim.
 */
export async function authenticateSession({
  ws,
  waitForMessage,
  id = "hello",
  identity = null,
  deviceId = "dev:test",
  clientName = "test",
  clientVersion = "test",
  wsPath = "/ws",
  timeoutMs = 5_000,
} = {}) {
  if (!ws || typeof waitForMessage !== "function") {
    throw new Error("authenticateSession requires ws and waitForMessage");
  }
  const sessionIdentity = identity || createSessionIdentity();

  sendRecord(ws, id, new SessionHello({
    contractVersion: CONTRACT_VERSION,
    clientName,
    clientVersion,
    deviceId,
    accountIdentityPublicKeyB64: sessionIdentity.accountIdentityPublicKeyB64,
  }));

  const challengeFrame = await waitForMessage(
    ws,
    (msg) => msg.id === id
      && (msg.t === REZ_CONTRACT_TYPES.SESSION_CHALLENGE || msg.t === REZ_CONTRACT_TYPES.ERROR),
    timeoutMs,
  );
  if (challengeFrame?.t === REZ_CONTRACT_TYPES.ERROR) {
    throw new Error(`session challenge failed: ${challengeFrame?.body?.code || "UNKNOWN"} ${challengeFrame?.body?.message || ""}`.trim());
  }

  const challenge = (challengeFrame && challengeFrame.body) || {};
  const authPayload = {
    kind: "session-auth",
    challengeId: String(challenge.challengeId || ""),
    nonceB64: String(challenge.nonceB64 || ""),
    nodeKeyId: String(challenge.nodeKeyId || ""),
    nodePublicKeyB64: String(challenge.nodePublicKeyB64 || ""),
    relayKeyId: String(challenge.relayKeyId || ""),
    publicKeyB64: sessionIdentity.accountIdentityPublicKeyB64,
    deviceId,
    wsPath: String(challenge.wsPath || wsPath),
  };
  const authSig = CRYPTO.sign({
    privateKey: sessionIdentity.privateKey,
    msg: signedPayloadBytes(authPayload),
  });

  sendRawFrame(ws, {
    id,
    type: REZ_CONTRACT_TYPES.SESSION_AUTHENTICATE,
    body: {
      challengeId: String(challenge.challengeId || ""),
      signatureB64: bytesToBase64(authSig),
    },
  });

  const readyFrame = await waitForMessage(
    ws,
    (msg) => msg.id === id
      && (msg.t === REZ_CONTRACT_TYPES.SESSION_READY || msg.t === REZ_CONTRACT_TYPES.ERROR),
    timeoutMs,
  );
  if (readyFrame?.t === REZ_CONTRACT_TYPES.ERROR) {
    throw new Error(`session authenticate failed: ${readyFrame?.body?.code || "UNKNOWN"} ${readyFrame?.body?.message || ""}`.trim());
  }
  return {
    identity: sessionIdentity,
    challenge: challengeFrame,
    ready: readyFrame,
  };
}
