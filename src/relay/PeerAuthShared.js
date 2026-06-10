import { RelayDescriptorV1, base64ToBytes } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { canonicalJSONStringify } from "../util/canonicalize.js";

const PEER_AUTH_CRYPTO = new NodeCryptoProvider();
const DESCRIPTOR_TTL_MS = 86_400_000 * 30;

// v4 (TRUST-9): the connecting node contributes a fresh `clientNonceB64` in
// peer.hello which the relay MUST bind into the signed peer.challenge AND
// peer.accept. Without it the relay only ever signed its own self-chosen nonce,
// so a recorded (challenge, accept) pair could be replayed to impersonate the
// relay without its private key. expiresAtMs is now inside the signed challenge
// too (was unsigned, hence forgeable). Wire-breaking — relays + nodes deploy in
// lockstep. The reverse direction (relay authenticating the node) was already
// replay-proof: the relay mints + consumes a fresh nonce per handshake.
export const PEER_AUTH_PROTOCOL_VERSION = 4;

export function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

export function meshPeerAuthPayload({ challengeId, nonceB64, relayKeyId = null, nodeKeyId } = {}) {
  return {
    kind: "mesh-peer-auth",
    challengeId,
    nonceB64,
    relayKeyId: relayKeyId || null,
    nodeKeyId,
  };
}

export function meshPeerChallengePayload({ challengeId, nonceB64, clientNonceB64 = null, relayKeyId = null, nodeKeyId, expiresAtMs = null } = {}) {
  return {
    kind: "mesh-peer-challenge",
    challengeId,
    nonceB64,
    // TRUST-9: bind the connecting node's fresh nonce + the (now signed) expiry
    // so this challenge cannot be replayed to a different/later session.
    clientNonceB64: clientNonceB64 || null,
    relayKeyId: relayKeyId || null,
    nodeKeyId,
    expiresAtMs: expiresAtMs === null || expiresAtMs === undefined ? null : Number(expiresAtMs),
  };
}

export function meshPeerAcceptPayload({ challengeId, acceptedAs, clientNonceB64 = null, relayKeyId = null, nodeKeyId, trustLevel } = {}) {
  return {
    kind: "mesh-peer-accept",
    challengeId,
    acceptedAs,
    // TRUST-9: the accept is likewise bound to the connecting node's nonce.
    clientNonceB64: clientNonceB64 || null,
    relayKeyId: relayKeyId || null,
    nodeKeyId,
    trustLevel,
  };
}

export function buildRelayDescriptorSigningPayload(descriptor) {
  if (!descriptor || typeof descriptor !== "object") {
    throw new Error("relay descriptor signing payload requires descriptor object");
  }
  return {
    v: descriptor.v ?? 1,
    relayKeyId: descriptor.relayKeyId,
    endpoints: Array.isArray(descriptor.endpoints) ? descriptor.endpoints : [],
    onionKeys: Array.isArray(descriptor.onionKeys)
      ? descriptor.onionKeys.map((key) => (typeof key?.toJSON === "function" ? key.toJSON() : key))
      : [],
    capabilities: descriptor.capabilities,
    expiresAt: descriptor.expiresAt,
    meta: descriptor.meta,
  };
}

export function signRelayDescriptorJson(descriptor, { nodeKeyId, nodePrivateKey } = {}) {
  if (!descriptor || typeof descriptor !== "object") {
    throw new Error("signRelayDescriptorJson requires descriptor");
  }
  const normalizedNodeKeyId = typeof nodeKeyId === "string" ? nodeKeyId.trim() : "";
  if (!normalizedNodeKeyId || !(nodePrivateKey instanceof Uint8Array)) {
    throw new Error("signRelayDescriptorJson requires nodeKeyId and private key");
  }
  const payload = buildRelayDescriptorSigningPayload(descriptor);
  const sigBytes = PEER_AUTH_CRYPTO.sign({
    privateKey: nodePrivateKey,
    msg: signedPayloadBytes(payload),
  });
  return {
    ...descriptor,
    sig: {
      scheme: "ed25519",
      keyId: normalizedNodeKeyId,
      sigB64: Buffer.from(sigBytes).toString("base64"),
    },
  };
}

export function verifyRelayDescriptorSignature(descriptor) {
  try {
    const keyId = typeof descriptor?.meta?.node?.keyId === "string" ? descriptor.meta.node.keyId.trim() : "";
    const publicKeyB64 = typeof descriptor?.meta?.node?.publicKeyB64 === "string" ? descriptor.meta.node.publicKeyB64.trim() : "";
    const sig = descriptor?.sig && typeof descriptor.sig === "object" ? descriptor.sig : null;
    const scheme = typeof sig?.scheme === "string" ? sig.scheme.trim() : "";
    const sigKeyId = typeof sig?.keyId === "string" ? sig.keyId.trim() : "";
    const sigB64 = typeof sig?.sigB64 === "string" ? sig.sigB64.trim() : "";
    if (!keyId || !publicKeyB64 || !sig || scheme !== "ed25519" || !sigKeyId || !sigB64) {
      return false;
    }
    if (sigKeyId !== keyId) {
      return false;
    }
    return PEER_AUTH_CRYPTO.verify({
      publicKey: base64ToBytes(publicKeyB64),
      msg: signedPayloadBytes(buildRelayDescriptorSigningPayload(descriptor)),
      sig: base64ToBytes(sigB64),
    });
  } catch {
    return false;
  }
}

/**
 * Derive local auth level and wire compatibility fields from two inputs:
 * whether the peer presented a relayKeyId, and whether that relay is known locally.
 *
 * @param {{ relayKeyId: string|null, knownRelay: boolean }} opts
 * @returns {{ authLevel: string, acceptedAs: string, wireTrustLevel: string }}
 */
export function derivePeerAuth({ relayKeyId = null, knownRelay = false } = {}) {
  if (!relayKeyId) {
    return { authLevel: "node", acceptedAs: "leaf", wireTrustLevel: "verified" };
  }
  if (knownRelay) {
    return { authLevel: "relay-verified", acceptedAs: "relay-known", wireTrustLevel: "verified" };
  }
  return { authLevel: "relay-provisional", acceptedAs: "relay-provisional", wireTrustLevel: "tofu" };
}

export function buildSignedRelayDescriptorJson({
  relayKeyId,
  advertisedHost,
  relayPort,
  tlsEnabled = false,
  keyRecords,
  nodeKeyId,
  nodePublicKeyB64,
  nodePrivateKeyB64,
  nowMs = Date.now(),
  expiresAt = nowMs + DESCRIPTOR_TTL_MS,
} = {}) {
  const host = typeof advertisedHost === "string" ? advertisedHost.trim() : "";
  const port = Number(relayPort);
  const normalizedNodeKeyId = typeof nodeKeyId === "string" ? nodeKeyId.trim() : "";
  const normalizedNodePublicKeyB64 = typeof nodePublicKeyB64 === "string" ? nodePublicKeyB64.trim() : "";
  const normalizedPrivateKeyB64 = typeof nodePrivateKeyB64 === "string" ? nodePrivateKeyB64.trim() : "";
  const onionKeys = Array.isArray(keyRecords) ? keyRecords : [];
  if (!relayKeyId || !host || !Number.isInteger(port) || port <= 0 || onionKeys.length === 0) {
    return null;
  }
  if (!normalizedNodeKeyId || !normalizedNodePublicKeyB64 || !normalizedPrivateKeyB64) {
    return null;
  }

  const descriptor = new RelayDescriptorV1({
    relayKeyId,
    endpoints: [{
      host,
      port,
      ...(tlsEnabled === true ? { tls: true } : {}),
    }],
    onionKeys,
    expiresAt,
    meta: {
      v: 1,
      node: {
        keyId: normalizedNodeKeyId,
        publicKeyB64: normalizedNodePublicKeyB64,
        protocolVersion: PEER_AUTH_PROTOCOL_VERSION,
      },
    },
    nowMs,
  }).toJSON();

  return signRelayDescriptorJson(descriptor, {
    nodeKeyId: normalizedNodeKeyId,
    nodePrivateKey: base64ToBytes(normalizedPrivateKeyB64),
  });
}
