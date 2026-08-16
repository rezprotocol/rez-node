/**
 * Test-support factory for self-certifying relay identities
 * (ADR-RELAY-IDENTITY). Every test that needs a relay/node identity or a
 * validly bound signed descriptor should mint it here instead of hand-typing
 * "relay-a"-style strings — free-string relay IDs no longer authenticate.
 */
import { generateKeyPairSync } from "node:crypto";
import { OnionKeyRecordV1 } from "@rezprotocol/core";
import { deriveRelayIdentity } from "../../src/util/relayKeyId.js";
import { buildSignedRelayDescriptorJson } from "../../src/relay/PeerAuthShared.js";

let counter = 0;

/**
 * Generate a full node identity with derived relayKeyId/nodeKeyId.
 * @param {{ label?: string }} [opts] label only affects account/device/inbox ids (metadata, never identity)
 */
export function makeRelayIdentity({ label } = {}) {
  counter += 1;
  const tag = label || "t" + counter;
  const { publicKey, privateKey } = generateKeyPairSync("ed25519", {
    publicKeyEncoding: { format: "der", type: "spki" },
    privateKeyEncoding: { format: "der", type: "pkcs8" },
  });
  const nodePublicKeyB64 = Buffer.from(publicKey).toString("base64");
  const nodePrivateKeyB64 = Buffer.from(privateKey).toString("base64");
  const { relayKeyId, nodeKeyId } = deriveRelayIdentity(nodePublicKeyB64);
  return {
    accountId: `rez:node:${tag}`,
    deviceId: `dev:${tag}`,
    localInboxId: `inbox:${tag}`,
    relayKeyId,
    nodeKeyId,
    nodePublicKeyB64,
    nodePrivateKeyB64,
  };
}

/**
 * Build a signed relay descriptor JSON whose identity binding is valid for
 * the given identity (or a freshly minted one).
 * @param {{ identity?: object, host?: string, port?: number, tlsEnabled?: boolean, nowMs?: number, ttlMs?: number, keyRecords?: object[] }} [opts]
 * @returns {{ identity: object, descriptor: object }}
 */
export function makeSignedDescriptor({
  identity = null,
  host = "127.0.0.1",
  port = 4600,
  tlsEnabled = false,
  nowMs = Date.now(),
  ttlMs = 3_600_000,
  keyRecords = null,
} = {}) {
  const id = identity || makeRelayIdentity();
  const records = Array.isArray(keyRecords) && keyRecords.length > 0 ? keyRecords : [
    new OnionKeyRecordV1({
      onionKeyId: `${id.deviceId}-onion`,
      publicKeyBytes: new Uint8Array(32).fill((counter % 250) + 1),
      format: "raw",
      createdAt: nowMs - 1000,
      notBefore: nowMs - 1000,
      notAfter: nowMs + ttlMs,
      status: "active",
    }),
  ];
  const descriptor = buildSignedRelayDescriptorJson({
    relayKeyId: id.relayKeyId,
    advertisedHost: host,
    relayPort: port,
    tlsEnabled,
    keyRecords: records,
    nodeKeyId: id.nodeKeyId,
    nodePublicKeyB64: id.nodePublicKeyB64,
    nodePrivateKeyB64: id.nodePrivateKeyB64,
    nowMs,
    // ttlMs bounds the DESCRIPTOR expiry as well as the onion-key window, so
    // `makeSignedDescriptor({ nowMs: past, ttlMs })` really is expired.
    expiresAt: nowMs + ttlMs,
  });
  return { identity: id, descriptor };
}
