/**
 * Relay-key-id normalization, in ONE place.
 *
 * There were three module-private `normalizeRelayKeyId` functions: two byte-identical string
 * normalizers (InboxRouter, resolveDeliveryDescriptor) and a third in RelayStore that took a
 * DESCRIPTOR OBJECT and read `.relayKeyId` off it. Same name, incompatible contracts — moving a
 * call site between those files would have compiled fine and silently normalized everything to "".
 *
 * The two are separated here by name rather than merged, because they answer different questions:
 * one cleans a key you already hold, the other extracts a key from a record.
 */

/** Trim a relay key id, or "" when it is absent/blank/not a string. */
export function normalizeRelayKeyId(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

/** The normalized relay key id carried BY a descriptor/relay record, or "" when it has none. */
export function relayKeyIdOf(record) {
  return record && typeof record.relayKeyId === "string" && record.relayKeyId.trim()
    ? record.relayKeyId.trim()
    : "";
}

// ---------------------------------------------------------------------------
// Self-certifying identity glue (ADR-RELAY-IDENTITY). Derivation itself lives
// in rez-core (`relayIdentity.js`) — this is the node-side SSOT that wraps it,
// so rez-node never grows a second derivation helper.
// ---------------------------------------------------------------------------

import {
  relayKeyIdForNodePublicKeyB64,
  nodeKeyIdForNodePublicKeyB64,
} from "@rezprotocol/core";

export class RelayIdentityMismatchError extends Error {
  constructor(message) {
    super(message);
    this.name = "RelayIdentityMismatchError";
    this.code = "RELAY_IDENTITY_MISMATCH";
  }
}

/**
 * Derive the canonical (relayKeyId, nodeKeyId) pair for a node public key.
 * Throws when the key is not a valid Ed25519 SPKI DER base64 string.
 * @param {string} nodePublicKeyB64
 * @returns {{ relayKeyId: string, nodeKeyId: string }}
 */
export function deriveRelayIdentity(nodePublicKeyB64) {
  return {
    relayKeyId: relayKeyIdForNodePublicKeyB64(nodePublicKeyB64),
    nodeKeyId: nodeKeyIdForNodePublicKeyB64(nodePublicKeyB64),
  };
}
