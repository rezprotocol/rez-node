import { DhtNodeId } from "./DhtNodeId.js";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import {
  base64ToBytes,
  DURABLE_RECORD_VERSION,
  durableRecordLocalId,
  durableRecordSignableBytes,
} from "@rezprotocol/core";

/**
 * Node-side verifier + routing-target derivation for DurableRecordV1.
 *
 * The canonical record shape, slot-key derivation, and signable bytes live
 * in @rezprotocol/core (durableRecordV1.js) so the SDK signer and this
 * verifier agree byte-for-byte. This module adds the parts that need a
 * concrete crypto provider and the DHT keyspace.
 */

const RECORD_CRYPTO = new NodeCryptoProvider();

/** Default ceiling on a record's `payloadB64` length (~16 KB). */
export const DEFAULT_MAX_RECORD_BYTES = 16384;

/**
 * Routing target for a record's slot. `localId` is already a sha256 (a
 * 256-bit position in the same keyspace as node ids), so the slot key
 * doubles as its own Kademlia target.
 *
 * @param {string} localId - 64-char sha256 hex
 * @returns {DhtNodeId}
 */
export function durableRecordTargetId(localId) {
  return DhtNodeId.fromHex(localId);
}

/**
 * One-shot, self-authenticating verification of a DurableRecordV1. No
 * registry lookup: the publisher key is embedded and the slot is bound to
 * it. Mirrors the inbox-node-delegation pattern (single Ed25519 verify +
 * self-expiry).
 *
 * On success returns `{ ok: true, localId }` where `localId` is the
 * publisher-bound slot key derived from the record's own fields — the
 * caller MUST check the announced wire key equals this to reject
 * slot-substitution.
 *
 * @param {object} record
 * @param {number} nowMs
 * @param {{ maxBytes?: number }} [options]
 * @returns {{ ok: boolean, reason: string|null, localId: string|null }}
 */
export function verifyDurableRecord(record, nowMs, { maxBytes = DEFAULT_MAX_RECORD_BYTES } = {}) {
  if (!record || typeof record !== "object") return fail("not-object");
  if (record.v !== DURABLE_RECORD_VERSION) return fail("bad-version");
  if (!Number.isFinite(nowMs)) return fail("bad-now");

  const kind = typeof record.recordKind === "string" ? record.recordKind.trim() : "";
  const id = typeof record.recordId === "string" ? record.recordId.trim() : "";
  const pub = typeof record.publisherPublicKeyB64 === "string" ? record.publisherPublicKeyB64.trim() : "";
  const payloadB64 = typeof record.payloadB64 === "string" ? record.payloadB64 : "";
  const sigB64 = typeof record.sigB64 === "string" ? record.sigB64.trim() : "";
  if (!kind || !id || !pub || !sigB64) return fail("missing-fields");

  if (!Number.isFinite(record.issuedAtMs) || !Number.isFinite(record.expiresAtMs)) {
    return fail("bad-timestamps");
  }
  if (record.expiresAtMs <= record.issuedAtMs) return fail("bad-expiry-window");
  if (record.expiresAtMs <= nowMs) return fail("expired");
  if (payloadB64.length > maxBytes) return fail("too-large");

  let localId;
  try {
    localId = durableRecordLocalId({ publisherPublicKeyB64: pub, recordKind: kind, recordId: id });
  } catch (err) {
    return fail("localid-derive-failed");
  }

  let verified = false;
  try {
    verified = RECORD_CRYPTO.verify({
      publicKey: base64ToBytes(pub),
      msg: durableRecordSignableBytes(record),
      sig: base64ToBytes(sigB64),
    });
  } catch (err) {
    return fail("verify-threw");
  }
  if (verified !== true) return fail("bad-signature");

  return { ok: true, reason: null, localId };
}

function fail(reason) {
  return { ok: false, reason, localId: null };
}
