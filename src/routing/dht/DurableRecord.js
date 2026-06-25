import { DhtNodeId } from "./DhtNodeId.js";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import {
  base64ToBytes,
  DURABLE_RECORD_VERSION,
  durableRecordLocalId,
  durableRecordSignableBytes,
  DURABLE_RECORD_V2_VERSION,
  verifyDurableRecordV2,
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
 * Default bound on how far a record's `issuedAtMs` may lead this node's clock.
 * A self-signed record carries an attacker-chosen `issuedAtMs`; the store
 * orders slots by it (newer wins), so a far-future stamp would poison the slot
 * — every honest later update reads as "older" until the poisoned
 * `expiresAtMs`. Bounding the lead to a few minutes of honest skew defuses that
 * without rejecting legitimately clock-skewed publishers.
 */
export const DEFAULT_MAX_FUTURE_SKEW_MS = 5 * 60_000;

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
export function verifyDurableRecord(record, nowMs, { maxBytes = DEFAULT_MAX_RECORD_BYTES, maxFutureSkewMs = DEFAULT_MAX_FUTURE_SKEW_MS } = {}) {
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
  if (Number.isFinite(maxFutureSkewMs) && record.issuedAtMs > nowMs + maxFutureSkewMs) {
    // Bounded clock skew: a far-future issuance would poison the slot (the
    // store orders by issuedAtMs, so honest later updates would read older).
    return fail("future-issuance");
  }
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

/**
 * Version-dispatching overlay verifier (S2.5 S8 / F2, V7). DurableRecordV1 takes
 * the unchanged synchronous self-authenticating path above; DurableRecordV2
 * (owner/signer separated) routes through the rez-core dual-mode helper, which
 * recomputes the slot from the OWNER key, checks the signature against the
 * SIGNER key, and decides owner→signer authority via `verifyAccountAuthority`
 * (DIRECT when signer == owner and no chain — the byte-for-byte V1 primary path
 * — else DELEGATED via the cert chain). The overlay's anti-squat/anti-poison
 * DoS guards (finite/ordered timestamps, bounded future skew, payload size) are
 * applied here, NOT in the pure core helper. Revocation state is the freshest
 * the caller holds (the overlay holds none → `null`; bounded-staleness
 * authority enforcement is the home registry's + the reader's job, not the
 * content-addressed overlay's).
 *
 * Returns the SAME `{ ok, reason, localId }` shape as `verifyDurableRecord` so
 * every existing call site is unchanged except for the `await`; v2 verdicts also
 * carry `{ mode, ownerPublicKeyB64, signerPublicKeyB64 }`.
 *
 * @param {object} record
 * @param {number} nowMs
 * @param {{ maxBytes?: number, maxFutureSkewMs?: number, revocationState?: object|null }} [options]
 * @returns {Promise<{ ok: boolean, reason: string|null, localId: string|null, mode?: string, ownerPublicKeyB64?: string, signerPublicKeyB64?: string }>}
 */
export async function verifyDurableRecordDual(record, nowMs, { maxBytes = DEFAULT_MAX_RECORD_BYTES, maxFutureSkewMs = DEFAULT_MAX_FUTURE_SKEW_MS, revocationState = null } = {}) {
  if (!record || typeof record !== "object") return fail("not-object");
  if (record.v === DURABLE_RECORD_VERSION) {
    return verifyDurableRecord(record, nowMs, { maxBytes, maxFutureSkewMs });
  }
  if (record.v === DURABLE_RECORD_V2_VERSION) {
    return _verifyDurableRecordV2Node(record, nowMs, { maxBytes, maxFutureSkewMs, revocationState });
  }
  return fail("bad-version");
}

async function _verifyDurableRecordV2Node(record, nowMs, { maxBytes, maxFutureSkewMs, revocationState }) {
  if (!Number.isFinite(nowMs)) return fail("bad-now");
  // DoS guards the pure core helper does not cover (it is authorization logic,
  // not the overlay's anti-poison/size posture). Mirror the V1 fast-path bounds.
  if (!Number.isFinite(record.issuedAtMs) || !Number.isFinite(record.expiresAtMs)) return fail("bad-timestamps");
  if (record.expiresAtMs <= record.issuedAtMs) return fail("bad-expiry-window");
  if (Number.isFinite(maxFutureSkewMs) && record.issuedAtMs > nowMs + maxFutureSkewMs) return fail("future-issuance");
  const payloadB64 = typeof record.payloadB64 === "string" ? record.payloadB64 : "";
  if (payloadB64.length > maxBytes) return fail("too-large");

  let res;
  try {
    res = await verifyDurableRecordV2({ record, crypto: RECORD_CRYPTO, nowMs, revocationState });
  } catch (err) {
    return fail("verify-threw");
  }
  if (!res.ok) return { ok: false, reason: res.reason || "bad-v2-record", localId: null };
  return {
    ok: true,
    reason: null,
    localId: res.localId,
    mode: res.mode,
    ownerPublicKeyB64: res.ownerPublicKeyB64,
    signerPublicKeyB64: res.signerPublicKeyB64,
  };
}

function fail(reason) {
  return { ok: false, reason, localId: null };
}
