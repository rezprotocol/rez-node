import { DURABLE_RECORD_V2_VERSION } from "@rezprotocol/core";

/**
 * Resolving a durable record's OWNER account revocation state — in one place.
 *
 * Two paths need this and must agree exactly: the home's `record.put` (RecordHandler, where we
 * decide whether to ACCEPT a publication) and the overlay read-repair (DhtNode, where we decide
 * whether to CACHE what a lookup returned). They differ in what they do with the answer; they must
 * not differ in how they get it, or one door ends up laxer than the other.
 *
 * The account-agnostic overlay itself still resolves NOTHING — a replica that does not home an
 * account cannot learn its revocation state, and pretending otherwise would break replication for
 * every foreign record. See verifyDurableRecordDual's note.
 */

/**
 * The home could not be reached / errored. TRANSIENT: the next attempt may reach a healthy home,
 * so callers surface it as retryable. Never means "not revoked".
 */
export const REVOCATION_STATE_UNAVAILABLE = "REVOCATION_STATE_UNAVAILABLE";

/**
 * The home answered, with a shape that cannot be trusted. NOT transient — retrying returns the same
 * broken contract — so callers must not advertise it as retryable. Kept distinct from
 * UNAVAILABLE because collapsing the two would tell a client to retry a bug forever.
 */
export const REVOCATION_STATE_MALFORMED = "REVOCATION_STATE_MALFORMED";

function coded(message, code) {
  const err = new Error(message);
  err.code = code;
  return err;
}

/**
 * Does this record carry a delegation that a revocation could invalidate?
 *
 * V1 folds publisher/signer into one key and carries no chain; a V2 record with an empty chain is
 * direct mode (signer == owner). Neither has anything revocable, so neither is worth a database
 * round trip — and skipping them keeps the common path free of a failure mode it cannot benefit
 * from.
 */
export function recordCarriesDelegation(record) {
  if (!record || typeof record !== "object") return false;
  if (record.v !== DURABLE_RECORD_V2_VERSION) return false;
  return Array.isArray(record.certChain) && record.certChain.length > 0;
}

/**
 * The owner account's current revocation state, as the verifiers consume it.
 *
 * @param {object} args
 * @param {{ getAuthorityState(account:string):Promise<object> }|null} args.serializer
 * @param {string} args.ownerPublicKeyB64
 * @returns {Promise<{revokedCertIds: string[], minValidIssuedAtMs: number}|null>}
 *   null means "there is nothing to apply here" — either this deployment homes no accounts at all
 *   (fs / desktop / relay-only, which wire no authority), or the owner key is absent. It never
 *   means "checked, and clean": that case returns a real, empty state.
 * @throws {Error & {code: REVOCATION_STATE_UNAVAILABLE}} when the home could not answer (transient)
 * @throws {Error & {code: REVOCATION_STATE_MALFORMED}} when it answered an untrustworthy shape
 *   (not transient). Callers must treat BOTH as unknown — never as permission.
 */
export async function resolveOwnerRevocationState({ serializer, ownerPublicKeyB64 } = {}) {
  const owner = typeof ownerPublicKeyB64 === "string" ? ownerPublicKeyB64.trim() : "";
  if (owner.length === 0) return null;
  if (!serializer || typeof serializer.getAuthorityState !== "function") return null;

  let current;
  try {
    current = await serializer.getAuthorityState(owner);
  } catch (err) {
    throw coded("the owner account's authority state could not be read: "
      + (err && err.message ? err.message : "unknown"), REVOCATION_STATE_UNAVAILABLE);
  }
  // Projected STRICTLY. A malformed backend shape coerced into a plausible empty state would
  // silently re-open exactly the hole this closes, and it would do so invisibly — an empty state
  // is indistinguishable from a healthy account with no revocations.
  if (!current || typeof current !== "object"
      || !Array.isArray(current.revokedCertIds)
      || !current.revokedCertIds.every((certId) => typeof certId === "string")
      || typeof current.minValidIssuedAtMs !== "number"
      || !Number.isFinite(current.minValidIssuedAtMs)) {
    throw coded("the owner account's authority state is malformed", REVOCATION_STATE_MALFORMED);
  }
  // An account this cluster does not home reads as { revokedCertIds: [], minValidIssuedAtMs: 0 } —
  // the same thing a null state meant — so foreign records behave exactly as before.
  return { revokedCertIds: current.revokedCertIds, minValidIssuedAtMs: current.minValidIssuedAtMs };
}

/**
 * Bind a serializer into the NARROW capability a consumer actually needs: "given an owner key, what
 * is its revocation state". Everything about how that answer is obtained — which backend, what shape
 * it returns, which failures are transient — stays inside this module.
 *
 * This exists so the routing layer never holds a storage object. DhtNode's read-repair needs one
 * question answered; handing it a serializer would have made a persistence interface part of the
 * DHT's API, and the generic overlay boundary is worth more than the convenience.
 *
 * @param {{ serializer: {getAuthorityState(account:string):Promise<object>}|null }} args
 * @returns {(ownerPublicKeyB64: string) => Promise<{revokedCertIds:string[], minValidIssuedAtMs:number}|null>}
 */
export function createOwnerRevocationResolver({ serializer } = {}) {
  return (ownerPublicKeyB64) => resolveOwnerRevocationState({ serializer, ownerPublicKeyB64 });
}

/**
 * Whether a resolved state can possibly change a verification verdict. An account with no revoked
 * certs and no issued-at cutoff cannot, so callers skip the (expensive) revocation-aware re-verify
 * entirely. Mirrors the null-when-empty convention in AccountAuthorityRevocationCache.
 */
export function revocationStateIsEmpty(state) {
  if (state === null || state === undefined) return true;
  const certs = Array.isArray(state.revokedCertIds) ? state.revokedCertIds : [];
  const cutoff = Number.isFinite(state.minValidIssuedAtMs) ? state.minValidIssuedAtMs : 0;
  return certs.length === 0 && cutoff === 0;
}
