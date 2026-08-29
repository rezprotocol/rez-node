import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import {
  recordCarriesDelegation,
  resolveOwnerRevocationState,
  REVOCATION_STATE_UNAVAILABLE,
} from "../ownerRevocationState.js";

const T = REZ_CONTRACT_TYPES;

/**
 * Durable signed-record gateway handler. Bridges authenticated client
 * sessions (record.put / record.get) to the DHT durable-record store
 * (`runtime.recordDht`, a DhtNode). Generic by wire-type — no per-directive
 * facade; the client invokes it through the same generic request path as
 * mailbox.deposit.
 *
 * Records are self-authenticating (publisher-signed), so the handler does
 * NOT bind a record to the session owner — it only requires an authenticated
 * session as the node-boundary gate. Per-publisher quotas and per-peer/IP
 * rate limits live in the DHT layer.
 *
 * REVOCATION (audit P0 follow-on, 2026-07-26). Not binding the record to the session is deliberate
 * and stays — but it made record.put a door through which a REVOKED delegated device could publish
 * a record its certificate no longer authorizes, because the overlay verifier is handed no
 * revocation state. The overlay is right to hold none: it is account-agnostic, and a replica that
 * does not home an account cannot learn its revocation state, so an attacker would simply push to
 * another replica.
 *
 * This node CAN evaluate it for the accounts its cluster homes, and this handler is that door. So a
 * DELEGATED record's chain is now checked against the owner account's own current authority state
 * before it is accepted. Scope, deliberately narrow:
 *   - only records that CARRY a cert chain — a direct/V1 record has no delegation to revoke, and
 *     checking would cost a database round trip on every put for nothing;
 *   - a non-homed account reads as an empty authority state, which is exactly what null meant, so
 *     foreign records behave as before;
 *   - the overlay ingress (dht.rec_store) and read-repair are UNCHANGED — replica behavior is
 *     preserved, since neither can answer the question.
 * A backend failure while resolving the state is NOT an allow: for a delegated record we would be
 * guessing at exactly the thing being checked, so it surfaces as a retryable error.
 */
export class RecordHandler {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
  }

  #dht() {
    return this.#ctx.runtime && this.#ctx.runtime.recordDht ? this.#ctx.runtime.recordDht : null;
  }

  #serializer() {
    return this.#ctx.runtime && this.#ctx.runtime.accountMutationSerializer
      ? this.#ctx.runtime.accountMutationSerializer
      : null;
  }

  /**
   * The owner account's current revocation state, or null when there is nothing to apply.
   *
   * Returns `{ ok: false }` (after sending an error) only when the state was NEEDED and could not
   * be established — never for a record that carries no delegation.
   *
   * @returns {Promise<{ ok: true, revocationState: object|null }|{ ok: false }>}
   */
  async #resolveOwnerRevocationState(requestId, record) {
    // Nothing revocable ⇒ no database round trip, and no failure mode the common path cannot
    // benefit from. fs / desktop / relay-only deployments wire no authority at all and resolve to
    // null here — the unchanged path, not a bypass of one that exists.
    if (!recordCarriesDelegation(record)) return { ok: true, revocationState: null };
    try {
      const revocationState = await resolveOwnerRevocationState({
        serializer: this.#serializer(),
        ownerPublicKeyB64: record.ownerPublicKeyB64,
      });
      return { ok: true, revocationState };
    } catch (err) {
      // NOT an allow: refusing the publication is the only answer that does not guess at exactly
      // the thing being checked. Retryable, because the next attempt may reach a healthy home.
      const unavailable = err && err.code === REVOCATION_STATE_UNAVAILABLE;
      console.warn("[RecordHandler] record.put: refusing a delegated publication rather than"
        + " accepting it unchecked — " + (err && err.message ? err.message : err));
      this.#ctx.sendError({
        id: requestId,
        code: unavailable ? "SERVICE_UNAVAILABLE" : "INTERNAL",
        message: "account authority state temporarily unavailable",
        retryable: unavailable,
      });
      return { ok: false };
    }
  }

  async handlePut(requestId, body) {
    const dht = this.#dht();
    if (!dht || typeof dht.putRecord !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable record store unavailable", retryable: false });
      return;
    }
    const record = body && typeof body.record === "object" ? body.record : null;
    if (!record) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "record required", retryable: false });
      return;
    }
    const resolved = await this.#resolveOwnerRevocationState(requestId, record);
    if (!resolved.ok) return;
    let result;
    try {
      result = await dht.putRecord(record, { revocationState: resolved.revocationState });
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_PUT_FAILED", message: err && err.message ? err.message : "record publish failed", retryable: true });
      return;
    }
    if (!result.storedLocally) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_REJECTED", message: "record rejected: " + result.reason, retryable: false });
      return;
    }
    // Wire-compat adapter (P4.3): the SDK response keeps its shape, but
    // `replicas` is now the ACKNOWLEDGED remote-holder count — send attempts
    // are no longer reported as replicas. Internal result classes never leak
    // to rez-sdk.
    this.#ctx.sendResponse(requestId, T.RECORD_PUT_RES, {
      localId: result.localId,
      replicas: result.acknowledgedRemote,
    });
  }

  async handleGet(requestId, body) {
    const dht = this.#dht();
    if (!dht || typeof dht.getRecord !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable record store unavailable", retryable: false });
      return;
    }
    const recordKind = body && typeof body.recordKind === "string" ? body.recordKind.trim() : "";
    const recordId = body && typeof body.recordId === "string" ? body.recordId.trim() : "";
    const publisherPublicKeyB64 = body && typeof body.publisherPublicKeyB64 === "string" ? body.publisherPublicKeyB64.trim() : "";
    if (!recordKind || !recordId || !publisherPublicKeyB64) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "recordKind, recordId, publisherPublicKeyB64 required", retryable: false });
      return;
    }
    let record;
    try {
      record = await dht.getRecord({ recordKind, recordId, publisherPublicKeyB64 });
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_GET_FAILED", message: err && err.message ? err.message : "record fetch failed", retryable: true });
      return;
    }
    this.#ctx.sendResponse(requestId, T.RECORD_GET_RES, { record: record || null });
  }
}
