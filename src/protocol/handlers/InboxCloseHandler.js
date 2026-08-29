import { REZ_CONTRACT_TYPES, TerminalInboxCloseV1, verifyTerminalInboxClose } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;

/**
 * inbox.close — accept a TerminalInboxClose (portable inbox lease L1,
 * plans/PORTABLE_INBOX_LEASE_SPEC.md §4).
 *
 * THE RECORD AUTHORIZES ITSELF: acceptance is decided by the close-key
 * signature verified against the closePublicKey registered in the stored
 * claim, and by generation equality. The carrying session's principal
 * contributes NO authority — the op is ANY_PRINCIPAL, and the account stays
 * off the wire even for the kill switch. Transport authentication gates
 * protocol access; it never becomes close authorization.
 *
 * Effects (frozen semantics): the (inboxId, finalGeneration) tombstone is
 * recorded — admission (deposits, claims, leases at ≤ G) dies immediately
 * and permanently; already-stored ciphertext is RETAINED so the claimant can
 * drain through the grace window (CLOSED = drain-your-mail-then-die;
 * physical reclamation is L2). Idempotent.
 */
export class InboxCloseHandler {
  #ctx;
  #crypto;

  constructor(ctx, { crypto = null } = {}) {
    this.#ctx = ctx;
    this.#crypto = crypto || new NodeCryptoProvider();
  }

  async handleClose(requestId, body) {
    const registry = this.#ctx.runtime && this.#ctx.runtime.inboxClaimRegistry;
    if (!registry || typeof registry.getClaim !== "function" || typeof registry.recordTerminalClose !== "function") {
      // The pg/hosted registry is the LEGACY path (F9) and carries no lease
      // surface — closable inboxes live on the portable path.
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "terminal close is not supported by this provider", retryable: false });
      return;
    }

    let close;
    try {
      close = new TerminalInboxCloseV1(body && typeof body === "object" ? body : {});
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err && err.message ? err.message : "invalid TerminalInboxClose", retryable: false });
      return;
    }

    const claim = await registry.getClaim(close.inboxId);
    if (!claim) {
      this.#ctx.sendError({ id: requestId, code: "UNKNOWN_INBOX", message: "no claim exists for this inbox", retryable: false });
      return;
    }
    if (typeof claim.closePublicKeyB64 !== "string" || claim.closePublicKeyB64.length === 0
      || !Number.isInteger(claim.generation)) {
      // A legacy claim has no close key: not closable by record — its lease
      // simply lapses (expiry is the safety net).
      this.#ctx.sendError({ id: requestId, code: "INBOX_NOT_CLOSABLE", message: "this inbox's claim carries no close key", retryable: false });
      return;
    }
    if (close.finalGeneration !== claim.generation) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "finalGeneration does not match the claimed generation", retryable: false });
      return;
    }

    const verified = await verifyTerminalInboxClose({
      close,
      expectedClosePublicKeyB64: claim.closePublicKeyB64,
      crypto: this.#crypto,
    });
    if (verified !== true) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "terminal close signature did not verify", retryable: false });
      return;
    }

    let stored;
    try {
      stored = await registry.recordTerminalClose({
        inboxId: close.inboxId,
        finalGeneration: close.finalGeneration,
        closedAtMs: close.closedAtMs,
      });
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "INTERNAL_ERROR", message: err && err.message ? err.message : "terminal close failed", retryable: false });
      return;
    }

    this.#ctx.sendResponse(requestId, T.INBOX_CLOSE_RES, {
      inboxId: stored.inboxId,
      finalGeneration: stored.finalGeneration,
      closed: true,
    });
  }
}
