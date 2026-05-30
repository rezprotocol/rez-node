import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * Open-registration claim of an inbox at this node.
 *
 * The SDK generates a random inboxId, an ed25519 claimant keypair, and signs
 * the canonical-JSON of (inboxId, claimantPublicKeyB64, claimedAtMs) with the
 * claimant privkey. The node verifies the signature and stores the inbox →
 * claimantPublicKey mapping. See docs/CAPABILITY_MODEL.md §6.
 *
 * Inbox IDs are opaque random tokens generated client-side. They MUST NOT be
 * derived from any account identifier or other linkable value — derivation
 * defeats the inbox-only privacy property (see feedback_node_multitenant.md).
 */
export class InboxClaimRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.INBOX_CLAIM;

  constructor({ inboxId, claimantPublicKeyB64, claimedAtMs, signatureB64 } = {}) {
    super();
    this.inboxId = inboxId == null ? "" : String(inboxId);
    this.claimantPublicKeyB64 = claimantPublicKeyB64 == null ? "" : String(claimantPublicKeyB64);
    this.claimedAtMs = Number(claimedAtMs);
    this.signatureB64 = signatureB64 == null ? "" : String(signatureB64);
    if (this.constructor === InboxClaimRequest) this._seal();
  }

  validate() {
    this.assert(this.inboxId.trim().length > 0, "inboxId must be non-empty");
    this.assert(this.claimantPublicKeyB64.trim().length > 0, "claimantPublicKeyB64 must be non-empty");
    this.assert(Number.isFinite(this.claimedAtMs) && this.claimedAtMs > 0, "claimedAtMs must be a positive number");
    this.assert(this.signatureB64.trim().length > 0, "signatureB64 must be non-empty");
  }
}
