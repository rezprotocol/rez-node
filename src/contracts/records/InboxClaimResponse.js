import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * Response to a successful inbox claim. The node echoes the claimed inbox and
 * the timestamp recorded in its registry. On failure, the node sends an error
 * response instead (e.g. INBOX_ALREADY_CLAIMED, INVALID_SIGNATURE).
 */
export class InboxClaimResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.INBOX_CLAIM_RES;

  constructor({ inboxId, claimedAtMs } = {}) {
    super();
    this.inboxId = inboxId == null ? "" : String(inboxId);
    this.claimedAtMs = Number(claimedAtMs);
    if (this.constructor === InboxClaimResponse) this._seal();
  }

  validate() {
    this.assert(this.inboxId.trim().length > 0, "inboxId must be non-empty");
    this.assert(Number.isFinite(this.claimedAtMs) && this.claimedAtMs > 0, "claimedAtMs must be a positive number");
  }
}
