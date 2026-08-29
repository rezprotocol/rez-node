import { RRecord } from "@rezprotocol/core";

export class WsErrorDetail extends RRecord {
  constructor({ retryable = false, appContextId, messageId, closeReason, finalGeneration } = {}) {
    super();
    this.retryable = retryable === true;
    this.appContextId = appContextId == null ? null : String(appContextId);
    this.messageId = messageId == null ? null : String(messageId);
    // M6 (rez-chat plans/MOBILE_LIFECYCLE_ADAPTER_PLAN.md §7e): INBOX_CLOSED
    // refusals carry the tombstone's authoritative semantics so a client's
    // re-mint policy keys on typed fields, never parsed error text.
    // "reclaimed" = expiry reclamation (generations ≤ finalGeneration dead);
    // "terminal" = close-key kill (the inboxId lineage is dead forever).
    this.closeReason = closeReason == null ? null : String(closeReason);
    this.finalGeneration = finalGeneration == null ? null : Number(finalGeneration);
    if (this.constructor === WsErrorDetail) this._seal();
  }

  validate() {
    if (this.appContextId != null) this.assert(this.appContextId.trim().length > 0, "appContextId must be non-empty when provided");
    if (this.messageId != null) this.assert(this.messageId.trim().length > 0, "messageId must be non-empty when provided");
    if (this.closeReason != null) {
      this.assert(this.closeReason === "reclaimed" || this.closeReason === "terminal",
        "closeReason must be \"reclaimed\" or \"terminal\" when provided");
    }
    if (this.finalGeneration != null) {
      this.assert(Number.isInteger(this.finalGeneration) && this.finalGeneration >= 1,
        "finalGeneration must be a positive integer when provided");
    }
  }
}
