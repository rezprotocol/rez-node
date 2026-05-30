import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * Server-pushed notification fired when a PersistentOutboundQueue entry
 * transitions state. Status values:
 *   - "queued"    — entry was just enqueued (initial state)
 *   - "delivered" — RetryScheduler succeeded; entry removed from queue
 *   - "expired"   — entry exceeded 72h TTL without delivery; removed
 *
 * Routed to the originating session's owner via
 * `sessionRegistry.broadcastToOwner`, keyed off the entry's
 * `ownerPublicKeyB64`. The chat-server uses `deliverInboxId` (paired with
 * its own (deliverInboxId → messageId) tracking) to correlate the event
 * back to a specific outbound message and update its UI status.
 */
export class OutboundQueueStatusEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.EVT_OUTBOUND_STATUS;

  constructor({ queueId, deliverInboxId, status, attemptedAtMs } = {}) {
    super();
    this.queueId = queueId == null ? "" : String(queueId);
    this.deliverInboxId = deliverInboxId == null ? "" : String(deliverInboxId);
    this.status = status == null ? "" : String(status);
    this.attemptedAtMs = Number.isFinite(Number(attemptedAtMs)) ? Number(attemptedAtMs) : 0;
    if (this.constructor === OutboundQueueStatusEvent) this._seal();
  }

  validate() {
    this.assert(this.queueId.trim().length > 0, "queueId must be non-empty");
    this.assert(this.deliverInboxId.trim().length > 0, "deliverInboxId must be non-empty");
    this.assert(
      this.status === "queued" || this.status === "delivered" || this.status === "expired",
      "status must be queued|delivered|expired",
    );
  }
}
