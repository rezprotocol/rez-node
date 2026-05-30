import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxDepositResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_DEPOSIT_RES;

  // queued=true means the gateway couldn't synchronously deliver but
  // successfully persisted the message into PersistentOutboundQueue.
  // RetryScheduler will keep attempting delivery in the background; a
  // later notification (see OutboundQueueStatusV1 wiring) reports the
  // eventual outcome. When queued is true, eventId is empty.
  constructor({ mailboxId, eventId, queued } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.queued = queued === true;
    if (this.constructor === MailboxDepositResponse) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    if (this.queued) {
      this.assert(this.eventId.length === 0, "queued response must not carry eventId");
    } else {
      this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
    }
  }
}
