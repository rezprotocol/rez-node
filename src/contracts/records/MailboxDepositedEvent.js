import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxDepositedEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.EVT_MAILBOX_DEPOSITED;

  constructor({ mailboxId, eventId, objectId, createdAtMs } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.objectId = objectId == null ? null : String(objectId);
    this.createdAtMs = createdAtMs == null ? null : Number(createdAtMs);
    if (this.constructor === MailboxDepositedEvent) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
  }
}
