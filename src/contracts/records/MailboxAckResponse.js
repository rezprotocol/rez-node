import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxAckResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_ACK_RES;

  constructor({ mailboxId, eventId, removed } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.removed = removed === true;
    if (this.constructor === MailboxAckResponse) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
  }
}
