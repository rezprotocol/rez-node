import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxAckRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_ACK;

  constructor({ mailboxId, eventId, capChain } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.capChain = Array.isArray(capChain) ? capChain : null;
    if (this.constructor === MailboxAckRequest) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
  }
}
