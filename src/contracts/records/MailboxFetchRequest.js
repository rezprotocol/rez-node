import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxFetchRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_FETCH;

  constructor({ mailboxId, eventId, capChain } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.capChain = Array.isArray(capChain) ? capChain : null;
    if (this.constructor === MailboxFetchRequest) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
  }
}
