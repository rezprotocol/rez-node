import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxListResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_LIST_RES;

  constructor({ mailboxId, items, nextCursor } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.items = Array.isArray(items) ? items : [];
    this.nextCursor = nextCursor == null ? null : String(nextCursor);
    if (this.constructor === MailboxListResponse) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(Array.isArray(this.items), "items must be an array");
  }
}
