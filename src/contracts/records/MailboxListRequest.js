import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxListRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_LIST;

  constructor({ mailboxId, cursor, limit, sinceMs, capChain } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.cursor = cursor == null ? null : String(cursor);
    this.limit = limit == null ? 50 : Number(limit);
    this.sinceMs = sinceMs == null ? null : Number(sinceMs);
    this.capChain = Array.isArray(capChain) ? capChain : null;
    if (this.constructor === MailboxListRequest) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(Number.isInteger(this.limit) && this.limit > 0, "limit must be positive integer");
  }
}
