import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxFetchResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_FETCH_RES;

  constructor({ mailboxId, eventId, objectId, ciphertextB64, metadata, createdAtMs } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.objectId = objectId == null ? null : String(objectId);
    this.ciphertextB64 = ciphertextB64 == null ? null : String(ciphertextB64);
    this.metadata = metadata != null && typeof metadata === "object" ? metadata : {};
    this.createdAtMs = createdAtMs == null ? null : Number(createdAtMs);
    if (this.constructor === MailboxFetchResponse) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
  }
}
