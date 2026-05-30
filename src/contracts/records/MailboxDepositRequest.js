import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxDepositRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_DEPOSIT;

  constructor({ mailboxId, objectId, ciphertextB64, metadata, capChain } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.objectId = objectId == null ? "" : String(objectId);
    this.ciphertextB64 = ciphertextB64 == null ? "" : String(ciphertextB64);
    this.metadata = metadata != null && typeof metadata === "object" ? metadata : {};
    this.capChain = Array.isArray(capChain) ? capChain : null;
    if (this.constructor === MailboxDepositRequest) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.objectId.trim().length > 0, "objectId must be non-empty");
    this.assert(this.ciphertextB64.length > 0, "ciphertextB64 must be non-empty");
  }
}
