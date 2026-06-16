import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class MailboxDepositedEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.EVT_MAILBOX_DEPOSITED;

  constructor({ mailboxId, eventId, objectId, createdAtMs, seq } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.objectId = objectId == null ? null : String(objectId);
    this.createdAtMs = createdAtMs == null ? null : Number(createdAtMs);
    // S2 durable-inbox delivery: the per-inbox monotonic durable seq. Present
    // only from durable (pg-cluster) home nodes; null on fs/desktop nodes whose
    // EVT comes from the transient RMailbox. The client dedups on seq and
    // advances its cursor via mailbox.cursorAck (instead of mailbox.ack delete).
    this.seq = seq == null ? null : Number(seq);
    if (this.constructor === MailboxDepositedEvent) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
  }
}
