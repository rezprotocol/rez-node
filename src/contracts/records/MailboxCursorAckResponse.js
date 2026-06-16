import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * mailbox.cursorAck.res — the stored cursor after a cursorAck advance.
 *
 * `lastSeq` is the device's cursor as actually stored (monotonic, bounded to
 * what was delivered to that device), so a client can detect a clamped advance.
 */
export class MailboxCursorAckResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_CURSOR_ACK_RES;

  constructor({ mailboxId, deviceId, lastSeq } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.deviceId = deviceId == null ? "" : String(deviceId);
    this.lastSeq = lastSeq == null ? 0 : Number(lastSeq);
    if (this.constructor === MailboxCursorAckResponse) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.deviceId.trim().length > 0, "deviceId must be non-empty");
    this.assert(Number.isInteger(this.lastSeq) && this.lastSeq >= 0, "lastSeq must be a non-negative integer");
  }
}
