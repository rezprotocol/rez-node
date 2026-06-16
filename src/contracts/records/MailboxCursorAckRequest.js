import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * mailbox.cursorAck — advance a device's durable cursor on the home log.
 *
 * Unlike `mailbox.ack` (which DELETES an event from the transient RMailbox),
 * cursorAck advances the device's read watermark on the durable home inbox
 * (PgDurableInbox) WITHOUT deleting the row — pruning happens separately below
 * the slowest live device's cursor. The cursor advances ONLY on chat-side
 * `consumed` (decrypt/apply or dedup-hit), never on receive/socket-write.
 *
 * `throughSeq` is the per-inbox durable seq the device has consumed through.
 *
 * There is deliberately NO `deviceId` in the request: the cursor's device is
 * bound to the authenticated session (`ctx.sessionDeviceId`) at the handler, so
 * a session can only advance ITS OWN cursor. Carrying a client-supplied deviceId
 * on a data-loss primitive would be split-brain — the handler would ignore it
 * anyway. (S2.5 multi-device keeps one device per session, so this stays true.)
 * The storage layer additionally enforces monotonic + delivered-bounded advance.
 */
export class MailboxCursorAckRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.MAILBOX_CURSOR_ACK;

  constructor({ mailboxId, throughSeq, capChain } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.throughSeq = throughSeq == null ? 0 : Number(throughSeq);
    this.capChain = Array.isArray(capChain) ? capChain : null;
    if (this.constructor === MailboxCursorAckRequest) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(Number.isInteger(this.throughSeq) && this.throughSeq >= 0, "throughSeq must be a non-negative integer");
  }
}
