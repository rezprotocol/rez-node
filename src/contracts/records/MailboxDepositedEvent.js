import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * The live `evt.mailbox.deposited` server-push body — modelled to match the ACTUAL
 * wire payload emitted by RelayDepositRouter (which now BUILDS the frame from this
 * record via toJSON(), so the record and the wire frame cannot drift; see the
 * record==frame equality test). Carries the DECODED outer-packet body as
 * `ciphertextB64` (identical bytes to the catch-up path) so a live client applies
 * it directly, plus the durable `seq` for cursor-model clients (null on the
 * transient RMailbox path, which has no per-inbox seq).
 */
export class MailboxDepositedEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.EVT_MAILBOX_DEPOSITED;

  constructor({ mailboxId, eventId, ciphertextB64 = null, seq = null } = {}) {
    super();
    this.mailboxId = mailboxId == null ? "" : String(mailboxId);
    this.eventId = eventId == null ? "" : String(eventId);
    this.ciphertextB64 = ciphertextB64 == null ? null : String(ciphertextB64);
    this.seq = seq == null ? null : Number(seq);
    if (this.constructor === MailboxDepositedEvent) this._seal();
  }

  validate() {
    this.assert(this.mailboxId.trim().length > 0, "mailboxId must be non-empty");
    this.assert(this.eventId.trim().length > 0, "eventId must be non-empty");
    this.assert(
      this.seq === null || (Number.isInteger(this.seq) && this.seq >= 0),
      "seq must be a non-negative integer when present",
    );
  }
}
