import { randomUUID } from "node:crypto";
import { CONTRACT_VERSION, REZ_CONTRACT_TYPES, decodeOuterPacket } from "@rezprotocol/core";
import { MailboxDepositedEvent } from "../contracts/records/MailboxDepositedEvent.js";

const T = REZ_CONTRACT_TYPES;

/**
 * SSOT for the live `evt.mailbox.deposited` wire frame. EVERY emit site — the
 * direct local broadcast (RelayDepositRouter), the cross-node bus-drain
 * (GatewaySession), and any future pusher — builds the frame here, so the wire
 * shape cannot drift between paths. The body IS the record's toJSON() (the same
 * record↔frame guarantee from audit P2).
 */
export function buildMailboxDepositedFrame({ mailboxId, eventId, ciphertextB64 = null, seq = null } = {}) {
  const record = new MailboxDepositedEvent({
    mailboxId,
    eventId,
    ciphertextB64: ciphertextB64 || null,
    seq: seq == null ? null : seq,
  });
  return {
    id: `${T.EVT_MAILBOX_DEPOSITED}:${Date.now()}:${randomUUID()}`,
    t: T.EVT_MAILBOX_DEPOSITED,
    v: CONTRACT_VERSION,
    body: record.toJSON(),
  };
}

/**
 * Base64 of the DECODED outer-packet body — the exact bytes a live client
 * applies. SSOT decode shared by the catch-up list (MailboxHandler) and the
 * cross-node drain so they hand the client identical ciphertext. A stored value
 * that is not a framed outer packet is returned unchanged (defensive; durable
 * home deposits are always outer packets).
 */
export function outerPacketBodyB64(bytes) {
  if (!(bytes instanceof Uint8Array)) return null;
  let body = bytes;
  try {
    body = decodeOuterPacket(bytes).bodyBytesView;
  } catch {
    body = bytes;
  }
  return Buffer.from(body).toString("base64");
}
