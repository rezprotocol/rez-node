import { NodeCryptoProvider } from "../../src/crypto/NodeCryptoProvider.js";
import { buildDurableRecordV1, durableRecordSignableBytes, durableRecordLocalId } from "@rezprotocol/core";

const CRYPTO = new NodeCryptoProvider();

/**
 * Build a signed DurableRecordV1 for tests. Returns the record plus the
 * publisher key material and the derived slot id.
 *
 * @param {object} [opts]
 * @param {{ publicKey: Uint8Array, privateKey: Uint8Array }} [opts.keypair] - reuse a publisher
 * @param {string} [opts.recordKind]
 * @param {string} [opts.recordId]
 * @param {string} [opts.payloadText] - convenience: utf8 → base64 payload
 * @param {string} [opts.payloadB64]
 * @param {number} [opts.issuedAtMs]
 * @param {number} [opts.expiresAtMs]
 * @returns {{ record: object, publicKeyB64: string, privateKey: Uint8Array, localId: string }}
 */
export function makeSignedRecord(opts = {}) {
  const keypair = opts.keypair || CRYPTO.generateSigningKeyPair();
  const publicKeyB64 = Buffer.from(keypair.publicKey).toString("base64");
  const recordKind = opts.recordKind || "test-record";
  const recordId = opts.recordId || "rec-" + Math.random().toString(36).slice(2, 10);
  const issuedAtMs = Number.isFinite(opts.issuedAtMs) ? opts.issuedAtMs : 1000;
  const expiresAtMs = Number.isFinite(opts.expiresAtMs) ? opts.expiresAtMs : issuedAtMs + 3_600_000;
  const payloadB64 = typeof opts.payloadB64 === "string"
    ? opts.payloadB64
    : Buffer.from(typeof opts.payloadText === "string" ? opts.payloadText : "payload").toString("base64");

  const record = buildDurableRecordV1({
    recordKind,
    recordId,
    publisherPublicKeyB64: publicKeyB64,
    payloadB64,
    issuedAtMs,
    expiresAtMs,
  });
  const sig = CRYPTO.sign({ privateKey: keypair.privateKey, msg: durableRecordSignableBytes(record) });
  record.sigB64 = Buffer.from(sig).toString("base64");

  const localId = durableRecordLocalId({ publisherPublicKeyB64: publicKeyB64, recordKind, recordId });
  return { record, publicKeyB64, privateKey: keypair.privateKey, keypair, localId };
}

export { CRYPTO as testCrypto };
