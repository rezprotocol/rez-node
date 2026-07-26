import { NodeCryptoProvider } from "../../src/crypto/NodeCryptoProvider.js";
import {
  buildDurableRecordV1,
  durableRecordSignableBytes,
  durableRecordLocalId,
  bytesToBase64,
  AccountAuthorityStateV1,
  ACCOUNT_AUTHORITY_STATE_PURPOSE,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  DURABLE_RECORD_V2_VERSION,
  durableRecordV2Slot,
  durableRecordV2SignableBytes,
} from "@rezprotocol/core";

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

/**
 * Build a genuine ROOT-SIGNED account-authority-state publication: a direct-mode DurableRecordV2
 * (signer == owner, empty cert chain, no capability) wrapping a validly-signed
 * AccountAuthorityStateV1 at `epoch`.
 *
 * There is no shortcut version of this fixture, deliberately. The kind is root-signed-only AND
 * epoch-ordered AND payload/owner-bound, so a placeholder payload is refused by the verifier and by
 * the store's epoch gate — which is the point. Any test that wants this kind in a slot has to build
 * the real thing.
 *
 * @param {object} [opts]
 * @param {{ publicKey: Uint8Array, privateKey: Uint8Array }} [opts.keypair] - reuse an account root
 * @param {number} [opts.epoch]
 * @param {string[]} [opts.revokedCertIds]
 * @param {number} [opts.issuedAtMs]
 * @param {number} [opts.ttlMs]
 * @param {string} [opts.recordId]
 * @returns {{ record: object, publicKeyB64: string, keypair: object, localId: string, epoch: number }}
 */
export function makeSignedAuthorityStateRecord(opts = {}) {
  const keypair = opts.keypair || CRYPTO.generateSigningKeyPair();
  const publicKeyB64 = bytesToBase64(keypair.publicKey);
  const epoch = Number.isInteger(opts.epoch) ? opts.epoch : 1;
  const revokedCertIds = Array.isArray(opts.revokedCertIds) ? opts.revokedCertIds : [];
  const issuedAtMs = Number.isFinite(opts.issuedAtMs) ? opts.issuedAtMs : 1_700_000_000_000;
  const ttlMs = Number.isFinite(opts.ttlMs) ? opts.ttlMs : 86_400_000;
  const recordId = typeof opts.recordId === "string" && opts.recordId.length > 0 ? opts.recordId : "v1";

  const stateBody = {
    v: 1,
    purpose: ACCOUNT_AUTHORITY_STATE_PURPOSE,
    accountIdentityPublicKeyB64: publicKeyB64,
    epoch,
    revokedCertIds,
    minValidIssuedAtMs: 0,
    issuedAtMs,
    signerPublicKeyB64: publicKeyB64,
  };
  const stateSig = CRYPTO.sign({ privateKey: keypair.privateKey, msg: AccountAuthorityStateV1.signableBytes(stateBody) });
  const state = new AccountAuthorityStateV1({ ...stateBody, sig: { alg: "ed25519", sigB64: bytesToBase64(stateSig) } });

  const envelope = {
    v: DURABLE_RECORD_V2_VERSION,
    recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
    recordId,
    ownerPublicKeyB64: publicKeyB64,
    signerPublicKeyB64: publicKeyB64,
    certChain: [],
    requiredCapability: null,
    issuedAtMs,
    expiresAtMs: issuedAtMs + ttlMs,
    payloadB64: bytesToBase64(new TextEncoder().encode(JSON.stringify(state.toJSON()))),
  };
  const record = {
    ...envelope,
    sigB64: bytesToBase64(CRYPTO.sign({ privateKey: keypair.privateKey, msg: durableRecordV2SignableBytes(envelope) })),
  };
  const localId = durableRecordV2Slot({
    ownerPublicKeyB64: publicKeyB64,
    recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
    recordId,
  });
  return { record, publicKeyB64, keypair, localId, epoch };
}

export { CRYPTO as testCrypto };
