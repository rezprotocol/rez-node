import { randomBytes } from "node:crypto";
import { StorageVerifier } from "@rezprotocol/core";
import { StorageChallengeV1, StorageChallengeResponseV1 } from "@rezprotocol/core";
import { canonicalJSONStringify } from "../util/canonicalize.js";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { generateSettlementId } from "./settlementUtil.js";

const DEFAULT_CHALLENGE_TTL_MS = 60 * 1000; // 60 seconds to respond
const DEFAULT_BYTE_LENGTH = 4096;

const crypto = new NodeCryptoProvider();

/**
 * SHA-256 byte-range challenge-response storage verifier.
 *
 * Extends StorageVerifier with real cryptographic challenges:
 * - Challenger picks a random byte range from an object's canonical JSON
 * - Target reads the object, extracts the byte range, hashes it
 * - Challenger independently computes the same hash and compares
 *
 * All challenges and responses are Ed25519-signed via ReceiptSigner.
 */
export class ChallengeResponseVerifier extends StorageVerifier {
  static type = "ChallengeResponseVerifier";

  #receiptSigner;
  #selfRelayKeyId;
  #objectStore;
  #challengeTtlMs;

  /**
   * @param {object} opts
   * @param {ReceiptSigner} opts.receiptSigner — signs challenges and responses
   * @param {string} opts.selfRelayKeyId — this relay's key ID
   * @param {ObjectStore} opts.objectStore — local object storage for reading stored data
   * @param {number} [opts.challengeTtlMs] — how long target has to respond (default 60s)
   */
  constructor({ receiptSigner, selfRelayKeyId, objectStore, challengeTtlMs = DEFAULT_CHALLENGE_TTL_MS }) {
    super();
    if (!receiptSigner) throw new Error("ChallengeResponseVerifier requires receiptSigner");
    if (!selfRelayKeyId || typeof selfRelayKeyId !== "string") throw new Error("ChallengeResponseVerifier requires selfRelayKeyId");
    if (!objectStore) throw new Error("ChallengeResponseVerifier requires objectStore");
    this.#receiptSigner = receiptSigner;
    this.#selfRelayKeyId = selfRelayKeyId;
    this.#objectStore = objectStore;
    this.#challengeTtlMs = challengeTtlMs;
  }

  /**
   * Issue a storage challenge to a target relay.
   *
   * Picks a random byte range from the canonical JSON representation
   * of the specified object. The target must produce a SHA-256 hash
   * of that byte range to prove it stores the object.
   *
   * @param {string} targetRelayKeyId
   * @param {string} objectId
   * @returns {Promise<StorageChallengeV1>}
   */
  async issueChallenge(targetRelayKeyId, objectId) {
    if (!targetRelayKeyId || typeof targetRelayKeyId !== "string") {
      throw new Error("issueChallenge requires targetRelayKeyId");
    }
    if (!objectId || typeof objectId !== "string") {
      throw new Error("issueChallenge requires objectId");
    }

    // Read the object to determine its canonical byte length
    const envelope = await this.#objectStore.get(objectId);
    if (!envelope) {
      throw new Error(`Object not found: ${objectId}`);
    }
    const canonicalBytes = this.#canonicalizeEnvelope(envelope);
    const totalLen = canonicalBytes.length;
    if (totalLen === 0) {
      throw new Error(`Object is empty: ${objectId}`);
    }

    // Pick a random byte range
    const byteLength = Math.min(DEFAULT_BYTE_LENGTH, totalLen);
    const maxOffset = Math.max(0, totalLen - byteLength);
    const byteOffset = maxOffset > 0 ? randomInt(maxOffset + 1) : 0;

    const nowMs = Date.now();
    const body = {
      v: 1,
      challengeId: generateSettlementId(),
      challengerRelayKeyId: this.#selfRelayKeyId,
      targetRelayKeyId,
      objectId,
      byteOffset,
      byteLength,
      createdAtMs: nowMs,
      expiresAtMs: nowMs + this.#challengeTtlMs,
    };
    const sig = await this.#receiptSigner.sign(body);
    return new StorageChallengeV1({ ...body, sig });
  }

  /**
   * Respond to a storage challenge by producing the SHA-256 hash
   * of the requested byte range.
   *
   * @param {StorageChallengeV1} challenge
   * @returns {Promise<StorageChallengeResponseV1>}
   */
  async respondToChallenge(challenge) {
    if (!challenge || challenge.type !== "StorageChallengeV1") {
      throw new Error("respondToChallenge requires StorageChallengeV1");
    }

    const envelope = await this.#objectStore.get(challenge.objectId);
    if (!envelope) {
      throw new Error(`Object not found: ${challenge.objectId}`);
    }

    const canonicalBytes = this.#canonicalizeEnvelope(envelope);
    const slice = canonicalBytes.slice(challenge.byteOffset, challenge.byteOffset + challenge.byteLength);
    const hashBytes = crypto.hashSha256(slice);

    const body = {
      v: 1,
      challengeId: challenge.challengeId,
      targetRelayKeyId: this.#selfRelayKeyId,
      hashAlg: "sha256",
      createdAtMs: Date.now(),
    };
    const sig = await this.#receiptSigner.sign({ ...body, hashBytes: Array.from(hashBytes) });
    return new StorageChallengeResponseV1({ ...body, hashBytes, sig });
  }

  /**
   * Verify a challenge response by independently computing the expected hash.
   *
   * @param {StorageChallengeV1} challenge
   * @param {StorageChallengeResponseV1} response
   * @returns {Promise<{valid: boolean, reason?: string}>}
   */
  async verifyResponse(challenge, response) {
    if (!challenge || challenge.type !== "StorageChallengeV1") {
      return { valid: false, reason: "invalid challenge" };
    }
    if (!response || response.type !== "StorageChallengeResponseV1") {
      return { valid: false, reason: "invalid response" };
    }
    if (response.challengeId !== challenge.challengeId) {
      return { valid: false, reason: "challengeId mismatch" };
    }
    if (response.targetRelayKeyId !== challenge.targetRelayKeyId) {
      return { valid: false, reason: "targetRelayKeyId mismatch" };
    }
    if (response.hashAlg !== "sha256") {
      return { valid: false, reason: "unsupported hashAlg" };
    }

    // Check expiry
    if (Date.now() > challenge.expiresAtMs) {
      return { valid: false, reason: "challenge expired" };
    }

    // Recompute expected hash from our local copy
    const envelope = await this.#objectStore.get(challenge.objectId);
    if (!envelope) {
      return { valid: false, reason: "object not found locally — cannot verify" };
    }

    const canonicalBytes = this.#canonicalizeEnvelope(envelope);
    const slice = canonicalBytes.slice(challenge.byteOffset, challenge.byteOffset + challenge.byteLength);
    const expectedHash = crypto.hashSha256(slice);

    // Constant-time comparison
    if (response.hashBytes.length !== expectedHash.length) {
      return { valid: false, reason: "hash length mismatch" };
    }
    let diff = 0;
    for (let i = 0; i < expectedHash.length; i++) {
      diff |= response.hashBytes[i] ^ expectedHash[i];
    }
    if (diff !== 0) {
      return { valid: false, reason: "hash mismatch — storage proof failed" };
    }

    return { valid: true };
  }

  #canonicalizeEnvelope(envelope) {
    const json = typeof envelope.toJSON === "function" ? envelope.toJSON() : envelope;
    return new TextEncoder().encode(canonicalJSONStringify(json));
  }
}

function randomInt(max) {
  if (max <= 1) return 0;
  const buf = randomBytes(4);
  return ((buf[0] << 24 | buf[1] << 16 | buf[2] << 8 | buf[3]) >>> 0) % max;
}
