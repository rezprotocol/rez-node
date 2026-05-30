import { StorageChallengeV1, StorageChallengeResponseV1 } from "@rezprotocol/core";
import { sendControlMessage } from "../network/tcp/TcpFraming.js";

const CONTROL_STORAGE_CHALLENGE = "storage.challenge";
const CONTROL_STORAGE_RESPONSE = "storage.response";

/**
 * Gossips storage challenges and responses between relays.
 *
 * Registered as control message handlers via ControlMessageRegistry.
 * Follows the same pattern as AttestationExchange.
 *
 * Flow:
 * 1. Challenger relay sends storage.challenge to target relay
 * 2. Target relay responds with storage.response
 * 3. Challenger verifies response, invokes onVerificationFailed callback on failure
 *
 * Slashing is handled externally — this class reports failures via the
 * onVerificationFailed callback. The bootstrap layer or a future
 * StorageCommitmentManager resolves the escrow ID and calls slash().
 */
export class StorageVerificationExchange {
  #verifier;
  #metrics;
  #onVerificationFailed;
  #peerSockets = new Map(); // relayKeyId → socket

  /** @type {Map<string, { challenge: StorageChallengeV1, socket: object, sentAtMs: number }>} */
  #pendingChallenges = new Map(); // challengeId → pending info

  /**
   * @param {object} opts
   * @param {ChallengeResponseVerifier} opts.verifier — issues/responds/verifies challenges
   * @param {NodeMetrics} [opts.metrics] — for tracking
   * @param {function({targetRelayKeyId: string, objectId: string, reason: string}): void} [opts.onVerificationFailed] — called when a storage proof fails
   */
  constructor({ verifier, metrics = null, onVerificationFailed = null }) {
    if (!verifier) throw new Error("StorageVerificationExchange requires verifier");
    this.#verifier = verifier;
    this.#metrics = metrics;
    this.#onVerificationFailed = typeof onVerificationFailed === "function" ? onVerificationFailed : null;
  }

  /**
   * Register control message handlers.
   * @param {ControlMessageRegistry} registry
   */
  install(registry) {
    registry.register(CONTROL_STORAGE_CHALLENGE, (ctlObj, socket) => {
      this.#handleChallenge(ctlObj, socket);
    });
    registry.register(CONTROL_STORAGE_RESPONSE, (ctlObj, _socket) => {
      this.#handleResponse(ctlObj);
    });
  }

  /**
   * Track a peer socket for sending challenges.
   * @param {string} relayKeyId
   * @param {object} socket
   */
  addPeer(relayKeyId, socket) {
    this.#peerSockets.set(relayKeyId, socket);
  }

  /**
   * Remove a peer by relayKeyId.
   * @param {string} relayKeyId
   */
  removePeer(relayKeyId) {
    this.#peerSockets.delete(relayKeyId);
  }

  /**
   * Remove a peer by socket reference.
   * @param {object} socket
   */
  removePeerBySocket(socket) {
    for (const [key, s] of this.#peerSockets) {
      if (s === socket) {
        this.#peerSockets.delete(key);
        break;
      }
    }
  }

  /**
   * Send a challenge to a target relay.
   * @param {string} targetRelayKeyId
   * @param {string} objectId
   * @returns {Promise<StorageChallengeV1|null>} the challenge sent, or null if peer not connected
   */
  async sendChallenge(targetRelayKeyId, objectId) {
    const socket = this.#peerSockets.get(targetRelayKeyId);
    if (!socket || socket.destroyed) return null;

    let challenge;
    try {
      challenge = await this.#verifier.issueChallenge(targetRelayKeyId, objectId);
    } catch (err) {
      console.error("[STORAGE-VERIFY] Failed to issue challenge:", err && err.message ? err.message : err);
      return null;
    }

    const sent = sendControlMessage(socket, {
      _ctl: CONTROL_STORAGE_CHALLENGE,
      challenge: challenge.toJSON(),
    });
    if (!sent) return null;

    this.#pendingChallenges.set(challenge.challengeId, {
      challenge,
      socket,
      sentAtMs: Date.now(),
    });
    return challenge;
  }

  async #handleChallenge(ctlObj, socket) {
    if (!ctlObj || !ctlObj.challenge) return;
    let challenge;
    try {
      challenge = StorageChallengeV1.fromJSON(ctlObj.challenge);
    } catch (_err) {
      return;
    }

    let response;
    try {
      response = await this.#verifier.respondToChallenge(challenge);
    } catch (err) {
      console.error("[STORAGE-VERIFY] Failed to respond to challenge:", err && err.message ? err.message : err);
      return;
    }

    sendControlMessage(socket, {
      _ctl: CONTROL_STORAGE_RESPONSE,
      response: response.toJSON(),
    });
  }

  async #handleResponse(ctlObj) {
    if (!ctlObj || !ctlObj.response) return;
    let response;
    try {
      response = StorageChallengeResponseV1.fromJSON(ctlObj.response);
    } catch (_err) {
      return;
    }

    const pending = this.#pendingChallenges.get(response.challengeId);
    if (!pending) return;
    this.#pendingChallenges.delete(response.challengeId);

    let result;
    try {
      result = await this.#verifier.verifyResponse(pending.challenge, response);
    } catch (err) {
      console.error("[STORAGE-VERIFY] Verification error:", err && err.message ? err.message : err);
      return;
    }

    if (result.valid) {
      if (this.#metrics) {
        this.#metrics.increment("storeReadsTotal");
      }
      return;
    }

    console.warn("[STORAGE-VERIFY] Storage proof FAILED for relay=" + pending.challenge.targetRelayKeyId
      + " object=" + pending.challenge.objectId + " reason=" + result.reason);

    if (this.#metrics) {
      this.#metrics.increment("errorsTotal");
    }

    if (this.#onVerificationFailed) {
      this.#onVerificationFailed({
        targetRelayKeyId: pending.challenge.targetRelayKeyId,
        objectId: pending.challenge.objectId,
        reason: result.reason,
      });
    }
  }

  /**
   * Clean up expired pending challenges.
   */
  pruneExpired() {
    const now = Date.now();
    for (const [challengeId, pending] of this.#pendingChallenges) {
      if (now > pending.challenge.expiresAtMs) {
        this.#pendingChallenges.delete(challengeId);
      }
    }
  }
}
