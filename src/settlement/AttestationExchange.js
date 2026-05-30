import { PeerUptimeAttestationV1 } from "@rezprotocol/core";
import { encodeControlMessage, sendControlMessage } from "../network/tcp/TcpFraming.js";

const CONTROL_ATTESTATION_ANNOUNCE = "attestation.announce";
const MAX_ATTESTATIONS_PER_MESSAGE = 200;

/**
 * Gossips PeerUptimeAttestationV1 records between relays.
 *
 * Follows the same push-based pattern as DescriptorExchange:
 * - When a relay produces new attestations, it announces them to all peers
 * - Peers merge received attestations into their PeerAttestationService
 * - No reply/exchange — attestations are fire-and-forget
 *
 * Registered as a control message handler via ControlMessageRegistry.
 */
export class AttestationExchange {
  #attestationService;
  #peerSockets = new Set();
  #maxPeers;

  /**
   * @param {object} opts
   * @param {PeerAttestationService} opts.attestationService — stores received attestations
   * @param {number} [opts.maxPeers] — max tracked peer sockets
   */
  constructor({ attestationService, maxPeers = 100 }) {
    if (!attestationService) throw new Error("AttestationExchange requires attestationService");
    this.#attestationService = attestationService;
    this.#maxPeers = maxPeers;
  }

  /**
   * Register control message handlers with the ControlMessageRegistry.
   * @param {ControlMessageRegistry} registry
   */
  install(registry) {
    registry.register(CONTROL_ATTESTATION_ANNOUNCE, (ctlObj, _socket) => {
      this.#handleAnnounce(ctlObj);
    });
  }

  /**
   * Add a relay-verified peer socket for attestation broadcast.
   * @param {object} socket
   */
  addPeer(socket) {
    if (this.#peerSockets.size >= this.#maxPeers) return;
    this.#peerSockets.add(socket);
  }

  /**
   * Remove a disconnected peer socket.
   * @param {object} socket
   */
  removePeer(socket) {
    this.#peerSockets.delete(socket);
  }

  /**
   * Broadcast attestations to all connected peers.
   * Called after PeerAttestationService produces new attestations.
   * @param {PeerUptimeAttestationV1[]} attestations
   */
  announceToAllPeers(attestations) {
    if (!attestations || attestations.length === 0) return;
    const batch = attestations.slice(0, MAX_ATTESTATIONS_PER_MESSAGE);
    const payload = {
      _ctl: CONTROL_ATTESTATION_ANNOUNCE,
      attestations: batch.map((a) => a.toJSON()),
    };
    const frame = encodeControlMessage(payload);
    for (const socket of this.#peerSockets) {
      if (socket.destroyed === true) {
        this.#peerSockets.delete(socket);
        continue;
      }
      try {
        socket.write(frame);
      } catch (_err) {
        this.#peerSockets.delete(socket);
      }
    }
  }

  #handleAnnounce(ctlObj) {
    if (!ctlObj || !Array.isArray(ctlObj.attestations)) return;
    for (const raw of ctlObj.attestations) {
      if (!raw || typeof raw !== "object") continue;
      try {
        const attestation = PeerUptimeAttestationV1.fromJSON(raw);
        this.#attestationService.receiveAttestation(attestation);
      } catch (_err) {
        // Skip invalid attestations silently — don't trust remote data
      }
    }
  }
}
