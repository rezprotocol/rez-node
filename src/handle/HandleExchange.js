import { HandleClaimV1 } from "@rezprotocol/core";
import { encodeControlMessage } from "../network/tcp/TcpFraming.js";

const CONTROL_HANDLE_ANNOUNCE = "handle.announce";
const MAX_CLAIMS_PER_MESSAGE = 100;

/**
 * Gossips HandleClaimV1 records between relays.
 *
 * Push-based gossip: when a handle is registered locally, announce
 * it to all connected peers. When peers announce handles, merge them
 * into the local HandleRegistry (first-come-first-served).
 *
 * Follows the same pattern as AttestationExchange.
 */
export class HandleExchange {
  #handleRegistry;
  #peerSockets = new Set();
  #maxPeers;

  /**
   * @param {object} opts
   * @param {HandleRegistry} opts.handleRegistry — stores accepted claims
   * @param {number} [opts.maxPeers]
   */
  constructor({ handleRegistry, maxPeers = 100 }) {
    if (!handleRegistry) throw new Error("HandleExchange requires handleRegistry");
    this.#handleRegistry = handleRegistry;
    this.#maxPeers = maxPeers;
  }

  /**
   * Register control message handler.
   * @param {ControlMessageRegistry} registry
   */
  install(registry) {
    registry.register(CONTROL_HANDLE_ANNOUNCE, (ctlObj, _socket) => {
      this.#handleAnnounce(ctlObj);
    });
  }

  /**
   * Add a relay-verified peer socket.
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
   * Broadcast handle claims to all connected peers.
   * @param {HandleClaimV1[]} claims
   */
  announceToAllPeers(claims) {
    if (!claims || claims.length === 0) return;
    const batch = claims.slice(0, MAX_CLAIMS_PER_MESSAGE);
    const payload = {
      _ctl: CONTROL_HANDLE_ANNOUNCE,
      claims: batch.map((c) => c.toJSON()),
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

  /**
   * Send all known claims to a newly connected peer.
   * @param {object} socket
   */
  async announceAllToPeer(socket) {
    if (!socket || socket.destroyed) return;
    const claims = await this.#handleRegistry.listClaims();
    if (claims.length === 0) return;
    const payload = {
      _ctl: CONTROL_HANDLE_ANNOUNCE,
      claims: claims.slice(0, MAX_CLAIMS_PER_MESSAGE).map((c) => c.toJSON()),
    };
    const frame = encodeControlMessage(payload);
    try {
      socket.write(frame);
    } catch (_err) {
      // ignore write failures
    }
  }

  async #handleAnnounce(ctlObj) {
    if (!ctlObj || !Array.isArray(ctlObj.claims)) return;
    for (const raw of ctlObj.claims) {
      if (!raw || typeof raw !== "object") continue;
      try {
        const claim = HandleClaimV1.fromJSON(raw);
        await this.#handleRegistry.acceptGossipedClaim(claim);
      } catch (_err) {
        // Skip invalid claims
      }
    }
  }
}
