import { isNonEmptyString } from "@rezprotocol/core";
import { sendControlMessage } from "../network/tcp/TcpFraming.js";

/**
 * TCP-based relay descriptor gossip.
 *
 * Exchanges RelayDescriptorV1 records over existing TCP peer connections,
 * replacing the need for dedicated HTTP directory servers.
 *
 * Protocol:
 *   { _ctl: "descriptor.announce", descriptors: [...] }  — sent by initiator after relay.identify
 *   { _ctl: "descriptor.exchange", descriptors: [...] }  — reply (no further reply, prevents ping-pong)
 */

const MAX_DESCRIPTORS_PER_MESSAGE = 100;

export class DescriptorExchange {
  constructor({
    relayStore,
    validateDescriptor,
    maxDescriptorsPerMessage = MAX_DESCRIPTORS_PER_MESSAGE,
    maxPeers = 100,
    onDescriptorsAccepted = null,
    logger = console,
    nowMs = () => Date.now(),
  } = {}) {
    if (!relayStore || typeof relayStore.mergeDescriptors !== "function") {
      throw new Error("DescriptorExchange requires relayStore with mergeDescriptors");
    }
    if (typeof validateDescriptor !== "function") {
      throw new Error("DescriptorExchange requires validateDescriptor function");
    }
    this._relayStore = relayStore;
    this._validate = validateDescriptor;
    this._maxPerMessage = Math.max(1, Math.min(500, Number(maxDescriptorsPerMessage) || MAX_DESCRIPTORS_PER_MESSAGE));
    this._maxPeers = Math.max(1, Number(maxPeers) || 100);
    this._onAccepted = typeof onDescriptorsAccepted === "function" ? onDescriptorsAccepted : null;
    this._logger = logger ?? console;
    this._nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();
    this._peerSockets = new Set();
  }

  // ---------------------------------------------------------------------------
  // Peer lifecycle
  // ---------------------------------------------------------------------------

  addPeer(socket) {
    if (socket) this._peerSockets.add(socket);
  }

  removePeer(socket) {
    this._peerSockets.delete(socket);
  }

  // ---------------------------------------------------------------------------
  // Outbound: build announce payload for handshake
  // ---------------------------------------------------------------------------

  /**
   * Build a descriptor.announce message as framing-ready bytes.
   * Sent by RelayConnectionPool after relay.identify.
   * @returns {Uint8Array}
   */
  buildAnnounceBytes() {
    const descriptors = this._getDescriptorsJson();
    return new TextEncoder().encode(JSON.stringify({
      _ctl: "descriptor.announce",
      descriptors,
    }));
  }

  // ---------------------------------------------------------------------------
  // Inbound: SocketFrameRouter handlers
  // ---------------------------------------------------------------------------

  /**
   * Handle incoming descriptor.announce — merge descriptors, reply with exchange.
   * @returns {boolean}
   */
  handleAnnounce(ctlObj, socket) {
    if (!ctlObj || typeof ctlObj !== "object") return false;
    const accepted = this._mergeDescriptors(ctlObj.descriptors);
    // Reply with our known descriptors (no further reply expected)
    const reply = {
      _ctl: "descriptor.exchange",
      descriptors: this._getDescriptorsJson(),
    };
    this._sendCtl(socket, reply);
    if (accepted > 0 && this._onAccepted) {
      try { this._onAccepted(accepted); } catch (cbErr) {
        console.error("[DescriptorExchange] onAccepted callback failed: " + (cbErr && cbErr.message ? cbErr.message : cbErr));
      }
    }
    return true;
  }

  /**
   * Handle incoming descriptor.exchange — merge descriptors only, no reply.
   * @returns {boolean}
   */
  handleExchange(ctlObj) {
    if (!ctlObj || typeof ctlObj !== "object") return false;
    const accepted = this._mergeDescriptors(ctlObj.descriptors);
    if (accepted > 0 && this._onAccepted) {
      try { this._onAccepted(accepted); } catch (cbErr) {
        console.error("[DescriptorExchange] onAccepted callback failed: " + (cbErr && cbErr.message ? cbErr.message : cbErr));
      }
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // Push: announce to all connected peers
  // ---------------------------------------------------------------------------

  /**
   * Push all known descriptors to every connected peer.
   */
  announceToAllPeers() {
    const descriptors = this._getDescriptorsJson();
    const ctl = { _ctl: "descriptor.announce", descriptors };
    for (const socket of this._peerSockets) {
      this._sendCtl(socket, ctl);
    }
  }

  /**
   * Push a single self descriptor update to all peers.
   * Called when OnionKeyRotator updates the self descriptor.
   * @param {object} selfDescriptorJson - toJSON() output of the self descriptor
   */
  announceSelfToAllPeers(selfDescriptorJson) {
    if (!selfDescriptorJson || typeof selfDescriptorJson !== "object") return;
    const ctl = { _ctl: "descriptor.announce", descriptors: [selfDescriptorJson] };
    for (const socket of this._peerSockets) {
      this._sendCtl(socket, ctl);
    }
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  _getDescriptorsJson() {
    const nowMs = this._nowMs();
    const descriptors = this._relayStore.listDescriptors({ nowMs });
    return descriptors
      .slice(0, this._maxPerMessage)
      .map((d) => (d && typeof d.toJSON === "function" ? d.toJSON() : d));
  }

  _mergeDescriptors(rawDescriptors) {
    if (!Array.isArray(rawDescriptors) || rawDescriptors.length === 0) return 0;
    const nowMs = this._nowMs();
    const validated = [];
    for (const raw of rawDescriptors.slice(0, this._maxPerMessage)) {
      if (!raw || typeof raw !== "object") continue;
      const result = this._validate(raw, { nowMs });
      if (result.ok) {
        validated.push(result.descriptor);
      }
    }
    if (validated.length === 0) return 0;
    const merge = this._relayStore.mergeDescriptors(validated, {
      source: "peer",
      receivedAtMs: nowMs,
      maxPeers: this._maxPeers,
    });
    const count = merge.accepted || 0;
    if (count > 0) {
      if (this._logger && typeof this._logger.debug === "function") {
        this._logger.debug("[DescriptorExchange] accepted", count, "descriptors from peer");
      }
    }
    return count;
  }

  _sendCtl(socket, ctlObj) {
    sendControlMessage(socket, ctlObj);
  }
}
