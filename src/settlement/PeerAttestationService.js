import { PeerUptimeAttestationV1 } from "@rezprotocol/core";

const DEFAULT_ATTEST_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000;  // 24 hours
const MAX_ATTESTATIONS_PER_TARGET = 100;

/**
 * Tracks connected peers and produces signed PeerUptimeAttestationV1 records.
 *
 * Periodically pings connected peers and creates attestations.
 * Stores both self-produced and gossip-received attestations
 * for consumption by ReputationScorer.
 */
export class PeerAttestationService {
  #receiptSigner;
  #selfRelayKeyId;
  #metrics;
  #attestIntervalMs;
  #retentionMs;
  #timer = null;
  #onAttestationsProduced = null;

  /** @type {Map<string, { socket: object, connectedAtMs: number }>} relayKeyId → peer info */
  #connectedPeers = new Map();

  /** @type {Map<string, PeerUptimeAttestationV1[]>} targetRelayKeyId → attestations */
  #attestations = new Map();

  /**
   * @param {object} opts
   * @param {ReceiptSigner} opts.receiptSigner — signs attestation records
   * @param {string} opts.selfRelayKeyId — this relay's key ID
   * @param {NodeMetrics} [opts.metrics] — for tracking attestation counts
   * @param {number} [opts.attestIntervalMs] — how often to attest peers (default 5 min)
   * @param {number} [opts.retentionMs] — how long to keep attestations (default 24h)
   */
  constructor({ receiptSigner, selfRelayKeyId, metrics = null, attestIntervalMs = DEFAULT_ATTEST_INTERVAL_MS, retentionMs = DEFAULT_RETENTION_MS }) {
    if (!receiptSigner) throw new Error("PeerAttestationService requires receiptSigner");
    if (!selfRelayKeyId || typeof selfRelayKeyId !== "string") throw new Error("PeerAttestationService requires selfRelayKeyId");
    this.#receiptSigner = receiptSigner;
    this.#selfRelayKeyId = selfRelayKeyId;
    this.#metrics = metrics;
    this.#attestIntervalMs = attestIntervalMs;
    this.#retentionMs = retentionMs;
  }

  /**
   * Set a callback invoked after each attestation cycle with the new attestations.
   * Used by AttestationExchange to broadcast attestations to peers.
   * @param {function(PeerUptimeAttestationV1[]): void} fn
   */
  onAttestationsProduced(fn) {
    this.#onAttestationsProduced = typeof fn === "function" ? fn : null;
  }

  /**
   * Start the periodic attestation loop.
   */
  start() {
    if (this.#timer) return;
    this.#timer = setInterval(() => this.#attestAllPeers(), this.#attestIntervalMs);
    if (this.#timer.unref) this.#timer.unref();
  }

  /**
   * Stop the periodic attestation loop.
   */
  stop() {
    if (this.#timer) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
  }

  /**
   * Register a connected peer. Called when a relay-verified peer joins.
   * @param {string} relayKeyId
   * @param {object} socket
   */
  addPeer(relayKeyId, socket) {
    if (!relayKeyId || typeof relayKeyId !== "string") return;
    this.#connectedPeers.set(relayKeyId, { socket, connectedAtMs: Date.now() });
    if (this.#metrics) {
      this.#metrics.setGauge("activeAttestationPeers", this.#connectedPeers.size);
    }
  }

  /**
   * Remove a disconnected peer. Called when a peer drops.
   * @param {string} relayKeyId
   */
  removePeer(relayKeyId) {
    this.#connectedPeers.delete(relayKeyId);
    if (this.#metrics) {
      this.#metrics.setGauge("activeAttestationPeers", this.#connectedPeers.size);
    }
  }

  /**
   * Remove a peer by socket reference (when relayKeyId is unknown at disconnect time).
   * @param {object} socket
   */
  removePeerBySocket(socket) {
    let found = false;
    for (const [relayKeyId, info] of this.#connectedPeers) {
      if (info.socket === socket) {
        this.#connectedPeers.delete(relayKeyId);
        found = true;
        break;
      }
    }
    if (found && this.#metrics) {
      this.#metrics.setGauge("activeAttestationPeers", this.#connectedPeers.size);
    }
  }

  /**
   * Accept an attestation received via gossip from another relay.
   * @param {PeerUptimeAttestationV1} attestation
   */
  receiveAttestation(attestation) {
    if (!attestation || attestation.type !== "PeerUptimeAttestationV1") return;
    if (attestation.attesterId === this.#selfRelayKeyId) return; // ignore own echoed attestations
    this.#storeAttestation(attestation);
    if (this.#metrics) {
      this.#metrics.increment("attestationsReceivedTotal");
    }
  }

  /**
   * Get all attestations for a target relay (from all attesters).
   * @param {string} targetRelayKeyId
   * @returns {PeerUptimeAttestationV1[]}
   */
  getAttestationsFor(targetRelayKeyId) {
    return this.#attestations.get(targetRelayKeyId) || [];
  }

  /**
   * Get all recent attestations produced by this relay.
   * @returns {PeerUptimeAttestationV1[]}
   */
  getOwnAttestations() {
    const result = [];
    for (const attestations of this.#attestations.values()) {
      for (const a of attestations) {
        if (a.attesterId === this.#selfRelayKeyId) {
          result.push(a);
        }
      }
    }
    return result;
  }

  /**
   * Get all stored attestations (for gossip broadcast).
   * @returns {PeerUptimeAttestationV1[]}
   */
  getAllAttestations() {
    const result = [];
    for (const attestations of this.#attestations.values()) {
      for (const a of attestations) {
        result.push(a);
      }
    }
    return result;
  }

  /**
   * Number of connected peers.
   * @returns {number}
   */
  get peerCount() {
    return this.#connectedPeers.size;
  }

  async #attestAllPeers() {
    try {
      this.#pruneExpired();
      const produced = [];
      const nowMs = Date.now();
      for (const [relayKeyId, info] of this.#connectedPeers) {
        const reachable = info.socket && info.socket.destroyed !== true;
        const body = {
          v: 1,
          attesterId: this.#selfRelayKeyId,
          targetRelayKeyId: relayKeyId,
          reachable,
          latencyMs: null,
          createdAtMs: nowMs,
        };
        const sig = await this.#receiptSigner.sign(body);
        const attestation = new PeerUptimeAttestationV1({ ...body, sig });
        this.#storeAttestation(attestation);
        produced.push(attestation);
        if (this.#metrics) {
          this.#metrics.increment("attestationsIssuedTotal");
        }
      }
      if (produced.length > 0 && this.#onAttestationsProduced) {
        this.#onAttestationsProduced(produced);
      }
    } catch (err) {
      console.error("[ATTESTATION] attestAllPeers failed:", err && err.message ? err.message : err);
    }
  }

  #storeAttestation(attestation) {
    const key = attestation.targetRelayKeyId;
    let list = this.#attestations.get(key);
    if (!list) {
      list = [];
      this.#attestations.set(key, list);
    }
    list.push(attestation);
    if (list.length > MAX_ATTESTATIONS_PER_TARGET) {
      list.splice(0, list.length - MAX_ATTESTATIONS_PER_TARGET);
    }
  }

  #pruneExpired() {
    const cutoff = Date.now() - this.#retentionMs;
    for (const [key, list] of this.#attestations) {
      const filtered = list.filter((a) => a.createdAtMs >= cutoff);
      if (filtered.length === 0) {
        this.#attestations.delete(key);
      } else {
        this.#attestations.set(key, filtered);
      }
    }
  }
}
