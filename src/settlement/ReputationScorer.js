/**
 * Computes reputation scores for relays from local data.
 *
 * Not abstract — one class, always computed locally from gossiped data.
 * Clients and relays compute scores independently. No central authority.
 *
 * Score components (weighted):
 *   Uptime    (40%) — % of attestations reporting reachable
 *   Throughput(20%) — packets routed relative to network average
 *   Peers     (15%) — number of mesh peers maintained
 *   Longevity (15%) — days since first seen
 *   Disputes  (10%) — penalty for slash receipts (negative)
 *
 * All inputs are from locally available data:
 *   - PeerUptimeAttestationV1 records (from PeerAttestationService)
 *   - RelayStore descriptors (for first-seen timestamps)
 *   - NodeMetrics snapshots (for throughput, peer counts)
 */

const WEIGHT_UPTIME = 0.40;
const WEIGHT_THROUGHPUT = 0.20;
const WEIGHT_PEERS = 0.15;
const WEIGHT_LONGEVITY = 0.15;
const WEIGHT_DISPUTES = 0.10;

const MAX_LONGEVITY_DAYS = 365;
const MAX_PEERS_SCORE = 32;

export class ReputationScorer {
  #attestationService;
  #relayStore;

  /**
   * @param {object} opts
   * @param {PeerAttestationService} opts.attestationService — source of attestations
   * @param {RelayStore} opts.relayStore — for relay metadata (first-seen, peer count)
   */
  constructor({ attestationService, relayStore }) {
    if (!attestationService) throw new Error("ReputationScorer requires attestationService");
    if (!relayStore) throw new Error("ReputationScorer requires relayStore");
    this.#attestationService = attestationService;
    this.#relayStore = relayStore;
  }

  /**
   * Compute the reputation score for a relay.
   * @param {string} relayKeyId
   * @param {object} [context] — optional overrides for scoring inputs
   * @param {number} [context.networkAvgPacketsPerMin] — network-wide avg packets/min
   * @param {number} [context.slashCount] — number of slash receipts for this relay
   * @returns {{ score: number, known: boolean, components: { uptime: number, throughput: number, peers: number, longevity: number, disputes: number } }}
   */
  score(relayKeyId, context = {}) {
    const desc = this.#relayStore.getDescriptor(relayKeyId, { nowMs: Date.now() });
    const known = desc !== null && desc !== undefined;

    const uptime = this.#computeUptime(relayKeyId);
    const throughput = this.#computeThroughput(desc, context);
    const peers = this.#computePeers(relayKeyId);
    const longevity = known ? this.#computeLongevity(desc) : 0;
    const disputes = this.#computeDisputes(relayKeyId, context);

    const score = Math.max(0, Math.min(1,
      uptime * WEIGHT_UPTIME
      + throughput * WEIGHT_THROUGHPUT
      + peers * WEIGHT_PEERS
      + longevity * WEIGHT_LONGEVITY
      - disputes * WEIGHT_DISPUTES
    ));

    return {
      score,
      known,
      components: { uptime, throughput, peers, longevity, disputes },
    };
  }

  /**
   * Score all known relays.
   * @param {object} [context]
   * @returns {Map<string, { score: number, components: object }>}
   */
  scoreAll(context = {}) {
    const nowMs = Date.now();
    const descriptors = this.#relayStore.listDescriptors({ nowMs });
    const scores = new Map();
    for (const desc of descriptors) {
      const relayKeyId = desc.relayKeyId || desc.id;
      if (relayKeyId) {
        scores.set(relayKeyId, this.score(relayKeyId, context));
      }
    }
    return scores;
  }

  /**
   * Uptime score: weighted fraction of attestations reporting reachable.
   * Newer attestations count more (exponential decay over 24h).
   * 0.0 = never reachable, 1.0 = always reachable.
   */
  #computeUptime(relayKeyId) {
    const attestations = this.#attestationService.getAttestationsFor(relayKeyId);
    if (attestations.length === 0) return 0;
    const nowMs = Date.now();
    const decayMs = 24 * 60 * 60 * 1000; // 24h half-life
    let weightedReachable = 0;
    let weightTotal = 0;
    for (const a of attestations) {
      const ageMs = Math.max(0, nowMs - a.createdAtMs);
      const weight = Math.pow(0.5, ageMs / decayMs);
      weightTotal += weight;
      if (a.reachable === true) {
        weightedReachable += weight;
      }
    }
    if (weightTotal <= 0) return 0;
    return weightedReachable / weightTotal;
  }

  /**
   * Throughput score: this relay's throughput relative to network average.
   * Uses descriptor metadata if available. Falls back to 0.5 (neutral).
   * @param {object|null} desc — descriptor from relayStore (passed from score())
   */
  #computeThroughput(desc, context) {
    const networkAvg = context.networkAvgPacketsPerMin;
    if (!networkAvg || networkAvg <= 0) return 0.5;
    if (!desc || !desc.descriptor || !desc.descriptor.meta || !desc.descriptor.meta.metrics) return 0.5;
    const relayPackets = desc.descriptor.meta.metrics.packetsPerMin;
    if (typeof relayPackets !== "number" || relayPackets <= 0) return 0.5;
    return Math.min(1, relayPackets / (networkAvg * 2));
  }

  /**
   * Peer count score: how many peers this relay maintains.
   * Normalized against MAX_PEERS_SCORE.
   */
  #computePeers(relayKeyId) {
    // Use attestation count as a proxy for peer connectivity —
    // more attesters means more peers can reach this relay.
    const attestations = this.#attestationService.getAttestationsFor(relayKeyId);
    const uniqueAttesters = new Set(attestations.map((a) => a.attesterId)).size;
    return Math.min(1, uniqueAttesters / MAX_PEERS_SCORE);
  }

  /**
   * Longevity score: days since first seen, normalized against MAX_LONGEVITY_DAYS.
   * @param {object} desc — descriptor from relayStore (passed from score() to avoid re-fetch)
   */
  #computeLongevity(desc) {
    if (!desc || !desc.receivedAtMs) return 0;
    const daysSinceFirstSeen = (Date.now() - desc.receivedAtMs) / (24 * 60 * 60 * 1000);
    return Math.min(1, Math.max(0, daysSinceFirstSeen) / MAX_LONGEVITY_DAYS);
  }

  /**
   * Dispute score: penalty based on slash receipt count.
   * 0.0 = no disputes, 1.0 = max penalty.
   */
  #computeDisputes(_relayKeyId, context) {
    const slashCount = context.slashCount || 0;
    if (slashCount <= 0) return 0;
    // Logarithmic penalty: each slash increases penalty with diminishing returns
    return Math.min(1, Math.log2(slashCount + 1) / 5);
  }
}
