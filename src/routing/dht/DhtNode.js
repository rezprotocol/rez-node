import { DhtNodeId } from "./DhtNodeId.js";
import { KBucketTable } from "./KBucketTable.js";
import { DhtValueStore } from "./DhtValueStore.js";
import { DhtLookup } from "./DhtLookup.js";
import { DhtProtocol } from "./DhtProtocol.js";
import { DhtRouteResolver } from "./DhtRouteResolver.js";
import { DhtRouteAnnouncer } from "./DhtRouteAnnouncer.js";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";

/**
 * Orchestrates the DHT subsystem. Owns the k-bucket table, value store,
 * lookup engine, protocol handlers, and the resolver/announcer strategies
 * that plug into the existing routing infrastructure.
 *
 * Usage:
 *   const dht = new DhtNode({ selfRelayKeyId, controlMessageRegistry, encodeCtl, trySendFrame });
 *   dht.install();  // register protocol handlers
 *   // Inject into routing:
 *   gatewayLoop.routeResolver = dht.routeResolver;
 *   inboxRouter._routeAnnouncer = dht.routeAnnouncer;
 *   // On peer connect/disconnect:
 *   dht.addPeer(relayKeyId, socket);
 *   dht.removePeer(relayKeyId);
 */
export class DhtNode {
  /** @type {DhtNodeId} */
  #selfNodeId;

  /** @type {KBucketTable} */
  #kBuckets;

  /** @type {DhtValueStore} */
  #valueStore;

  /** @type {DhtLookup} */
  #lookup;

  /** @type {DhtProtocol} */
  #protocol;

  /** @type {DhtRouteResolver} */
  #resolver;

  /** @type {DhtRouteAnnouncer} */
  #announcer;

  /** @type {() => number} */
  #nowMs;

  /**
   * @param {object} options
   * @param {string} options.selfRelayKeyId
   * @param {import("../ControlMessageRegistry.js").ControlMessageRegistry} options.controlMessageRegistry
   * @param {(obj: object) => Uint8Array} options.encodeCtl
   * @param {(socket: object, bytes: Uint8Array) => void} options.trySendFrame
   * @param {import("../RouteResolver.js").RouteResolver|null} [options.fallbackResolver]
   * @param {{ k?: number, alpha?: number, queryTimeoutMs?: number, valueTtlMs?: number, republishIntervalMs?: number, storeRateLimitWindowMs?: number, storeRateLimitMax?: number }} [options.config]
   * @param {() => number} [options.nowMs]
   * @param {(socket: object) => string|null} [options.getPeerKey] LOW-6: maps socket -> peer rate-limit key (relayKeyId)
   * @param {(socket: object) => string|null} [options.getPeerIp] MED-13: maps socket -> /64-aggregated IP key
   */
  constructor({
    selfRelayKeyId,
    controlMessageRegistry,
    encodeCtl,
    trySendFrame,
    fallbackResolver = null,
    config = {},
    nowMs = () => Date.now(),
    getPeerKey = null,
    getPeerIp = null,
  }) {
    if (typeof selfRelayKeyId !== "string" || !selfRelayKeyId.trim()) {
      throw new Error("DhtNode requires selfRelayKeyId");
    }
    if (!controlMessageRegistry) {
      throw new Error("DhtNode requires controlMessageRegistry");
    }
    if (typeof encodeCtl !== "function") {
      throw new Error("DhtNode requires encodeCtl function");
    }
    if (typeof trySendFrame !== "function") {
      throw new Error("DhtNode requires trySendFrame function");
    }

    const k = config.k || 20;
    const alpha = config.alpha || 3;
    const queryTimeoutMs = config.queryTimeoutMs || 3000;
    const valueTtlMs = config.valueTtlMs || 86_400_000;
    const republishIntervalMs = config.republishIntervalMs || 3_600_000;

    this.#nowMs = nowMs;
    this.#selfNodeId = DhtNodeId.fromRelayKeyId(selfRelayKeyId);
    this.#kBuckets = new KBucketTable(this.#selfNodeId, { k });
    this.#valueStore = new DhtValueStore({ defaultTtlMs: valueTtlMs });
    this.#lookup = new DhtLookup(this.#kBuckets, { alpha, k });

    const storeRateLimiter = new SlidingWindowRateLimiter({
      windowMs: config.storeRateLimitWindowMs,
      maxAttempts: config.storeRateLimitMax,
    });

    this.#protocol = new DhtProtocol({
      kBuckets: this.#kBuckets,
      valueStore: this.#valueStore,
      registry: controlMessageRegistry,
      selfNodeId: this.#selfNodeId,
      selfRelayKeyId,
      encodeCtl,
      trySendFrame,
      queryTimeoutMs,
      k,
      nowMs,
      storeRateLimiter,
      getPeerKey,
      getPeerIp,
    });

    this.#resolver = new DhtRouteResolver({
      lookup: this.#lookup,
      protocol: this.#protocol,
      valueStore: this.#valueStore,
      fallbackResolver,
      nowMs,
    });

    this.#announcer = new DhtRouteAnnouncer({
      protocol: this.#protocol,
      kBuckets: this.#kBuckets,
      k,
      republishIntervalMs,
      nowMs,
    });
  }

  /**
   * Register DHT protocol handlers on the ControlMessageRegistry.
   */
  install() {
    this.#protocol.install();
  }

  /**
   * Unregister DHT protocol handlers.
   */
  uninstall() {
    this.#protocol.uninstall();
  }

  /**
   * Add a peer to the k-bucket table.
   * @param {string} relayKeyId
   * @param {object} socket
   */
  addPeer(relayKeyId, socket) {
    if (typeof relayKeyId !== "string" || !relayKeyId.trim()) return;
    const nodeId = DhtNodeId.fromRelayKeyId(relayKeyId);
    this.#kBuckets.addOrUpdate(nodeId, relayKeyId, socket, this.#nowMs());
  }

  /**
   * Remove a peer from the k-bucket table by relayKeyId.
   * @param {string} relayKeyId
   */
  removePeer(relayKeyId) {
    this.#kBuckets.remove(relayKeyId);
  }

  /**
   * Remove all peers with the given socket from k-buckets.
   * @param {object} socket
   * @returns {string[]} removed relayKeyIds
   */
  removePeerBySocket(socket) {
    return this.#kBuckets.removeBySocket(socket);
  }

  /**
   * Evict expired values from the DHT value store.
   * @param {number} nowMs
   * @returns {number} count evicted
   */
  evictExpiredValues(nowMs) {
    return this.#valueStore.evictExpired(nowMs);
  }

  /** @returns {DhtRouteResolver} */
  get routeResolver() {
    return this.#resolver;
  }

  /** @returns {DhtRouteAnnouncer} */
  get routeAnnouncer() {
    return this.#announcer;
  }

  /** @returns {KBucketTable} for diagnostics */
  get kBuckets() {
    return this.#kBuckets;
  }

  /** @returns {DhtValueStore} for diagnostics */
  get valueStore() {
    return this.#valueStore;
  }
}
