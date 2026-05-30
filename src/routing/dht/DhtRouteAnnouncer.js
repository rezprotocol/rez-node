import { RouteAnnouncer } from "../RouteAnnouncer.js";
import { DhtNodeId } from "./DhtNodeId.js";

/**
 * DHT-based route announcer. Stores routes on k-closest nodes
 * instead of flooding to all peers.
 *
 * - announceRoutes: STORE on k-closest for each inboxId
 * - announceWithdraw: STORE tombstone (null) on k-closest
 * - announceAllToPeer: No-op — new DHT peers bootstrap via FIND_NODE
 * - reannounceAll: Rate-limited republish of locally hosted routes
 */
export class DhtRouteAnnouncer extends RouteAnnouncer {
  /** @type {import("./DhtProtocol.js").DhtProtocol} */
  #protocol;

  /** @type {import("./KBucketTable.js").KBucketTable} */
  #kBuckets;

  /** @type {number} */
  #k;

  /** @type {number|null} */
  #lastRepublishMs;

  /** @type {number} */
  #republishIntervalMs;

  /** @type {() => number} */
  #nowMs;

  /**
   * @param {object} options
   * @param {import("./DhtProtocol.js").DhtProtocol} options.protocol
   * @param {import("./KBucketTable.js").KBucketTable} options.kBuckets
   * @param {number} [options.k]
   * @param {number} [options.republishIntervalMs]
   * @param {() => number} [options.nowMs]
   */
  constructor({ protocol, kBuckets, k = 20, republishIntervalMs = 3_600_000, nowMs = () => Date.now() }) {
    super();
    if (!protocol) throw new Error("DhtRouteAnnouncer requires protocol");
    if (!kBuckets) throw new Error("DhtRouteAnnouncer requires kBuckets");

    this.#protocol = protocol;
    this.#kBuckets = kBuckets;
    this.#k = k;
    this.#lastRepublishMs = null;
    this.#republishIntervalMs = republishIntervalMs;
    this.#nowMs = nowMs;
  }

  /**
   * Announce new routes by STOREing on k-closest nodes.
   *
   * Only direct routes (route.direct === true with a claimant-signed
   * registration) are eligible for DHT storage. Transitively-learned
   * routes carry no signature the receiving relay can verify, so storing
   * them in the DHT would only ever pollute lookups — see
   * docs/SECURITY_AUDIT.md HIGH-8.
   */
  announceRoutes(inboxIds, hops, ctx) {
    for (const inboxId of inboxIds) {
      const route = ctx.routeTable.get(inboxId);
      if (!route) continue;
      const entry = ctx.createAnnouncedRouteEntry(inboxId, route, hops);
      if (!entry) continue;
      if (!entry.registration) continue;
      this.#storeOnClosest(inboxId, entry);
    }
  }

  /**
   * Announce route entries to k-closest, excluding the given socket.
   *
   * Skips entries without a registration — see `announceRoutes`.
   */
  announceRoutesExcept(excludeSocket, entries, ctx) {
    if (entries.length === 0) return;
    for (const entry of entries) {
      if (!entry || !entry.inboxId) continue;
      if (!entry.registration) continue;
      const targetId = DhtNodeId.fromRelayKeyId(entry.inboxId);
      const closest = this.#kBuckets.findClosest(targetId, this.#k);
      for (const peer of closest) {
        if (peer.socket === excludeSocket) continue;
        this.#protocol.sendStore(peer.socket, entry.inboxId, entry);
      }
    }
  }

  /**
   * Withdraw routes by STOREing a null/tombstone on k-closest.
   */
  announceWithdraw(inboxIds, ctx) {
    for (const inboxId of inboxIds) {
      const targetId = DhtNodeId.fromRelayKeyId(inboxId);
      const closest = this.#kBuckets.findClosest(targetId, this.#k);
      for (const peer of closest) {
        this.#protocol.sendStore(peer.socket, inboxId, null);
      }
    }
  }

  /**
   * No-op for DHT — new peers bootstrap via FIND_NODE, not route flood.
   */
  announceAllToPeer(peerSocket, ctx) {
    // Intentionally empty. In DHT, peers discover routes on demand.
  }

  /**
   * Rate-limited republish of locally hosted routes to k-closest.
   * Called by MeshCoordinator every 30s, but only actually republishes
   * when republishIntervalMs has elapsed.
   */
  reannounceAll(ctx) {
    const now = this.#nowMs();
    if (this.#lastRepublishMs !== null && (now - this.#lastRepublishMs) < this.#republishIntervalMs) {
      return;
    }
    this.#lastRepublishMs = now;

    for (const [inboxId, route] of ctx.routeTable.getAll()) {
      if (!route.direct) continue; // only republish our own routes
      const entry = ctx.createAnnouncedRouteEntry(inboxId, route, 1);
      if (!entry) continue;
      if (!entry.registration) continue; // HIGH-8: never DHT-store unsigned entries
      this.#storeOnClosest(inboxId, entry);
    }
  }

  /**
   * STORE a route entry on the k-closest nodes to the inboxId.
   */
  #storeOnClosest(inboxId, routeEntry) {
    const targetId = DhtNodeId.fromRelayKeyId(inboxId);
    const closest = this.#kBuckets.findClosest(targetId, this.#k);
    if (process.env.REZ_GW_DEBUG === "1") {
      console.log("[DHT] STORE " + inboxId + " on " + closest.length + " closest peers");
    }
    for (const peer of closest) {
      this.#protocol.sendStore(peer.socket, inboxId, routeEntry);
    }
  }
}
