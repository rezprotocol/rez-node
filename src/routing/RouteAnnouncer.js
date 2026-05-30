/**
 * Base class for route announcement strategies.
 *
 * A RouteAnnouncer is responsible for propagating route information to peers.
 * Subclasses implement different propagation mechanisms (flood gossip, DHT STORE, etc).
 *
 * All methods receive a context object with the primitives needed to send frames:
 *   { peerSockets, routeTable, selfRelayKeyId, encodeCtl, trySendFrame, createAnnouncedRouteEntry }
 *
 * @abstract
 */
export class RouteAnnouncer {
  /**
   * Announce new routes to all peers.
   * @param {string[]} inboxIds
   * @param {number} hops
   * @param {object} ctx
   */
  announceRoutes(inboxIds, hops, ctx) {
    throw new Error("RouteAnnouncer.announceRoutes must be implemented by subclass");
  }

  /**
   * Announce route entries to all peers except the given socket.
   * @param {object} excludeSocket
   * @param {object[]} entries
   * @param {object} ctx
   */
  announceRoutesExcept(excludeSocket, entries, ctx) {
    throw new Error("RouteAnnouncer.announceRoutesExcept must be implemented by subclass");
  }

  /**
   * Announce withdrawal of routes to all peers.
   * @param {string[]} inboxIds
   * @param {object} ctx
   */
  announceWithdraw(inboxIds, ctx) {
    throw new Error("RouteAnnouncer.announceWithdraw must be implemented by subclass");
  }

  /**
   * Send all known routes to a single peer (used when a new peer joins).
   * @param {object} peerSocket
   * @param {object} ctx
   */
  announceAllToPeer(peerSocket, ctx) {
    throw new Error("RouteAnnouncer.announceAllToPeer must be implemented by subclass");
  }

  /**
   * Re-announce the full route table to all peers (periodic anti-entropy).
   * @param {object} ctx
   */
  reannounceAll(ctx) {
    throw new Error("RouteAnnouncer.reannounceAll must be implemented by subclass");
  }
}
