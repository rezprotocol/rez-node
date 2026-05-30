/**
 * Base class for route resolution strategies.
 *
 * A RouteResolver is responsible for finding a route entry for a given inboxId.
 * Subclasses implement different discovery mechanisms (gossip broadcast, DHT lookup, etc).
 *
 * @abstract
 */
export class RouteResolver {
  /**
   * Resolve a route for the given inboxId.
   *
   * @param {string} inboxId - target inbox to find a route to
   * @param {{ routeTable: object|null, relayConnectionPool: object|null }} ctx
   * @returns {Promise<object|null>} route entry or null if not found
   */
  async resolve(inboxId, ctx) {
    throw new Error("RouteResolver.resolve must be implemented by subclass");
  }
}
