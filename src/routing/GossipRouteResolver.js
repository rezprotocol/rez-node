import { RouteResolver } from "./RouteResolver.js";

/**
 * Gossip-based route resolver. Checks the local RouteTable first, then
 * broadcasts an inbox.query to all connected relay peers via RelayConnectionPool.
 *
 * This is the default routing strategy used by GatewayLoop.
 */
export class GossipRouteResolver extends RouteResolver {
  /**
   * @param {string} inboxId
   * @param {{ routeTable: object|null, relayConnectionPool: object|null }} ctx
   * @returns {Promise<object|null>}
   */
  async resolve(inboxId, { routeTable, relayConnectionPool }) {
    const debug = process.env.REZ_GW_DEBUG === "1";
    const cached = routeTable ? routeTable.get(inboxId) : null;
    if (cached) return cached;

    if (relayConnectionPool && typeof relayConnectionPool.queryRoute === "function") {
      if (debug) console.log("[GW] no local route for " + inboxId + ", querying upstream relays (pool=true)");
      try {
        const resolved = await relayConnectionPool.queryRoute(inboxId);
        if (debug) console.log("[GW] upstream route query result: resolved=" + resolved);
        if (resolved) {
          const route = routeTable ? routeTable.get(inboxId) : null;
          if (debug) console.log("[GW] route table after query: " + (route ? "found" : "not-found"));
          return route;
        }
      } catch (queryErr) {
        if (debug) console.log("[GW] upstream route query error:", queryErr && queryErr.message ? queryErr.message : queryErr);
      }
    } else {
      if (debug) console.log("[GW] no local route for " + inboxId + ", querying upstream relays (pool=false)");
    }

    return null;
  }
}
