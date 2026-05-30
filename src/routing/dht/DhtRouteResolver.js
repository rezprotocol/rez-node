import { RouteResolver } from "../RouteResolver.js";
import { DhtNodeId } from "./DhtNodeId.js";
import { validateStoredRouteEntry } from "./DhtProtocol.js";

/**
 * TTL applied to remote routes installed in the local RouteTable from a
 * successful FIND_VALUE. Bounded so a single race-win by a malicious
 * relay-verified responder cannot become a permanent next-hop —
 * docs/SECURITY_AUDIT.md MED-9. Anti-entropy republish (1h default)
 * keeps fresh values available in the DHT for re-discovery.
 */
const DHT_INSTALLED_ROUTE_TTL_MS = 5 * 60 * 1000;

/**
 * DHT-based route resolver. Lookup chain:
 *   1. Local RouteTable (same as gossip — fast path)
 *   2. Local DhtValueStore (this node may be storing the value for the DHT)
 *   3. DHT FIND_VALUE iterative lookup
 *   4. Optional gossip fallback (safety net for sparse k-buckets)
 */
export class DhtRouteResolver extends RouteResolver {
  /** @type {import("./DhtLookup.js").DhtLookup} */
  #lookup;

  /** @type {import("./DhtProtocol.js").DhtProtocol} */
  #protocol;

  /** @type {import("./DhtValueStore.js").DhtValueStore} */
  #valueStore;

  /** @type {RouteResolver|null} */
  #fallbackResolver;

  /** @type {() => number} */
  #nowMs;

  /**
   * @param {object} options
   * @param {import("./DhtLookup.js").DhtLookup} options.lookup
   * @param {import("./DhtProtocol.js").DhtProtocol} options.protocol
   * @param {import("./DhtValueStore.js").DhtValueStore} options.valueStore
   * @param {RouteResolver|null} [options.fallbackResolver]
   * @param {() => number} [options.nowMs]
   */
  constructor({ lookup, protocol, valueStore, fallbackResolver = null, nowMs = () => Date.now() }) {
    super();
    if (!lookup) throw new Error("DhtRouteResolver requires lookup");
    if (!protocol) throw new Error("DhtRouteResolver requires protocol");
    if (!valueStore) throw new Error("DhtRouteResolver requires valueStore");

    this.#lookup = lookup;
    this.#protocol = protocol;
    this.#valueStore = valueStore;
    this.#fallbackResolver = fallbackResolver;
    this.#nowMs = nowMs;
  }

  /**
   * @param {string} inboxId
   * @param {{ routeTable: object|null, relayConnectionPool: object|null }} ctx
   * @returns {Promise<object|null>}
   */
  async resolve(inboxId, ctx) {
    const debug = process.env.REZ_GW_DEBUG === "1";

    // 1. Check local RouteTable
    const cached = ctx.routeTable ? ctx.routeTable.get(inboxId) : null;
    if (cached) return cached;

    // 2. Check local DhtValueStore. Re-validate the entry — HIGH-8
    // defense-in-depth so a value that somehow slipped past inbound
    // validation never authorizes routing decisions here.
    const stored = this.#valueStore.get(inboxId, this.#nowMs());
    if (stored) {
      if (validateStoredRouteEntry(inboxId, stored)) {
        if (debug) console.log("[DHT] resolved " + inboxId + " from local value store");
        return stored;
      }
      this.#valueStore.remove(inboxId);
      if (debug) console.log("[DHT] evicted invalid local value-store entry for " + inboxId);
    }

    // 3. DHT FIND_VALUE lookup
    const targetId = DhtNodeId.fromRelayKeyId(inboxId);
    if (debug) console.log("[DHT] FIND_VALUE for " + inboxId);

    try {
      // Track the peer that returned the value so we can install a
      // delivery route through it. The DHT-stored route entry tells us
      // *which node* hosts the target inbox, but not how to reach that
      // node — the path to it is "through whoever returned the value".
      // For NAT'd target hosts this is the only viable delivery path
      // (the target is reachable only via its outbound TCP peers; we
      // need to forward through one of them).
      let responder = null;
      const result = await this.#lookup.findValue(
        targetId,
        async (entry, tid) => {
          const reply = await this.#protocol.queryFindValue(entry.socket, tid, inboxId);
          if (reply && reply.value && !responder) responder = entry;
          return reply;
        },
      );
      // HIGH-8: validate the returned value before trusting it for
      // routing. A peer in the lookup path could have replied with a
      // forged routeEntry; reject anything not anchored to a valid
      // claimant delegation for THIS inboxId.
      if (result.value && validateStoredRouteEntry(inboxId, result.value)) {
        if (debug) console.log("[DHT] FIND_VALUE resolved " + inboxId);
        // Install a remote route via the responder. Future deliveries
        // (and the gateway's inboxRouter.routeDelivery fallback) can
        // then forward `inbox.deposit` through this peer rather than
        // requiring an onion-descriptor for the (potentially NAT'd)
        // hosting node.
        if (ctx.routeTable && typeof ctx.routeTable.addRemote === "function" && responder) {
          const installedAt = this.#nowMs();
          ctx.routeTable.addRemote(inboxId, {
            hops: 1,
            peerSocket: responder.socket,
            nextHopRelayKeyId: responder.relayKeyId,
            deliveryRelayKeyId: result.value.deliveryRelayKeyId
              || (result.value.registration && result.value.registration.relayKeyId)
              || responder.relayKeyId,
            registration: result.value.registration || null,
            nowMs: installedAt,
            expiresAtMs: installedAt + DHT_INSTALLED_ROUTE_TTL_MS,
          });
        }
        return result.value;
      }
      if (result.value && debug) {
        console.log("[DHT] FIND_VALUE returned invalid value for " + inboxId + " — dropped");
      }
    } catch (err) {
      if (debug) console.log("[DHT] FIND_VALUE error for " + inboxId + ": " + (err && err.message ? err.message : err));
    }

    // 4. Fallback to gossip broadcast
    if (this.#fallbackResolver) {
      if (debug) console.log("[DHT] falling back to gossip for " + inboxId);
      return this.#fallbackResolver.resolve(inboxId, ctx);
    }

    return null;
  }
}
