import { RouteAnnouncer } from "../RouteAnnouncer.js";
import { DhtNodeId } from "./DhtNodeId.js";

/**
 * Cap on announcements held for retry. A node whose k-buckets never fill
 * must not accumulate these without bound; overflow is logged, never
 * silently discarded — a dropped announcement means an unreachable inbox.
 */
const MAX_PENDING_ANNOUNCEMENTS = 256;

/**
 * DHT-based route announcer. Stores routes on k-closest nodes
 * instead of flooding to all peers.
 *
 * - announceRoutes: STORE on k-closest for each inboxId
 * - announceWithdraw: STORE tombstone (null) on k-closest
 * - announceAllToPeer: No-op — new DHT peers bootstrap via FIND_NODE
 * - reannounceAll: Rate-limited republish of locally hosted routes
 *
 * ## Publishing to an empty k-bucket table
 *
 * `findClosest` returns whatever peers are known *at this instant*. A node
 * that registers its inbox before its k-buckets are populated therefore
 * STOREs it on zero peers. That used to be indistinguishable from success:
 * `#storeOnClosest` looped over an empty list and returned, so the inbox was
 * never published anywhere and nothing retried it. The only recovery was
 * `reannounceAll`, rate-limited to `republishIntervalMs` (1h in production) —
 * so a startup race made an inbox undiscoverable for an hour.
 *
 * That is not a test-only race. It is a plain lost write whenever peering is
 * slower than registration: a cold start, a slow or congested network, a
 * relay restart. It was found because CI is slow enough to lose the race
 * reliably — every inbox that logged "STORE … on 0 closest peers" failed
 * every subsequent FIND_VALUE, with no exceptions in either direction.
 *
 * So a zero-peer STORE is now recorded as *unpublished* and retried the
 * moment the DHT gains a peer (`DhtNode.addPeer` → `flushPendingAnnouncements`).
 * Publishing is driven by the event that makes it possible rather than by a
 * timer that hopes it already is.
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
   * Announcements whose STORE reached zero peers, keyed by inboxId, awaiting
   * a peer to publish to.
   * @type {Map<string, { entry: object, hops: number }>}
   */
  #pendingAnnouncements;

  /**
   * The most recent announcer context, retained so a retry can rebuild the
   * entry from the live route table instead of republishing a stale snapshot.
   * @type {object|null}
   */
  #announcerCtx;

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
    this.#pendingAnnouncements = new Map();
    this.#announcerCtx = null;
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
    this.#rememberCtx(ctx);
    for (const inboxId of inboxIds) {
      const route = ctx.routeTable.get(inboxId);
      if (!route) continue;
      const entry = ctx.createAnnouncedRouteEntry(inboxId, route, hops);
      if (!entry) continue;
      if (!entry.registration) continue;
      this.#publish(inboxId, entry, hops);
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
    this.#rememberCtx(ctx);
    for (const inboxId of inboxIds) {
      // Drop any retry still queued for this inbox. Publishing it after a
      // withdraw would resurrect a route the owner has already taken down.
      this.#pendingAnnouncements.delete(inboxId);
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
    this.#rememberCtx(ctx);
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
      this.#publish(inboxId, entry, 1);
    }
  }

  /**
   * Retry every announcement that previously reached zero peers.
   *
   * Called when the DHT gains a peer — the event that makes publishing
   * possible in the first place. Cheap and safe to call on every peer
   * contact: it returns immediately when nothing is pending, which is the
   * steady state.
   *
   * @returns {number} how many pending announcements were published
   */
  flushPendingAnnouncements() {
    if (this.#pendingAnnouncements.size === 0) return 0;
    let published = 0;
    for (const [inboxId, pending] of Array.from(this.#pendingAnnouncements)) {
      const entry = this.#refreshPendingEntry(inboxId, pending);
      if (!entry) {
        // The route is gone or no longer ours — nothing left to publish.
        this.#pendingAnnouncements.delete(inboxId);
        continue;
      }
      if (this.#storeOnClosest(inboxId, entry) > 0) {
        this.#pendingAnnouncements.delete(inboxId);
        published += 1;
      }
    }
    return published;
  }

  /**
   * Inboxes still waiting to reach a peer. Exposed for diagnostics and tests;
   * a non-empty set means those inboxes are not yet discoverable.
   * @returns {string[]}
   */
  get pendingAnnouncementIds() {
    return Array.from(this.#pendingAnnouncements.keys());
  }

  /**
   * STORE an entry, and remember it for retry if it reached nobody.
   */
  #publish(inboxId, entry, hops) {
    if (this.#storeOnClosest(inboxId, entry) > 0) {
      this.#pendingAnnouncements.delete(inboxId);
      return;
    }
    if (!this.#pendingAnnouncements.has(inboxId)
      && this.#pendingAnnouncements.size >= MAX_PENDING_ANNOUNCEMENTS) {
      console.warn("[DHT] pending-announcement buffer full (" + MAX_PENDING_ANNOUNCEMENTS
        + " entries); not retrying " + inboxId
        + " — it stays undiscoverable until the next republish");
      return;
    }
    this.#pendingAnnouncements.set(inboxId, { entry, hops });
  }

  /**
   * Rebuild a pending entry from the live route table so a retry publishes
   * current state, not the snapshot captured when the DHT had no peers.
   *
   * @returns {object|null} null when the route should no longer be published
   */
  #refreshPendingEntry(inboxId, pending) {
    const ctx = this.#announcerCtx;
    if (!ctx || !ctx.routeTable || typeof ctx.createAnnouncedRouteEntry !== "function") {
      return pending.entry;
    }
    const route = ctx.routeTable.get(inboxId);
    // Withdrawn, expired, or downgraded to a transitively-learned route while
    // the announcement was queued: publishing now would advertise a route this
    // node can no longer vouch for.
    if (!route || route.direct !== true) return null;
    const rebuilt = ctx.createAnnouncedRouteEntry(inboxId, route, pending.hops);
    // HIGH-8: an entry that lost its claimant signature must never be stored.
    if (!rebuilt || !rebuilt.registration) return null;
    return rebuilt;
  }

  #rememberCtx(ctx) {
    if (ctx && ctx.routeTable) this.#announcerCtx = ctx;
  }

  /**
   * STORE a route entry on the k-closest nodes to the inboxId.
   * @returns {number} how many peers the entry was sent to
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
    return closest.length;
  }
}
