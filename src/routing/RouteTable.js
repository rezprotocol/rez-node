/**
 * RouteTable — pure route-state container.
 *
 * Owns the route map (inboxId → RouteEntry) and the reverse socket index
 * (socket → Set<inboxId>).  No I/O, no control messages, no TCP framing.
 *
 * RouteEntry shape:
 *   { socket, hops, direct, addedAtMs, expiresAtMs, announceToPeers,
 *     nextHopRelayKeyId, deliveryRelayKeyId, relayKeyId,
 *     registration, installerSocket }
 *
 * `expiresAtMs` is set only on routes installed by a route resolver (e.g.
 * DhtRouteResolver) so the bound time-to-live forces periodic
 * re-discovery. Closes docs/SECURITY_AUDIT.md MED-9 — without expiry a
 * single race-win by a relay-verified responder would otherwise become
 * the permanent next-hop for that inboxId.
 */

import { isNonEmptyString } from "@rezprotocol/core";

export class RouteTable {
  /** @type {Map<string, object>} inboxId → RouteEntry */
  #routes = new Map();

  /** @type {Map<object, Set<string>>} socket → set of inboxIds reachable through that socket */
  #socketInboxes = new Map();

  /** @type {Map<object, Set<string>>} installerSocket → set of inboxIds installed by that socket */
  #installerSocketInboxes = new Map();

  /** @type {((inboxIds: string[]) => void) | null} */
  #onRouteAdded = null;

  #addSocketIndex(map, socket, inboxId) {
    if (!socket) return;
    let set = map.get(socket);
    if (!set) {
      set = new Set();
      map.set(socket, set);
    }
    set.add(inboxId);
  }

  #removeSocketIndex(map, socket, inboxId) {
    if (!socket) return;
    const set = map.get(socket);
    if (!set) return;
    set.delete(inboxId);
    if (set.size === 0) {
      map.delete(socket);
    }
  }

  // ---------------------------------------------------------------------------
  // Local routes (direct socket or hosted inbox)
  // ---------------------------------------------------------------------------

  /**
   * Register inboxIds as directly reachable via the given socket.
   *
   * @param {string} inboxId
   * @param {object|null} socket
   * @param {{ selfRelayKeyId?: string, nowMs?: number, announceToPeers?: boolean }} opts
   */
  addLocal(inboxId, socket, { selfRelayKeyId = null, nowMs = Date.now(), announceToPeers = true,
                              registration = null, installerSocket = null } = {}) {
    if (!isNonEmptyString(inboxId)) return;

    const existing = this.#routes.get(inboxId);
    // If a stale direct socket exists for the same inbox, clean up the reverse index.
    if (existing && existing.direct && existing.socket && existing.socket !== socket) {
      this.#removeSocketIndex(this.#socketInboxes, existing.socket, inboxId);
    }
    // Clean stale installer index when overwriting
    if (existing && existing.installerSocket && existing.installerSocket !== installerSocket) {
      this.#removeSocketIndex(this.#installerSocketInboxes, existing.installerSocket, inboxId);
    }

    this.#routes.set(inboxId, {
      socket: socket || null,
      hops: 0,
      direct: true,
      addedAtMs: nowMs,
      announceToPeers,
      nextHopRelayKeyId: selfRelayKeyId || undefined,
      deliveryRelayKeyId: selfRelayKeyId || undefined,
      relayKeyId: selfRelayKeyId || undefined,
      registration: registration || null,
      installerSocket: installerSocket || null,
    });

    if (socket) {
      this.#addSocketIndex(this.#socketInboxes, socket, inboxId);
    }
    if (installerSocket) {
      this.#addSocketIndex(this.#installerSocketInboxes, installerSocket, inboxId);
    }
  }

  // ---------------------------------------------------------------------------
  // Remote routes (learned via peer gossip)
  // ---------------------------------------------------------------------------

  /**
   * @param {string} inboxId
   * @param {{ hops: number, peerSocket?: object, nextHopRelayKeyId: string, deliveryRelayKeyId: string, nowMs?: number, expiresAtMs?: number }} opts
   * @returns {boolean} true if the route was accepted (new or shorter)
   */
  addRemote(inboxId, { hops, peerSocket = null, nextHopRelayKeyId, deliveryRelayKeyId,
                       nowMs = Date.now(), installerSocket = null, registration = null,
                       expiresAtMs = null } = {}) {
    if (!isNonEmptyString(inboxId)) return false;

    const existing = this.#routes.get(inboxId);
    // Direct routes always win
    if (existing && existing.direct) return false;
    // Prefer shorter paths
    if (existing && existing.hops <= hops) return false;

    // Clean stale socket index when overwriting a remote route whose
    // peerSocket is being replaced. Without this the old socket's entry
    // in #socketInboxes would leak; the disconnect cleanup
    // (removeAllForSocket) only removes routes whose socket still
    // matches, so the stale index would persist forever.
    if (existing && existing.socket && existing.socket !== peerSocket) {
      this.#removeSocketIndex(this.#socketInboxes, existing.socket, inboxId);
    }
    // Clean stale installer index when overwriting
    if (existing && existing.installerSocket && existing.installerSocket !== installerSocket) {
      this.#removeSocketIndex(this.#installerSocketInboxes, existing.installerSocket, inboxId);
    }

    this.#routes.set(inboxId, {
      socket: peerSocket || null,
      hops,
      direct: false,
      addedAtMs: nowMs,
      expiresAtMs: Number.isFinite(expiresAtMs) && expiresAtMs > 0 ? expiresAtMs : null,
      nextHopRelayKeyId,
      deliveryRelayKeyId,
      relayKeyId: deliveryRelayKeyId,
      registration: registration || null,
      installerSocket: installerSocket || null,
    });

    // Index peerSocket so that on socket-disconnect (removeAllForSocket)
    // the route is cleaned up. Closes MED-9 — without this index a
    // DHT-installed remote route would survive its responder dropping.
    if (peerSocket) {
      this.#addSocketIndex(this.#socketInboxes, peerSocket, inboxId);
    }
    if (installerSocket) {
      this.#addSocketIndex(this.#installerSocketInboxes, installerSocket, inboxId);
    }

    return true;
  }

  // ---------------------------------------------------------------------------
  // Lookups
  // ---------------------------------------------------------------------------

  /**
   * @param {string} inboxId
   * @returns {object|null} RouteEntry or null
   *
   * Side-effect: a remote route whose `expiresAtMs` is in the past is
   * evicted from the table (and its reverse indices cleaned) before
   * returning null. This forces a fresh route resolve on the next
   * lookup — closes MED-9 by bounding how long any single FIND_VALUE
   * race outcome can dictate the next-hop.
   */
  get(inboxId) {
    if (!isNonEmptyString(inboxId)) return null;
    const entry = this.#routes.get(inboxId);
    if (!entry) return null;
    if (entry.expiresAtMs && Date.now() > entry.expiresAtMs) {
      this.#evict(inboxId, entry);
      return null;
    }
    return entry;
  }

  #evict(inboxId, entry) {
    this.#routes.delete(inboxId);
    if (entry.socket) {
      this.#removeSocketIndex(this.#socketInboxes, entry.socket, inboxId);
    }
    if (entry.installerSocket) {
      this.#removeSocketIndex(this.#installerSocketInboxes, entry.installerSocket, inboxId);
    }
  }

  /**
   * @param {string} inboxId
   * @returns {boolean}
   */
  has(inboxId) {
    if (!isNonEmptyString(inboxId)) return false;
    return this.#routes.has(inboxId);
  }

  /**
   * @param {string} inboxId
   * @returns {boolean} true if the route is direct with no socket (hosted locally)
   */
  isLocalHosted(inboxId) {
    if (!isNonEmptyString(inboxId)) return false;
    const entry = this.#routes.get(inboxId);
    return !!(entry && entry.direct && !entry.socket);
  }

  // ---------------------------------------------------------------------------
  // Removal
  // ---------------------------------------------------------------------------

  /**
   * Remove a single route by inboxId.
   */
  remove(inboxId) {
    this.#routes.delete(inboxId);
  }

  /**
   * Remove all routes installed by the given socket.
   *
   * A direct local route that carries a signed claimant registration
   * (entry.direct && entry.registration) is the relay's record of
   * "this inbox is hosted here" — independent of whether the owner's
   * node currently has a live socket. Owner disconnect must not erase
   * that record: subsequent deposits should buffer in inboxStore via
   * the entry.direct && !entry.socket branch of routeDelivery, and
   * peer relays must keep their cached route. Such entries survive
   * here with installerSocket nulled; they are removed only by
   * explicit inbox.withdraw or by being overwritten when the owner
   * re-registers.
   *
   * @param {object} socket
   * @returns {string[]} list of withdrawn inboxIds (preserved entries excluded)
   */
  removeAllForInstallerSocket(socket) {
    if (!socket) return [];
    const ids = this.#installerSocketInboxes.get(socket);
    this.#installerSocketInboxes.delete(socket);
    if (!ids || ids.size === 0) return [];

    const withdrawn = [];
    for (const id of ids) {
      const entry = this.#routes.get(id);
      if (!entry || entry.installerSocket !== socket) continue;
      if (entry.direct && entry.registration) {
        entry.installerSocket = null;
        continue;
      }
      this.#routes.delete(id);
      // Also clean socket reverse index if present
      if (entry.socket) {
        this.#removeSocketIndex(this.#socketInboxes, entry.socket, id);
      }
      withdrawn.push(id);
    }
    return withdrawn;
  }

  /**
   * Remove all routes whose socket matches the given one.
   *
   * Same survival rule as removeAllForInstallerSocket: a direct local
   * route with a signed registration is preserved (socket nulled,
   * entry kept) so deposits during owner downtime land in the relay's
   * inboxStore and drain on reconnect.
   *
   * @param {object} socket
   * @returns {string[]} list of withdrawn inboxIds (preserved entries excluded)
   */
  removeAllForSocket(socket) {
    if (!socket) return [];
    const ids = this.#socketInboxes.get(socket);
    this.#socketInboxes.delete(socket);
    if (!ids || ids.size === 0) return [];

    const withdrawn = [];
    for (const id of ids) {
      const entry = this.#routes.get(id);
      if (!entry || entry.socket !== socket) continue;
      if (entry.direct && entry.registration) {
        entry.socket = null;
        continue;
      }
      this.#routes.delete(id);
      withdrawn.push(id);
    }
    return withdrawn;
  }

  // ---------------------------------------------------------------------------
  // Iteration / diagnostics
  // ---------------------------------------------------------------------------

  /**
   * @returns {Map<string, object>} shallow copy of the internal routes map
   */
  getAll() {
    return new Map(this.#routes);
  }

  get size() {
    return this.#routes.size;
  }

  // ---------------------------------------------------------------------------
  // Callback
  // ---------------------------------------------------------------------------

  setOnRouteAdded(fn) {
    this.#onRouteAdded = typeof fn === "function" ? fn : null;
  }

  /**
   * Fire the onRouteAdded callback. Called by InboxRouter after announcements.
   * @param {string[]} inboxIds
   */
  notifyRoutesAdded(inboxIds) {
    if (this.#onRouteAdded && inboxIds.length > 0) {
      this.#onRouteAdded(inboxIds);
    }
  }
}
