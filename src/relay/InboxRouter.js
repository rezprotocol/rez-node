import { base64ToBytes, isNonEmptyString } from "@rezprotocol/core";
import { encodeFrame, encodeControlMessage, sendControlMessage } from "../network/tcp/TcpFraming.js";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { canonicalJSONStringify } from "../util/canonicalize.js";
import { signedPayloadBytes } from "./PeerAuthShared.js";
import { RouteTable } from "../routing/RouteTable.js";
import { GossipRouteAnnouncer } from "../routing/GossipRouteAnnouncer.js";

/**
 * InboxRouter manages a routing table for inbox delivery.
 *
 * Each entry maps an inboxId to a RouteEntry describing how to reach it:
 *   - direct socket (node connected to this relay)
 *   - remote next hop relay (relayKeyId, hops)
 *
 * Control messages:
 *   { _ctl: "inbox.register", inboxIds: [...] }
 *   { _ctl: "inbox.route",    entries: [{ inboxId, hops, nextHopRelayKeyId, deliveryRelayKeyId }] }
 *   { _ctl: "inbox.withdraw", inboxIds: [...] }
 *   { _ctl: "inbox.deposit",  inboxId, inner: <base64> }
 */

const REGISTRATION_CRYPTO = new NodeCryptoProvider();

/**
 * Per-inbox cap on the relay's offline-deposit buffer. SECURITY_AUDIT MED-12:
 * without this, a peer-authenticated attacker can flood `inbox.deposit` frames
 * at a victim's offline inbox and exhaust the relay's disk. 10,000 items per
 * inbox is comfortably above any organic burst (multi-thousand message
 * backlog) but well below disk-fill territory at ~4KB average ciphertext
 * (~40 MiB). We cap on item count, not bytes, because counting items via
 * the inbox store's `list` API is cheap and avoids reading each event body.
 */
export const MAX_BUFFERED_ITEMS_PER_INBOX = 10_000;

/**
 * Companion byte cap for buffered inbox deposits. The item cap above bounds the
 * file COUNT but not their size: 10,000 items at the ~8 MiB max frame would be
 * ~80 GiB. This bounds total bytes per inbox well below disk-fill while staying
 * far above any organic offline backlog (e.g. ~1,000 buffered 0.5 MiB image
 * messages). Enforced in RMailbox via an O(1) persisted counter, so — like the
 * item cap — it covers every deposit path, not just the offline-buffered one.
 */
export const MAX_BUFFERED_BYTES_PER_INBOX = 512 * 1024 * 1024; // 512 MiB

/**
 * Canonical payload signed by the inbox claimant (with their private key)
 * to authorize a node to advertise their inbox to the relay mesh.
 *
 * The claimant pubkey embedded in the registration record is the trust root.
 * One signature check by every relay; no derivation, no extra lookups.
 *
 * The delegation binds BOTH the node's crypto identity (`nodeKeyId` +
 * `nodePublicKeyB64`) AND its routing-layer address (`relayKeyId`).
 * Without the routing-layer binding, a peer that re-broadcasts a stored
 * route via DHT could swap in its own `deliveryRelayKeyId` and route
 * inbox deliveries through itself — see docs/SECURITY_AUDIT.md HIGH-8.
 */
function claimantNodeDelegationPayload({ inboxId, claimantPublicKeyB64, nodeKeyId, nodePublicKeyB64, relayKeyId, issuedAtMs, expiresAtMs } = {}) {
  return {
    kind: "inbox-node-delegation",
    inboxId,
    claimantPublicKeyB64,
    nodeKeyId,
    nodePublicKeyB64,
    relayKeyId,
    issuedAtMs,
    expiresAtMs,
  };
}

export class InboxRouter {
  /** @type {Map<string, number>} per-inbox buffered-item count for MED-12 quota */
  #bufferedCountByInbox = new Map();

  /**
   * @type {Map<string, string>}
   * inboxId -> claimantPublicKeyB64 seen on the most recent verified
   * registration. SECURITY_AUDIT MED-11: the offline-deposit drain in
   * _replayPendingToSocket trusts that the registrant is the same identity
   * the items were buffered for. By recording the binding at register
   * time we can refuse to drain to a registrant whose claimant pubkey
   * differs from the one buffered items were originally addressed to.
   * Restarting the relay resets this map; that case is benign because
   * the buffered items remain locked to the same inboxId, and re-binding
   * requires a fresh claimant-signed registration which the upstream
   * verification already enforces.
   */
  #claimantPubkeyByInbox = new Map();

  constructor({ transport = null, inboxStore = null, relayPeerDirectory = null, logger = console, nowMs = () => Date.now(), selfRelayKeyId = null, routeTable = null, routeAnnouncer = null, onPeerAdded = null, onPeerRemoved = null } = {}) {
    /** @type {RouteTable} shared route state */
    this._routeTable = routeTable instanceof RouteTable ? routeTable : new RouteTable();

    /** @type {Set<object>} connected peer sockets for route announcements */
    this._peerSockets = new Set();

    /** @type {string|null} this relay's identity for ID-based routing announcements */
    this._selfRelayKeyId = typeof selfRelayKeyId === "string" && selfRelayKeyId.trim() ? selfRelayKeyId.trim() : null;

    this._transport = transport;
    this._inboxStore = inboxStore;
    this._relayPeerDirectory = relayPeerDirectory ?? null;
    this._logger = logger || console;
    this._nowMs = nowMs;
    /** @type {Map<string, { resolve: Function, timer: number }>} pending route queries by queryId */
    this._pendingQueries = new Map();
    this._routeAnnouncer = routeAnnouncer || new GossipRouteAnnouncer();
    this._onPeerAdded = typeof onPeerAdded === "function" ? onPeerAdded : null;
    this._onPeerRemoved = typeof onPeerRemoved === "function" ? onPeerRemoved : null;

    /** Cached context object for the RouteAnnouncer strategy. */
    this._announcerCtx = {
      peerSockets: this._peerSockets,
      routeTable: this._routeTable,
      selfRelayKeyId: this._selfRelayKeyId,
      encodeCtl: (obj) => this._encodeCtl(obj),
      trySendFrame: (socket, bytes) => this._trySendFrame(socket, bytes),
      createAnnouncedRouteEntry: (id, route, hops) => this._createAnnouncedRouteEntry(id, route, hops),
    };
  }

  /**
   * Expose the underlying RouteTable for direct access by other subsystems.
   */
  get routeTable() {
    return this._routeTable;
  }

  /**
   * Swap the route announcer strategy and rebuild the cached context.
   * @param {import("../routing/RouteAnnouncer.js").RouteAnnouncer} announcer
   */
  setRouteAnnouncer(announcer) {
    if (!announcer) throw new Error("InboxRouter.setRouteAnnouncer requires a non-null announcer");
    this._routeAnnouncer = announcer;
    this._announcerCtx = {
      peerSockets: this._peerSockets,
      routeTable: this._routeTable,
      selfRelayKeyId: this._selfRelayKeyId,
      encodeCtl: (obj) => this._encodeCtl(obj),
      trySendFrame: (socket, bytes) => this._trySendFrame(socket, bytes),
      createAnnouncedRouteEntry: (id, route, hops) => this._createAnnouncedRouteEntry(id, route, hops),
    };
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Register inboxIds as directly reachable via the given socket.
   * Called when a node sends inbox.register after connecting.
   */
  registerLocal(inboxIds, socket, options = {}) {
    if (!Array.isArray(inboxIds)) return;
    const announce = options && options.announce !== undefined ? options.announce !== false : true;
    const registrations = Array.isArray(options.registrations) ? options.registrations : [];
    const registered = [];
    const drainable = [];
    for (const id of inboxIds) {
      if (!isNonEmptyString(id)) continue;
      const reg = registrations.find(function (item) { return item && item.inboxId === id; }) || null;
      // SECURITY_AUDIT MED-11: assert the inbox→claimant-pubkey binding.
      // The drain path below would otherwise hand buffered ciphertext to
      // whichever identity registered most recently, even if that differs
      // from the identity items were originally addressed to. Refuse the
      // drain on mismatch but still install the route — the new claimant
      // can resume forward-direction delivery; buffered items stay quarantined.
      const claimantPubkeyB64 = reg && typeof reg.claimantPublicKeyB64 === "string"
        ? reg.claimantPublicKeyB64.trim() : "";
      let allowDrain = true;
      if (claimantPubkeyB64) {
        const prior = this.#claimantPubkeyByInbox.get(id);
        if (prior && prior !== claimantPubkeyB64) {
          this._logger.warn(
            "[InboxRouter] suppressing buffered drain for " + id
            + " — registrant claimant pubkey changed (prior=" + prior
            + ", new=" + claimantPubkeyB64 + "). Buffered items quarantined.",
          );
          allowDrain = false;
        } else {
          this.#claimantPubkeyByInbox.set(id, claimantPubkeyB64);
        }
      }
      this._routeTable.addLocal(id, socket, {
        selfRelayKeyId: this._selfRelayKeyId,
        nowMs: this._nowMs(),
        announceToPeers: announce,
        registration: reg,
        installerSocket: socket,
      });
      registered.push(id);
      if (allowDrain) drainable.push(id);
    }
    if (registered.length > 0 && announce) {
      this._routeAnnouncer.announceRoutes(registered, 1, this._announcerCtx);
      this._routeTable.notifyRoutesAdded(registered);
    }
    // If this relay stored deposits for these inboxes while the owner was
    // disconnected (the entry.direct && !entry.socket branch in
    // routeDelivery), drain them to the just-attached socket now. Bytes
    // land on the receiving node's local inboxStore via depositFromWire
    // and surface through the usual EVT_MAILBOX_DEPOSITED push to the
    // chat-server. Without this, the relay's on-disk inbox store grows
    // unbounded and the receiver never sees its offline mail.
    if (this._inboxStore
        && typeof this._inboxStore.list === "function"
        && typeof this._inboxStore.fetch === "function") {
      for (const id of drainable) {
        this._replayPendingToSocket(id, socket).catch((err) => {
          const msg = err && err.message ? err.message : String(err);
          console.error("[InboxRouter] replayPendingToSocket failed inboxId=" + id + ": " + msg);
        });
      }
    }
  }

  async _replayPendingToSocket(inboxId, socket) {
    let cursor = null;
    while (true) {
      const page = await this._inboxStore.list(inboxId, { cursor, limit: 50 });
      const items = page && Array.isArray(page.items) ? page.items : [];
      if (items.length === 0) return;
      for (const item of items) {
        const eventId = item && typeof item.eventId === "string" ? item.eventId : "";
        if (!eventId) continue;
        const evt = await this._inboxStore.fetch(inboxId, eventId);
        const bytes = evt && evt.bytes instanceof Uint8Array ? evt.bytes : null;
        if (!bytes) continue;
        const ctl = { _ctl: "inbox.deposit", inboxId, inner: Buffer.from(bytes).toString("base64") };
        const ctlBytes = this._encodeCtl(ctl);
        const sent = this._sendToSocket(socket, ctlBytes);
        if (!sent) return;
        // Delivered → drop it from the relay's on-disk buffer. This buffer is a
        // transient hand-off for offline mail (the owner persists each deposit
        // on receipt), not durable storage. Leaving delivered deposits in place
        // let the store grow without bound — tens of thousands of files — so
        // every reconnect re-walked the whole tree (FileSystemDataStore.list is
        // O(files)) and pegged the CPU. ack() removes the event; keep the
        // in-memory buffered counter in step (clamped at 0).
        if (typeof this._inboxStore.ack === "function") {
          const removed = await this._inboxStore.ack(inboxId, eventId);
          if (removed) {
            const remaining = (this.#bufferedCountByInbox.get(inboxId) || 0) - 1;
            this.#bufferedCountByInbox.set(inboxId, remaining > 0 ? remaining : 0);
          }
        }
      }
      const nextCursor = page && typeof page.nextCursor === "string" ? page.nextCursor : null;
      if (!nextCursor) return;
      cursor = nextCursor;
    }
  }

  addRemoteRoute(inboxId, routeOrVia, hopsArg, peerSocketArg = null, relayKeyIdArg = null) {
    if (!isNonEmptyString(inboxId)) return;
    const route = normalizeRemoteRouteArgs(routeOrVia, hopsArg, peerSocketArg, relayKeyIdArg);
    if (!route) return;
    const { hops, peerSocket, nextHopRelayKeyId, deliveryRelayKeyId, registration = null } = route;
    const accepted = this._routeTable.addRemote(inboxId, {
      hops,
      peerSocket,
      nextHopRelayKeyId,
      deliveryRelayKeyId,
      nowMs: this._nowMs(),
      installerSocket: peerSocket,
      registration,
    });
    if (!accepted) return;
    const reAnnounceNextHopRelayKeyId = normalizeRelayKeyId(this._selfRelayKeyId) || nextHopRelayKeyId;
    const reAnnounceEntries = [{
      inboxId,
      hops: hops + 1,
      nextHopRelayKeyId: reAnnounceNextHopRelayKeyId,
      deliveryRelayKeyId,
      relayKeyId: deliveryRelayKeyId,
    }];
    this._routeAnnouncer.announceRoutesExcept(peerSocket, reAnnounceEntries, this._announcerCtx);
    this._routeTable.notifyRoutesAdded([inboxId]);
  }

  /**
   * Handle a connection being dropped — withdraw all routes associated with socket.
   */
  removeConnection(socket) {
    if (!socket) return;
    if (this._relayPeerDirectory && typeof this._relayPeerDirectory.remove === "function") {
      this._relayPeerDirectory.remove(socket);
    }
    this._peerSockets.delete(socket);
    const directWithdrawn = this._routeTable.removeAllForSocket(socket);
    const installerWithdrawn = this._routeTable.removeAllForInstallerSocket(socket);
    const allWithdrawn = [...new Set([...directWithdrawn, ...installerWithdrawn])];
    if (allWithdrawn.length > 0) {
      this._routeAnnouncer.announceWithdraw(allWithdrawn, this._announcerCtx);
    }
    if (this._onPeerRemoved) this._onPeerRemoved(socket);
  }

  /**
   * Route delivery of innerBytes to the target inboxId.
   * Returns true if routed, false if no route found.
   *
   * On forward failure for a remote (non-direct) route, the stale
   * entry is evicted so the next deposit re-resolves through the
   * route-resolver chain. Without eviction a permanently-dead route
   * (peer disappeared or auth was revoked) would fail every delivery
   * forever — and a malicious responder's race-win route would persist
   * far beyond its TTL upper bound. See docs/SECURITY_AUDIT.md MED-9.
   */
  async routeDelivery(inboxId, innerBytes) {
    if (!isNonEmptyString(inboxId) || !(innerBytes instanceof Uint8Array)) return false;
    const entry = this._routeTable.get(inboxId);
    if (!entry) return false;

    if (entry.direct && entry.socket) {
      const ctl = { _ctl: "inbox.deposit", inboxId, inner: Buffer.from(innerBytes).toString("base64") };
      const ctlBytes = this._encodeCtl(ctl);
      return this._sendToSocket(entry.socket, ctlBytes);
    }

    if (entry.direct && !entry.socket && this._inboxStore) {
      return await this.#depositToBufferedStore(inboxId, innerBytes);
    }

    const nextHopRelayKeyId =
      normalizeRelayKeyId(entry.nextHopRelayKeyId)
      || normalizeRelayKeyId(entry.relayKeyId)
      || normalizeRelayKeyId(entry.deliveryRelayKeyId);
    if (!nextHopRelayKeyId || !this._relayPeerDirectory) {
      this._routeTable.remove(inboxId);
      return false;
    }
    const peerSocket = this._relayPeerDirectory.getSocket(nextHopRelayKeyId);
    if (!peerSocket) {
      this._routeTable.remove(inboxId);
      return false;
    }
    const ctl = { _ctl: "inbox.deposit", inboxId, inner: Buffer.from(innerBytes).toString("base64") };
    const ctlBytes = this._encodeCtl(ctl);
    if (this._sendToSocket(peerSocket, ctlBytes)) {
      entry.socket = peerSocket;
      return true;
    }
    this._routeTable.remove(inboxId);
    return false;
  }

  isLocalHostedInbox(inboxId) {
    return this._routeTable.isLocalHosted(inboxId);
  }

  /**
   * Get route entry for an inboxId (used by sender to build onion path).
   */
  getRouteTo(inboxId) {
    return this._routeTable.get(inboxId);
  }



  /**
   * Register a peer socket for route announcements.
   * Send all known routes to this peer.
   */
  addPeer(peerSocket) {
    if (!peerSocket) return;
    this._peerSockets.add(peerSocket);
    this._routeAnnouncer.announceAllToPeer(peerSocket, this._announcerCtx);
    if (this._onPeerAdded) {
      const relayKeyId = this._relayPeerDirectory
        ? this._relayPeerDirectory.getRelayKeyIdForSocket(peerSocket)
        : null;
      if (relayKeyId) this._onPeerAdded(relayKeyId, peerSocket);
    }
  }

  /**
   * Replay the full current route table to every connected peer.
   * Used as periodic anti-entropy in case a prior announcement was missed.
   */
  reannounceAllRoutesToPeers() {
    this._routeAnnouncer.reannounceAll(this._announcerCtx);
  }

  /**
   * Dispatch an incoming control message.
   * Returns true if handled, false otherwise. May return a Promise for inbox.deposit.
   */
  handleControlMessage(ctlObj, socket) {
    if (!ctlObj || typeof ctlObj !== "object") return false;
    const type = ctlObj._ctl;
    if (!isNonEmptyString(type)) return false;

    switch (type) {
      case "inbox.register":
        return this._handleRegister(ctlObj, socket);
      case "inbox.route":
        return this._handleRoute(ctlObj, socket);
      case "inbox.withdraw":
        return this._handleWithdraw(ctlObj, socket);
      case "inbox.query":
        return this._handleQuery(ctlObj, socket);
      case "inbox.query.reply":
        return this._handleQueryReply(ctlObj, socket);
      case "inbox.deposit":
        // Require authenticated socket — unauthenticated clients must not inject deposits.
        if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedSocket(socket)) return false;
        if (!isNonEmptyString(ctlObj.inboxId) || typeof ctlObj.inner !== "string") return false;
        return this._handleDepositAsync(
          ctlObj.inboxId,
          new Uint8Array(Buffer.from(ctlObj.inner, "base64")),
        );
      default:
        return false;
    }
  }

  /**
   * Number of routes in the table.
   */
  get size() {
    return this._routeTable.size;
  }

  /**
   * Set callback invoked when routes are added (registerLocal or addRemoteRoute).
   */
  setOnRouteAdded(fn) {
    this._routeTable.setOnRouteAdded(fn);
  }

  // ---------------------------------------------------------------------------
  // Control message handlers
  // ---------------------------------------------------------------------------

  _handleRegister(ctlObj, socket) {
    const debug = process.env.REZ_INBOX_DEBUG === "1";
    if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedSocket(socket)) {
      if (debug) console.warn("[INBOX-DEBUG] _handleRegister: unauthenticated socket");
      return false;
    }
    const auth = this._relayPeerDirectory.getAuth(socket);
    const registrations = Array.isArray(ctlObj.registrations) ? ctlObj.registrations : [];
    if (registrations.length === 0 || !auth) {
      if (debug) console.warn("[INBOX-DEBUG] _handleRegister: empty registrations or no auth", { count: registrations.length, hasAuth: !!auth });
      return false;
    }
    const MAX_REGISTRATIONS_PER_MESSAGE = 100;
    if (registrations.length > MAX_REGISTRATIONS_PER_MESSAGE) {
      if (debug) console.warn("[INBOX-DEBUG] _handleRegister: too many registrations", { count: registrations.length });
      return false;
    }
    const validInboxIds = [];
    const validRegistrations = [];
    for (const registration of registrations) {
      const verified = verifyHostedInboxRegistration(registration, auth);
      if (verified) {
        validInboxIds.push(verified.inboxId);
        validRegistrations.push(registration);
      }
    }
    if (debug) {
      const incomingIds = registrations.map((r) => (r && typeof r.inboxId === "string") ? r.inboxId : "?");
      console.log("[INBOX-DEBUG] _handleRegister: accepted=" + validInboxIds.length + "/" + registrations.length,
        { authNodeKeyId: auth.nodeKeyId, authRelayKeyId: auth.relayKeyId, incomingIds, acceptedIds: validInboxIds });
    }
    if (validInboxIds.length === 0) return false;
    this.registerLocal(validInboxIds, socket, {
      announce: this._relayPeerDirectory.isAuthenticatedRelaySocket(socket),
      registrations: validRegistrations,
    });
    return true;
  }

  /**
   * Register a pending query that will be resolved when inbox.query.reply arrives.
   * @param {string} queryId
   * @param {number} timeoutMs
   * @returns {Promise<boolean>} true if routes were found and installed
   */
  waitForQueryReply(queryId, timeoutMs) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this._pendingQueries.delete(queryId);
        resolve(false);
      }, timeoutMs);
      this._pendingQueries.set(queryId, { resolve, timer });
    });
  }

  /**
   * Handle inbox.query.reply — install returned route entries into local route
   * table and resolve any pending query promise.
   *
   * MED-7 (docs/SECURITY_AUDIT.md): hops=0 entries advertise "owner-
   * registered with the peer relay". Without an ownership-proof check,
   * any authenticated peer relay could race-inject a hops=0 entry
   * pointing at itself for an arbitrary inboxId, hijacking deposits.
   * Mirror _handleRoute's verifyHostedInboxRegistration anchor here.
   */
  _handleQueryReply(ctlObj, socket) {
    if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedSocket(socket)) return false;
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    const entries = Array.isArray(ctlObj.entries) ? ctlObj.entries : [];
    const peerAuth = this._relayPeerDirectory.getAuth(socket);
    if (!peerAuth) return false;
    let installed = 0;
    for (const entry of entries) {
      if (!entry || typeof entry !== "object") continue;
      const hops = Number(entry.hops);
      const deliveryRelayKeyId =
        normalizeRelayKeyId(entry.deliveryRelayKeyId)
        || normalizeRelayKeyId(entry.relayKeyId);
      const nextHopRelayKeyId = this._relayPeerDirectory.getRelayKeyIdForSocket(socket) || null;
      if (!deliveryRelayKeyId || !nextHopRelayKeyId || !Number.isInteger(hops) || hops < 0) continue;
      // MED-8: gossip-via-query accepts ONLY hops=0 entries. See _handleRoute.
      if (hops !== 0) continue;
      const verifiedRegistration = verifyHostedInboxRegistration(entry.registration, peerAuth);
      if (!verifiedRegistration) continue;
      if (verifiedRegistration.inboxId !== entry.inboxId) continue;
      if (deliveryRelayKeyId !== peerAuth.relayKeyId) continue;
      this.addRemoteRoute(entry.inboxId, {
        hops,
        peerSocket: socket,
        nextHopRelayKeyId,
        deliveryRelayKeyId,
        registration: entry.registration,
      });
      installed += 1;
    }
    const pending = this._pendingQueries.get(queryId);
    if (pending) {
      clearTimeout(pending.timer);
      this._pendingQueries.delete(queryId);
      pending.resolve(installed > 0);
    }
    return true;
  }

  /**
   * Handle inbox.query — respond with inbox.route entries for any queried inboxIds
   * that exist in the local route table. Allows leaf nodes to discover routes
   * on demand without full route gossip membership.
   */
  _handleQuery(ctlObj, socket) {
    if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedSocket(socket)) return false;
    const inboxIds = Array.isArray(ctlObj.inboxIds) ? ctlObj.inboxIds : [];
    if (inboxIds.length === 0 || inboxIds.length > 100) return false;
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    const entries = [];
    for (const id of inboxIds) {
      if (!isNonEmptyString(id)) continue;
      const route = this._routeTable.get(id);
      if (!route) continue;
      // Query replies include ALL known routes, including leaf-registered inboxes
      // that are excluded from gossip announcements (announceToPeers=false).
      const deliveryRelayKeyId =
        normalizeRelayKeyId(route.deliveryRelayKeyId)
        || normalizeRelayKeyId(route.relayKeyId)
        || normalizeRelayKeyId(this._selfRelayKeyId);
      const nextHopRelayKeyId =
        normalizeRelayKeyId(this._selfRelayKeyId)
        || normalizeRelayKeyId(route.nextHopRelayKeyId)
        || null;
      if (!deliveryRelayKeyId || !nextHopRelayKeyId) continue;
      entries.push({
        inboxId: id,
        nextHopRelayKeyId,
        deliveryRelayKeyId,
        relayKeyId: deliveryRelayKeyId,
        hops: route.hops + 1,
      });
    }
    // Always reply — empty entries means "not found"
    const ctlBytes = this._encodeCtl({ _ctl: "inbox.query.reply", queryId, entries });
    this._trySendFrame(socket, ctlBytes);
    return true;
  }

  _handleRoute(ctlObj, socket) {
    if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedRelaySocket(socket)) return false;
    const peerAuth = this._relayPeerDirectory.getAuth(socket);
    if (!peerAuth) return false;
    const entries = Array.isArray(ctlObj.entries) ? ctlObj.entries : [];
    if (entries.length === 0) return false;
    const MAX_ROUTE_ENTRIES = 500;
    if (entries.length > MAX_ROUTE_ENTRIES) return false;
    let accepted = 0;
    for (const entry of entries) {
      if (!entry || typeof entry !== "object") continue;
      const hops = Number(entry.hops);
      const deliveryRelayKeyId =
        normalizeRelayKeyId(entry.deliveryRelayKeyId)
        || normalizeRelayKeyId(entry.relayKeyId);
      const nextHopRelayKeyId = this._relayPeerDirectory.getRelayKeyIdForSocket(socket) || null;
      if (!deliveryRelayKeyId || !nextHopRelayKeyId || !Number.isInteger(hops) || hops < 0) continue;
      // MED-8 (docs/SECURITY_AUDIT.md): gossip accepts ONLY hops=0 entries
      // — peers can announce inboxes they directly host (proven by a
      // claimant-signed registration), nothing else. Transitive hops>0
      // entries carry no proof; any peer relay could otherwise advertise
      // itself as next hop for arbitrary inboxes. Cross-mesh discovery is
      // the DHT's job (HIGH-8 anchored its writes to claimant sigs).
      if (hops !== 0) continue;
      const verifiedRegistration = verifyHostedInboxRegistration(entry.registration, peerAuth);
      if (!verifiedRegistration) continue;
      if (verifiedRegistration.inboxId !== entry.inboxId) continue;
      if (deliveryRelayKeyId !== peerAuth.relayKeyId) continue;
      this.addRemoteRoute(entry.inboxId, {
        hops,
        peerSocket: socket || null,
        nextHopRelayKeyId,
        deliveryRelayKeyId,
        registration: entry.registration,
      });
      accepted += 1;
    }
    return accepted > 0;
  }

  _handleWithdraw(ctlObj, socket) {
    if (!this._relayPeerDirectory || !this._relayPeerDirectory.isAuthenticatedRelaySocket(socket)) return false;
    const inboxIds = Array.isArray(ctlObj.inboxIds) ? ctlObj.inboxIds : [];
    if (inboxIds.length === 0) return false;
    const withdrawn = [];
    for (const id of inboxIds) {
      if (!isNonEmptyString(id)) continue;
      const entry = this._routeTable.get(id);
      if (!entry) continue;
      // Only the installer can withdraw a route
      if (entry.installerSocket && entry.installerSocket !== socket) continue;
      this._routeTable.remove(id);
      withdrawn.push(id);
    }
    if (withdrawn.length > 0) {
      this._routeAnnouncer.announceWithdraw(withdrawn, this._announcerCtx);
    }
    return true;
  }

  _handleDeposit(ctlObj) {
    if (!isNonEmptyString(ctlObj.inboxId)) return false;
    if (typeof ctlObj.inner !== "string") return false;
    const innerBytes = new Uint8Array(Buffer.from(ctlObj.inner, "base64"));
    return this._handleDepositAsync(ctlObj.inboxId, innerBytes);
  }

  async _handleDepositAsync(inboxId, innerBytes) {
    const routed = await this.routeDelivery(inboxId, innerBytes);
    if (routed) return true;
    if (this._inboxStore && this.isLocalHostedInbox(inboxId)) {
      return await this.#depositToBufferedStore(inboxId, innerBytes);
    }
    return false;
  }

  /**
   * Persist innerBytes to the local inboxStore subject to the per-inbox
   * buffer quota (SECURITY_AUDIT MED-12). Returns false when the cap is
   * hit so the caller can surface a routing failure to the depositor;
   * returns true on successful persistence.
   */
  async #depositToBufferedStore(inboxId, innerBytes) {
    await this.#ensureBufferedCount(inboxId);
    const current = this.#bufferedCountByInbox.get(inboxId) || 0;
    if (current >= MAX_BUFFERED_ITEMS_PER_INBOX) {
      this._logger.warn(
        "[InboxRouter] dropped buffered deposit for "
        + inboxId
        + " — per-inbox cap reached ("
        + current + " >= " + MAX_BUFFERED_ITEMS_PER_INBOX + ")",
      );
      return false;
    }
    await this._inboxStore.depositFromWire(inboxId, innerBytes);
    this.#bufferedCountByInbox.set(inboxId, current + 1);
    return true;
  }

  /**
   * Lazy-seed the in-memory buffered-item counter from the persistent
   * store. On a fresh process the counter is empty; the first deposit or
   * drain for a given inboxId pays a one-time list-scan cost so we don't
   * undercount items that survived a relay restart. After seeding the
   * counter stays in memory and is maintained incrementally.
   */
  async #ensureBufferedCount(inboxId) {
    if (this.#bufferedCountByInbox.has(inboxId)) return;
    if (!this._inboxStore || typeof this._inboxStore.list !== "function") {
      this.#bufferedCountByInbox.set(inboxId, 0);
      return;
    }
    let count = 0;
    let cursor;
    while (true) {
      const page = await this._inboxStore.list(inboxId, { cursor, limit: 200 });
      const items = page && Array.isArray(page.items) ? page.items : [];
      count += items.length;
      const next = page && typeof page.nextCursor === "string" ? page.nextCursor : null;
      if (!next) break;
      cursor = next;
    }
    this.#bufferedCountByInbox.set(inboxId, count);
  }

  // ---------------------------------------------------------------------------
  // Route entry construction (shared by announcer and query replies)
  // ---------------------------------------------------------------------------

  _createAnnouncedRouteEntry(inboxId, route, defaultHops) {
    if (route && route.announceToPeers === false && !route.registration) return null;
    const deliveryRelayKeyId =
      normalizeRelayKeyId(route ? route.deliveryRelayKeyId : "")
      || normalizeRelayKeyId(route ? route.relayKeyId : "")
      || normalizeRelayKeyId(this._selfRelayKeyId);
    const nextHopRelayKeyId =
      normalizeRelayKeyId(this._selfRelayKeyId)
      || normalizeRelayKeyId(route ? route.nextHopRelayKeyId : "")
      || null;
    if (!deliveryRelayKeyId || !nextHopRelayKeyId) return null;
    const entry = {
      inboxId,
      nextHopRelayKeyId,
      deliveryRelayKeyId,
      relayKeyId: deliveryRelayKeyId,
      hops: defaultHops,
    };
    if (route && route.direct === true && route.hops === 0 && route.registration) {
      entry.hops = 0;
      entry.registration = route.registration;
    }
    return entry;
  }

  _sendToSocket(socket, frameBytes) {
    if (!socket || socket.destroyed) {
      this._logger.error("[InboxRouter] _sendToSocket: socket is " + (socket ? "destroyed" : "null"));
      return false;
    }
    try {
      socket.write(frameBytes);
      return true;
    } catch (err) {
      this._logger.error("[InboxRouter] _sendToSocket write failed: " + (err && err.message ? err.message : err));
      return false;
    }
  }

  _encodeCtl(ctlObj) {
    return encodeControlMessage(ctlObj);
  }

  _trySendFrame(socket, bytes) {
    if (!socket || socket.destroyed) {
      this._logger.error("[InboxRouter] _trySendFrame: socket is " + (socket ? "destroyed" : "null"));
      return;
    }
    try {
      socket.write(bytes);
    } catch (err) {
      this._logger.error("[InboxRouter] _trySendFrame write failed: " + (err && err.message ? err.message : err));
    }
  }
}

function normalizeRelayKeyId(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function normalizeRemoteRouteArgs(routeOrVia, hopsArg, peerSocketArg, relayKeyIdArg) {
  if (routeOrVia && typeof routeOrVia === "object" && !Array.isArray(routeOrVia)) {
    const hops = Number(routeOrVia.hops);
    const nextHopRelayKeyId = normalizeRelayKeyId(routeOrVia.nextHopRelayKeyId);
    const deliveryRelayKeyId =
      normalizeRelayKeyId(routeOrVia.deliveryRelayKeyId)
      || normalizeRelayKeyId(routeOrVia.relayKeyId);
    if (!nextHopRelayKeyId || !deliveryRelayKeyId || !Number.isInteger(hops) || hops < 0) return null;
    return {
      hops,
      peerSocket: routeOrVia.peerSocket || peerSocketArg || null,
      nextHopRelayKeyId,
      deliveryRelayKeyId,
      registration: routeOrVia.registration || null,
    };
  }

  // Legacy call shape: addRemoteRoute(inboxId, via, hops, peerSocket, relayKeyId)
  // ID-only routing requires a relay key ID even on legacy calls.
  const hops = Number(hopsArg);
  const deliveryRelayKeyId = normalizeRelayKeyId(relayKeyIdArg);
  if (!deliveryRelayKeyId || !Number.isInteger(hops) || hops < 0) return null;
  return {
    hops,
    peerSocket: peerSocketArg || null,
    nextHopRelayKeyId: deliveryRelayKeyId,
    deliveryRelayKeyId,
    registration: null,
  };
}

/**
 * Verify a pubkey-only inbox-node delegation. One signature check against the
 * embedded claimant pubkey — that key is the trust root for the inbox per the
 * cap model (see docs/CAPABILITY_MODEL.md). No accountId, no derivation, no
 * lookup against a separate identity registry.
 *
 * When `auth` is supplied (peer relay auth context), the delegation must name
 * the same node it was actually issued for — i.e. nodeKeyId/nodePublicKeyB64
 * in the record must match the peer's authenticated identity. This prevents
 * a relay from re-advertising another node's delegation as its own.
 */
export function verifyClaimantNodeDelegation(registration, auth = null) {
  const debug = process.env.REZ_INBOX_DEBUG === "1";
  const fail = (reason, extra) => {
    if (debug) console.warn("[INBOX-DEBUG] verifyClaimantNodeDelegation reject: " + reason, extra || "");
    return null;
  };
  if (!registration || typeof registration !== "object") return fail("registration-not-object");
  const inboxId = isNonEmptyString(registration.inboxId) ? registration.inboxId.trim() : "";
  const claimantPublicKeyB64 = isNonEmptyString(registration.claimantPublicKeyB64)
    ? registration.claimantPublicKeyB64.trim()
    : "";
  const nodeKeyId = isNonEmptyString(registration.nodeKeyId) ? registration.nodeKeyId.trim() : "";
  const nodePublicKeyB64 = isNonEmptyString(registration.nodePublicKeyB64) ? registration.nodePublicKeyB64.trim() : "";
  const relayKeyId = isNonEmptyString(registration.relayKeyId) ? registration.relayKeyId.trim() : "";
  const delegationSigB64 = isNonEmptyString(registration.delegationSigB64) ? registration.delegationSigB64.trim() : "";
  const issuedAtMs = Number(registration.issuedAtMs);
  const expiresAtMs = Number(registration.expiresAtMs);
  if (!inboxId) return fail("missing-inboxId");
  if (!claimantPublicKeyB64) return fail("missing-claimantPublicKeyB64", { inboxId });
  if (!nodeKeyId) return fail("missing-nodeKeyId", { inboxId });
  if (!nodePublicKeyB64) return fail("missing-nodePublicKeyB64", { inboxId });
  if (!relayKeyId) return fail("missing-relayKeyId", { inboxId });
  if (!delegationSigB64) return fail("missing-delegationSigB64", { inboxId });
  if (!Number.isFinite(issuedAtMs)) return fail("invalid-issuedAtMs", { inboxId, issuedAtMs: registration.issuedAtMs });
  if (!Number.isFinite(expiresAtMs)) return fail("invalid-expiresAtMs", { inboxId, expiresAtMs: registration.expiresAtMs });
  if (expiresAtMs <= Date.now()) return fail("expired", { inboxId, expiresAtMs, nowMs: Date.now() });
  if (expiresAtMs <= issuedAtMs) return fail("expires-le-issued", { inboxId, issuedAtMs, expiresAtMs });
  if (auth && nodeKeyId !== auth.nodeKeyId) return fail("nodeKeyId-mismatch-auth", { inboxId, delegationNodeKeyId: nodeKeyId, authNodeKeyId: auth.nodeKeyId });
  if (auth && nodePublicKeyB64 !== auth.nodePublicKeyB64) return fail("nodePublicKeyB64-mismatch-auth", { inboxId });
  if (auth && relayKeyId !== auth.relayKeyId) return fail("relayKeyId-mismatch-auth", { inboxId, delegationRelayKeyId: relayKeyId, authRelayKeyId: auth.relayKeyId });
  let claimantPublicKey;
  let delegationSig;
  try {
    claimantPublicKey = base64ToBytes(claimantPublicKeyB64);
    delegationSig = base64ToBytes(delegationSigB64);
  } catch (decodeErr) {
    return fail("base64-decode-failed", { inboxId, err: decodeErr && decodeErr.message ? decodeErr.message : decodeErr });
  }
  const verified = REGISTRATION_CRYPTO.verify({
    publicKey: claimantPublicKey,
    msg: signedPayloadBytes(claimantNodeDelegationPayload({
      inboxId,
      claimantPublicKeyB64,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      issuedAtMs,
      expiresAtMs,
    })),
    sig: delegationSig,
  });
  if (verified !== true) return fail("signature-verify-failed", { inboxId });
  if (debug) console.log("[INBOX-DEBUG] verifyClaimantNodeDelegation OK", { inboxId, nodeKeyId, relayKeyId });
  return { inboxId, claimantPublicKeyB64, nodeKeyId, nodePublicKeyB64, relayKeyId, issuedAtMs, expiresAtMs };
}

function verifyHostedInboxRegistration(registration, auth) {
  if (!auth) return null;
  const normalized = verifyClaimantNodeDelegation(registration, auth);
  if (!normalized) return null;
  return { inboxId: normalized.inboxId, claimantPublicKeyB64: normalized.claimantPublicKeyB64 };
}
