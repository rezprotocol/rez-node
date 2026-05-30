import { RouteAnnouncer } from "./RouteAnnouncer.js";

/**
 * Gossip-based route announcer. Broadcasts route changes to all connected
 * relay peers via TCP control messages. This is the default announcement
 * strategy used by InboxRouter.
 */
export class GossipRouteAnnouncer extends RouteAnnouncer {
  /**
   * Announce new routes to all peers.
   */
  announceRoutes(inboxIds, hops, ctx) {
    const entries = this.#buildRouteEntries(inboxIds, hops, ctx);
    if (entries.length === 0) return;
    const ctlBytes = ctx.encodeCtl({ _ctl: "inbox.route", entries });
    for (const peerSocket of ctx.peerSockets) {
      ctx.trySendFrame(peerSocket, ctlBytes);
    }
  }

  /**
   * Announce route entries to all peers except the given socket.
   */
  announceRoutesExcept(excludeSocket, entries, ctx) {
    if (entries.length === 0) return;
    const ctlBytes = ctx.encodeCtl({ _ctl: "inbox.route", entries });
    for (const peerSocket of ctx.peerSockets) {
      if (peerSocket === excludeSocket) continue;
      ctx.trySendFrame(peerSocket, ctlBytes);
    }
  }

  /**
   * Announce withdrawal of routes to all peers.
   */
  announceWithdraw(inboxIds, ctx) {
    const ctlBytes = ctx.encodeCtl({ _ctl: "inbox.withdraw", inboxIds });
    for (const peerSocket of ctx.peerSockets) {
      ctx.trySendFrame(peerSocket, ctlBytes);
    }
  }

  /**
   * Send all known routes to a single peer.
   */
  announceAllToPeer(peerSocket, ctx) {
    const entries = [];
    for (const [id, entry] of ctx.routeTable.getAll()) {
      const announcedEntry = ctx.createAnnouncedRouteEntry(id, entry, entry.hops + 1);
      if (announcedEntry) entries.push(announcedEntry);
    }
    if (entries.length === 0) return;
    const ctlBytes = ctx.encodeCtl({ _ctl: "inbox.route", entries });
    ctx.trySendFrame(peerSocket, ctlBytes);
  }

  /**
   * Re-announce the full route table to all peers (periodic anti-entropy).
   */
  reannounceAll(ctx) {
    for (const peerSocket of ctx.peerSockets) {
      this.announceAllToPeer(peerSocket, ctx);
    }
  }

  #buildRouteEntries(inboxIds, hops, ctx) {
    const entries = [];
    for (const id of inboxIds) {
      const route = ctx.routeTable.get(id);
      const entry = ctx.createAnnouncedRouteEntry(id, route, hops);
      if (entry) entries.push(entry);
    }
    return entries;
  }
}
