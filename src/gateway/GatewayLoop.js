import { GatewayRelaySelector } from "./GatewayRelaySelector.js";
import { GatewayPathPlanner } from "./GatewayPathPlanner.js";
import { buildOnionPacketV2 } from "./buildOnionPacketV2.js";
import { GatewaySender } from "./GatewaySender.js";
import { resolveDeliveryDescriptor } from "../network/resolveDeliveryDescriptor.js";
import { NoUsableOnionKeyError, descriptorHasUsableOnionKey, OnionKeyRecordV1 } from "@rezprotocol/core";
import { GossipRouteResolver } from "../routing/GossipRouteResolver.js";
import { RouteOutcomeV1, RouteOutcomeStream, durationBucketFor } from "./RouteOutcome.js";

export class RoutingFailedError extends Error {
  constructor(message, { deliverInboxId, reason } = {}) {
    super(message);
    this.name = "RoutingFailedError";
    this.deliverInboxId = deliverInboxId;
    this.reason = reason;
  }
}

export class GatewayLoop {
  constructor({
    relaySelector,
    pathPlanner,
    sender,
    crypto,
    relayStore = null,
    relayConnectionPool = null,
    routeTable = null,
    inboxRouter = null,
    inboxStore = null,
    isHostedHere = null,
    routePolicy = null,
    outboundQueue = null,
    routeResolver = null,
    routeOutcomes = null,
    nowMs = () => Date.now(),
  } = {}) {
    if (!(relaySelector instanceof GatewayRelaySelector)) {
      throw new Error("GatewayLoop requires relaySelector");
    }
    if (!(pathPlanner instanceof GatewayPathPlanner)) {
      throw new Error("GatewayLoop requires pathPlanner");
    }
    if (!(sender instanceof GatewaySender)) {
      throw new Error("GatewayLoop requires sender");
    }
    if (!crypto) {
      throw new Error("GatewayLoop requires crypto");
    }
    if (isHostedHere !== null && typeof isHostedHere !== "function") {
      throw new Error("GatewayLoop isHostedHere must be a function");
    }
    if (isHostedHere && (!inboxStore || typeof inboxStore.depositFromWire !== "function")) {
      throw new Error("GatewayLoop isHostedHere requires an inboxStore");
    }

    this.relaySelector = relaySelector;
    this.pathPlanner = pathPlanner;
    this.sender = sender;
    this.crypto = crypto;
    this.relayStore = relayStore;
    this.relayConnectionPool = relayConnectionPool ?? null;
    this.routeTable = routeTable || null;
    this.inboxRouter = inboxRouter;
    this.inboxStore = inboxStore;
    this.isHostedHere = isHostedHere;
    this.routePolicy = routePolicy && typeof routePolicy === "object" ? routePolicy : {};
    this.outboundQueue = outboundQueue ?? null;
    this.routeResolver = routeResolver || new GossipRouteResolver();
    // P5.3: private in-process outcome stream — ONION-SEND execution only.
    // The shared-home, direct-cache, and routeDelivery fallback paths below
    // deliberately emit nothing (see RouteOutcome.js scope note).
    this.routeOutcomes = routeOutcomes instanceof RouteOutcomeStream ? routeOutcomes : null;
    this.nowMs = nowMs;
    /** Optional: called when route.failed is received for a packet we sent. */
    this.onRouteFailureCallback = null;
  }

  async sendToInbox({
    innerBytes,
    deliverInboxId,
    receiptInboxId,
    hops,
    minHops,
    maxHops,
    forceOnionRouting = false,
    excludeRelayKeyIds = [],
    ownerPublicKeyB64 = null,
  } = {}) {
    const params = {
      innerBytes,
      deliverInboxId,
      receiptInboxId,
      hops,
      minHops,
      maxHops,
      forceOnionRouting,
      excludeRelayKeyIds,
    };
    try {
      return await this._sendToInboxInternal(params);
    } catch (err) {
      const isRoutingFailure =
        err instanceof RoutingFailedError || err instanceof NoUsableOnionKeyError;
      if (!isRoutingFailure) throw err;

      if (this.outboundQueue && deliverInboxId) {
        if (typeof this.outboundQueue.enqueue === "function") {
          try {
            // DT-005: receiptInboxId is deliberately NOT threaded into the
            // queued entry — receipts are retired, so persisting it would
            // store inert sender-linking metadata at rest.
            await this.outboundQueue.enqueue({
              deliverInboxId,
              innerBytes: params.innerBytes,
              ownerPublicKeyB64: ownerPublicKeyB64 || null,
            });
            err.queued = true;
          } catch (enqErr) {
            console.error("[GW] failed to persist queued message: " + (enqErr && enqErr.message ? enqErr.message : enqErr));
          }
        }
      }
      throw err;
    }
  }

  /**
   * Called when route.failed is received for a packet we sent. Logs and optionally notifies callback.
   */
  recordRouteFailure(packetId, relayKeyId, reason) {
    const gwDebug = process.env.REZ_GW_DEBUG === "1" || process.env.REZ_ROUTE_DEBUG === "1";
    if (gwDebug) console.log("[GW] route.failed received", { packetId, relayKeyId, reason });
    if (this.routeOutcomes) {
      this.routeOutcomes.emit(new RouteOutcomeV1({
        packetId: packetId || null,
        outcomeClass: "route-failed",
        relayKeyIds: typeof relayKeyId === "string" && relayKeyId ? [relayKeyId] : [],
        advisorMode: this.relaySelector.advisorMode || "off",
        durationBucket: "lt100ms",
        reasonClass: typeof reason === "string" ? reason.slice(0, 64) : null,
        atMs: this.nowMs(),
      }));
    }
    if (this.onRouteFailureCallback) this.onRouteFailureCallback({ packetId, relayKeyId, reason });
  }

  async _sendToInboxInternal({
    innerBytes,
    deliverInboxId,
    receiptInboxId,
    hops,
    minHops,
    maxHops,
    forceOnionRouting = false,
    excludeRelayKeyIds = [],
  } = {}) {
    const gwDebug = process.env.REZ_GW_DEBUG === "1";
    const defaultHops = clampHopCount(this.routePolicy && this.routePolicy.defaultHops ? this.routePolicy.defaultHops : undefined, 1);
    const explicitHops = clampHopCount(hops, null);
    const resolvedMinHops = explicitHops != null ? explicitHops : clampHopCount(minHops, defaultHops);
    const resolvedMaxHops = explicitHops != null ? explicitHops : Math.max(
      resolvedMinHops,
      clampHopCount(maxHops, resolvedMinHops),
    );
    const mustUseOnion =
      forceOnionRouting === true || (this.routePolicy && this.routePolicy.forceOnionRouting === true);

    // A Pg-backed cluster is one durable home even when the client entered
    // through another load-balanced process. The shared claim registry is the
    // authority for that fact; a process-local route table is not. Commit to
    // the shared inbox before attempting WAN routing so a non-sticky gateway
    // can accept deposits for claims created through any sibling node.
    if (!mustUseOnion && this.isHostedHere && await this.isHostedHere(deliverInboxId)) {
      if (gwDebug) console.log("[GW-DEBUG] SHARED-HOME durable deposit");
      await this.inboxStore.depositFromWire(deliverInboxId, innerBytes);
      return { plan: null, entryRelayKeyId: null, local: true };
    }

    // --- Route cache: check RouteTable before relay store lookup ---
    if (!mustUseOnion && this.routeTable) {
      const cached = this.routeTable.get(deliverInboxId);
      if (gwDebug) console.log("[GW-DEBUG] getRouteTo", deliverInboxId, cached ? { direct: cached.direct, hasSocket: !!cached.socket, hops: cached.hops } : "no-route");
      if (cached) {
        // Local delivery: deposit directly, skip relay network entirely
        if (cached.direct && !cached.socket && this.inboxStore) {
          if (gwDebug) console.log("[GW-DEBUG] LOCAL deposit (direct, no socket)");
          await this.inboxStore.depositFromWire(deliverInboxId, innerBytes);
          return { plan: null, entryRelayKeyId: null, local: true };
        }
        // Direct socket: target node is connected to this relay via TCP
        if (cached.direct && cached.socket && this.inboxRouter) {
          if (gwDebug) console.log("[GW-DEBUG] DIRECT socket route");
          const routed = await this.inboxRouter.routeDelivery(deliverInboxId, innerBytes);
          if (routed) return { plan: null, entryRelayKeyId: null, local: true };
          if (gwDebug) console.log("[GW-DEBUG] direct socket route FAILED, falling to relay store lookup");
          // Fall through to relay store lookup if routing failed
        }
      }
    } else if (!mustUseOnion) {
      if (gwDebug) console.log("[GW-DEBUG] no routeTable");
    } else {
      if (gwDebug) console.log("[GW-DEBUG] force onion routing enabled; bypassing direct route cache");
    }

    // --- Route resolution via pluggable resolver ---
    if (gwDebug) console.log("[GW-DEBUG] building onion path to deliverInboxId=" + deliverInboxId);
    const routeToTarget = await this.routeResolver.resolve(deliverInboxId, {
      routeTable: this.routeTable,
      relayConnectionPool: this.relayConnectionPool,
    });
    if (!routeToTarget) {
      const msg = "[GW] routing failed: no relay for destination deliverInboxId=" + deliverInboxId + " (no route to target)";
      console.error(msg);
      throw new RoutingFailedError(msg, { deliverInboxId, reason: "no route to target" });
    }
    if (routeToTarget.direct) {
      if (gwDebug) console.log("[GW-DEBUG] route to target: direct (this relay)");
    } else {
      if (gwDebug) {
        console.log(
          "[GW-DEBUG] route to target: deliveryRelayKeyId",
          routeToTarget.deliveryRelayKeyId || routeToTarget.relayKeyId,
          "nextHopRelayKeyId",
          routeToTarget.nextHopRelayKeyId,
          "hops=" + routeToTarget.hops,
        );
      }
    }

    // Get descriptors from relay store (populated by TCP gossip)
    const nowMs = this.nowMs();
    let descriptors = this.relayStore && typeof this.relayStore.listDescriptors === "function"
      ? this.relayStore.listDescriptors({ nowMs })
      : [];
    descriptors = descriptors.filter((d) => descriptorHasUsableOnionKey(d, nowMs));
    if (gwDebug) console.log("[GW-DEBUG] relayStore returned", descriptors.length, "descriptors");

    const deliveryDescriptor = resolveDeliveryDescriptor(routeToTarget, {
      descriptors,
      relayStore: this.relayStore ?? null,
      nowMs: this.nowMs(),
    });

    if (!deliveryDescriptor) {
      // Route exists but no descriptor for onion construction — try relay forwarding
      // via the TCP layer before giving up. This handles cases where the route was
      // learned via gossip but the descriptor hasn't arrived yet.
      if (this.inboxRouter && typeof this.inboxRouter.routeDelivery === "function") {
        const relayRouted = await this.inboxRouter.routeDelivery(deliverInboxId, innerBytes);
        if (relayRouted) {
          if (gwDebug) console.log("[GW-DEBUG] relay-forwarded deposit (no descriptor, TCP route available)");
          return { plan: null, entryRelayKeyId: null, local: false, relayForwarded: true };
        }
      }
      const deliveryRelayKeyId = (routeToTarget && routeToTarget.deliveryRelayKeyId) || (routeToTarget && routeToTarget.relayKeyId) || "";
      const reason = !routeToTarget
        ? "no route to target"
        : routeToTarget.direct
          ? "no self descriptor"
          : deliveryRelayKeyId
            ? "no descriptor for relayKeyId " + deliveryRelayKeyId
            : "route missing delivery relay key id";
      const msg = "[GW] routing failed: no relay for destination deliverInboxId=" + deliverInboxId + " (" + reason + ")";
      console.error(msg);
      throw new RoutingFailedError(msg, { deliverInboxId, reason });
    }

    const intermediateMin = Math.max(0, resolvedMinHops - 1);
    const intermediateMax = Math.max(0, resolvedMaxHops - 1);
    const excludeWithDelivery = [...excludeRelayKeyIds, deliveryDescriptor.relayKeyId].filter(Boolean);
    // P5.2 order of operations: node eligibility → optional advisor ranking
    // (validated permutation, strict deadline, full fallback) → choice. The
    // advisor sees only already-admitted eligible candidates and never the
    // route discovery above.
    const intermediates = await this.relaySelector.selectRanked({
      descriptors,
      minHops: intermediateMin,
      maxHops: intermediateMax,
      excludeRelayKeyIds: excludeWithDelivery,
      nowMs: this.nowMs(),
    });
    const selected = [...intermediates, deliveryDescriptor];
    if (gwDebug) console.log("[GW-DEBUG] selected", selected.length, "relays for path");
    const normalizedSelected = selected.map((d) => normalizeDescriptorOnionKeys(d));
    const plan = this.pathPlanner.plan({ descriptors: normalizedSelected });
    if (gwDebug) {
      console.log("[GW-DEBUG] path:", plan.hops.length, "hops to deliverInboxId=" + deliverInboxId);
      for (let i = 0; i < plan.hops.length; i += 1) {
        const h = plan.hops[i];
        console.log("[GW-DEBUG]   hop " + i + ": relayKeyId=" + (h.relayKeyId || ""));
      }
    }

    const finalHop = plan.hops[plan.hops.length - 1];
    const finalRelayKeyId = finalHop.relayKeyId;
    if (gwDebug) console.log("[GW-DEBUG] final hop: relayKeyId=" + (finalHop.relayKeyId || ""));
    if (gwDebug) {
      if (!routeToTarget) {
        console.log("[GW-DEBUG] final hop can deliver to target inbox: unknown (no route to target)");
      } else if (routeToTarget.direct) {
        console.log("[GW-DEBUG] final hop can deliver to target inbox: unknown (target is direct on this relay)");
      } else {
        console.log("[GW-DEBUG] final hop can deliver to target inbox: yes (final is delivery relay)");
      }
    }

    // DT-005 hard retirement: no receipt metadata is emitted. The relay-side
    // receipt path is deleted, so building and transmitting a SURB-style
    // return path (or embedding receiptInboxId in the final hop layer) was
    // dead weight on every onion packet. `receiptInboxId` is still ACCEPTED
    // by the public send APIs for compatibility, but it is inert here; the
    // relay parser tolerates packets from older senders that still carry the
    // fields. See rez-core/docs/RECEIPTS_AND_DELIVERY_STATES.md.
    const built = await buildOnionPacketV2({
      crypto: this.crypto,
      innerBytes,
      deliverInboxId,
      pathEntries: plan.pathEntries.map((entry, idx) => ({
        ...entry,
        onionKeyId: plan.hops[idx].onionKeyId,
        onionPubKeyBytes: normalizedSelected[idx].onionKeys.find((k) => k.onionKeyId === plan.hops[idx].onionKeyId)
          ? normalizedSelected[idx].onionKeys.find((k) => k.onionKeyId === plan.hops[idx].onionKeyId).publicKeyBytes
          : undefined,
      })),
      finalRelayKeyId,
      nowMs: this.nowMs(),
    });

    const packetId = built.envelope && built.envelope.header ? built.envelope.header.id : null;
    const entryRelayKeyId = plan.hops[0].relayKeyId;

    const sendResult = { plan, entryRelayKeyId };
    const routeDebug = process.env.REZ_ROUTE_DEBUG === "1";
    if (routeDebug || gwDebug) {
      console.log("[GW-DEBUG] sending onion packet", { packetId, entryRelayKeyId, deliverInboxId });
    }

    const sendStartedAtMs = this.nowMs();
    try {
      await this.sender.sendOnionPacket({ entryRelayKeyId, packetBytes: built.packetBytes });
    } catch (err) {
      if (this.routeOutcomes) {
        // A pool send failure at the entry hop is a disconnect-class local
        // outcome; nothing here is delivery truth.
        this.routeOutcomes.emit(new RouteOutcomeV1({
          packetId,
          outcomeClass: "send-disconnected",
          relayKeyIds: plan.hops.map((h) => h.relayKeyId).filter(Boolean),
          advisorMode: this.relaySelector.advisorMode || "off",
          durationBucket: durationBucketFor(this.nowMs() - sendStartedAtMs),
          reasonClass: "entry-send-failed",
          atMs: this.nowMs(),
        }));
      }
      throw err;
    }
    if (this.routeOutcomes) {
      // Entry socket accepted the write — explicitly NOT delivery proof.
      this.routeOutcomes.emit(new RouteOutcomeV1({
        packetId,
        outcomeClass: "entry-send-accepted",
        relayKeyIds: plan.hops.map((h) => h.relayKeyId).filter(Boolean),
        advisorMode: this.relaySelector.advisorMode || "off",
        durationBucket: durationBucketFor(this.nowMs() - sendStartedAtMs),
        reasonClass: null,
        atMs: this.nowMs(),
      }));
    }

    return sendResult;
  }
}

function clampHopCount(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(3, Math.floor(parsed)));
}

/**
 * Returns a descriptor with onionKeys normalized to OnionKeyRecordV1 instances.
 * Plain objects (e.g. from JSON/store) are hydrated via OnionKeyRecordV1.fromJSON.
 * Used so path planning and packet building receive strict types from rez-core.
 */
function normalizeDescriptorOnionKeys(descriptor) {
  if (!descriptor || typeof descriptor !== "object") return descriptor;
  const keys = Array.isArray(descriptor.onionKeys) ? descriptor.onionKeys : [];
  const hydrated = keys.map((k) =>
    k instanceof OnionKeyRecordV1 ? k : OnionKeyRecordV1.fromJSON(k),
  );
  return { ...descriptor, onionKeys: hydrated };
}
