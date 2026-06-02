import { DhtNodeId } from "./DhtNodeId.js";
import { verifyClaimantNodeDelegation } from "../../relay/InboxRouter.js";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";
import { DhtQueryWaiter } from "./DhtQueryWaiter.js";
import { peerRateLimitKey, peerRateLimitIpKey } from "./peerRateLimitKeys.js";

const CTL_FIND_NODE = "dht.find_node";
const CTL_FIND_NODE_REPLY = "dht.find_node.reply";
const CTL_FIND_VALUE = "dht.find_value";
const CTL_FIND_VALUE_REPLY = "dht.find_value.reply";
const CTL_STORE = "dht.store";

/**
 * Validate a `dht.store` `routeEntry` against the receiving relay's trust
 * model. Closes docs/SECURITY_AUDIT.md HIGH-8 — without this check any
 * peer relay could pollute the DHT with arbitrary `inboxId → delivery`
 * mappings.
 *
 * A valid stored entry MUST carry a `registration` record that is:
 *   1. A claimant-signed inbox-node delegation (verifyClaimantNodeDelegation
 *      → one Ed25519 verify against the embedded claimant pubkey).
 *   2. Bound to the same `inboxId` the store is keyed under.
 *   3. Bound to the `deliveryRelayKeyId` claimed in the route entry — so a
 *      hostile peer can't take a legit delegation for `R_real` and rewrap
 *      it as a route delivering through `R_evil`. The claimant signs
 *      `relayKeyId` into the delegation alongside `nodeKeyId`, so this
 *      binding is verifiable end-to-end without trusting any peer's auth
 *      context.
 *
 * `auth` is intentionally null here: the storing peer is NOT required to
 * be the delegated node. Kademlia replicates across k peers regardless of
 * which one is closest. The trust root is the claimant's signature on the
 * delegation, which is self-authenticating to any verifier.
 *
 * @returns {object|null} the validated registration's normalized fields, or null
 */
function validateStoredRouteEntry(inboxId, routeEntry) {
  if (!routeEntry || typeof routeEntry !== "object") return null;
  const registration = routeEntry.registration;
  if (!registration || typeof registration !== "object") return null;
  const normalized = verifyClaimantNodeDelegation(registration);
  if (!normalized) return null;
  if (normalized.inboxId !== inboxId) return null;
  const deliveryRelayKeyId = typeof routeEntry.deliveryRelayKeyId === "string"
    ? routeEntry.deliveryRelayKeyId.trim()
    : "";
  if (!deliveryRelayKeyId) return null;
  // The routeEntry says "deliver via relay X"; the delegation says
  // "claimant authorized relay R for this inbox". Require X === R.
  if (deliveryRelayKeyId !== normalized.relayKeyId) return null;
  return normalized;
}

export { validateStoredRouteEntry };

/**
 * DHT protocol layer. Registers control message handlers on the
 * ControlMessageRegistry and provides query primitives used by
 * DhtLookup's sendQuery callbacks.
 */
export class DhtProtocol {
  /** @type {import("./KBucketTable.js").KBucketTable} */
  #kBuckets;

  /** @type {import("./DhtValueStore.js").DhtValueStore} */
  #valueStore;

  /** @type {import("../ControlMessageRegistry.js").ControlMessageRegistry} */
  #registry;

  /** @type {DhtNodeId} */
  #selfNodeId;

  /** @type {string} */
  #selfRelayKeyId;

  /** @type {(obj: object) => Uint8Array} */
  #encodeCtl;

  /** @type {(socket: object, bytes: Uint8Array) => void} */
  #trySendFrame;

  /** @type {DhtQueryWaiter} */
  #queryWaiter;

  /** @type {number} */
  #k;

  /** @type {() => number} */
  #nowMs;

  /** @type {SlidingWindowRateLimiter} */
  #storeRateLimiter;

  /** @type {SlidingWindowRateLimiter} per-IP outer cap (SECURITY_AUDIT MED-13) */
  #storeIpRateLimiter;

  /** @type {(socket: object) => string|null} */
  #getPeerKey;

  /** @type {(socket: object) => string|null} */
  #getPeerIp;

  /**
   * @param {object} options
   * @param {import("./KBucketTable.js").KBucketTable} options.kBuckets
   * @param {import("./DhtValueStore.js").DhtValueStore} options.valueStore
   * @param {import("../ControlMessageRegistry.js").ControlMessageRegistry} options.registry
   * @param {DhtNodeId} options.selfNodeId
   * @param {string} options.selfRelayKeyId
   * @param {(obj: object) => Uint8Array} options.encodeCtl
   * @param {(socket: object, bytes: Uint8Array) => void} options.trySendFrame
   * @param {number} [options.queryTimeoutMs]
   * @param {number} [options.k]
   * @param {() => number} [options.nowMs]
   * @param {SlidingWindowRateLimiter} [options.storeRateLimiter] LOW-6: defaults to a fresh instance
   * @param {(socket: object) => string|null} [options.getPeerKey] LOW-6: socket -> rate-limit key (relayKeyId). If null, the socket itself keys the bucket.
   * @param {SlidingWindowRateLimiter} [options.storeIpRateLimiter] MED-13: outer per-IP cap. Catches sybil keypair-rotation behind a single source IP. Defaults to 5000/min.
   * @param {(socket: object) => string|null} [options.getPeerIp] MED-13: socket -> IP-prefix key (/64 for IPv6). If null, the per-IP gate is skipped.
   */
  constructor({ kBuckets, valueStore, registry, selfNodeId, selfRelayKeyId, encodeCtl, trySendFrame, queryTimeoutMs = 3000, k = 20, nowMs = () => Date.now(), storeRateLimiter = null, getPeerKey = null, storeIpRateLimiter = null, getPeerIp = null }) {
    if (!kBuckets) throw new Error("DhtProtocol requires kBuckets");
    if (!valueStore) throw new Error("DhtProtocol requires valueStore");
    if (!registry) throw new Error("DhtProtocol requires registry");
    if (!(selfNodeId instanceof DhtNodeId)) throw new Error("DhtProtocol requires selfNodeId");
    if (typeof selfRelayKeyId !== "string" || !selfRelayKeyId.trim()) throw new Error("DhtProtocol requires selfRelayKeyId");
    if (typeof encodeCtl !== "function") throw new Error("DhtProtocol requires encodeCtl function");
    if (typeof trySendFrame !== "function") throw new Error("DhtProtocol requires trySendFrame function");

    this.#kBuckets = kBuckets;
    this.#valueStore = valueStore;
    this.#registry = registry;
    this.#selfNodeId = selfNodeId;
    this.#selfRelayKeyId = selfRelayKeyId;
    this.#encodeCtl = encodeCtl;
    this.#trySendFrame = trySendFrame;
    this.#queryWaiter = new DhtQueryWaiter({ queryTimeoutMs, idPrefix: "dht-q" });
    this.#k = k;
    this.#nowMs = nowMs;
    this.#storeRateLimiter = storeRateLimiter || new SlidingWindowRateLimiter();
    this.#getPeerKey = typeof getPeerKey === "function" ? getPeerKey : null;
    // SECURITY_AUDIT MED-13: outer per-IP gate above the per-relayKeyId
    // limiter. relayKeyIds are free to generate (Ed25519 keygen ~0.1ms);
    // without an IP-level cap an attacker behind one IP can run N parallel
    // relayKeyIds and multiply their effective dht.store budget by N. The
    // /64-aggregated IPv6 key (peerIpKey) prevents lower-64-bit rotation
    // from re-leaking the bypass.
    this.#storeIpRateLimiter = storeIpRateLimiter
      || new SlidingWindowRateLimiter({ windowMs: 60_000, maxAttempts: 5000 });
    this.#getPeerIp = typeof getPeerIp === "function" ? getPeerIp : null;
  }

  /**
   * Register all DHT control message handlers.
   */
  install() {
    this.#registry.register(CTL_FIND_NODE, (ctlObj, socket) => this.#handleFindNode(ctlObj, socket));
    this.#registry.register(CTL_FIND_NODE_REPLY, (ctlObj, socket) => this.#handleFindNodeReply(ctlObj, socket));
    this.#registry.register(CTL_FIND_VALUE, (ctlObj, socket) => this.#handleFindValue(ctlObj, socket));
    this.#registry.register(CTL_FIND_VALUE_REPLY, (ctlObj, socket) => this.#handleFindValueReply(ctlObj, socket));
    this.#registry.register(CTL_STORE, (ctlObj, socket) => this.#handleStore(ctlObj, socket));
  }

  /**
   * Unregister all DHT control message handlers.
   */
  uninstall() {
    this.#registry.unregister(CTL_FIND_NODE);
    this.#registry.unregister(CTL_FIND_NODE_REPLY);
    this.#registry.unregister(CTL_FIND_VALUE);
    this.#registry.unregister(CTL_FIND_VALUE_REPLY);
    this.#registry.unregister(CTL_STORE);
    this.#queryWaiter.clear();
  }

  // ---------------------------------------------------------------------------
  // Query primitives (used by DhtLookup's sendQuery callback)
  // ---------------------------------------------------------------------------

  /**
   * Send dht.find_node to a peer and wait for reply.
   * @param {object} socket
   * @param {DhtNodeId} targetId
   * @returns {Promise<{ nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>}
   */
  queryFindNode(socket, targetId) {
    const queryId = this.#queryWaiter.newQueryId();
    const bytes = this.#encodeCtl({
      _ctl: CTL_FIND_NODE,
      queryId,
      targetIdHex: targetId.hex,
    });
    this.#trySendFrame(socket, bytes);
    return this.#queryWaiter.wait(queryId, socket);
  }

  /**
   * Send dht.find_value to a peer and wait for reply.
   * @param {object} socket
   * @param {DhtNodeId} targetId
   * @param {string} inboxId - original key (value store is keyed by inboxId, not DHT hash)
   * @returns {Promise<{ value: object|null, nodes: Array<{ nodeIdHex: string, relayKeyId: string }> }>}
   */
  queryFindValue(socket, targetId, inboxId) {
    const queryId = this.#queryWaiter.newQueryId();
    const bytes = this.#encodeCtl({
      _ctl: CTL_FIND_VALUE,
      queryId,
      targetIdHex: targetId.hex,
      inboxId: inboxId || "",
    });
    this.#trySendFrame(socket, bytes);
    return this.#queryWaiter.wait(queryId, socket);
  }

  /**
   * Send dht.store to a peer (fire-and-forget).
   * @param {object} socket
   * @param {string} inboxId
   * @param {object} routeEntry
   */
  sendStore(socket, inboxId, routeEntry) {
    const bytes = this.#encodeCtl({
      _ctl: CTL_STORE,
      inboxId,
      routeEntry,
    });
    this.#trySendFrame(socket, bytes);
  }

  // ---------------------------------------------------------------------------
  // Incoming message handlers
  // ---------------------------------------------------------------------------

  #handleFindNode(ctlObj, socket) {
    const targetIdHex = typeof ctlObj.targetIdHex === "string" ? ctlObj.targetIdHex : "";
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    if (!targetIdHex || targetIdHex.length !== 64 || !queryId) return;

    let targetId;
    try {
      targetId = DhtNodeId.fromHex(targetIdHex);
    } catch (err) {
      console.warn("[DHT] dht.find_node: invalid targetIdHex:", err && err.message ? err.message : err);
      return;
    }

    const closest = this.#kBuckets.findClosest(targetId, this.#k);
    const nodes = closest.map(function (entry) {
      return { nodeIdHex: entry.nodeId.hex, relayKeyId: entry.relayKeyId };
    });

    const replyBytes = this.#encodeCtl({
      _ctl: CTL_FIND_NODE_REPLY,
      queryId,
      nodes,
    });
    this.#trySendFrame(socket, replyBytes);
  }

  #handleFindNodeReply(ctlObj, socket) {
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    const nodes = Array.isArray(ctlObj.nodes) ? ctlObj.nodes : [];
    // HIGH-9: the reply MUST arrive on the same socket the query was sent
    // to. Without this, any peer relay observing the network (or sitting
    // on an iterative-lookup path) could race-forge a reply.
    if (this.#queryWaiter.resolve(queryId, socket, { value: null, nodes }) === "socket-mismatch") {
      console.warn("[DHT] dht.find_node.reply: dropped reply on mismatched socket");
    }
  }

  #handleFindValue(ctlObj, socket) {
    const targetIdHex = typeof ctlObj.targetIdHex === "string" ? ctlObj.targetIdHex : "";
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    if (!targetIdHex || targetIdHex.length !== 64 || !queryId) return;

    // The inboxId IS the key we're looking up. For DHT, the targetIdHex
    // is derived from the inboxId, but we need the original inboxId to
    // look up the value store. The caller includes it in the message.
    const inboxId = typeof ctlObj.inboxId === "string" ? ctlObj.inboxId : "";

    // Check local value store first. Re-validate the entry before
    // returning it — HIGH-8 defense-in-depth in case any code path
    // bypassed the inbound store check.
    if (inboxId) {
      const value = this.#valueStore.get(inboxId, this.#nowMs());
      if (value && validateStoredRouteEntry(inboxId, value)) {
        const replyBytes = this.#encodeCtl({
          _ctl: CTL_FIND_VALUE_REPLY,
          queryId,
          value,
          nodes: [],
        });
        this.#trySendFrame(socket, replyBytes);
        return;
      }
      if (value) {
        // Evict the bad entry so we don't keep serving it.
        this.#valueStore.remove(inboxId);
      }
    }

    // No value — respond with k-closest nodes
    let targetId;
    try {
      targetId = DhtNodeId.fromHex(targetIdHex);
    } catch (err) {
      console.warn("[DHT] dht.find_value: invalid targetIdHex:", err && err.message ? err.message : err);
      return;
    }

    const closest = this.#kBuckets.findClosest(targetId, this.#k);
    const nodes = closest.map(function (entry) {
      return { nodeIdHex: entry.nodeId.hex, relayKeyId: entry.relayKeyId };
    });

    const replyBytes = this.#encodeCtl({
      _ctl: CTL_FIND_VALUE_REPLY,
      queryId,
      value: null,
      nodes,
    });
    this.#trySendFrame(socket, replyBytes);
  }

  #handleFindValueReply(ctlObj, socket) {
    const queryId = typeof ctlObj.queryId === "string" ? ctlObj.queryId : "";
    const value = ctlObj.value && typeof ctlObj.value === "object" ? ctlObj.value : null;
    const nodes = Array.isArray(ctlObj.nodes) ? ctlObj.nodes : [];
    // HIGH-9: the reply MUST arrive on the same socket the query was sent to.
    if (this.#queryWaiter.resolve(queryId, socket, { value, nodes }) === "socket-mismatch") {
      console.warn("[DHT] dht.find_value.reply: dropped reply on mismatched socket");
    }
  }

  #handleStore(ctlObj, socket) {
    const inboxId = typeof ctlObj.inboxId === "string" ? ctlObj.inboxId.trim() : "";
    if (!inboxId) return;

    // LOW-6: per-peer rate limit. Even with HIGH-8 (every stored entry is
    // a genuine claimant-signed delegation), an attacker can replay
    // legitimate delegations they have observed off the wire. Without a
    // cap they consume up to 24h-TTL'd local storage. Drop the store
    // silently when the peer exceeds its sliding-window budget; their
    // legitimate stores in the same window are also dropped, which is
    // the correct behavior — they're already past their fair share.
    const peerKey = peerRateLimitKey(socket, this.#getPeerKey);
    if (!this.#storeRateLimiter.record(peerKey, this.#nowMs())) {
      console.warn("[DHT] dht.store: rejected entry for " + inboxId
        + " — peer rate limit exceeded (peerKey=" + peerKey + ")");
      return;
    }
    // MED-13: outer per-IP cap closes the sybil-bypass that the
    // relayKeyId-keyed limiter alone leaves open. Empty IP skips the gate.
    const ipKey = peerRateLimitIpKey(socket, this.#getPeerIp);
    if (ipKey && !this.#storeIpRateLimiter.record(ipKey, this.#nowMs())) {
      console.warn("[DHT] dht.store: rejected entry for " + inboxId
        + " — per-IP rate limit exceeded (ipKey=" + ipKey + ")");
      return;
    }

    // Tombstones (null routeEntry) carry no proof of who is allowed to
    // withdraw. Until a withdrawal-proof schema exists, drop them rather
    // than let an arbitrary peer evict any inbox's route. The hosting
    // relay's anti-entropy republish (DhtRouteAnnouncer.reannounceAll,
    // hourly default) regenerates stale entries.
    if (ctlObj.routeEntry === null) {
      console.warn("[DHT] dht.store: rejected tombstone for " + inboxId
        + " — withdraw-proof not yet supported");
      return;
    }
    const routeEntry = ctlObj.routeEntry && typeof ctlObj.routeEntry === "object"
      ? ctlObj.routeEntry
      : null;
    if (!routeEntry) return;

    // HIGH-8: a route entry must carry a claimant-signed delegation that
    // names this inboxId and the same deliveryRelayKeyId the entry
    // advertises. Without this anchor any peer could pollute the DHT
    // with arbitrary inboxId → delivery mappings.
    if (!validateStoredRouteEntry(inboxId, routeEntry)) {
      console.warn("[DHT] dht.store: rejected entry for " + inboxId
        + " — missing/invalid claimant delegation");
      return;
    }

    const result = this.#valueStore.store(inboxId, routeEntry, this.#nowMs());
    if (!result.stored && result.reason === "older-delegation") {
      // Delegation-replay-overwrite (docs/SECURITY_AUDIT.md pass 3
      // observations): a peer tried to replay a still-valid older
      // claimant-signed delegation on top of a newer one. The valueStore
      // rejected it. Log so unexpected drops are visible in friendlies
      // tests.
      console.warn("[DHT] dht.store: rejected entry for " + inboxId
        + " — older delegation than current entry");
    }
  }

}
