/**
 * Stable string hash (djb2) for sharding inbox channels.
 */
function djb2(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i += 1) {
    h = ((h << 5) + h + str.charCodeAt(i)) >>> 0;
  }
  return h >>> 0;
}

/**
 * LivenessBus — cluster-wide deposit notification over Redis pub/sub.
 *
 * A deposit persists to the durable Pg log first (system of record), then the
 * depositing node PUBLISHes a liveness ping; the node holding the owner's live
 * socket receives it and drains from the log. This is the only cross-node signal
 * — inbox ITEMS never traverse Redis, only "inbox X has new mail up to seq N".
 *
 * Channels are SHARDED (default 64) rather than one-per-inbox: each node
 * subscribes to all shard channels once and filters to inboxes it serves
 * locally. This bounds Redis channel cardinality regardless of inbox count
 * (the red-team channel-explosion concern); the trade-off is every node sees
 * every ping (filtered cheaply in-process). Presence keys carry a TTL so a dead
 * node's presence self-expires.
 *
 * Needs TWO ioredis connections: a subscriber (in subscribe mode, can't issue
 * normal commands) and a publisher.
 */
export class LivenessBus {
  #pub;
  #sub;
  #prefix;
  #shardCount;
  #handlers;
  #started;
  #presenceTtlMs;
  #onMessage;

  /**
   * @param {{ publisher: object, subscriber: object, channelPrefix?: string,
   *           shardCount?: number, presenceTtlMs?: number }} opts
   */
  constructor({ publisher, subscriber, channelPrefix = "rez", shardCount = 64, presenceTtlMs = 30000 } = {}) {
    if (!publisher || typeof publisher.publish !== "function") {
      throw new Error("LivenessBus requires an ioredis publisher");
    }
    if (!subscriber || typeof subscriber.subscribe !== "function") {
      throw new Error("LivenessBus requires an ioredis subscriber");
    }
    this.#pub = publisher;
    this.#sub = subscriber;
    this.#prefix = channelPrefix;
    this.#shardCount = Math.max(1, Number(shardCount) || 64);
    this.#presenceTtlMs = Math.max(1000, Number(presenceTtlMs) || 30000);
    /** @type {Map<string, Set<Function>>} inboxId -> handlers (live sockets on this node) */
    this.#handlers = new Map();
    this.#started = false;
    // One stable bound listener so close() can remove the exact reference it
    // added — otherwise a close()/start() cycle on a reused bus would stack a
    // second "message" listener and double-dispatch every ping.
    this.#onMessage = (_channel, message) => {
      this.#dispatch(message);
    };
  }

  #shardChannel(shard) {
    return `${this.#prefix}:dep:${shard}`;
  }

  #channelForInbox(inboxId) {
    return this.#shardChannel(djb2(String(inboxId)) % this.#shardCount);
  }

  #presenceKey(inboxId) {
    return `${this.#prefix}:presence:${inboxId}`;
  }

  /** Subscribe to all shard channels and wire the dispatch handler. Idempotent. */
  async start() {
    if (this.#started) {
      return;
    }
    this.#sub.on("message", this.#onMessage);
    const channels = [];
    for (let s = 0; s < this.#shardCount; s += 1) {
      channels.push(this.#shardChannel(s));
    }
    try {
      await this.#sub.subscribe(...channels);
    } catch (err) {
      // Don't leave the listener attached on a failed start — a retry would
      // stack a second one and double-dispatch every ping once Redis recovers.
      this.#sub.removeListener("message", this.#onMessage);
      throw err;
    }
    this.#started = true;
  }

  #dispatch(message) {
    let payload;
    try {
      payload = JSON.parse(message);
    } catch (err) {
      // A malformed ping is a bug/abuse on the bus, not silently ignorable.
      console.error("[LivenessBus] dropping unparseable ping: " + (err && err.message ? err.message : err));
      return;
    }
    const inboxId = payload && typeof payload.inboxId === "string" ? payload.inboxId : null;
    if (!inboxId) {
      return;
    }
    const handlers = this.#handlers.get(inboxId);
    if (!handlers || handlers.size === 0) {
      return; // not served by this node
    }
    for (const handler of handlers) {
      Promise.resolve()
        .then(() => handler(payload))
        .catch((err) => {
          console.error("[LivenessBus] inbox handler failed for " + inboxId + ": "
            + (err && err.message ? err.message : err));
        });
    }
  }

  /**
   * Register interest in an inbox (this node holds its live socket). The handler
   * fires for every cross-node deposit ping for that inbox.
   * @returns {() => void} unregister
   */
  registerInbox(inboxId, handler) {
    if (typeof handler !== "function") {
      throw new Error("LivenessBus.registerInbox requires a handler");
    }
    const id = String(inboxId);
    let set = this.#handlers.get(id);
    if (!set) {
      set = new Set();
      this.#handlers.set(id, set);
    }
    set.add(handler);
    return () => this.unregisterInbox(id, handler);
  }

  unregisterInbox(inboxId, handler) {
    const id = String(inboxId);
    const set = this.#handlers.get(id);
    if (!set) {
      return;
    }
    set.delete(handler);
    if (set.size === 0) {
      this.#handlers.delete(id);
    }
  }

  /** Publish a deposit ping. Carries seq so a receiver can drain from-cursor. */
  async publishDeposit(inboxId, { seq = null, dedupeKey = null } = {}) {
    const payload = JSON.stringify({ inboxId: String(inboxId), seq, dedupeKey });
    await this.#pub.publish(this.#channelForInbox(inboxId), payload);
  }

  /** Mark that this node holds a live socket for an inbox (TTL-bounded). */
  async setPresence(inboxId, nodeId) {
    await this.#pub.set(this.#presenceKey(inboxId), String(nodeId), "PX", this.#presenceTtlMs);
  }

  async clearPresence(inboxId) {
    await this.#pub.del(this.#presenceKey(inboxId));
  }

  /** Is any node currently holding a live socket for this inbox? */
  async isPresent(inboxId) {
    const v = await this.#pub.exists(this.#presenceKey(inboxId));
    return v === 1;
  }

  async close() {
    this.#handlers.clear();
    if (typeof this.#sub.removeListener === "function") {
      this.#sub.removeListener("message", this.#onMessage);
    }
    if (this.#started && typeof this.#sub.unsubscribe === "function") {
      await this.#sub.unsubscribe();
    }
    this.#started = false;
  }
}
