import Redis from "ioredis";
import { LivenessBus } from "./LivenessBus.js";

/**
 * Build a LivenessBus over two real ioredis connections: a SUBSCRIBER (which in
 * subscribe mode cannot issue normal commands) and a PUBLISHER (publish + the
 * presence SET/EXISTS/DEL commands). Returns the bus plus a close() that shuts
 * the bus down and quits both connections. The caller owns start()/lifecycle.
 *
 * @param {{ url: string, shardCount?: number, presenceTtlMs?: number }} opts
 * @returns {{ bus: LivenessBus, checkReadiness: () => Promise<boolean>, close: () => Promise<void> }}
 */
export function createLivenessBus({ url, shardCount = 64, presenceTtlMs = 30000 } = {}) {
  if (typeof url !== "string" || url.trim().length === 0) {
    throw new Error("createLivenessBus requires a redis url");
  }
  // maxRetriesPerRequest bounds how long a command blocks while Redis is down so
  // a deposit/presence write fails fast rather than hanging the delivery path.
  const opts = { maxRetriesPerRequest: 1 };
  const publisher = new Redis(url, opts);
  const subscriber = new Redis(url, opts);
  const bus = new LivenessBus({ publisher, subscriber, shardCount, presenceTtlMs });
  return {
    bus,
    async checkReadiness() {
      return (await publisher.ping()) === "PONG";
    },
    async close() {
      await bus.close().catch((err) => {
        console.error("[LivenessBus] close failed: " + (err && err.message ? err.message : err));
      });
      // quit() flushes then disconnects; on an already-down connection it may
      // reject — log rather than swallow, but never let cleanup throw.
      await publisher.quit().catch((err) => {
        console.error("[LivenessBus] publisher quit failed: " + (err && err.message ? err.message : err));
      });
      await subscriber.quit().catch((err) => {
        console.error("[LivenessBus] subscriber quit failed: " + (err && err.message ? err.message : err));
      });
    },
  };
}
