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
  const opts = { maxRetriesPerRequest: 1, connectTimeout: 2000, commandTimeout: 2000 };
  const publisher = new Redis(url, opts);
  const subscriber = new Redis(url, opts);
  const bus = new LivenessBus({ publisher, subscriber, shardCount, presenceTtlMs });
  let closed = false;
  publisher.on("error", () => console.error("[LivenessBus] publisher connection unavailable"));
  subscriber.on("error", () => console.error("[LivenessBus] subscriber connection unavailable"));
  const onReady = () => {
    if (closed) return;
    bus.start().then(() => {
      if (!closed) bus.resync();
    }).catch(() => console.error("[LivenessBus] subscription unavailable; retrying on reconnect"));
  };
  subscriber.on("ready", onReady);
  return {
    bus,
    async checkReadiness() {
      return subscriber.status === "ready" && (await publisher.ping()) === "PONG";
    },
    async close() {
      closed = true;
      subscriber.removeListener("ready", onReady);
      await bus.close().catch(() => {
        console.error("[LivenessBus] close failed");
      });
      // No queued Redis command is authoritative. Force-close avoids hanging
      // shutdown on a paused/unreachable Redis server.
      publisher.disconnect();
      subscriber.disconnect();
    },
  };
}
