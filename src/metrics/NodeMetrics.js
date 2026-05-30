import { EventEmitter } from "node:events";

const WINDOW_SECONDS = 60;

function int(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function nonNegative(value) {
  return Math.max(0, int(value, 0));
}

export class NodeMetrics extends EventEmitter {
  constructor({ now = () => Date.now() } = {}) {
    super();
    this._now = now;
    this.startTimeMs = nonNegative(this._now());

    this._counters = {
      packetsRoutedTotal: 0,
      packetsReceivedTotal: 0,
      packetsSentTotal: 0,
      bytesInTotal: 0,
      bytesOutTotal: 0,
      inboxDepositsTotal: 0,
      storeReadsTotal: 0,
      storeWritesTotal: 0,
      errorsTotal: 0,
      creditsIssuedTotal: 0,
      creditsConsumedTotal: 0,
      attestationsIssuedTotal: 0,
      attestationsReceivedTotal: 0,
    };

    this._gauges = {
      activeConnections: 0,
      activePeers: 0,
      wsClients: 0,
      memoryRssBytes: 0,
      activeAttestationPeers: 0,
    };

    this._rateBuckets = new Map(); // second -> { packets, bytes }
  }

  increment(name, delta = 1) {
    const d = nonNegative(delta);
    if (!Object.prototype.hasOwnProperty.call(this._counters, name)) {
      throw new Error(`Unknown counter: ${name}`);
    }
    this._counters[name] += d;
    this._emitCounter(name, d);
  }

  addTraffic({ packets = 0, bytes = 0 } = {}) {
    const p = nonNegative(packets);
    const b = nonNegative(bytes);
    if (p <= 0 && b <= 0) return;

    const sec = Math.floor(this._now() / 1000);
    const existing = this._rateBuckets.get(sec) || { packets: 0, bytes: 0 };
    existing.packets += p;
    existing.bytes += b;
    this._rateBuckets.set(sec, existing);
    this._pruneRates(sec);
  }

  setGauge(name, value) {
    if (!Object.prototype.hasOwnProperty.call(this._gauges, name)) {
      throw new Error(`Unknown gauge: ${name}`);
    }
    this._gauges[name] = nonNegative(value);
    this.emit("event", {
      type: "event",
      name: `gauge.${name}`,
      atMs: this._now(),
      data: { value: this._gauges[name] },
    });
  }

  snapshot() {
    const nowMs = nonNegative(this._now());
    const sec = Math.floor(nowMs / 1000);
    this._pruneRates(sec);
    const rss = process.memoryUsage?.().rss;
    if (Number.isFinite(rss)) {
      this._gauges.memoryRssBytes = nonNegative(rss);
    }

    let packetsPerMin = 0;
    let bytesPerMin = 0;
    for (const bucket of this._rateBuckets.values()) {
      packetsPerMin += bucket.packets;
      bytesPerMin += bucket.bytes;
    }

    return {
      ...this._counters,
      ...this._gauges,
      startTimeMs: this.startTimeMs,
      uptimeMs: Math.max(0, nowMs - this.startTimeMs),
      packetsPerMin,
      bytesPerMin,
    };
  }

  _emitCounter(name, delta) {
    this.emit("event", {
      type: "event",
      name: `counter.${name}`,
      atMs: this._now(),
      data: { delta, total: this._counters[name] },
    });
  }

  _pruneRates(currentSec) {
    const minSec = currentSec - WINDOW_SECONDS + 1;
    for (const sec of this._rateBuckets.keys()) {
      if (sec < minSec) this._rateBuckets.delete(sec);
    }
  }
}
