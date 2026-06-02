import { RelayStore } from "../network/RelayStore.js";
import { descriptorHasUsableOnionKey } from "@rezprotocol/core";

const DEFAULT_DISCOVERY_INTERVAL_MS = 30_000;
const DEFAULT_STARTUP_RETRY_INTERVAL_MS = 1_000;
const DEFAULT_STARTUP_RETRY_WINDOW_MS = 15_000;

export class MeshCoordinator {
  constructor({
    relayStore,
    relayConnectionPool = null,
    inboxRouter = null,
    metrics = null,
    logger = console,
    nowMs = () => Date.now(),
    meshConfig,
    setTimer = setTimeout,
    clearTimer = clearTimeout,
    setIntervalFn = setInterval,
    clearIntervalFn = clearInterval,
  } = {}) {
    if (!(relayStore instanceof RelayStore)) {
      throw new Error("MeshCoordinator requires RelayStore");
    }
    this.relayStore = relayStore;
    this.relayConnectionPool = relayConnectionPool ?? null;
    this.inboxRouter = inboxRouter ?? null;
    this.descriptorExchange = null;
    this.metrics = metrics;
    this.logger = logger || console;
    this.nowMs = nowMs;
    this.config = normalizeMeshConfig(meshConfig);
    this.setTimer = typeof setTimer === "function" ? setTimer : setTimeout;
    this.clearTimer = typeof clearTimer === "function" ? clearTimer : clearTimeout;
    this.setIntervalFn = typeof setIntervalFn === "function" ? setIntervalFn : setInterval;
    this.clearIntervalFn = typeof clearIntervalFn === "function" ? clearIntervalFn : clearInterval;

    this._started = false;
    this._discoveryTimer = null;
    this._startupRetryTimer = null;
    this._startupRetryStartedAtMs = null;
    this._seedReachable = new Map();
    this._lastDiscoveryAtMs = null;
    this._routeStats = {
      evicted: 0,
    };
    this._statusHandlers = new Set();
    /** @type {((nowMs: number) => void)|null} extra work to run each sync tick */
    this._onSyncTick = null;
  }

  /**
   * Register an extra callback to run on each route-sync tick (the existing
   * 30s churn cadence). Used to drive durable-record re-replication +
   * eviction off the same timer rather than a new one.
   * @param {((nowMs: number) => void)|null} fn
   */
  setOnSyncTick(fn) {
    this._onSyncTick = typeof fn === "function" ? fn : null;
  }

  async start(options = {}) {
    if (this._started) return;
    const skipInitialConnect = options && options.skipInitialConnect === true;
    this._started = true;
    this._startupRetryStartedAtMs = this.nowMs();
    await this.refresh({ skipConnect: skipInitialConnect });
    this._scheduleStartupRetryIfNeeded();
    this._discoveryTimer = this.setIntervalFn(() => {
      this.refresh().catch((err) => {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator refresh failed", err);
        }
      });
    }, this.config.discoveryIntervalMs);
    if (this._discoveryTimer && typeof this._discoveryTimer.unref === "function") {
      this._discoveryTimer.unref();
    }
  }

  async stop() {
    if (!this._started) return;
    this._started = false;
    this._clearStartupRetry();
    this._startupRetryStartedAtMs = null;
    if (this._discoveryTimer) {
      this.clearIntervalFn(this._discoveryTimer);
      this._discoveryTimer = null;
    }
  }

  onStatusChanged(handler) {
    if (typeof handler !== "function") return () => {};
    this._statusHandlers.add(handler);
    return () => {
      this._statusHandlers.delete(handler);
    };
  }

  async refresh(options = {}) {
    const skipConnect = options && options.skipConnect === true;
    // Evict expired, warn about expiring keys, connect to peers
    const nowMs = this.nowMs();
    this._lastDiscoveryAtMs = nowMs;
    const evicted = this.relayStore.evictExpired({ nowMs });
    this._routeStats.evicted += evicted;

    for (const relay of this.relayStore.getAll()) {
      const descriptor = relay && relay.descriptor ? relay.descriptor : null;
      if (!descriptor || !descriptor.onionKeys || !descriptor.onionKeys.length) continue;
      if (descriptorHasUsableOnionKey(descriptor, nowMs)) {
        const maxNotAfter = Math.max(
          ...descriptor.onionKeys.map((k) => Number(k && k.notAfter)).filter(Number.isFinite)
        );
        if (Number.isFinite(maxNotAfter) && maxNotAfter - nowMs <= this.config.discoveryIntervalMs) {
          if (this.logger && typeof this.logger.warn === "function") {
            this.logger.warn("MeshCoordinator descriptor keys approaching expiration", {
              relayKeyId: descriptor.relayKeyId,
              notAfter: maxNotAfter,
              nowMs,
            });
          }
        }
      }
    }

    this.metrics && typeof this.metrics.setGauge === "function" && this.metrics.setGauge("activePeers", this.relayStore.getAll().length);
    if (skipConnect !== true) {
      await this.connectNewPeers();
    }
    await this._syncRouteState();
    this.refreshSeedReachabilityFromStore();
    this.refreshSeedReachabilityFromConnections();
    this._emitStatusChanged();
    this._scheduleStartupRetryIfNeeded();
  }

  /**
   * Connect to any relays in RelayStore that the pool doesn't already have connections to.
   * Called after TCP descriptor exchange or on a timer.
   */
  async connectNewPeers() {
    if (!this.relayConnectionPool || typeof this.relayConnectionPool.connectToKnownRelays !== "function") return;
    const records = this.relayStore.getAll().filter((record) => record && record.source !== "self" && record.bindingTrust !== "tofu");
    await this.relayConnectionPool.connectToKnownRelays(records).catch((err) => {
      if (this.logger && typeof this.logger.warn === "function") {
        this.logger.warn("MeshCoordinator connectNewPeers failed", err && err.message ? err.message : err);
      }
    });
  }

  setDescriptorExchange(exchange) {
    this.descriptorExchange = exchange ?? null;
  }

  /**
   * Scan RelayStore for relay descriptors whose endpoint host matches a configured seed URL.
   * When found, mark that seed as reachable — reflecting that TCP descriptor exchange
   * has provided routing info for this seed (even if HTTP directory queries failed).
   */
  refreshSeedReachabilityFromStore() {
    if (this.config.seeds.length === 0) return;
    for (const relay of this.relayStore.getAll()) {
      if (!relay || !relay.descriptor) continue;
      const endpoints = Array.isArray(relay.descriptor.endpoints) ? relay.descriptor.endpoints : [];
      for (const endpoint of endpoints) {
        const host = typeof endpoint === "object" && endpoint !== null && typeof endpoint.host === "string"
          ? endpoint.host.trim().toLowerCase()
          : "";
        if (!host) continue;
        for (const seed of this.config.seeds) {
          const seedHost = extractHostFromSeedUrl(seed);
          if (seedHost && seedHost === host) {
            this._seedReachable.set(seed, true);
          }
        }
      }
    }
  }

  refreshSeedReachabilityFromConnections() {
    if (this.config.seeds.length === 0) return;
    if (!this.relayConnectionPool || typeof this.relayConnectionPool.listActiveConnectionEndpoints !== "function") {
      return;
    }
    const endpoints = this.relayConnectionPool.listActiveConnectionEndpoints();
    for (const endpoint of endpoints) {
      const host = typeof endpoint === "object" && endpoint !== null && typeof endpoint.host === "string"
        ? endpoint.host.trim().toLowerCase()
        : "";
      if (!host) continue;
      for (const seed of this.config.seeds) {
        const seedHost = extractHostFromSeedUrl(seed);
        if (seedHost && seedHost === host) {
          this._seedReachable.set(seed, true);
        }
      }
    }
  }

  async _syncRouteState() {
    if (this.relayConnectionPool && typeof this.relayConnectionPool.updateInboxIds === "function") {
      await this.relayConnectionPool.updateInboxIds().catch((err) => {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator inbox re-register failed", err && err.message ? err.message : err);
        }
      });
    }
    if (this.inboxRouter && typeof this.inboxRouter.reannounceAllRoutesToPeers === "function") {
      try {
        this.inboxRouter.reannounceAllRoutesToPeers();
      } catch (err) {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator route resync failed", err && err.message ? err.message : err);
        }
      }
    }
    if (this.descriptorExchange && typeof this.descriptorExchange.announceToAllPeers === "function") {
      try {
        this.descriptorExchange.announceToAllPeers();
      } catch (err) {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator descriptor resync failed", err && err.message ? err.message : err);
        }
      }
    }
    if (this._onSyncTick) {
      try {
        this._onSyncTick(this.nowMs());
      } catch (err) {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator sync-tick hook failed", err && err.message ? err.message : err);
        }
      }
    }
  }

  getStatus() {
    const nowMs = this.nowMs();
    const peers = this.relayStore.snapshotPeers({
      nowMs,
      failureThreshold: this.config.policy.failureThreshold,
    });
    const seedReachable = {};
    for (const seed of this.config.seeds) {
      seedReachable[seed] = this._seedReachable.get(seed) === true;
    }
    return {
      enabled: this.config.enabled,
      mode: this.config.mode,
      participateInRouting: this.config.participateInRouting,
      peerCount: peers.length,
      seedReachable,
      lastDiscoveryAtMs: this._lastDiscoveryAtMs,
      routeStats: { ...this._routeStats },
      policy: { ...this.config.policy },
      peers,
    };
  }

  _emitStatusChanged() {
    const status = this.getStatus();
    for (const handler of [...this._statusHandlers]) {
      try {
        handler(status);
      } catch (err) {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator status handler failed", err && err.message ? err.message : err);
        }
      }
    }
  }

  _scheduleStartupRetryIfNeeded() {
    if (this._started !== true) return;
    if (!this._needsStartupRetry()) {
      this._clearStartupRetry();
      return;
    }
    if (this._startupRetryTimer) {
      return;
    }
    const startedAtMs = Number(this._startupRetryStartedAtMs);
    const nowMs = this.nowMs();
    if (!Number.isFinite(startedAtMs)) {
      this._startupRetryStartedAtMs = nowMs;
    } else if (nowMs - startedAtMs >= this.config.startupRetryWindowMs) {
      return;
    }
    this._startupRetryTimer = this.setTimer(() => {
      this._startupRetryTimer = null;
      this.refresh().catch((err) => {
        if (this.logger && typeof this.logger.warn === "function") {
          this.logger.warn("MeshCoordinator startup retry failed", err && err.message ? err.message : err);
        }
      });
    }, this.config.startupRetryIntervalMs);
    if (this._startupRetryTimer && typeof this._startupRetryTimer.unref === "function") {
      this._startupRetryTimer.unref();
    }
  }

  _clearStartupRetry() {
    if (!this._startupRetryTimer) {
      return;
    }
    this.clearTimer(this._startupRetryTimer);
    this._startupRetryTimer = null;
  }

  _needsStartupRetry() {
    const status = this.getStatus();
    if (hasReachableSeed(status.seedReachable)) {
      return false;
    }
    if (this.relayConnectionPool && Number(this.relayConnectionPool.connectionCount || 0) > 0) {
      return false;
    }
    return true;
  }

}

function uniqueStrings(list) {
  const out = [];
  const seen = new Set();
  for (const value of list || []) {
    const text = typeof value === "string" ? value.trim() : "";
    if (!text || seen.has(text)) continue;
    seen.add(text);
    out.push(text);
  }
  return out;
}

function normalizeMeshConfig(input) {
  const raw = input && typeof input === "object" ? input : {};
  const mode = raw.mode === "seed-only" ? "seed-only" : "seeded-gossip";
  return {
    enabled: true,
    mode,
    participateInRouting: true,
    seeds: uniqueStrings(raw.seeds),
    minPeers: clampInt(raw.minPeers, 1, 1000, 3),
    maxPeers: clampInt(raw.maxPeers, 1, 1000, 32),
    discoveryIntervalMs: clampInt(raw.discoveryIntervalMs, 1_000, 300_000, DEFAULT_DISCOVERY_INTERVAL_MS),
    discoveryTimeoutMs: clampInt(raw.discoveryTimeoutMs, 200, 30_000, 3_000),
    startupRetryIntervalMs: clampInt(raw.startupRetryIntervalMs, 200, 10_000, DEFAULT_STARTUP_RETRY_INTERVAL_MS),
    startupRetryWindowMs: clampInt(raw.startupRetryWindowMs, 1_000, 60_000, DEFAULT_STARTUP_RETRY_WINDOW_MS),
    limitPerSource: clampInt(raw.limitPerSource, 1, 500, 200),
    policy: {
      rateLimit: clampInt(raw && raw.policy ? raw.policy.rateLimit : undefined, 1, 100_000, 120),
      payloadMaxBytes: clampInt(raw && raw.policy ? raw.policy.payloadMaxBytes : undefined, 1024, 64 * 1024 * 1024, 1_048_576),
      failureThreshold: clampInt(raw && raw.policy ? raw.policy.failureThreshold : undefined, 1, 1000, 8),
      defaultHops: clampInt(raw && raw.policy ? raw.policy.defaultHops : undefined, 1, 3, 1),
      forceOnionRouting: raw && raw.policy && raw.policy.forceOnionRouting === true,
    },
    allowRelayKeyIds: new Set(uniqueStrings(raw.allowRelayKeyIds)),
    denyRelayKeyIds: new Set(uniqueStrings(raw.denyRelayKeyIds)),
  };
}

function hasReachableSeed(seedReachable) {
  if (!seedReachable || typeof seedReachable !== "object") return false;
  return Object.values(seedReachable).some((value) => value === true);
}

function clampInt(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const rounded = Math.floor(num);
  return Math.max(min, Math.min(max, rounded));
}

function extractHostFromSeedUrl(url) {
  const raw = typeof url === "string" ? url.trim() : "";
  if (!raw) return "";
  try {
    return new URL(raw).hostname.toLowerCase();
  } catch {
    return "";
  }
}
