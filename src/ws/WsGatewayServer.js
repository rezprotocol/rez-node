import { createServer } from "node:http";
import { createServer as createSecureServer } from "node:https";
import { readFileSync } from "node:fs";
import { WebSocketServer } from "ws";
import { UiSessionRegistry } from "./UiSessionRegistry.js";
import { resolveTrustedProxyClientIp } from "../util/trustedProxyClientIp.js";

function prometheusName(name) {
  return "rez_" + String(name).replace(/([a-z0-9])([A-Z])/g, "$1_$2").replace(/[^A-Za-z0-9_]/g, "_").toLowerCase();
}

function metricsText(metrics) {
  if (!metrics || typeof metrics.snapshot !== "function") return "";
  const snapshot = metrics.snapshot();
  const lines = [];
  for (const [name, value] of Object.entries(snapshot)) {
    if (typeof value !== "number" || !Number.isFinite(value)) continue;
    lines.push(prometheusName(name) + " " + value);
  }
  return lines.join("\n") + "\n";
}


export class WsGatewayServer {
  #readinessCache;
  #readinessInFlight;
  #readinessCacheMs;

  constructor({ runtime, host = "127.0.0.1", port = 8787, path = "/ws", metrics = null, protocolFactory = null, onInboundDeposit = null, storageProvider = null, nodeEnabled = true, tls = null, trustedProxyCidrs = [], readinessCacheMs = 1000 } = {}) {
    if (!runtime) throw new Error("runtime required");
    if (typeof protocolFactory !== "function") throw new Error("protocolFactory required");
    if (!Number.isFinite(readinessCacheMs) || readinessCacheMs < 0) {
      throw new Error("readinessCacheMs must be a nonnegative finite number");
    }
    // Track 2: TLS for the client-facing listener. Null = plaintext (local dev, or termination at
    // an upstream proxy). The credentials are read at START, not here, so a construction-time
    // config error and an unreadable-file error stay distinguishable.
    this.tls = tls && typeof tls === "object" ? tls : null;
    this.trustedProxyCidrs = Array.isArray(trustedProxyCidrs) ? [...trustedProxyCidrs] : [];
    this.runtime = runtime;
    this.host = host;
    this.port = port;
    this.path = normalizePath(path);
    this.metrics = metrics || (runtime && runtime.metrics ? runtime.metrics : null);
    this.httpServer = null;
    this.wss = null;
    this._connections = new Set();
    this._sessionRegistry = new UiSessionRegistry();
    this._protocolFactory = protocolFactory;
    this._onInboundDeposit = typeof onInboundDeposit === "function" ? onInboundDeposit : null;
    this.#readinessCache = null;
    this.#readinessInFlight = null;
    this.#readinessCacheMs = readinessCacheMs;
  }

  // Exposed so out-of-band signal sources (e.g. PersistentOutboundQueue's
  // status callback) can route server-push events to the right per-owner
  // sessions via broadcastToOwner.
  getSessionRegistry() {
    return this._sessionRegistry;
  }

  /** True when this listener terminates TLS itself (wss://) rather than serving plaintext. */
  get tlsEnabled() {
    return this.tls !== null;
  }

  // Read the configured credentials. A missing/unreadable file FAILS START — a node that silently
  // fell back to plaintext because a cert path was wrong would serve stranger registrations in the
  // clear while its operator believed otherwise.
  #readTlsOptions() {
    const options = {};
    try {
      options.key = readFileSync(this.tls.keyPath);
      options.cert = readFileSync(this.tls.certPath);
      if (this.tls.caPath !== null && this.tls.caPath !== undefined) {
        options.ca = readFileSync(this.tls.caPath);
      }
    } catch (err) {
      throw new Error(
        "WsGatewayServer: cannot read TLS credentials ("
          + (err && err.message ? err.message : String(err))
          + "). Refusing to start — a plaintext fallback would expose client traffic.",
      );
    }
    return options;
  }

  async #readiness(defaultOk = false) {
    const check = this.runtime && typeof this.runtime.checkReadiness === "function"
      ? this.runtime.checkReadiness.bind(this.runtime)
      : null;
    if (!check) {
      return { ok: defaultOk, components: { runtime: defaultOk } };
    }
    const nowMs = Date.now();
    if (this.#readinessCache && this.#readinessCache.expiresAtMs > nowMs) {
      return this.#readinessCache.value;
    }
    if (this.#readinessInFlight) return this.#readinessInFlight;
    const probe = (async () => {
      try {
        const readiness = await check();
        return readiness && typeof readiness === "object"
          ? readiness
          : { ok: false, components: { runtime: false } };
      } catch (err) {
        void err;
        return { ok: false, components: { runtime: false } };
      }
    })();
    this.#readinessInFlight = probe;
    try {
      const readiness = await probe;
      this.#readinessCache = {
        value: readiness,
        expiresAtMs: Date.now() + this.#readinessCacheMs,
      };
      return readiness;
    } finally {
      if (this.#readinessInFlight === probe) this.#readinessInFlight = null;
    }
  }

  async start() {
    const loopbackBound = _isLoopbackBind(this.host);
    const requestListener = async (req, res) => {
      // DNS-rebinding defense: when bound to loopback, reject non-loopback Host headers.
      if (loopbackBound && !_isLoopbackHost(req.headers.host)) {
        res.writeHead(403);
        res.end();
        return;
      }
      if (req.url === "/health") {
        const meshStatus = this.runtime && typeof this.runtime.getMeshStatus === "function"
          ? this.runtime.getMeshStatus()
          : null;
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({
          ok: true,
          tsMs: Date.now(),
          mesh: meshStatus ? {
            enabled: meshStatus.enabled === true,
            mode: meshStatus.mode || "seeded-gossip",
            participateInRouting: meshStatus.participateInRouting === true,
            peerCount: Number(meshStatus.peerCount || 0),
            lastDiscoveryAtMs: meshStatus.lastDiscoveryAtMs || null,
          } : null,
        }));
        return;
      }
      if (req.url === "/ready") {
        const readiness = await this.#readiness(false);
        const ready = readiness && readiness.ok === true;
        res.writeHead(ready ? 200 : 503, { "content-type": "application/json" });
        res.end(JSON.stringify({
          ok: ready,
          degraded: readiness && readiness.degraded === true,
          tsMs: Date.now(),
          components: readiness && readiness.components ? readiness.components : {},
        }));
        return;
      }
      if (req.url === "/metrics") {
        res.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
        res.end(metricsText(this.metrics));
        return;
      }
      res.writeHead(404);
      res.end();
    };

    // ONE listener, two transports. Everything above (health, DNS-rebinding defense) and the
    // upgrade path below are identical either way — TLS changes how bytes reach this server, not
    // what it does with them.
    this.httpServer = this.tlsEnabled
      ? createSecureServer(this.#readTlsOptions(), requestListener)
      : createServer(requestListener);

    this.wss = new WebSocketServer({ noServer: true, maxPayload: 1_048_576 });
    this._syncConnectionGauge();
    this.wss.on("connection", (ws, req) => {
      const clientIp = resolveTrustedProxyClientIp({ request: req || null, trustedProxyCidrs: this.trustedProxyCidrs });
      const protocol = this._protocolFactory({
        runtime: this.runtime,
        ws,
        request: req || null,
        sessionRegistry: this._sessionRegistry,
        clientIp,
      });
      this._connections.add(protocol);
      this._syncConnectionGauge();
      ws.once("close", () => {
        this._connections.delete(protocol);
        this._syncConnectionGauge();
      });
      protocol.start();
    });

    this.httpServer.on("upgrade", async (req, socket, head) => {
      const pathname = parsePath(req);
      if (pathname !== this.path) {
        socket.destroy();
        return;
      }
      // DNS-rebinding defense on WebSocket upgrade
      if (loopbackBound && !_isLoopbackHost(req.headers.host)) {
        socket.destroy();
        return;
      }
      if (loopbackBound) {
        const origin = req.headers.origin || "";
        if (origin && !_isLoopbackOrigin(origin)) {
          socket.destroy();
          return;
        }
      }
      // Readiness is an admission gate, not merely an observability hint. This
      // matters for load balancers without active upstream health removal: an
      // unready process must return a retryable HTTP response before accepting
      // a long-lived WebSocket. Runtimes predating checkReadiness retain their
      // existing behavior; hosted startRezNode runtimes always provide it.
      const readiness = await this.#readiness(true);
      if (socket.destroyed) return;
      if (!readiness || readiness.ok !== true) {
        if (socket.writable) {
          socket.write(
            "HTTP/1.1 503 Service Unavailable\r\n"
              + "Connection: close\r\n"
              + "Content-Length: 0\r\n\r\n",
          );
        }
        socket.destroy();
        return;
      }
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        this.wss.emit("connection", ws, req);
      });
    });

    await new Promise((resolve, reject) => {
      this.httpServer.listen(this.port, this.host, (err) => {
        if (err) reject(err);
        else resolve();
      });
      this.httpServer.once("error", reject);
    });

    const inboxStore = this.runtime ? this.runtime.inboxStore : undefined;
    if (inboxStore && typeof inboxStore.setOnDeposit === "function" && this._onInboundDeposit) {
      inboxStore.setOnDeposit((inboxId, packetId) => {
        Promise.resolve()
          .then(async () => {
            const depositEvent = await inboxStore.fetch(inboxId, packetId);
            const packetBytes = depositEvent ? depositEvent.bytes : undefined;
            if (!(packetBytes instanceof Uint8Array) || packetBytes.length === 0) return;
            // Durable-home deposits carry a per-inbox seq (cursor model); the
            // transient RMailbox path does not — leave it null there.
            const seq = depositEvent && depositEvent.seq != null ? depositEvent.seq : null;
            if (this.metrics) {
              this.metrics.increment("packetsReceivedTotal", 1);
              this.metrics.addTraffic({ packets: 1, bytes: packetBytes.length });
              this.metrics.increment("bytesInTotal", packetBytes.length);
              this.metrics.increment("inboxDepositsTotal", 1);
              this.metrics.increment("packetsRoutedTotal", 1);
            }

            await this._onInboundDeposit({
              inboxId,
              packetId,
              packetBytes,
              seq,
              sessionRegistry: this._sessionRegistry,
              runtime: this.runtime,
            });
          })
          .catch((err) => {
            // bad packet headers are dropped at gateway boundary
            if (this.metrics) this.metrics.increment("errorsTotal", 1);
            console.error("[WsGatewayServer] onDeposit error", inboxId, packetId, err && err.message ? err.message : err);
          });
      });
    }

    return this.address();
  }

  address() {
    if (!this.httpServer) return null;
    const addr = this.httpServer.address();
    if (typeof addr === "string") return { address: addr, port: this.port };
    return { address: addr.address, port: addr.port };
  }

  async stop() {
    for (const protocol of this._connections) {
      protocol.close();
    }

    if (this.wss) {
      await new Promise((resolve) => this.wss.close(resolve));
    }

    if (this.httpServer) {
      await new Promise((resolve) => this.httpServer.close(resolve));
    }

    for (const protocol of this._connections) {
      protocol.stop();
    }
    this._connections.clear();
    this._syncConnectionGauge();
  }

  _syncConnectionGauge() {
    const count = this._sessionRegistry.countAll();
    if (this.metrics) this.metrics.setGauge("activeConnections", count);
  }

}

function normalizePath(path) {
  if (typeof path !== "string") return "/ws";
  const trimmed = path.trim();
  if (!trimmed) return "/ws";
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function parsePath(req) {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "127.0.0.1"}`);
    return url.pathname || "/";
  } catch {
    return "/";
  }
}

// --- DNS-rebinding defense helpers ---

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "::1", "[::1]"]);

function _isLoopbackBind(bindHost) {
  if (!bindHost) return true;
  const h = String(bindHost).toLowerCase();
  return LOOPBACK_HOSTS.has(h);
}

function _isLoopbackHost(hostHeader) {
  if (!hostHeader || typeof hostHeader !== "string") return false;
  const hostname = hostHeader.replace(/:\d+$/, "").toLowerCase();
  return LOOPBACK_HOSTS.has(hostname);
}

function _isLoopbackOrigin(origin) {
  if (!origin || typeof origin !== "string") return false;
  try {
    const url = new URL(origin);
    return LOOPBACK_HOSTS.has(url.hostname.toLowerCase());
  } catch {
    return false;
  }
}
