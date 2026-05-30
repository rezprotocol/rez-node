import net from "node:net";
import path from "node:path";
import fs from "node:fs/promises";

const METRICS_INTERVAL_MS = 250;

function isWindows() {
  return process.platform === "win32";
}

function defaultControlEndpoint(dataDir) {
  if (isWindows()) return "\\\\.\\pipe\\rez-node-control";
  return path.join(dataDir, "control.sock");
}

function nowMs() {
  return Date.now();
}

function parseNdjsonLines(buffer, chunk) {
  const text = `${buffer}${chunk}`;
  const lines = text.split("\n");
  return {
    lines: lines.slice(0, -1),
    rest: lines[lines.length - 1] || "",
  };
}

function toJsonLine(value) {
  return `${JSON.stringify(value)}\n`;
}

export class ControlServer {
  constructor({
    metrics,
    dataDir,
    socketPath = null,
    version = "0.0.0",
    metricsIntervalMs = METRICS_INTERVAL_MS,
  } = {}) {
    if (!metrics) throw new Error("metrics required");
    this.metrics = metrics;
    this.dataDir = dataDir || process.cwd();
    this.socketPath = socketPath || defaultControlEndpoint(this.dataDir);
    this.version = String(version || "0.0.0");
    this.metricsIntervalMs = Math.max(100, Number(metricsIntervalMs) || METRICS_INTERVAL_MS);

    this.server = null;
    this.clients = new Set();
    this._ticker = null;
    this._metricsEventHandler = (evt) => this._broadcast(evt);
  }

  async start() {
    if (this.server) return this.address();

    if (!isWindows()) {
      await fs.mkdir(path.dirname(this.socketPath), { recursive: true });
      await fs.rm(this.socketPath, { force: true }).catch(() => {});
    }

    this.server = net.createServer((socket) => this._onConnection(socket));
    await new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.socketPath, () => {
        this.server?.off("error", reject);
        resolve();
      });
    });

    if (!isWindows()) {
      await fs.chmod(this.socketPath, 0o600).catch(() => {});
    }

    this.metrics.on("event", this._metricsEventHandler);
    this._ticker = setInterval(() => this._broadcastMetrics(), this.metricsIntervalMs);
    this._ticker.unref?.();
    return this.address();
  }

  address() {
    return this.socketPath;
  }

  async stop() {
    if (this._ticker) {
      clearInterval(this._ticker);
      this._ticker = null;
    }
    this.metrics.off("event", this._metricsEventHandler);

    for (const client of this.clients) {
      try { client.socket.destroy(); } catch {}
    }
    this.clients.clear();

    if (this.server) {
      await new Promise((resolve) => this.server.close(() => resolve()));
      this.server = null;
    }

    if (!isWindows()) {
      await fs.rm(this.socketPath, { force: true }).catch(() => {});
    }
  }

  _onConnection(socket) {
    const client = {
      socket,
      buffer: "",
      subscribedMetrics: false,
    };
    this.clients.add(client);
    this.metrics.setGauge("wsClients", this.clients.size);
    this._send(client, {
      type: "hello",
      version: this.version,
      startTimeMs: this.metrics.startTimeMs,
      atMs: nowMs(),
    });

    socket.setEncoding("utf8");
    socket.on("data", (chunk) => this._onData(client, chunk));
    socket.on("error", () => {
      this._removeClient(client);
    });
    socket.on("close", () => {
      this._removeClient(client);
    });
  }

  _removeClient(client) {
    if (!this.clients.has(client)) return;
    this.clients.delete(client);
    this.metrics.setGauge("wsClients", this.clients.size);
  }

  _onData(client, chunk) {
    const { lines, rest } = parseNdjsonLines(client.buffer, chunk);
    client.buffer = rest;
    for (const line of lines) {
      const text = String(line || "").trim();
      if (!text) continue;
      let msg;
      try {
        msg = JSON.parse(text);
      } catch {
        this._send(client, { type: "error", code: "BAD_JSON", message: "Invalid JSON", atMs: nowMs() });
        continue;
      }
      this._handleMessage(client, msg);
    }
  }

  _handleMessage(client, message) {
    if (!message || typeof message !== "object") {
      this._send(client, { type: "error", code: "BAD_REQUEST", message: "Message must be object", atMs: nowMs() });
      return;
    }

    const op = String(message.op || "");
    if (op === "subscribe") {
      const streams = Array.isArray(message.streams) ? message.streams.map((s) => String(s)) : [];
      client.subscribedMetrics = streams.includes("metrics");
      this._send(client, { type: "ack", op: "subscribe", streams, atMs: nowMs() });
      if (client.subscribedMetrics) {
        this._sendMetrics(client);
      }
      return;
    }

    if (op === "get" && String(message.name || "") === "metrics") {
      this._sendMetrics(client);
      return;
    }

    if (op === "tailLogs") {
      this._send(client, { type: "error", code: "NOT_IMPLEMENTED", message: "tailLogs unavailable in v0", atMs: nowMs() });
      return;
    }

    if (op === "shutdown" || op === "reloadConfig") {
      this._send(client, { type: "error", code: "NOT_IMPLEMENTED", message: `${op} unavailable in v0`, atMs: nowMs() });
      return;
    }

    this._send(client, { type: "error", code: "UNKNOWN_OP", message: `Unsupported op: ${op || "?"}`, atMs: nowMs() });
  }

  _sendMetrics(client) {
    this._send(client, {
      type: "metrics",
      atMs: nowMs(),
      data: this.metrics.snapshot(),
    });
  }

  _broadcastMetrics() {
    for (const client of this.clients) {
      if (!client.subscribedMetrics) continue;
      this._sendMetrics(client);
    }
  }

  _broadcast(frame) {
    for (const client of this.clients) {
      this._send(client, frame);
    }
  }

  _send(client, frame) {
    if (!client || !client.socket || client.socket.destroyed) return;
    try {
      client.socket.write(toJsonLine(frame));
    } catch {
      this._removeClient(client);
      try { client.socket.destroy(); } catch {}
    }
  }
}

export function defaultControlSocketPath(dataDir) {
  return defaultControlEndpoint(dataDir);
}
