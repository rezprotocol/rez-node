import net from "node:net";
import tls from "node:tls";
import { isNonEmptyString, isBytes } from "@rezprotocol/core";
import { encodeFrame, createFrameDecoder } from "./TcpFraming.js";

export class EConnectTimeout extends Error {
  constructor(endpoint) {
    super(`Connect timeout: ${endpoint}`);
    this.name = "EConnectTimeout";
    this.code = "EConnectTimeout";
  }
}

export class EConnectFailed extends Error {
  constructor(endpoint) {
    super(`Connect failed: ${endpoint}`);
    this.name = "EConnectFailed";
    this.code = "EConnectFailed";
  }
}

export class ESocketClosed extends Error {
  constructor(endpoint) {
    super(`Socket closed: ${endpoint}`);
    this.name = "ESocketClosed";
    this.code = "ESocketClosed";
  }
}

export class EQueueFull extends Error {
  constructor(endpoint) {
    super(`Queue full: ${endpoint}`);
    this.name = "EQueueFull";
    this.code = "EQueueFull";
  }
}

export class EConnLimit extends Error {
  constructor(limit) {
    super(`Connection limit exceeded: ${limit}`);
    this.name = "EConnLimit";
    this.code = "EConnLimit";
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitterDelay(ms, jitter) {
  if (!jitter) return ms;
  const delta = ms * jitter;
  return Math.max(0, ms + (Math.random() * 2 - 1) * delta);
}

export class TcpConnectionManager {
  constructor({
    resolve,
    maxConnections = 256,
    maxQueueBytesPerConn = 8 * 1024 * 1024,
    maxQueueItemsPerConn = 1024,
    connectTimeoutMs = 2000,
    idleTimeoutMs = 30_000,
    keepAliveInitialDelayMs = 0,
    retry = { maxAttempts: 3, baseDelayMs: 50, maxDelayMs: 1000, jitter: 0.2 },
    onInboundFrame = null,
    onConnectionOpen = null,
    onConnectionClose = null,
    now = Date.now,
    setTimer = setTimeout,
    clearTimer = clearTimeout,
  } = {}) {
    if (typeof resolve !== "function") {
      throw new Error("TcpConnectionManager requires resolve(endpoint) function");
    }
    if (typeof now !== "function") {
      throw new Error("TcpConnectionManager requires now() function");
    }
    if (typeof setTimer !== "function" || typeof clearTimer !== "function") {
      throw new Error("TcpConnectionManager requires setTimer/clearTimer functions");
    }

    this.resolve = resolve;
    this.onInboundFrame = typeof onInboundFrame === "function" ? onInboundFrame : null;
    this.onConnectionOpen = typeof onConnectionOpen === "function" ? onConnectionOpen : null;
    this.onConnectionClose = typeof onConnectionClose === "function" ? onConnectionClose : null;
    this.maxConnections = maxConnections;
    this.maxQueueBytesPerConn = maxQueueBytesPerConn;
    this.maxQueueItemsPerConn = maxQueueItemsPerConn;
    this.connectTimeoutMs = connectTimeoutMs;
    this.idleTimeoutMs = idleTimeoutMs;
    this.keepAliveInitialDelayMs = Number.isFinite(Number(keepAliveInitialDelayMs)) && keepAliveInitialDelayMs > 0 ? keepAliveInitialDelayMs : 0;
    this.retry = retry;
    this.now = now;
    this.setTimer = setTimer;
    this.clearTimer = clearTimer;
    this.connections = new Map();
    this.closed = false;
    this._pendingSleeps = new Set();
  }

  async send(endpoint, bytes) {
    if (!isNonEmptyString(endpoint)) {
      throw new Error("TcpConnectionManager.send(endpoint, bytes) requires endpoint");
    }
    if (!isBytes(bytes)) {
      throw new Error("TcpConnectionManager.send(endpoint, bytes) requires Uint8Array bytes");
    }
    if (this.closed) {
      throw new ESocketClosed(endpoint);
    }

    const frame = encodeFrame(bytes);
    if (frame.length > this.maxQueueBytesPerConn || 1 > this.maxQueueItemsPerConn) {
      const { host, port } = this.resolve(endpoint) || {};
      const key = isNonEmptyString(host) && Number.isInteger(port) ? `${host}:${port}` : endpoint;
      throw new EQueueFull(key);
    }

    const { host, port, tls: useTls = false, tlsAuto = false } = this.resolve(endpoint) || {};
    if (!isNonEmptyString(host) || !Number.isInteger(port) || port <= 0) {
      throw new Error("TcpConnectionManager resolve(endpoint) must return { host, port[, tls] }");
    }

    const key = `${useTls ? "tls" : "tcp"}://${host}:${port}`;
    let conn = this.connections.get(key);
    if (!conn) {
      if (this.connections.size >= this.maxConnections) {
        throw new EConnLimit(this.maxConnections);
      }
      conn = this._createConnection(key, host, port, useTls, tlsAuto);
      this.connections.set(key, conn);
    }

    if (conn.queueItems + 1 > this.maxQueueItemsPerConn || conn.queueBytes + frame.length > this.maxQueueBytesPerConn) {
      throw new EQueueFull(key);
    }

    const promise = new Promise((resolve, reject) => {
      conn.queue.push({ frame, resolve, reject });
      conn.queueItems += 1;
      conn.queueBytes += frame.length;
    });

    this._touch(conn);
    this._flush(conn).catch(() => {});
    return promise;
  }

  async close() {
    this.closed = true;
    for (const cancel of this._pendingSleeps) {
      try {
        cancel();
      } catch (_err) {
        // ignore
      }
    }
    this._pendingSleeps.clear();
    const settle = [];
    for (const conn of this.connections.values()) {
      this._rejectAll(conn, new ESocketClosed(conn.key));
      if (conn.connecting) settle.push(conn.connecting.catch(() => {}));
      if (conn.socket) conn.socket.destroy();
      if (conn.idleTimer) this.clearTimer(conn.idleTimer);
      conn.idleTimer = null;
    }
    this.connections.clear();
    await Promise.all(settle);
  }

  _createConnection(key, host, port, useTls = false, tlsAuto = false) {
    const conn = {
      key,
      host,
      port,
      tls: useTls === true,
      tlsAuto: tlsAuto === true,
      socket: null,
      queue: [],
      queueItems: 0,
      queueBytes: 0,
      flushing: false,
      connecting: null,
      lastUsed: this.now(),
      idleTimer: null,
    };
    conn.connecting = this._connectWithRetry(conn);
    return conn;
  }

  async _connectWithRetry(conn) {
    const { maxAttempts, baseDelayMs, maxDelayMs, jitter } = this.retry || {};
    const attempts = Math.max(1, maxAttempts || 1);
    let lastErr = null;

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      if (this.closed) {
        this._rejectAll(conn, new ESocketClosed(conn.key));
        this.connections.delete(conn.key);
        throw new ESocketClosed(conn.key);
      }
      try {
        const socket = await this._connectOnce(conn.host, conn.port, conn.tls);
        conn.socket = socket;
        this._attachSocket(conn);
        return socket;
      } catch (err) {
        lastErr = err;
        if (attempt === attempts) break;
        const backoff = Math.min(maxDelayMs || baseDelayMs, (baseDelayMs || 50) * (2 ** (attempt - 1)));
        await this._sleep(jitterDelay(backoff, jitter || 0));
      }
    }

    // TLS-first fallback: if TLS was auto-inferred (not explicitly configured)
    // and all TLS attempts failed, retry once with plain TCP.
    if (conn.tls && conn.tlsAuto) {
      try {
        const socket = await this._connectOnce(conn.host, conn.port, false);
        conn.tls = false;
        conn.tlsAuto = false;
        conn.socket = socket;
        this._attachSocket(conn);
        return socket;
      } catch (fallbackErr) {
        lastErr = fallbackErr;
      }
    }

    this._rejectAll(conn, new EConnectFailed(conn.key));
    this.connections.delete(conn.key);
    throw lastErr || new EConnectFailed(conn.key);
  }

  _connectOnce(host, port, useTls = false) {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new ESocketClosed(`${host}:${port}`));
        return;
      }
      const socket = useTls
        ? tls.connect({ host, port, servername: host, minVersion: "TLSv1.2" })
        : net.createConnection({ host, port });
      const timeout = this.setTimer(() => {
        socket.destroy();
        reject(new EConnectTimeout(`${host}:${port}`));
      }, this.connectTimeoutMs);

      socket.once("error", (err) => {
        this.clearTimer(timeout);
        socket.destroy();
        reject(err || new EConnectFailed(`${host}:${port}`));
      });

      const eventName = useTls ? "secureConnect" : "connect";
      socket.once(eventName, () => {
        this.clearTimer(timeout);
        socket.setNoDelay(true);
        resolve(socket);
      });
    });
  }

  _attachSocket(conn) {
    if (!conn.socket) return;

    if (this.keepAliveInitialDelayMs > 0) {
      try {
        conn.socket.setKeepAlive(true, this.keepAliveInitialDelayMs);
      } catch {
        // ignore if platform doesn't support
      }
    }

    // Bidirectional: set up frame decoder for inbound data on outbound connections
    if (this.onInboundFrame) {
      const decoder = createFrameDecoder((bytes) => {
        this.onInboundFrame(bytes, conn.socket);
      });
      conn.socket.on("data", (chunk) => {
        try {
          decoder.push(chunk);
        } catch {
          // Malformed or oversized frame — destroy connection
          conn.socket.destroy();
        }
      });
    }

    this.onConnectionOpen?.(conn.key, conn.socket);

    conn.socket.on("error", () => {
      this.onConnectionClose?.(conn.key, conn.socket);
      this._rejectAll(conn, new ESocketClosed(conn.key));
      this.connections.delete(conn.key);
    });
    conn.socket.on("close", () => {
      this.onConnectionClose?.(conn.key, conn.socket);
      this._rejectAll(conn, new ESocketClosed(conn.key));
      this.connections.delete(conn.key);
    });
  }

  _touch(conn) {
    conn.lastUsed = this.now();
    if (conn.idleTimer) this.clearTimer(conn.idleTimer);
    conn.idleTimer = this.setTimer(() => this._maybeIdleClose(conn), this.idleTimeoutMs);
    if (conn.idleTimer?.unref) conn.idleTimer.unref();
  }

  _maybeIdleClose(conn) {
    if (conn.queue.length > 0 || conn.flushing) return;
    const idleMs = this.now() - conn.lastUsed;
    if (idleMs < this.idleTimeoutMs) {
      this._touch(conn);
      return;
    }
    if (conn.socket) conn.socket.destroy();
    this.connections.delete(conn.key);
  }

  sweepIdle() {
    for (const conn of this.connections.values()) {
      this._maybeIdleClose(conn);
    }
  }

  async _flush(conn) {
    if (conn.flushing) return;
    conn.flushing = true;
    try {
      await conn.connecting;
      while (conn.queue.length > 0) {
        const item = conn.queue.shift();
        if (!item) break;
        conn.queueItems -= 1;
        conn.queueBytes -= item.frame.length;
        await this._writeFrame(conn.socket, item.frame);
        item.resolve();
      }
    } catch (err) {
      this._rejectAll(conn, err || new ESocketClosed(conn.key));
    } finally {
      conn.flushing = false;
    }
  }

  _writeFrame(socket, frame) {
    if (!socket || socket.destroyed) {
      return Promise.reject(new ESocketClosed("socket"));
    }
    return new Promise((resolve, reject) => {
      const wrote = socket.write(frame, (err) => {
        if (err) reject(err);
      });
      if (wrote) {
        resolve();
        return;
      }
      socket.once("drain", resolve);
      socket.once("error", reject);
      socket.once("close", () => reject(new ESocketClosed("socket")));
    });
  }

  _rejectAll(conn, err) {
    while (conn.queue.length > 0) {
      const item = conn.queue.shift();
      if (item) {
        conn.queueItems -= 1;
        conn.queueBytes -= item.frame.length;
        item.reject(err);
      }
    }
  }

  _sleep(ms) {
    return new Promise((resolve) => {
      const timer = this.setTimer(() => {
        this._pendingSleeps.delete(cancel);
        resolve();
      }, ms);
      const cancel = () => {
        this.clearTimer(timer);
        this._pendingSleeps.delete(cancel);
        resolve();
      };
      this._pendingSleeps.add(cancel);
    });
  }
}
