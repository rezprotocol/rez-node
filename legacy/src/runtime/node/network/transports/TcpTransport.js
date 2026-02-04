import net from "node:net";
import crypto from "node:crypto";

import Transport from "../../../../core/network/interfaces/Transport.js";

function base64Url(bytes) {
  return Buffer.from(bytes)
    .toString("base64")
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

function randomId(prefix = "tcp") {
  return `${prefix}:${base64Url(crypto.randomBytes(12))}`;
}

function parseTcpTarget(to) {
  if (!to) return null;
  const s = String(to);
  if (s.startsWith("tcp://")) {
    const u = new URL(s);
    const host = u.hostname;
    const port = Number(u.port || 0);
    if (!host || !Number.isFinite(port) || port <= 0) throw new Error("Invalid tcp:// target");
    return { host, port, key: `tcp://${host}:${port}` };
  }
  // host:port
  if (/^[^:]+:\d+$/.test(s)) {
    const [host, portStr] = s.split(":");
    const port = Number(portStr);
    if (!host || !Number.isFinite(port) || port <= 0) throw new Error("Invalid host:port target");
    return { host, port, key: `tcp://${host}:${port}` };
  }
  return null;
}

function encodeFrame(payloadBytes) {
  const len = payloadBytes.length >>> 0;
  const buf = Buffer.allocUnsafe(4 + len);
  buf.writeUInt32BE(len, 0);
  Buffer.from(payloadBytes).copy(buf, 4);
  return buf;
}

/**
 * TcpTransport
 *
 * Node-only transport for server-to-server comms using node:net.
 *
 * Wire format:
 * - 4-byte big-endian length prefix
 * - payload bytes
 * First payload on a connection must be UTF-8 JSON: { t:"hello", peerId:"..." }
 */
export default class TcpTransport extends Transport {
  constructor() {
    super();
    this._localId = null;
    this._listenHost = null;
    this._listenPort = null;
    this._server = null;

    /** @type {Map<string, net.Socket>} */
    this._peers = new Map(); // peerId -> socket
    /** @type {Map<string, Promise<net.Socket>>} */
    this._dialing = new Map(); // tcp://host:port -> promise
  }

  async init(options = {}) {
    this._localId = options.localId ?? randomId();
    this._listenHost = options.listenHost ?? null;
    this._listenPort = options.listenPort ?? null;
    this._maxFrameBytes = options.maxFrameBytes ?? 1024 * 1024; // 1MB
    this._started = false;
  }

  getLocalId() {
    return this._localId;
  }

  getListenAddresses() {
    if (!this._server) return [];
    const addr = this._server.address();
    if (!addr || typeof addr === "string") return [];
    return [`tcp://${addr.address}:${addr.port}`];
  }

  async start() {
    if (this._started) return;

    if (this._listenPort != null || this._listenHost != null) {
      const host = this._listenHost ?? "127.0.0.1";
      const port = this._listenPort ?? 0;
      this._server = net.createServer((socket) => this._handleSocket(socket, { outbound: false }));
      await new Promise((resolve, reject) => {
        this._server.once("error", reject);
        this._server.listen(port, host, () => {
          this._server.off("error", reject);
          resolve();
        });
      });
    }

    this._started = true;
  }

  async stop() {
    if (!this._started) return;
    this._started = false;

    for (const sock of this._peers.values()) {
      try {
        sock.destroy();
      } catch {
        // ignore
      }
    }
    this._peers.clear();
    this._dialing.clear();

    if (this._server) {
      const s = this._server;
      this._server = null;
      await new Promise((resolve) => s.close(() => resolve()));
    }
  }

  async send(to, bytes, options = {}) {
    void options;
    if (!(bytes instanceof Uint8Array)) throw new Error("TcpTransport.send requires Uint8Array bytes");
    if (!this._started) throw new Error("TcpTransport not started");

    // peerId fast-path
    const peerId = String(to);
    const existing = this._peers.get(peerId);
    if (existing) {
      this._writeDataFrame(existing, bytes);
      return;
    }

    // otherwise treat as address
    const target = parseTcpTarget(to);
    if (!target) throw new Error("TcpTransport.send unknown target (expected peerId or tcp://host:port)");
    const sock = await this._dial(target.host, target.port, { expectedPeerId: options.expectedPeerId ?? null });
    this._writeDataFrame(sock, bytes);
  }

  _writeDataFrame(socket, bytes) {
    try {
      socket.write(encodeFrame(bytes));
    } catch (err) {
      this.emit("error", err);
    }
  }

  _writeHello(socket) {
    const json = JSON.stringify({ t: "hello", peerId: this._localId });
    socket.write(encodeFrame(new TextEncoder().encode(json)));
  }

  _handleSocket(socket, { outbound }) {
    socket.setNoDelay(true);
    this._writeHello(socket);

    let buf = Buffer.alloc(0);
    let remotePeerId = null;
    let closed = false;

    const cleanup = () => {
      if (closed) return;
      closed = true;
      if (remotePeerId && this._peers.get(remotePeerId) === socket) {
        this._peers.delete(remotePeerId);
        this.emit("disconnection", { peerId: remotePeerId });
      }
    };

    socket.on("close", cleanup);
    socket.on("end", cleanup);
    socket.on("error", (err) => this.emit("error", err));

    socket.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= 4) {
        const len = buf.readUInt32BE(0);
        if (len > this._maxFrameBytes) {
          this.emit("error", new Error("TcpTransport frame too large"));
          socket.destroy();
          return;
        }
        if (buf.length < 4 + len) return;
        const payload = buf.subarray(4, 4 + len);
        buf = buf.subarray(4 + len);

        if (!remotePeerId) {
          // first frame is hello
          let obj;
          try {
            obj = JSON.parse(new TextDecoder().decode(payload));
          } catch {
            this.emit("error", new Error("TcpTransport invalid hello"));
            socket.destroy();
            return;
          }
          if (!obj || obj.t !== "hello" || typeof obj.peerId !== "string" || !obj.peerId) {
            this.emit("error", new Error("TcpTransport invalid hello fields"));
            socket.destroy();
            return;
          }
          remotePeerId = obj.peerId;
          this._peers.set(remotePeerId, socket);
          this.emit("connection", { peerId: remotePeerId, outbound: !!outbound });
          continue;
        }

        this.emit("frame", { from: remotePeerId, bytes: new Uint8Array(payload) });
      }
    });
  }

  async _dial(host, port, { expectedPeerId }) {
    const key = `tcp://${host}:${port}`;
    if (this._dialing.has(key)) return await this._dialing.get(key);

    const p = new Promise((resolve, reject) => {
      const socket = net.connect({ host, port }, () => {
        this._handleSocket(socket, { outbound: true });
      });

      let done = false;
      const onErr = (err) => {
        if (done) return;
        done = true;
        cleanup();
        reject(err);
      };

      const onConn = ({ peerId }) => {
        if (done) return;
        // Make sure this is the handshake for *this* socket.
        if (this._peers.get(peerId) !== socket) return;
        if (expectedPeerId && peerId !== expectedPeerId) {
          done = true;
          socket.destroy();
          cleanup();
          reject(new Error("TcpTransport peerId mismatch"));
          return;
        }
        done = true;
        cleanup();
        resolve(socket);
      };

      const cleanup = () => {
        this.off("connection", onConn);
        socket.off("error", onErr);
        socket.off("close", onClose);
      };

      const onClose = () => {
        if (done) return;
        done = true;
        cleanup();
        reject(new Error("TcpTransport connect closed"));
      };

      this.on("connection", onConn);
      socket.on("error", onErr);
      socket.on("close", onClose);
    }).finally(() => {
      this._dialing.delete(key);
    });

    this._dialing.set(key, p);
    return await p;
  }
}
