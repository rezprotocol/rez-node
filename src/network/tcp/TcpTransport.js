// NOTE:
// TcpTransport is a byte-level transport only.
// It must not implement retries, backoff, routing decisions,
// peer identity negotiation, or protocol parsing.
// Those are layered above this transport in later phases.
import fs from "node:fs";
import net from "node:net";
import tls from "node:tls";
import { RTransport, WirePacket, isNonEmptyString } from "@rezprotocol/core";
import { createFrameDecoder } from "./TcpFraming.js";
import { TcpConnectionManager } from "./TcpConnectionManager.js";

function peerAddress(socket) {
  const host = socket.remoteAddress || "unknown";
  const port = socket.remotePort || 0;
  return `${host}:${port}`;
}

export class TcpTransport extends RTransport {
  constructor({ endpointId, listenHost = "127.0.0.1", listenPort = 0, resolve, tlsOptions = null } = {}) {
    super();

    if (!isNonEmptyString(endpointId)) {
      throw new Error("TcpTransport requires endpointId");
    }
    if (!isNonEmptyString(listenHost)) {
      throw new Error("TcpTransport requires listenHost");
    }
    if (!Number.isInteger(listenPort) || listenPort < 0) {
      throw new Error("TcpTransport requires listenPort >= 0");
    }
    if (typeof resolve !== "function") {
      throw new Error("TcpTransport requires resolve(to) function");
    }

    this.endpointId = endpointId;
    this.listenHost = listenHost;
    this.listenPort = listenPort;
    this.resolve = resolve;
    this.tlsOptions = tlsOptions && typeof tlsOptions === "object" ? { ...tlsOptions } : null;

    this.server = null;
    this.handlers = new Set();
    this.sockets = new Set();
    this.connectionManager = new TcpConnectionManager({
      resolve: this.resolve,
      onInboundFrame: (bytes, socket) => this._handleInboundFrame(bytes, socket),
    });
  }

  _handleInboundFrame(bytes, socket) {
    const peer = peerAddress(socket);
    const packet = new WirePacket({
      bytes,
      to: this.endpointId,
      from: peer,
      meta: { peer, socket },
    });
    for (const handler of this.handlers) {
      handler(packet);
    }
  }

  _attachSocketHandlers(socket) {
    const decoder = createFrameDecoder((bytes) => {
      this._handleInboundFrame(bytes, socket);
    });

    socket.on("data", (chunk) => {
      try {
        decoder.push(chunk);
      } catch {
        // Malformed or oversized frame (e.g. HTTP request on TCP port).
        // Close the offending connection instead of crashing the process.
        socket.destroy();
      }
    });
    socket.on("error", () => {
      decoder.reset();
    });
    socket.on("close", () => {
      this.sockets.delete(socket);
      if (typeof this.onSocketClose === "function") this.onSocketClose(socket);
    });
  }

  getListenAddress() {
    if (!this.server) return { host: this.listenHost, port: this.listenPort };
    const addr = this.server.address();
    if (typeof addr === "object" && addr) {
      return { host: this.listenHost, port: addr.port };
    }
    return { host: this.listenHost, port: this.listenPort };
  }

  onPacket(handler) {
    if (typeof handler !== "function") {
      throw new Error("TcpTransport.onPacket(handler) requires a function");
    }
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  async start(options = {}) {
    if (this.server) return;
    const { onSocketClose } = options;
    this.onSocketClose = typeof onSocketClose === "function" ? onSocketClose : null;

    const onConnection = (socket) => {
      this.sockets.add(socket);
      this._attachSocketHandlers(socket);
    };

    this.server = this.tlsOptions?.enabled === true
      ? tls.createServer(this._loadTlsServerOptions(), onConnection)
      : net.createServer(onConnection);

    await new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.listenPort, this.listenHost, () => {
        this.server.off("error", reject);
        resolve();
      });
    });
  }

  async stop() {
    if (!this.server) return;

    for (const socket of this.sockets) {
      socket.destroy();
    }
    this.sockets.clear();

    await this.connectionManager.close();

    await new Promise((resolve) => this.server.close(resolve));
    this.server = null;
  }

  async send(packet) {
    if (!(packet?.bytes instanceof Uint8Array)) {
      throw new Error("TcpTransport.send(packet) requires packet.bytes Uint8Array");
    }
    if (!isNonEmptyString(packet.to)) {
      throw new Error("TcpTransport.send(packet) requires packet.to");
    }
    await this.connectionManager.send(packet.to, packet.bytes);
  }

  _loadTlsServerOptions() {
    const certPath = typeof this.tlsOptions?.certPath === "string" ? this.tlsOptions.certPath.trim() : "";
    const keyPath = typeof this.tlsOptions?.keyPath === "string" ? this.tlsOptions.keyPath.trim() : "";
    if (!certPath || !keyPath) {
      throw new Error("TcpTransport TLS requires certPath and keyPath");
    }
    return {
      cert: fs.readFileSync(certPath),
      key: fs.readFileSync(keyPath),
      minVersion: "TLSv1.2",
    };
  }
}
