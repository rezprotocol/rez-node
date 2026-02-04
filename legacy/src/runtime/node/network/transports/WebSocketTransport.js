import http from "node:http";
import net from "node:net";
import crypto from "node:crypto";

import Transport from "../../../../core/network/interfaces/Transport.js";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function base64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function base64Url(bytes) {
  return Buffer.from(bytes)
    .toString("base64")
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

function randomId(prefix = "ws") {
  return `${prefix}:${base64Url(crypto.randomBytes(12))}`;
}

function wsAccept(secKey) {
  return base64(crypto.createHash("sha1").update(secKey + WS_GUID).digest());
}

function parseWsUrl(url) {
  const u = new URL(String(url));
  if (u.protocol !== "ws:") throw new Error("Only ws:// supported in MVP");
  const host = u.hostname;
  const port = Number(u.port || 80);
  const path = u.pathname || "/";
  return { host, port, path, href: u.href };
}

function buildFrame({ opcode, payload, mask }) {
  const finOpcode = 0x80 | (opcode & 0x0f);
  const len = payload.length >>> 0;

  let lenByte = 0;
  let extLen = Buffer.alloc(0);
  if (len < 126) {
    lenByte = len;
  } else if (len < 65536) {
    lenByte = 126;
    extLen = Buffer.allocUnsafe(2);
    extLen.writeUInt16BE(len, 0);
  } else {
    lenByte = 127;
    extLen = Buffer.allocUnsafe(8);
    // JS number is safe for our MVP sizes; write high 32 = 0.
    extLen.writeUInt32BE(0, 0);
    extLen.writeUInt32BE(len, 4);
  }

  let maskKey = Buffer.alloc(0);
  let body = Buffer.from(payload);
  if (mask) {
    maskKey = crypto.randomBytes(4);
    const out = Buffer.allocUnsafe(body.length);
    for (let i = 0; i < body.length; i++) out[i] = body[i] ^ maskKey[i & 3];
    body = out;
  }

  const header = Buffer.from([finOpcode, (mask ? 0x80 : 0) | lenByte]);
  return Buffer.concat([header, extLen, maskKey, body]);
}

function tryParseFrame(buffer, { expectMasked, maxPayloadBytes }) {
  if (buffer.length < 2) return null;
  const b0 = buffer[0];
  const b1 = buffer[1];
  const fin = (b0 & 0x80) !== 0;
  const opcode = b0 & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let len = b1 & 0x7f;
  let offset = 2;

  if (!fin) throw new Error("Fragmented frames not supported");
  if (expectMasked && !masked) throw new Error("Expected masked client frame");
  if (!expectMasked && masked) throw new Error("Unexpected masked server frame");

  if (len === 126) {
    if (buffer.length < offset + 2) return null;
    len = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (len === 127) {
    if (buffer.length < offset + 8) return null;
    const hi = buffer.readUInt32BE(offset);
    const lo = buffer.readUInt32BE(offset + 4);
    offset += 8;
    if (hi !== 0) throw new Error("Frame too large");
    len = lo;
  }

  if (len > maxPayloadBytes) throw new Error("WS payload too large");

  let maskKey = null;
  if (masked) {
    if (buffer.length < offset + 4) return null;
    maskKey = buffer.subarray(offset, offset + 4);
    offset += 4;
  }

  if (buffer.length < offset + len) return null;
  let payload = buffer.subarray(offset, offset + len);
  const rest = buffer.subarray(offset + len);

  if (masked && maskKey) {
    const out = Buffer.allocUnsafe(payload.length);
    for (let i = 0; i < payload.length; i++) out[i] = payload[i] ^ maskKey[i & 3];
    payload = out;
  }

  return { frame: { opcode, payload }, rest };
}

class WsConnection {
  /**
   * @param {net.Socket} socket
   * @param {{ maskOutgoing:boolean, expectMasked:boolean, maxPayloadBytes:number }} options
   */
  constructor(socket, options) {
    this._socket = socket;
    this._maskOutgoing = !!options.maskOutgoing;
    this._expectMasked = !!options.expectMasked;
    this._maxPayloadBytes = options.maxPayloadBytes;
    this._buf = Buffer.alloc(0);
    this._handlers = { message: null, close: null, error: null };

    socket.on("data", (chunk) => this._onData(chunk));
    socket.on("close", () => this._handlers.close?.());
    socket.on("end", () => this._handlers.close?.());
    socket.on("error", (err) => this._handlers.error?.(err));
  }

  onMessage(fn) {
    this._handlers.message = fn;
  }

  onClose(fn) {
    this._handlers.close = fn;
  }

  onError(fn) {
    this._handlers.error = fn;
  }

  sendText(text) {
    const bytes = new TextEncoder().encode(String(text));
    this._socket.write(buildFrame({ opcode: 1, payload: bytes, mask: this._maskOutgoing }));
  }

  sendBinary(bytes) {
    this._socket.write(buildFrame({ opcode: 2, payload: bytes, mask: this._maskOutgoing }));
  }

  close() {
    try {
      this._socket.end(buildFrame({ opcode: 8, payload: new Uint8Array(0), mask: this._maskOutgoing }));
    } catch {
      this._socket.destroy();
    }
  }

  _onData(chunk) {
    this._buf = Buffer.concat([this._buf, chunk]);
    while (true) {
      const parsed = tryParseFrame(this._buf, {
        expectMasked: this._expectMasked,
        maxPayloadBytes: this._maxPayloadBytes,
      });
      if (!parsed) return;
      this._buf = parsed.rest;

      const { opcode, payload } = parsed.frame;
      if (opcode === 8) {
        this.close();
        return;
      }
      if (opcode === 9) {
        // ping -> pong
        this._socket.write(buildFrame({ opcode: 10, payload, mask: this._maskOutgoing }));
        continue;
      }
      if (opcode === 10) continue; // pong

      if (opcode === 1) {
        const text = new TextDecoder().decode(payload);
        this._handlers.message?.({ opcode, text, bytes: new Uint8Array(payload) });
      } else if (opcode === 2) {
        this._handlers.message?.({ opcode, text: null, bytes: new Uint8Array(payload) });
      } else {
        throw new Error("Unsupported WS opcode");
      }
    }
  }
}

/**
 * WebSocketTransport
 *
 * Node-only MVP transport that supports:
 * - server mode (listenHost/listenPort)
 * - client mode (url)
 *
 * First message over the WS connection must be a text JSON hello:
 *   { t:"hello", peerId:"..." }
 */
export default class WebSocketTransport extends Transport {
  constructor() {
    super();
    this._localId = null;
    this._mode = null; // 'server' | 'client'

    this._listenHost = null;
    this._listenPort = null;
    this._path = "/";
    this._httpServer = null;

    this._url = null;
    this._expectedPeerId = null;

    this._maxPayloadBytes = 1024 * 1024;

    /** @type {Map<string, WsConnection>} */
    this._peers = new Map(); // peerId -> conn
    /** @type {WsConnection|null} */
    this._uplink = null;
    this._uplinkPeerId = null;
  }

  async init(options = {}) {
    this._localId = options.localId ?? randomId();
    this._expectedPeerId = options.expectedPeerId ?? null;
    this._maxPayloadBytes = options.maxPayloadBytes ?? 1024 * 1024;

    if (options.url) {
      this._mode = "client";
      this._url = String(options.url);
    } else {
      this._mode = "server";
      this._listenHost = options.listenHost ?? "127.0.0.1";
      this._listenPort = options.listenPort ?? 0;
      this._path = options.path ?? "/";
    }

    this._started = false;
  }

  getLocalId() {
    return this._localId;
  }

  getListenAddresses() {
    if (this._mode !== "server" || !this._httpServer) return [];
    const addr = this._httpServer.address();
    if (!addr || typeof addr === "string") return [];
    return [`ws://${addr.address}:${addr.port}${this._path}`];
  }

  async start() {
    if (this._started) return;
    if (this._mode === "server") {
      await this._startServer();
      this._started = true;
      return;
    }
    if (this._mode === "client") {
      try {
        await this._startClient();
        this._started = true;
        return;
      } catch (err) {
        // Ensure sockets are closed on failed start.
        await this.stop();
        throw err;
      }
    }
    throw new Error("WebSocketTransport not initialized");
  }

  async stop() {
    this._started = false;

    for (const c of this._peers.values()) {
      try {
        c.close();
      } catch {
        // ignore
      }
    }
    this._peers.clear();
    this._uplinkPeerId = null;

    if (this._uplink) {
      try {
        this._uplink.close();
      } catch {
        // ignore
      }
      this._uplink = null;
    }

    if (this._httpServer) {
      const s = this._httpServer;
      this._httpServer = null;
      await new Promise((resolve) => s.close(() => resolve()));
    }
  }

  async send(to, bytes) {
    if (!(bytes instanceof Uint8Array)) throw new Error("WebSocketTransport.send requires Uint8Array bytes");
    if (!this._started) throw new Error("WebSocketTransport not started");

    const target = String(to);
    if (this._mode === "server") {
      const conn = this._peers.get(target);
      if (!conn) throw new Error("Unknown peerId");
      conn.sendBinary(bytes);
      return;
    }

    // client mode: only uplink
    if (!this._uplink) throw new Error("Not connected");
    if (target !== this._url && target !== this._uplinkPeerId) {
      throw new Error("Client WebSocketTransport can only send to its connected relay");
    }
    this._uplink.sendBinary(bytes);
  }

  async _startServer() {
    this._httpServer = http.createServer((req, res) => {
      res.writeHead(404);
      res.end();
    });

    this._httpServer.on("upgrade", (req, socket, head) => {
      try {
        if (req.url !== this._path) {
          socket.destroy();
          return;
        }
        const key = req.headers["sec-websocket-key"];
        if (!key) throw new Error("Missing Sec-WebSocket-Key");
        const accept = wsAccept(String(key));

        socket.write(
          [
            "HTTP/1.1 101 Switching Protocols",
            "Upgrade: websocket",
            "Connection: Upgrade",
            `Sec-WebSocket-Accept: ${accept}`,
            "\r\n",
          ].join("\r\n")
        );

        // Any remaining bytes belong to WS data frames
        const conn = new WsConnection(socket, {
          maskOutgoing: false,
          expectMasked: true,
          maxPayloadBytes: this._maxPayloadBytes,
        });

        if (head?.length) socket.unshift(head);
        this._wireWsConn(conn, { outbound: false });
      } catch (err) {
        try {
          socket.destroy();
        } catch {
          // ignore
        }
        this.emit("error", err);
      }
    });

    await new Promise((resolve, reject) => {
      this._httpServer.once("error", reject);
      this._httpServer.listen(this._listenPort, this._listenHost, () => {
        this._httpServer.off("error", reject);
        resolve();
      });
    });
  }

  async _startClient() {
    const { host, port, path, href } = parseWsUrl(this._url);

    const secKey = base64(crypto.randomBytes(16));
    const expectedAccept = wsAccept(secKey);

    const socket = net.connect({ host, port });

    const handshakePromise = new Promise((resolve, reject) => {
      let buf = Buffer.alloc(0);
      const onErr = (err) => {
        cleanup();
        reject(err);
      };
      const onClose = () => {
        cleanup();
        reject(new Error("WebSocket closed during handshake"));
      };
      const cleanup = () => {
        socket.off("data", onData);
        socket.off("error", onErr);
        socket.off("close", onClose);
      };

      const onData = (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        const idx = buf.indexOf("\r\n\r\n");
        if (idx === -1) return;
        const head = buf.subarray(0, idx).toString("utf8");
        const rest = buf.subarray(idx + 4);
        cleanup();

        const lines = head.split("\r\n");
        const status = lines[0] || "";
        if (!status.includes("101")) {
          reject(new Error("WebSocket handshake failed"));
          return;
        }

        const headers = new Map();
        for (const line of lines.slice(1)) {
          const i = line.indexOf(":");
          if (i === -1) continue;
          headers.set(line.slice(0, i).trim().toLowerCase(), line.slice(i + 1).trim());
        }
        const accept = headers.get("sec-websocket-accept");
        if (accept !== expectedAccept) {
          reject(new Error("WebSocket accept mismatch"));
          return;
        }

        resolve({ rest });
      };

      socket.on("data", onData);
      socket.on("error", onErr);
      socket.on("close", onClose);
    });

    socket.write(
      [
        `GET ${path} HTTP/1.1`,
        `Host: ${host}:${port}`,
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Key: ${secKey}`,
        "Sec-WebSocket-Version: 13",
        "\r\n",
      ].join("\r\n")
    );

    const handshake = await handshakePromise;
    const conn = new WsConnection(socket, {
      maskOutgoing: true,
      expectMasked: false,
      maxPayloadBytes: this._maxPayloadBytes,
    });

    this._uplink = conn;

    // Wait until hello completes and peerId verified
    const waitHello = new Promise((resolve, reject) => {
      const onConn = ({ peerId, outbound }) => {
        if (!outbound) return;
        this.off("connection", onConn);
        this.off("error", onErr);
        this._uplinkPeerId = peerId;
        resolve();
      };
      const onErr = (err) => {
        this.off("connection", onConn);
        this.off("error", onErr);
        reject(err);
      };
      this.on("connection", onConn);
      this.on("error", onErr);
    });

    this._wireWsConn(conn, { outbound: true, uplinkHref: href });
    if (handshake.rest?.length) socket.unshift(handshake.rest);
    await waitHello;
  }

  _wireWsConn(conn, { outbound, uplinkHref = null }) {
    let remotePeerId = null;
    let closed = false;

    const cleanup = () => {
      if (closed) return;
      closed = true;
      if (remotePeerId && this._peers.get(remotePeerId) === conn) {
        this._peers.delete(remotePeerId);
        this.emit("disconnection", { peerId: remotePeerId, outbound: !!outbound });
      }
      if (outbound && this._uplink === conn) {
        this._uplink = null;
        this._uplinkPeerId = null;
      }
    };

    conn.onClose(cleanup);
    conn.onError((err) => this.emit("error", err));

    conn.onMessage(({ opcode, text, bytes }) => {
      if (!remotePeerId) {
        if (opcode !== 1) {
          this.emit("error", new Error("WebSocketTransport expected hello text frame"));
          conn.close();
          return;
        }
        let obj;
        try {
          obj = JSON.parse(text);
        } catch {
          this.emit("error", new Error("WebSocketTransport invalid hello JSON"));
          conn.close();
          return;
        }
        if (!obj || obj.t !== "hello" || typeof obj.peerId !== "string" || !obj.peerId) {
          this.emit("error", new Error("WebSocketTransport invalid hello fields"));
          conn.close();
          return;
        }

        remotePeerId = obj.peerId;

        if (outbound && this._expectedPeerId && remotePeerId !== this._expectedPeerId) {
          this.emit("error", new Error("WebSocketTransport peerId mismatch"));
          conn.close();
          return;
        }

        this._peers.set(remotePeerId, conn);
        this.emit("connection", { peerId: remotePeerId, outbound: !!outbound, url: uplinkHref });
        return;
      }

      if (opcode !== 2) return;
      this.emit("frame", { from: remotePeerId, bytes });
    });

    // Send hello after handlers are wired (avoids dropping buffered server hello on client start)
    const helloMsg = JSON.stringify({ t: "hello", peerId: this._localId });
    conn.sendText(helloMsg);
  }
}
