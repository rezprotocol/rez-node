import { TcpTransport } from "../network/tcp/TcpTransport.js";

function parseEndpointId(endpointId) {
  if (typeof endpointId !== "string") return null;
  const text = endpointId.trim();
  if (!text) return null;
  const protocol = text.startsWith("tls://")
    ? "tls"
    : (text.startsWith("tcp://") ? "tcp" : null);
  const body = protocol ? text.slice(protocol.length + 3) : text;
  const idx = body.lastIndexOf(":");
  if (idx <= 0) return null;
  const host = body.slice(0, idx);
  const port = Number(body.slice(idx + 1));
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  return { host, port, tls: protocol === "tls" };
}

export class TcpRelayTransport {
  constructor({ endpointId, listenHost = "127.0.0.1", listenPort = 0, resolve, tlsOptions = null } = {}) {
    const resolveFn = resolve || ((endpointIdValue) => parseEndpointId(endpointIdValue));
    this.transport = new TcpTransport({ endpointId, listenHost, listenPort, resolve: resolveFn, tlsOptions });
    this.unsubscribe = null;
  }

  getListenAddress() {
    return this.transport.getListenAddress();
  }

  async start({ onBytes, onSocketClose } = {}) {
    if (typeof onBytes !== "function") {
      throw new Error("TcpRelayTransport.start requires onBytes function");
    }
    this.unsubscribe = this.transport.onPacket((packet) => {
      onBytes(packet.bytes, packet.meta && packet.meta.socket ? packet.meta.socket : null);
    });
    await this.transport.start({ onSocketClose });
  }

  async sendBytes(endpoint, bytes) {
    if (!endpoint || typeof endpoint !== "object") {
      throw new Error("TcpRelayTransport.sendBytes requires endpoint object");
    }
    const protocol = endpoint.tls === true ? "tls" : "tcp";
    const endpointId = `${protocol}://${endpoint.host}:${endpoint.port}`;
    await this.transport.send({ to: endpointId, bytes });
  }

  async stop() {
    if (this.unsubscribe) this.unsubscribe();
    this.unsubscribe = null;
    await this.transport.stop();
  }
}
