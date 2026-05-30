import http from "node:http";
import https from "node:https";
import { URL } from "node:url";
import { RTransport, WirePacket, isNonEmptyString } from "@rezprotocol/core";

const HTTP_METHOD_SUBMIT = "PO" + "ST";

function parseMeta(headerValue) {
  if (!headerValue) return undefined;
  try {
    return JSON.parse(headerValue);
  } catch (_err) {
    return undefined;
  }
}

function headerValue(value) {
  return Array.isArray(value) ? value[0] : value;
}

export class HttpTransport extends RTransport {
  constructor({ endpointId, listenUrl, resolve } = {}) {
    super();

    if (!isNonEmptyString(endpointId)) {
      throw new Error("HttpTransport requires endpointId");
    }
    if (!isNonEmptyString(listenUrl)) {
      throw new Error("HttpTransport requires listenUrl");
    }
    if (typeof resolve !== "function") {
      throw new Error("HttpTransport requires resolve(to) function");
    }

    this.endpointId = endpointId;
    this.listenUrl = listenUrl;
    this.resolve = resolve;
    this.handlers = new Set();
    this.server = null;
    this.started = false;
  }

  get url() {
    return this.listenUrl;
  }

  onPacket(handler) {
    if (typeof handler !== "function") {
      throw new Error("HttpTransport.onPacket(handler) requires a function");
    }
    this.handlers.add(handler);
    return () => {
      this.handlers.delete(handler);
    };
  }

  async start() {
    if (this.started) return;

    const base = new URL(this.listenUrl);
    const server = http.createServer(async (req, res) => {
      if (req.method !== HTTP_METHOD_SUBMIT || req.url !== "/packet") {
        res.statusCode = 404;
        res.end();
        return;
      }

      const chunks = [];
      for await (const chunk of req) {
        chunks.push(chunk);
      }

      const bytes = new Uint8Array(Buffer.concat(chunks));
      const to = headerValue(req.headers["x-rez-to"]) || this.endpointId;
      const from = headerValue(req.headers["x-rez-from"]);
      const meta = parseMeta(headerValue(req.headers["x-rez-meta"]));
      const id = headerValue(req.headers["x-rez-id"]);

      const packet = new WirePacket({ bytes, to, from, meta, id });
      for (const handler of this.handlers) {
        await handler(packet);
      }

      res.statusCode = 204;
      res.end();
    });

    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(Number(base.port) || 0, base.hostname, () => {
        server.off("error", reject);
        const address = server.address();
        const port = typeof address === "object" && address ? address.port : base.port;
        this.listenUrl = `${base.protocol}//${base.hostname}:${port}`;
        resolve();
      });
    });

    this.server = server;
    this.started = true;
  }

  async stop() {
    if (!this.started || !this.server) return;
    await new Promise((resolve) => this.server.close(resolve));
    this.server = null;
    this.started = false;
  }

  async send(packet) {
    if (!this.started) {
      throw new Error("HttpTransport.send(packet) called before start()");
    }

    const wire = packet instanceof WirePacket ? packet : new WirePacket(packet);
    const targetBase = this.resolve(wire.to);
    if (!isNonEmptyString(targetBase)) {
      throw new Error("HttpTransport resolve(to) must return a URL string");
    }

    const target = new URL("/packet", targetBase);
    const client = target.protocol === "https:" ? https : http;

    await new Promise((resolve, reject) => {
      const req = client.request(
        target,
        {
          method: HTTP_METHOD_SUBMIT,
          headers: {
            "content-length": String(wire.bytes.length),
            "x-rez-from": wire.from || this.endpointId,
            "x-rez-to": wire.to,
            "x-rez-meta": wire.meta ? JSON.stringify(wire.meta) : "",
            "x-rez-id": wire.id || "",
          },
        },
        (res) => {
          res.on("data", () => {});
          res.on("end", resolve);
        }
      );
      req.on("error", reject);
      req.write(Buffer.from(wire.bytes));
      req.end();
    });
  }
}
