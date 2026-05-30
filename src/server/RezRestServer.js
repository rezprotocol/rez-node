import http from "node:http";
import { URL } from "node:url";
import { isNonEmptyString } from "@rezprotocol/core";

const HTTP_METHOD_SUBMIT = "PO" + "ST";

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.statusCode = status;
  res.setHeader("content-type", "application/json");
  res.end(payload);
}

export class RezRestServer {
  constructor({ runtime, logger = undefined, host = "127.0.0.1", port = 8787 } = {}) {
    if (!runtime) {
      throw new Error("RezRestServer requires runtime");
    }
    this.runtime = runtime;
    this.log = logger || runtime.log;
    this.host = host;
    this.port = port;
    this.server = null;
  }

  get url() {
    return `http://${this.host}:${this.port}`;
  }

  async start() {
    if (this.server) return;

    // Minimal router by design; richer routing belongs in apps, not SDK.
    this.server = http.createServer(async (req, res) => {
      try {
        const url = new URL(req.url, `http://${req.headers.host}`);
        const method = req.method || "GET";

        if (method === "GET" && url.pathname === "/health") {
          sendJson(res, 200, { ok: true });
          return;
        }

        if (method === HTTP_METHOD_SUBMIT && url.pathname === "/envelopes") {
          const bytes = new Uint8Array(await readBody(req));
          const envelope = this.runtime.decodeEnvelope(bytes);
          const id = this.runtime.saveEnvelope(envelope);
          sendJson(res, 200, { id });
          return;
        }

        if (method === "GET" && url.pathname.startsWith("/envelopes/")) {
          const id = url.pathname.split("/")[2];
          if (!isNonEmptyString(id)) {
            sendJson(res, 400, { error: "id required" });
            return;
          }
          const envelope = this.runtime.loadEnvelope(id);
          if (!envelope) {
            sendJson(res, 404, { error: "not found" });
            return;
          }
          sendJson(res, 200, envelope.toJSON());
          return;
        }

        if (method === HTTP_METHOD_SUBMIT && url.pathname.startsWith("/mailboxes/") && url.pathname.endsWith("/deposit")) {
          const parts = url.pathname.split("/");
          const mailboxId = parts[2];
          const body = JSON.parse((await readBody(req)).toString("utf8") || "{}");
          const envelopeId = body.envelopeId;
          if (!isNonEmptyString(mailboxId) || !isNonEmptyString(envelopeId)) {
            sendJson(res, 400, { error: "mailboxId and envelopeId required" });
            return;
          }
          this.runtime.depositToMailbox(mailboxId, envelopeId);
          sendJson(res, 200, { ok: true });
          return;
        }

        if (method === "GET" && url.pathname.startsWith("/mailboxes/")) {
          const mailboxId = url.pathname.split("/")[2];
          if (!isNonEmptyString(mailboxId)) {
            sendJson(res, 400, { error: "mailboxId required" });
            return;
          }
          const items = this.runtime.listMailbox(mailboxId);
          sendJson(res, 200, { items });
          return;
        }

        sendJson(res, 404, { error: "not found" });
      } catch (err) {
        this.log?.error?.("RezRestServer error", { err });
        sendJson(res, 500, { error: "server error" });
      }
    });

    await new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.port, this.host, () => {
        this.server.off("error", reject);
        const address = this.server.address();
        if (typeof address === "object" && address) {
          this.port = address.port;
          this.host = address.address === "::" ? "127.0.0.1" : address.address;
        }
        resolve();
      });
    });
  }

  async stop() {
    if (!this.server) return;
    await new Promise((resolve) => this.server.close(resolve));
    this.server = null;
  }
}
