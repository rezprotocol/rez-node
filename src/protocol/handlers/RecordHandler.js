import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

/**
 * Durable signed-record gateway handler. Bridges authenticated client
 * sessions (record.put / record.get) to the DHT durable-record store
 * (`runtime.recordDht`, a DhtNode). Generic by wire-type — no per-directive
 * facade; the client invokes it through the same generic request path as
 * mailbox.deposit.
 *
 * Records are self-authenticating (publisher-signed), so the handler does
 * NOT bind a record to the session owner — it only requires an authenticated
 * session as the node-boundary gate. Per-publisher quotas and per-peer/IP
 * rate limits live in the DHT layer.
 */
export class RecordHandler {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
  }

  #dht() {
    return this.#ctx.runtime && this.#ctx.runtime.recordDht ? this.#ctx.runtime.recordDht : null;
  }

  async handlePut(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    const dht = this.#dht();
    if (!dht || typeof dht.putRecord !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable record store unavailable", retryable: false });
      return;
    }
    const record = body && typeof body.record === "object" ? body.record : null;
    if (!record) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "record required", retryable: false });
      return;
    }
    let result;
    try {
      result = await dht.putRecord(record);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_PUT_FAILED", message: err && err.message ? err.message : "record publish failed", retryable: true });
      return;
    }
    if (!result.stored) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_REJECTED", message: "record rejected: " + result.reason, retryable: false });
      return;
    }
    this.#ctx.sendResponse(requestId, T.RECORD_PUT_RES, { localId: result.localId, replicas: result.replicas });
  }

  async handleGet(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    const dht = this.#dht();
    if (!dht || typeof dht.getRecord !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable record store unavailable", retryable: false });
      return;
    }
    const recordKind = body && typeof body.recordKind === "string" ? body.recordKind.trim() : "";
    const recordId = body && typeof body.recordId === "string" ? body.recordId.trim() : "";
    const publisherPublicKeyB64 = body && typeof body.publisherPublicKeyB64 === "string" ? body.publisherPublicKeyB64.trim() : "";
    if (!recordKind || !recordId || !publisherPublicKeyB64) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "recordKind, recordId, publisherPublicKeyB64 required", retryable: false });
      return;
    }
    let record;
    try {
      record = await dht.getRecord({ recordKind, recordId, publisherPublicKeyB64 });
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_GET_FAILED", message: err && err.message ? err.message : "record fetch failed", retryable: true });
      return;
    }
    this.#ctx.sendResponse(requestId, T.RECORD_GET_RES, { record: record || null });
  }
}
