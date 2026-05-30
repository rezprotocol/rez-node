import { RRecord } from "@rezprotocol/core";

export class RouteReplyV1 extends RRecord {
  static type = "route.reply.v1";

  constructor({
    queryId,
    targetHandle,
    responderNodeId,
    found = false,
    nextHopNodeId = "",
    nextHopUrl = "",
    path = [],
    cacheTtlMs = 60_000,
    createdAtMs = Date.now(),
  } = {}) {
    super();
    this.queryId = queryId == null ? "" : String(queryId).trim();
    this.targetHandle = targetHandle == null ? "" : String(targetHandle).trim();
    this.responderNodeId = responderNodeId == null ? "" : String(responderNodeId).trim();
    this.found = found === true;
    this.nextHopNodeId = nextHopNodeId == null ? "" : String(nextHopNodeId).trim();
    this.nextHopUrl = nextHopUrl == null ? "" : String(nextHopUrl).trim();
    this.path = Array.isArray(path) ? path.map((value) => String(value || "").trim()).filter(Boolean) : [];
    this.cacheTtlMs = Number(cacheTtlMs);
    this.createdAtMs = Number(createdAtMs);
    if (this.constructor === RouteReplyV1) this._seal();
  }

  validate() {
    this.assert(this.queryId.length >= 8 && this.queryId.length <= 256, "queryId must be 8..256 chars");
    this.assert(this.targetHandle.length >= 16 && this.targetHandle.length <= 512, "targetHandle must be 16..512 chars");
    this.assert(this.responderNodeId.length >= 1 && this.responderNodeId.length <= 256, "responderNodeId must be 1..256 chars");
    this.assert(typeof this.found === "boolean", "found must be boolean");
    if (this.found) {
      this.assert(this.nextHopNodeId.length >= 1 && this.nextHopNodeId.length <= 256, "nextHopNodeId must be 1..256 chars when found=true");
      this.assert(this.nextHopUrl.length >= 1 && this.nextHopUrl.length <= 2048, "nextHopUrl must be 1..2048 chars when found=true");
    }
    this.assert(Array.isArray(this.path), "path must be array");
    this.assert(this.path.length <= 256, "path must be <= 256");
    this.assert(Number.isFinite(this.cacheTtlMs) && this.cacheTtlMs >= 0, "cacheTtlMs must be >= 0");
    this.assert(Number.isFinite(this.createdAtMs), "createdAtMs must be finite");
  }
}
