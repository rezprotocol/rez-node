import { RRecord } from "@rezprotocol/core";

export class RouteQueryV1 extends RRecord {
  static type = "route.query.v1";

  constructor({
    queryId,
    targetHandle,
    requesterNodeId,
    ttl = 3,
    createdAtMs = Date.now(),
    visited = [],
  } = {}) {
    super();
    this.queryId = queryId == null ? "" : String(queryId).trim();
    this.targetHandle = targetHandle == null ? "" : String(targetHandle).trim();
    this.requesterNodeId = requesterNodeId == null ? "" : String(requesterNodeId).trim();
    this.ttl = Number.isInteger(ttl) ? ttl : 0;
    this.createdAtMs = Number(createdAtMs);
    this.visited = Array.isArray(visited)
      ? visited.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    if (this.constructor === RouteQueryV1) this._seal();
  }

  validate() {
    this.assert(this.queryId.length >= 8 && this.queryId.length <= 256, "queryId must be 8..256 chars");
    this.assert(this.targetHandle.length >= 16 && this.targetHandle.length <= 512, "targetHandle must be 16..512 chars");
    this.assert(this.requesterNodeId.length >= 1 && this.requesterNodeId.length <= 256, "requesterNodeId must be 1..256 chars");
    this.assert(Number.isInteger(this.ttl) && this.ttl >= 0 && this.ttl <= 255, "ttl must be 0..255");
    this.assert(Number.isFinite(this.createdAtMs), "createdAtMs must be finite");
    this.assert(Array.isArray(this.visited), "visited must be array");
    this.assert(this.visited.length <= 256, "visited must be <= 256");
  }
}
