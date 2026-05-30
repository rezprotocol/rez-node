import { RRecord } from "@rezprotocol/core";

function isB64(value) {
  return /^[A-Za-z0-9+/=]+$/.test(value);
}

export class RouteEnvelopeV1 extends RRecord {
  static type = "route.envelope.v1";

  constructor({
    packetId,
    targetHandle,
    payloadB64,
    ttl = 8,
    originNodeId = "",
    hops = [],
    createdAtMs = Date.now(),
  } = {}) {
    super();
    this.packetId = packetId == null ? "" : String(packetId).trim();
    this.targetHandle = targetHandle == null ? "" : String(targetHandle).trim();
    this.payloadB64 = payloadB64 == null ? "" : String(payloadB64).trim();
    this.ttl = Number.isInteger(ttl) ? ttl : 0;
    this.originNodeId = originNodeId == null ? "" : String(originNodeId).trim();
    this.hops = Array.isArray(hops) ? hops.map((item) => String(item || "").trim()).filter(Boolean) : [];
    this.createdAtMs = Number(createdAtMs);
    if (this.constructor === RouteEnvelopeV1) this._seal();
  }

  validate() {
    this.assert(this.packetId.length >= 8 && this.packetId.length <= 256, "packetId must be 8..256 chars");
    this.assert(this.targetHandle.length >= 16 && this.targetHandle.length <= 512, "targetHandle must be 16..512 chars");
    this.assert(this.payloadB64.length > 0 && this.payloadB64.length <= 1_000_000, "payloadB64 must be 1..1000000 chars");
    this.assert(this.payloadB64.length % 4 === 0, "payloadB64 must be valid base64 length");
    this.assert(isB64(this.payloadB64), "payloadB64 must be base64");
    this.assert(Number.isInteger(this.ttl) && this.ttl >= 0 && this.ttl <= 255, "ttl must be 0..255");
    this.assert(this.originNodeId.length >= 1 && this.originNodeId.length <= 256, "originNodeId must be 1..256 chars");
    this.assert(Array.isArray(this.hops), "hops must be array");
    this.assert(this.hops.length <= 256, "hops must be <= 256 entries");
    this.assert(Number.isFinite(this.createdAtMs), "createdAtMs must be finite");
  }
}
