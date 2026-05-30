import { RRecord, CONTRACT_VERSION } from "@rezprotocol/core";

function isPlainObject(value) {
  if (!value || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export class WsEnvelope extends RRecord {
  static type = "ws.envelope";

  constructor({ id, t, body, v = CONTRACT_VERSION } = {}) {
    super();
    this.id = id == null ? RRecord.newId("req") : String(id);
    this.t = t == null ? "" : String(t);
    this.body = body instanceof RRecord ? body.toJSON() : body;
    this.v = Number(v);
    if (this.constructor === WsEnvelope) this._seal();
  }

  validate() {
    this.assert(this.id.trim().length > 0, "id must be non-empty string");
    this.assert(this.t.trim().length > 0, "t must be non-empty string");
    this.assert(Number.isInteger(this.v), "v must be integer");
    this.assert(this.v >= 1, "v must be >= 1");
    this.assert(isPlainObject(this.body), "body must be plain object");
  }
}
