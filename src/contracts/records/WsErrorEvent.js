import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { WsErrorDetail } from "../wireRecords/WsErrorDetail.js";
import { coerceNestedRecord } from "../wireRecords/_util.js";

export class WsErrorEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.ERROR;

  constructor({ code, message, detail } = {}) {
    super();
    this.code = code == null ? "" : String(code);
    this.message = message == null ? "" : String(message);
    this.detail = detail == null
      ? undefined
      : coerceNestedRecord(detail, WsErrorDetail, "detail");
    if (this.constructor === WsErrorEvent) this._seal();
  }

  validate() {
    this.assert(this.code.trim().length > 0, "code must be non-empty");
    this.assert(this.message.trim().length > 0, "message must be non-empty");
    if (this.detail != null) this.assert(this.detail instanceof WsErrorDetail, "detail must be WsErrorDetail");
  }
}
