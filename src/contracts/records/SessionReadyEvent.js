import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { SessionCapabilities } from "../wireRecords/SessionCapabilities.js";
import { coerceNestedRecord } from "../wireRecords/_util.js";

export class SessionReadyEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.SESSION_READY;

  constructor({ serverTime, capabilities } = {}) {
    super();
    this.serverTime = Number(serverTime);
    this.capabilities = capabilities == null
      ? undefined
      : coerceNestedRecord(capabilities, SessionCapabilities, "capabilities");
    if (this.constructor === SessionReadyEvent) this._seal();
  }

  validate() {
    this.assert(Number.isFinite(this.serverTime), "serverTime must be a number");
    this.assert(Number.isInteger(this.serverTime), "serverTime must be integer epoch milliseconds");
    this.assert(this.serverTime > 0, "serverTime must be epoch milliseconds");
    if (this.capabilities != null) this.assert(this.capabilities instanceof SessionCapabilities, "capabilities must be SessionCapabilities");
  }
}
