import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class NodeStatusResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.NODE_STATUS_RES;

  constructor({ accountId, meshEnabled, meshMode, peerCount, uptimeMs } = {}) {
    super();
    this.accountId = accountId == null ? null : String(accountId);
    this.meshEnabled = meshEnabled === true;
    this.meshMode = meshMode == null ? null : String(meshMode);
    this.peerCount = peerCount == null ? 0 : Number(peerCount);
    this.uptimeMs = uptimeMs == null ? null : Number(uptimeMs);
    if (this.constructor === NodeStatusResponse) this._seal();
  }

  validate() {
    if (this.peerCount != null) this.assert(Number.isInteger(this.peerCount) && this.peerCount >= 0, "peerCount must be non-negative integer");
  }
}
