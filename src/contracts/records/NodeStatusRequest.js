import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class NodeStatusRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.NODE_STATUS;

  constructor(fields = {}) {
    super();
    if (this.constructor === NodeStatusRequest) this._seal();
  }

  validate() {
    // no required fields
  }
}
