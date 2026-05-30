import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class ChannelOpenRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.CHANNEL_OPEN;

  constructor({ channelId, capabilityId } = {}) {
    super();
    this.channelId = channelId == null ? "" : String(channelId);
    this.capabilityId = capabilityId == null ? null : String(capabilityId);
    if (this.constructor === ChannelOpenRequest) this._seal();
  }

  validate() {
    this.assert(this.channelId.trim().length > 0, "channelId must be non-empty");
  }
}
