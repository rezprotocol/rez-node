import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class ChannelCloseRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.CHANNEL_CLOSE;

  constructor({ channelId } = {}) {
    super();
    this.channelId = channelId == null ? "" : String(channelId);
    if (this.constructor === ChannelCloseRequest) this._seal();
  }

  validate() {
    this.assert(this.channelId.trim().length > 0, "channelId must be non-empty");
  }
}
