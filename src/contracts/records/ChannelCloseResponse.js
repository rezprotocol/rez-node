import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class ChannelCloseResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.CHANNEL_CLOSE_RES;

  constructor({ channelId, code, message } = {}) {
    super();
    this.channelId = channelId == null ? "" : String(channelId);
    this.code = code == null ? null : String(code);
    this.message = message == null ? null : String(message);
    if (this.constructor === ChannelCloseResponse) this._seal();
  }

  validate() {
    this.assert(this.channelId.trim().length > 0, "channelId must be non-empty");
  }
}
