import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class ChannelOpenResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.CHANNEL_OPEN_RES;

  constructor({ channelId, code, message } = {}) {
    super();
    this.channelId = channelId == null ? "" : String(channelId);
    this.code = code == null ? null : String(code);
    this.message = message == null ? null : String(message);
    if (this.constructor === ChannelOpenResponse) this._seal();
  }

  validate() {
    this.assert(this.channelId.trim().length > 0, "channelId must be non-empty");
  }
}
