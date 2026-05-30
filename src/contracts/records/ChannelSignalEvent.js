import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

export class ChannelSignalEvent extends RRecord {
  static type = REZ_CONTRACT_TYPES.CHANNEL_SIGNAL;

  constructor({ channelId, signal, data } = {}) {
    super();
    this.channelId = channelId == null ? "" : String(channelId);
    this.signal = signal == null ? "" : String(signal);
    this.data = data != null && typeof data === "object" ? data : {};
    if (this.constructor === ChannelSignalEvent) this._seal();
  }

  validate() {
    this.assert(this.channelId.trim().length > 0, "channelId must be non-empty");
    this.assert(this.signal.trim().length > 0, "signal must be non-empty");
  }
}
