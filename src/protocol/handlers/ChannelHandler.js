import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

/**
 * ChannelHandler — stub implementation.
 * All methods return NOT_IMPLEMENTED until Channel support is built.
 */
export class ChannelHandler {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
  }

  async handleOpen(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    this.#ctx.sendResponse(requestId, T.CHANNEL_OPEN_RES, {
      channelId: body && body.channelId ? body.channelId : "",
      code: "NOT_IMPLEMENTED",
      message: "Channel support is not yet available",
    });
  }

  async handleClose(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    this.#ctx.sendResponse(requestId, T.CHANNEL_CLOSE_RES, {
      channelId: body && body.channelId ? body.channelId : "",
      code: "NOT_IMPLEMENTED",
      message: "Channel support is not yet available",
    });
  }
}
