import { RRecord } from "@rezprotocol/core";

export class WsErrorDetail extends RRecord {
  constructor({ retryable = false, appContextId, messageId } = {}) {
    super();
    this.retryable = retryable === true;
    this.appContextId = appContextId == null ? null : String(appContextId);
    this.messageId = messageId == null ? null : String(messageId);
    if (this.constructor === WsErrorDetail) this._seal();
  }

  validate() {
    if (this.appContextId != null) this.assert(this.appContextId.trim().length > 0, "appContextId must be non-empty when provided");
    if (this.messageId != null) this.assert(this.messageId.trim().length > 0, "messageId must be non-empty when provided");
  }
}
