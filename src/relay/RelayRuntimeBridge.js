/**
 * Bridge between relay runtime and outbound gateway.
 *
 * Relay runtime delegates receipt sending and route failure reporting
 * through this interface. The node layer provides the implementation
 * by calling setReceiptSender / setRouteFailedCallback after construction.
 *
 * When no callbacks are set (relay-only mode), both methods are no-ops.
 */
export class RelayRuntimeBridge {
  #receiptSender = null;
  #routeFailedCallback = null;

  /**
   * @param {{ sendToInbox: (opts: object) => Promise<any> }} sender
   */
  setReceiptSender(sender) {
    if (!sender || typeof sender.sendToInbox !== "function") {
      throw new Error("RelayRuntimeBridge.setReceiptSender requires { sendToInbox }");
    }
    this.#receiptSender = sender;
  }

  /**
   * @param {(info: { packetId: string, relayKeyId: string, reason: string }) => void} fn
   */
  setRouteFailedCallback(fn) {
    if (typeof fn !== "function") {
      throw new Error("RelayRuntimeBridge.setRouteFailedCallback requires a function");
    }
    this.#routeFailedCallback = fn;
  }

  /**
   * Send a delivery receipt via the outbound gateway.
   * No-op if no receipt sender is set (relay-only mode).
   */
  async sendReceipt(opts) {
    if (this.#receiptSender) {
      return this.#receiptSender.sendToInbox(opts);
    }
  }

  /**
   * Report a route failure to the outbound gateway.
   * No-op if no callback is set (relay-only mode).
   */
  reportRouteFailed({ packetId, relayKeyId, reason }) {
    if (this.#routeFailedCallback) {
      this.#routeFailedCallback({ packetId, relayKeyId, reason });
    }
  }

  get hasReceiptSender() {
    return this.#receiptSender !== null;
  }

  get hasRouteFailedCallback() {
    return this.#routeFailedCallback !== null;
  }
}
