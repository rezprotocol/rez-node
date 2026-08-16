/**
 * Bridge between relay runtime and outbound gateway.
 *
 * Relay runtime delegates route failure reporting through this interface.
 * The node layer provides the implementation by calling
 * setRouteFailedCallback after construction.
 *
 * When no callback is set (relay-only mode), the method is a no-op.
 *
 * The receipt-sender surface that used to live here was removed under
 * DT-005: relay-level receipts were dead code (never invoked); delivery
 * evidence is the end-to-end E2eeDeliveryAckV1 flow. See
 * rez-core/docs/RECEIPTS_AND_DELIVERY_STATES.md.
 */
export class RelayRuntimeBridge {
  #routeFailedCallback = null;

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
   * Report a route failure to the outbound gateway.
   * No-op if no callback is set (relay-only mode).
   */
  reportRouteFailed({ packetId, relayKeyId, reason }) {
    if (this.#routeFailedCallback) {
      this.#routeFailedCallback({ packetId, relayKeyId, reason });
    }
  }

  get hasRouteFailedCallback() {
    return this.#routeFailedCallback !== null;
  }
}
