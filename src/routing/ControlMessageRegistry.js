/**
 * Extensible registry for control message handlers.
 *
 * SocketFrameRouter uses this to dispatch control messages that aren't part
 * of the core protocol (peer auth, inbox routing, descriptor exchange).
 * Plugins like a DHT can register handlers for custom control message types
 * (e.g. "dht.find_node", "dht.store") without modifying SocketFrameRouter.
 */
export class ControlMessageRegistry {
  #handlers = new Map();

  /**
   * Register a handler for a control message type.
   * @param {string} ctlType - e.g. "dht.find_node"
   * @param {Function} handler - async (ctlObj, socket) => void
   */
  register(ctlType, handler) {
    if (typeof ctlType !== "string" || !ctlType.trim()) {
      throw new Error("ControlMessageRegistry: ctlType must be a non-empty string");
    }
    if (typeof handler !== "function") {
      throw new Error("ControlMessageRegistry: handler must be a function");
    }
    this.#handlers.set(ctlType.trim(), handler);
  }

  /**
   * Unregister a handler for a control message type.
   * @param {string} ctlType
   */
  unregister(ctlType) {
    this.#handlers.delete(ctlType);
  }

  /**
   * Check if a handler exists for the given type.
   * @param {string} ctlType
   * @returns {boolean}
   */
  has(ctlType) {
    return this.#handlers.has(ctlType);
  }

  /**
   * Dispatch to the registered handler. Returns true if handled, false if no handler.
   * @param {string} ctlType
   * @param {object} ctlObj
   * @param {object} socket
   * @returns {Promise<boolean>}
   */
  async dispatch(ctlType, ctlObj, socket) {
    const handler = this.#handlers.get(ctlType);
    if (!handler) return false;
    await handler(ctlObj, socket);
    return true;
  }
}
