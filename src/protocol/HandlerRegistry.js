/**
 * HandlerRegistry — replaces the 35+ if-chain in GatewaySession._handleSocketMessage.
 * Maps wire protocol type strings to { handler, method } pairs.
 */
export class HandlerRegistry {
  #handlers = new Map();

  /**
   * Register a handler method for a protocol type string.
   * @param {string} type — wire type e.g. "mailbox.deposit"
   * @param {object} handler — handler instance (e.g. MailboxHandler)
   * @param {string} method — method name on handler (e.g. "handleDeposit")
   */
  register(type, handler, method) {
    if (typeof type !== "string" || type.length === 0) {
      throw new Error("HandlerRegistry.register: type must be non-empty string");
    }
    if (!handler || typeof handler[method] !== "function") {
      throw new Error(`HandlerRegistry.register: handler.${method} must be a function`);
    }
    if (this.#handlers.has(type)) {
      throw new Error(`HandlerRegistry.register: type already registered: ${type}`);
    }
    this.#handlers.set(type, { handler, method });
  }

  /**
   * Dispatch a request to the registered handler.
   * @param {string} type — wire type
   * @param {string} requestId — request correlation id
   * @param {object} body — request body
   * @returns {Promise<void>}
   * @throws {Error} if type is not registered
   */
  async dispatch(type, requestId, body) {
    const entry = this.#handlers.get(type);
    if (!entry) {
      const err = new Error(`Unknown protocol type: ${type}`);
      err.code = "UNKNOWN_TYPE";
      throw err;
    }
    await entry.handler[entry.method](requestId, body);
  }

  /**
   * Check if a type is registered.
   * @param {string} type
   * @returns {boolean}
   */
  has(type) {
    return this.#handlers.has(type);
  }

  /**
   * List all registered type strings.
   * @returns {string[]}
   */
  listTypes() {
    return Array.from(this.#handlers.keys()).sort();
  }
}
