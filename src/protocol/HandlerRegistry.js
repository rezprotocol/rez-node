/**
 * HandlerRegistry — replaces the 35+ if-chain in GatewaySession._handleSocketMessage.
 * Maps wire protocol type strings to { handler, method, authority } triples.
 *
 * SESSION_AUTH_V5 slice 1: registration REQUIRES an AuthorityRequirement and
 * dispatch enforces it against the session's SessionPrincipal BEFORE the
 * handler is invoked. An operation cannot become reachable without an
 * authorization classification (a missing one fails at boot), and a principal
 * of the wrong class is refused centrally — never by deep, per-handler
 * validation that could leak operation internals to an unauthorized caller.
 */
import { AuthorityRequirement } from "./AuthorityRequirement.js";
import { SessionPrincipal } from "./SessionPrincipal.js";

export class HandlerRegistry {
  #handlers = new Map();

  /**
   * Register a handler method for a protocol type string.
   * @param {string} type — wire type e.g. "mailbox.deposit"
   * @param {object} handler — handler instance (e.g. MailboxHandler)
   * @param {string} method — method name on handler (e.g. "handleDeposit")
   * @param {string} authority — AuthorityRequirement value; REQUIRED. Boot
   *   fails on a missing/invalid value so an unclassified operation can never
   *   be dispatched.
   */
  register(type, handler, method, authority) {
    if (typeof type !== "string" || type.length === 0) {
      throw new Error("HandlerRegistry.register: type must be non-empty string");
    }
    if (!handler || typeof handler[method] !== "function") {
      throw new Error(`HandlerRegistry.register: handler.${method} must be a function`);
    }
    if (!AuthorityRequirement.isValid(authority)) {
      throw new Error(`HandlerRegistry.register: ${type} must declare a valid AuthorityRequirement`);
    }
    if (this.#handlers.has(type)) {
      throw new Error(`HandlerRegistry.register: type already registered: ${type}`);
    }
    this.#handlers.set(type, { handler, method, authority });
  }

  /**
   * Dispatch a request to the registered handler, enforcing the operation's
   * declared authority against the session principal first.
   * @param {string} type — wire type
   * @param {string} requestId — request correlation id
   * @param {object} body — request body
   * @param {SessionPrincipal} principal — the session's committed principal
   * @returns {Promise<void>}
   * @throws {Error} code "UNKNOWN_TYPE" if type is not registered;
   *   code "UNAUTHORIZED" if no principal is committed;
   *   code "FORBIDDEN" if the principal's class is not admitted. The handler
   *   is provably not invoked on any of these.
   */
  async dispatch(type, requestId, body, principal) {
    const entry = this.#handlers.get(type);
    if (!entry) {
      const err = new Error(`Unknown protocol type: ${type}`);
      err.code = "UNKNOWN_TYPE";
      throw err;
    }
    if (!(principal instanceof SessionPrincipal)) {
      const err = new Error("session.hello required");
      err.code = "UNAUTHORIZED";
      throw err;
    }
    if (!AuthorityRequirement.admits(entry.authority, principal)) {
      const err = new Error(`session principal lacks required authority for ${type}`);
      err.code = "FORBIDDEN";
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

  /**
   * The declared AuthorityRequirement for a registered type, or null when the
   * type is not registered. Consumed by the architecture guardrail test that
   * pins the operation → authority matrix.
   * @param {string} type
   * @returns {string|null}
   */
  requiredAuthority(type) {
    const entry = this.#handlers.get(type);
    return entry ? entry.authority : null;
  }
}
