/**
 * Facade over GatewaySession (formerly UiProtocol) for use by domain handlers.
 * Exposes session state, service accessors, and send/emit helpers so handlers
 * stay decoupled from the protocol's internals.
 */
import { CONTRACT_VERSION, RResource } from "@rezprotocol/core";

const SESSION_BOUND_CAPABILITY = Object.freeze({ source: "session-binding" });

export class ProtocolContext {
  #protocol;
  #capabilityMiddleware = null;
  #serviceGate = null;

  /** Error code and message for "session.hello required" (SSOT). */
  static SESSION_REQUIRED_CODE = "UNAUTHORIZED";
  static SESSION_REQUIRED_MESSAGE = "session.hello required";

  constructor(protocol) {
    this.#protocol = protocol;
  }

  // --- Capability middleware (set after session.ready) ---

  get capabilityMiddleware() {
    return this.#capabilityMiddleware;
  }

  setCapabilityMiddleware(middleware) {
    this.#capabilityMiddleware = middleware;
  }

  // --- Service gate (set during bootstrap, wraps capability + pricing + settlement) ---

  get serviceGate() {
    return this.#serviceGate;
  }

  setServiceGate(gate) {
    this.#serviceGate = gate;
  }

  // --- Session state ---

  /**
   * The session's committed, frozen SessionPrincipal — null before
   * authentication completes (SESSION_AUTH_V5 slice 1). The dispatcher
   * (HandlerRegistry.dispatch) has already enforced the operation's declared
   * AuthorityRequirement against it before any handler runs; handlers consult
   * the principal only for resource-level scope and identity reads.
   */
  get principal() {
    return this.#protocol.principal;
  }

  /**
   * The ACCOUNT principal's identity key. THROWS when the session holds no
   * ACCOUNT principal — a handler on this path promised account identity, and
   * absence is a broken invariant (the dispatcher's ACCOUNT gate should have
   * refused the dispatch), never a value to soften into null/"".
   * @returns {string}
   */
  accountPublicKeyB64OrThrow() {
    const principal = this.#protocol.principal;
    if (!principal || principal.isAccount() !== true) {
      throw new Error("account principal required but session holds none");
    }
    return principal.accountPublicKeyB64;
  }

  get localInboxId() {
    return this.#protocol.localInboxId;
  }

  get ownerPublicKeyB64() {
    return this.#protocol.ownerPublicKeyB64;
  }

  get authenticated() {
    return this.#protocol.authenticated === true;
  }

  get sessionDeviceId() {
    return this.#protocol.sessionDeviceId;
  }

  /**
   * The session's verified authority from session-auth (S2.5 S7): for a PRIMARY
   * device `{ mode:"direct", signerPublicKeyB64:B }`; for a DELEGATED device
   * `{ mode:"delegated", signerPublicKeyB64:C, leafCertId, grantedCapabilities,
   * accountIdentityPublicKeyB64:B }` — the cert chain C←…←B already proved at
   * authentication time. Per-op handlers consume this to enforce dual-mode
   * authority (a delegated device's leaf cert IS its registration). `null` when
   * the session did not authenticate through the dual-mode path.
   */
  get sessionAuthority() {
    const authority = this.#protocol.sessionAuthority;
    return authority && typeof authority === "object" ? authority : null;
  }

  /**
   * Peer IP for this connection — extracted from the HTTP upgrade
   * request's socket. Used to key per-source rate limits that survive
   * `session.hello` keypair rotation (docs/SECURITY_AUDIT.md LOW-4).
   * Returns "" when no IP is available; callers treat that as "no
   * IP-keyed gate."
   */
  get peerIp() {
    const value = this.#protocol.peerIp;
    return typeof value === "string" ? value : "";
  }

  get runtime() {
    return this.#protocol.runtime;
  }

  get sessionRegistry() {
    return this.#protocol.sessionRegistry;
  }

  // --- Session inbox bindings (see docs/CAPABILITY_MODEL.md §4) ---

  /**
   * Inbox IDs this session has proven ownership of via inbox.claim. Operations
   * targeting these inboxes are authorized implicitly by the session binding
   * without an explicit cap chain on the wire.
   */
  get boundInboxIds() {
    return this.#protocol.boundInboxIds;
  }

  /**
   * Claimant pubkeys this session has proven possession of (one per call to
   * inbox.claim). Used by future cap-chain verification to determine which
   * keypairs are session-bound as authoritative.
   */
  get boundClaimantPublicKeys() {
    return this.#protocol.boundClaimantPublicKeys;
  }

  /**
   * Record that the SDK has proven ownership of an inbox claimed by the given
   * pubkey. Called by InboxClaimHandler after signature verification succeeds.
   * Idempotent.
   *
   * Also registers the session under the claimant pubkey in the session
   * registry so inbox deliveries reach this session even when the claimant
   * pubkey differs from the session-auth identity — the privacy-preserving
   * multi-key claim path described in docs/CAPABILITY_MODEL.md §8.
   */
  bindInboxToSession(inboxId, claimantPublicKeyB64) {
    if (typeof inboxId !== "string" || !inboxId.trim()) return;
    if (typeof claimantPublicKeyB64 !== "string" || !claimantPublicKeyB64.trim()) return;
    const inbox = inboxId.trim();
    const claimant = claimantPublicKeyB64.trim();
    // SESSION_AUTH_V5 §3: the principal decides which claimant roots this
    // session may bind. ACCOUNT (v4) admits any — the legacy multi-key claim
    // path is preserved this slice; a CLAIMANT principal (slice 2) admits only
    // its own trust root. A refusal here is a broken dispatch invariant, not a
    // user-visible policy answer — fail loud.
    const principal = this.#protocol.principal;
    if (!principal || principal.admitsClaimantBinding(claimant) !== true) {
      const err = new Error("session principal does not admit binding this claimant key");
      err.code = "FORBIDDEN";
      throw err;
    }
    this.#protocol.boundInboxIds.add(inbox);
    this.#protocol.boundClaimantPublicKeys.add(claimant);
    if (typeof this.#protocol._bindClaimantSession === "function") {
      this.#protocol._bindClaimantSession(claimant);
    }
  }

  /**
   * Set the session's primary inbox (the one inbox.deposit/list/fetch will
   * default to). Called by InboxClaimHandler after a successful claim.
   */
  setSessionInbox(inboxId) {
    if (typeof inboxId !== "string" || !inboxId.trim()) return;
    this.#protocol.localInboxId = inboxId.trim();
    // Bind the liveness-bus drain in lockstep with localInboxId — the instant
    // getOwnerPublicKeysByInboxId(inboxId) starts returning this session, the
    // node also starts draining cross-node pings for it. One lifecycle event,
    // so the gate signal and the bus subscription can never drift apart.
    if (typeof this.#protocol._registerLivenessInbox === "function") {
      this.#protocol._registerLivenessInbox(this.#protocol.localInboxId);
    }
  }

  /**
   * Check whether the session has proven ownership of the given inbox.
   */
  isInboxBound(inboxId) {
    if (typeof inboxId !== "string" || !inboxId.trim()) return false;
    return this.#protocol.boundInboxIds.has(inboxId.trim());
  }

  // --- Authorization (SSOT for capability + settlement resolution) ---

  /**
   * Authorize a service request.
   *
   * Two authz paths per CAPABILITY_MODEL §7/§8 and docs/SECURITY_AUDIT.md
   * HIGH-2:
   *
   *   1. **Session-binding shortcut** (free, inbox/mailbox-scoped only):
   *      if the request targets an `inbox:` or `mailbox:` resource whose ID
   *      the session has bound via `inbox.claim`, authz is granted with no
   *      cap chain. Not usable for paid services (those must present a
   *      chain so the claimant's authorization to spend is explicit).
   *
   *   2. **Capability chain**: `capabilityChain` is an array of
   *      `RCapability` instances. ServiceGate routes them through
   *      `CapabilityMiddleware.resolveChain` which validates every
   *      signature, parent-child linkage, scope narrowing, and (for
   *      inbox/mailbox resources) that the chain root signer matches the
   *      inbox claimant in `InboxClaimRegistry`. The chain primitive is
   *      what closes docs/SECURITY_AUDIT.md MED-3.
   *
   * Sends error response on failure and returns null.
   * Returns a truthy sentinel on success (the resolved capability for the
   * chain path, a session-binding sentinel for the shortcut). Callers
   * treat any truthy return as authorized; the value itself carries no
   * other semantics today.
   *
   * @param {object} opts
   * @param {string} opts.requestId — for error responses
   * @param {import("@rezprotocol/core").RCapability[]|null} [opts.capabilityChain] — cap chain from the wire
   * @param {string} opts.action — e.g. "post", "read", "write"
   * @param {string} opts.resource — e.g. "mailbox:abc123"
   * @param {string|null} [opts.presenterPublicKeyB64] — pubkey of the entity presenting the chain
   * @param {string} [opts.serviceId] — pricing service ID (omit for free services)
   * @param {object} [opts.serviceParams] — service-specific params for pricing
   * @returns {Promise<object|null>} sentinel/capability on success, null if error was sent
   */
  async authorize({
    requestId,
    capabilityChain = null,
    action,
    resource,
    serviceId = null,
    serviceParams = {},
  }) {
    const hasChain = Array.isArray(capabilityChain) && capabilityChain.length > 0;
    // Authority comes exclusively from the committed session principal.
    const principal = this.principal;
    const presenterPublicKeyB64 = principal && principal.isClaimant()
      ? principal.claimantPublicKeyB64 : this.ownerPublicKeyB64;

    // Path 1: session-binding shortcut (free inbox/mailbox ops only)
    if (!hasChain && !serviceId) {
      const parsed = parseInboxResource(resource);
      if (parsed && this.isInboxBound(parsed.id)) {
        return SESSION_BOUND_CAPABILITY;
      }
    }

    // Path 2: cap-chain validation (required when chain present OR when
    // session-binding doesn't apply OR for any paid service)
    if (!hasChain) {
      this.sendError({
        id: requestId,
        code: "FORBIDDEN",
        message: serviceId
          ? "capability chain required for paid service"
          : "capability chain required; session is not bound to this inbox",
        retryable: false,
      });
      return null;
    }

    const gate = this.#serviceGate;
    if (gate) {
      const result = await gate.authorize({
        capabilityChain,
        requiredAction: action,
        requiredResource: resource,
        presenterPublicKeyB64,
        ownerPublicKeyB64: this.ownerPublicKeyB64,
        serviceId,
        serviceParams,
      });
      if (!result.ok) {
        this.sendError({ id: requestId, code: result.code || "FORBIDDEN", message: result.error, retryable: false });
        return null;
      }
      return result.capability;
    }
    const middleware = this.#capabilityMiddleware;
    if (!middleware) {
      this.sendError({ id: requestId, code: "UNAUTHORIZED", message: "Capability middleware not initialized", retryable: false });
      return null;
    }
    const result = await middleware.resolveChain({
      capabilityChain,
      requiredAction: action,
      requiredResource: resource,
      presenterPublicKeyB64,
    });
    if (!result.ok) {
      this.sendError({ id: requestId, code: "FORBIDDEN", message: result.error, retryable: false });
      return null;
    }
    return result.capability;
  }

  // --- Service accessors (protocol-level, not chat-specific) ---

  peerLinkService() {
    return this.#protocol._peerLinkService();
  }

  // --- Send helpers ---

  sendError(opts) {
    this.#protocol._sendErrorRecord(opts);
  }

  sendFrame(frame) {
    this.#protocol._safeSendRawFrame(frame);
  }

  /**
   * Send a response frame with standard envelope (id, t, v).
   * @param {string} [requestId]
   * @param {string} type - response type string
   * @param {object} body - response body
   */
  sendResponse(requestId, type, body) {
    this.sendFrame({
      id: requestId ?? this.eventId(type),
      t: type,
      body: body ?? {},
      v: CONTRACT_VERSION,
    });
  }

  sendRecord(record, id) {
    this.#protocol._safeSendRecord(record, id);
  }

  sendRawRecord(type, opts) {
    this.#protocol._safeSendRawRecord(type, opts);
  }

  emitToOwner(owner, frame) {
    this.#protocol._emitToOwner(owner, frame);
  }

  eventId(prefix) {
    return this.#protocol._eventId(prefix);
  }
}

function parseInboxResource(resourceString) {
  if (typeof resourceString !== "string" || !resourceString) return null;
  try {
    const parsed = RResource.parse(resourceString);
    if (parsed.kind === RResource.KINDS.INBOX || parsed.kind === RResource.KINDS.MAILBOX) {
      return parsed;
    }
    return null;
  } catch {
    return null;
  }
}
