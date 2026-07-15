import { randomBytes } from "node:crypto";
import { assertContractTree, base64ToBytes, bytesToBase64, CONTRACT_VERSION, REZ_CONTRACT_TYPES, verifyAccountAuthority, DeviceRegistrationV1 } from "@rezprotocol/core";
import { createJsonFrameCodec } from "../network/ws/index.js";
import { WsErrorEvent } from "../contracts/records/WsErrorEvent.js";
import { WsErrorDetail } from "../contracts/wireRecords/WsErrorDetail.js";
import { ProtocolContext } from "./ProtocolContext.js";
import { HandlerRegistry } from "./HandlerRegistry.js";
import { CapabilityMiddleware } from "./CapabilityMiddleware.js";
import { MailboxHandler } from "./handlers/MailboxHandler.js";
import { ChannelHandler } from "./handlers/ChannelHandler.js";
import { InboxClaimHandler } from "./handlers/InboxClaimHandler.js";
import { DepositPolicyHandler } from "./handlers/DepositPolicyHandler.js";
import { DeviceHandler } from "./handlers/DeviceHandler.js";
import { MeshStatusHandler } from "./handlers/MeshStatusHandler.js";
import { RecordHandler } from "./handlers/RecordHandler.js";
import { AccountMutationHandler } from "./handlers/AccountMutationHandler.js";
import { AccountDeviceBundleHandler } from "./handlers/AccountDeviceBundleHandler.js";
import { normalizeFrameShape } from "./protocolWireUtils.js";
import { handleSessionHello, buildAuthenticatedSession } from "./sessionBootstrap.js";
import { buildMailboxDepositedFrame, outerPacketBodyB64 } from "./mailboxDepositedFrame.js";
import { FloodGate } from "../network/ws/FloodGate.js";
import { SlidingWindowRateLimiter } from "../util/SlidingWindowRateLimiter.js";
import { peerIpKey } from "../util/peerIpKey.js";
import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { canonicalJSONStringify } from "../util/canonicalize.js";
import { toClientSafeError } from "./clientSafeErrors.js";
import { CapabilityValidator } from "@rezprotocol/core";
import { ServiceGate } from "../settlement/ServiceGate.js";
import { HandleHandler } from "../handle/HandleHandler.js";

const T = REZ_CONTRACT_TYPES;
const QUEUE_MAX = 256;
const SESSION_CHALLENGE_TYPE = T.SESSION_CHALLENGE;
const SESSION_AUTHENTICATE_TYPE = T.SESSION_AUTHENTICATE;
const SESSION_AUTH_CHALLENGE_TTL_MS = 60_000;
const RANDOM_ID_BYTES = 4;
const INBOUND_FLOOD_GATE = new FloodGate({
  perConnRate: 100,
  perConnBurst: 200,
  globalRate: 1000,
  globalBurst: 2000,
});
const SESSION_AUTH_CRYPTO = new NodeCryptoProvider();

// Bounds on the per-session serialized message backlog. Admission runs synchronously at
// arrival (flood gate + these caps) so the queue can never become an unbounded pre-rate-limit
// buffer; a client that outpaces the serialized head is closed for backpressure.
//   MAX_PENDING_FRAMES  — round-7 finding 1: frame-count cap (above the flood-gate burst so
//                         normal bursts are never penalized).
//   *_QUEUED_BYTES      — round-8 finding 1: BYTE caps (per-session + process-wide) so a few
//                         large (~1 MiB) frames cannot retain hundreds of MiB behind a blocked
//                         head. The queue is an explicit, CLEARABLE array (not an irrevocable
//                         promise chain), so stop() releases the retained bytes at once.
const MAX_PENDING_FRAMES = 512;
const MAX_SESSION_QUEUED_BYTES = 8 * 1024 * 1024; // 8 MiB backlog per session
const MAX_PROCESS_QUEUED_BYTES = 256 * 1024 * 1024; // 256 MiB backlog across all sessions
let PROCESS_QUEUED_BYTES = 0; // module-wide queued-bytes accounting (all live sessions)

/**
 * Per-IP `session.hello` rate limiter (docs/SECURITY_AUDIT.md pass-1
 * LOW: "session.hello accepts any well-formed pubkey and the node has
 * no rate-limit on session.hello per-IP"). LOW-4 mitigates the
 * rotation-evasion vector at the deposit layer; this is the cleaner
 * upstream cap.
 *
 * Process-wide singleton so concurrent connections from the same source
 * IP share the budget. The cap is generous enough that legitimate
 * electron-dev reconnects don't trip it (1/sec average sustained) but
 * tight enough that keypair-rotation floods get throttled at the source.
 *
 * Empty `peerIp` skips the gate — tests with synthetic sockets and any
 * connection where the IP can't be extracted are unaffected.
 */
const SESSION_HELLO_RATE_LIMITER = new SlidingWindowRateLimiter({
  windowMs: 60_000,
  maxAttempts: 60,
});

export { SESSION_HELLO_RATE_LIMITER };

function randomHex(bytes = RANDOM_ID_BYTES) {
  return Buffer.from(randomBytes(bytes)).toString("hex");
}

/**
 * Extract the peer IP address for this WS connection. Used to key
 * per-source rate limits that survive `session.hello` keypair rotation
 * (docs/SECURITY_AUDIT.md LOW-4). Returns "" when the IP can't be
 * determined — callers must treat empty as "no IP-keyed gate," not as
 * a default identity to bucket.
 */
function extractPeerIp(request) {
  if (!request) return "";
  // The HTTP upgrade socket carries the real peer address. We
  // intentionally do NOT consult `x-forwarded-for` here — without a
  // trusted-proxy allowlist that header is attacker-controlled and
  // would let any client spoof its IP for rate-limit evasion.
  //
  // SECURITY_AUDIT MED-14: IPv6 source addresses are truncated to /64
  // before keying the rate limiter — a /128-keyed limiter is trivially
  // bypassable by rotating the lower 64 bits, which cost a single
  // subscriber nothing.
  const sock = request.socket;
  if (!sock) return "";
  const addr = typeof sock.remoteAddress === "string" ? sock.remoteAddress.trim() : "";
  return peerIpKey(addr);
}

function signedPayloadBytes(payload) {
  return new TextEncoder().encode(canonicalJSONStringify(payload));
}

/**
 * GatewaySession — per-connection WebSocket protocol handler.
 *
 * Handles session authentication (hello / challenge / authenticate),
 * dispatches requests via HandlerRegistry, and manages connection lifecycle.
 *
 * Renamed from UiProtocol — has nothing to do with UI.
 */
export class GatewaySession {
  constructor({ runtime, ws, request = null, sessionRegistry = null, nodeEnabled = true } = {}) {
    if (!runtime) throw new Error("runtime required");
    if (!ws) throw new Error("ws required");

    this.runtime = runtime;
    this.ws = ws;
    this.request = request;
    this.sessionRegistry = sessionRegistry;
    this.peerIp = extractPeerIp(request);
    this._nodeEnabled = nodeEnabled !== false;
    this.clientId = `gw_${Date.now()}_${randomHex()}`;
    this.localInboxId = null;
    this.sessionDeviceId = null;
    this.ownerPublicKeyB64 = null;
    this.authenticated = false;
    // Liveness-bus drain subscription (pg + redis): set when this session binds
    // an inbox, torn down on close. Stored so register and unregister key on the
    // SAME inbox and can never drift from the sessionRegistry membership.
    this._livenessInboxId = null;
    this._livenessUnregister = null;
    // Inbox-ownership bindings established via inbox.claim (proof of claimant
    // privkey). Owner-scoped requests on a bound inbox are authorized by
    // session binding without an explicit cap chain. See docs/CAPABILITY_MODEL.md §4.
    /** @type {Set<string>} */
    this.boundInboxIds = new Set();
    /** @type {Set<string>} */
    this.boundClaimantPublicKeys = new Set();
    // Claimant pubkeys we've registered with sessionRegistry for inbox
    // delivery routing. Distinct from session-auth identity because a
    // single session may claim multiple inboxes under unlinked keys
    // (privacy primitive — see docs/CAPABILITY_MODEL.md §8).
    /** @type {Set<string>} */
    this._boundClaimantRegistrations = new Set();
    this._isRegistered = false;
    this.queueLen = 0;
    this._pendingSessionAuth = null;
    // Serializes session authentication: WS message callbacks are not ordered, so
    // this guards a one-time challenge from being consumed by two concurrent
    // authenticate frames (audit R4 L2c review round-7 P2).
    this._sessionAuthInFlight = false;
    this._inboundFloodStrikes = 0;
    // Per-dispatch epoch fast-path watermark for a DELEGATED session (review finding 1): the
    // authority epoch this session was last verified against. null ⇒ not a delegated session (or
    // not yet admitted) ⇒ the guard always takes the full re-verify path.
    this._admittedAuthorityEpoch = null;
    this._stopped = false;
    this._intakeClosed = false; // latched on the first terminal flood/backpressure response
    this._msgQueue = [];        // [{ data, size }] — CLEARABLE serialized intake queue
    this._queuedBytes = 0;      // bytes of frames still WAITING in _msgQueue
    this._inFlightBytes = 0;    // bytes of the frame currently in _handleSocketMessage (0 or one)
    this._draining = false;
    this._frameCodec = createJsonFrameCodec();
    this._ctx = new ProtocolContext(this);

    // --- Handler instances ---
    // Relay-level handlers (always available)
    this._mailboxHandler = new MailboxHandler(this._ctx);
    this._channelHandler = new ChannelHandler(this._ctx);
    this._inboxClaimHandler = new InboxClaimHandler(this._ctx);
    this._depositPolicyHandler = new DepositPolicyHandler(this._ctx);
    this._deviceHandler = new DeviceHandler(this._ctx);

    // Handle handler (always available — relay-level service)
    this._handleHandler = new HandleHandler(this._ctx);

    // Durable signed-record handler (relay-level; reaches runtime.recordDht)
    this._recordHandler = new RecordHandler(this._ctx);

    // Node-level handlers (only when node is enabled)
    this._meshStatusHandler = this._nodeEnabled ? new MeshStatusHandler(this._ctx) : null;

    // Serialized account device-mutation authority (S2.5 S11). Node/pg only —
    // the handler answers SERVICE_UNAVAILABLE when runtime.accountMutationSerializer
    // is null (fs/desktop).
    this._accountMutationHandler = this._nodeEnabled ? new AccountMutationHandler(this._ctx) : null;

    // Home-aggregated per-device prekey bundle serve (S2.5 S12). Node/pg only —
    // SERVICE_UNAVAILABLE when runtime.accountDeviceBundleStore is null.
    this._accountDeviceBundleHandler = this._nodeEnabled ? new AccountDeviceBundleHandler(this._ctx) : null;

    // --- Handler registry ---
    this._registry = new HandlerRegistry();
    this._registerHandlers();

    // Audit R4 F3-remediation round-6 finding 4: WS message callbacks are not ordered, so
    // several frames could pass the per-request authority guard concurrently and then all
    // execute after a revocation commits. Serialize message handling PER SESSION (via an
    // explicit clearable queue drained one at a time) — each frame's guard + dispatch
    // completes before the next begins — so once a revoke commits, the very next frame's
    // guard observes it. Admission (flood gate + count/byte caps) runs synchronously at
    // ARRIVAL in _enqueueMessage, before a frame is queued.
    this._onSocketMessage = (data) => this._enqueueMessage(data);
    this._onSocketClose = () => this.stop();
    this._onSocketError = () => {
      // best effort
    };
  }

  _registerHandlers() {
    const r = this._registry;

    // Mailbox
    r.register(T.MAILBOX_DEPOSIT, this._mailboxHandler, "handleDeposit");
    r.register(T.MAILBOX_LIST, this._mailboxHandler, "handleList");
    r.register(T.MAILBOX_FETCH, this._mailboxHandler, "handleFetch");
    r.register(T.MAILBOX_ACK, this._mailboxHandler, "handleAck");
    r.register(T.MAILBOX_CURSOR_ACK, this._mailboxHandler, "handleCursorAck");

    // Inbox claim (open registration)
    r.register(T.INBOX_CLAIM, this._inboxClaimHandler, "handleClaim");

    // Per-device home binding (S2.5 Slice 4)
    r.register(T.DEVICE_BIND, this._deviceHandler, "handleBind");
    r.register(T.DEVICE_REVOKE, this._deviceHandler, "handleRevoke");

    // Inbox deposit policy (claimant publishes blocklist/allowlist)
    r.register(T.INBOX_SET_DEPOSIT_POLICY, this._depositPolicyHandler, "handleSet");

    // Channel (stub)
    r.register(T.CHANNEL_OPEN, this._channelHandler, "handleOpen");
    r.register(T.CHANNEL_CLOSE, this._channelHandler, "handleClose");

    // Handle
    r.register(T.HANDLE_REGISTER, this._handleHandler, "handleRegister");
    r.register(T.HANDLE_RESOLVE, this._handleHandler, "handleResolve");
    r.register(T.HANDLE_RELEASE, this._handleHandler, "handleRelease");

    // Durable signed-record store (publish/fetch over the DHT overlay)
    r.register(T.RECORD_PUT, this._recordHandler, "handlePut");
    r.register(T.RECORD_GET, this._recordHandler, "handleGet");

    // Node-level handlers — only when node is enabled
    if (this._nodeEnabled) {
      r.register(T.NODE_STATUS, this._meshStatusHandler, "handleMeshStatus");

      // Serialized device add/revoke + authority-state serve (S2.5 S11, pg only)
      r.register(T.ACCOUNT_DEVICE_MUTATION_SUBMIT, this._accountMutationHandler, "handleSubmit");
      r.register(T.ACCOUNT_AUTHORITY_STATE_GET, this._accountMutationHandler, "handleGetAuthorityState");

      // Home-aggregated per-device bundle publish + device-set serve (S2.5 S12, pg only)
      r.register(T.ACCOUNT_DEVICE_BUNDLE_PUBLISH, this._accountDeviceBundleHandler, "handlePublish");
      r.register(T.ACCOUNT_DEVICE_SET_GET, this._accountDeviceBundleHandler, "handleGetDeviceSet");
    }
  }

  start() {
    this.ws.on("message", this._onSocketMessage);
    this.ws.on("close", this._onSocketClose);
    this.ws.on("error", this._onSocketError);
  }

  stop() {
    this._stopped = true; // round-7 finding 1: discard any queued/arriving frames
    this._clearQueue(); // round-8 finding 1: release retained frame bytes at once
    INBOUND_FLOOD_GATE.release(this.clientId); // round-8 finding 4: drop this conn's flood bucket
    this._unbindOwnerSession();
    this._pendingSessionAuth = null;
    this._sessionAuthInFlight = false;
    this.ws.off("message", this._onSocketMessage);
    this.ws.off("close", this._onSocketClose);
    this.ws.off("error", this._onSocketError);
  }

  close() {
    this.ws.close();
  }

  // --- Inbound message dispatch ---

  #frameSize(data) {
    if (data && typeof data.length === "number") return data.length;
    if (data && typeof data.byteLength === "number") return data.byteLength;
    return Buffer.byteLength(String(data));
  }

  /**
   * Synchronous ARRIVAL admission (round-7 finding 1 + round-8 findings 1/3) before a frame is
   * queued: latch-discard after an intake-terminating decision; inbound flood gate
   * (arrival-rate, not dequeue-rate); frame-count AND byte caps (per-session + process-wide),
   * closing the socket for backpressure when exceeded. Admitted frames go on a clearable queue.
   */
  _enqueueMessage(data) {
    if (this._stopped || this._intakeClosed) return undefined; // finding 3: latched — silent discard
    if (!INBOUND_FLOOD_GATE.allow(this.clientId)) {
      this._inboundFloodStrikes += 1;
      this._sendErrorRecord({ id: null, code: "RATE_LIMITED", message: "Inbound flood detected", retryable: false });
      if (this._inboundFloodStrikes >= 3) {
        this._closeIntake(1013, "rate_limited");
      }
      return undefined;
    }
    this._inboundFloodStrikes = 0;
    const size = this.#frameSize(data);
    // Round-9 finding: the per-session budget counts QUEUED + IN-FLIGHT bytes, and the
    // process-wide counter stays charged through handler settlement (below), so a frame that
    // is being processed (or is blocked in its handler) is NOT invisible to either cap — many
    // connections each parking one max-sized in-flight frame can no longer bypass the budget.
    if (this._msgQueue.length >= MAX_PENDING_FRAMES
        || this._queuedBytes + this._inFlightBytes + size > MAX_SESSION_QUEUED_BYTES
        || PROCESS_QUEUED_BYTES + size > MAX_PROCESS_QUEUED_BYTES) {
      this._closeIntake(1013, "backpressure");
      return undefined;
    }
    this._msgQueue.push({ data, size });
    this._queuedBytes += size;
    PROCESS_QUEUED_BYTES += size;
    return this._drainQueue();
  }

  async _drainQueue() {
    if (this._draining) return;
    this._draining = true;
    try {
      while (this._msgQueue.length > 0 && !this._stopped) {
        const { data, size } = this._msgQueue.shift();
        // Move queued -> in-flight; the PROCESS charge is UNCHANGED (round-9 finding: the
        // in-flight frame stays counted). It is released only after the handler settles.
        this._queuedBytes -= size;
        this._inFlightBytes += size;
        try {
          await this._handleSocketMessage(data);
        } catch (err) {
          console.error("[GatewaySession] serialized message handling error: " + (err && err.message ? err.message : err));
        } finally {
          this._inFlightBytes -= size;
          PROCESS_QUEUED_BYTES -= size;
          if (PROCESS_QUEUED_BYTES < 0) PROCESS_QUEUED_BYTES = 0;
        }
      }
    } finally {
      this._draining = false;
    }
  }

  // Latch intake closed, release the queued bytes, and close the socket exactly once
  // (round-8 finding 3: no per-frame error/close amplification once we've decided to terminate).
  _closeIntake(code, reason) {
    if (this._intakeClosed) return;
    this._intakeClosed = true;
    this._clearQueue();
    try {
      this.ws.close(code, reason);
    } catch (closeErr) {
      console.error("[GatewaySession] ws close on intake termination failed: " + (closeErr && closeErr.message ? closeErr.message : closeErr));
    }
  }

  // Release the QUEUED (not-yet-started) frame bytes at once (round-8 finding 1: a clearable
  // queue, unlike an irrevocable promise chain). The IN-FLIGHT frame is intentionally left
  // charged (round-9 finding): its handler's finally releases its charge on settlement, so a
  // stopped session's stuck head keeps counting against the process cap until it completes (a
  // handler timeout would make that release deterministic; a hung handler is a separate bug and
  // the cap correctly holds the charge meanwhile).
  _clearQueue() {
    PROCESS_QUEUED_BYTES -= this._queuedBytes;
    if (PROCESS_QUEUED_BYTES < 0) PROCESS_QUEUED_BYTES = 0;
    this._queuedBytes = 0;
    this._msgQueue.length = 0;
  }

  async _handleSocketMessage(data) {
    // NOTE: inbound flood admission + the count/byte caps run SYNCHRONOUSLY in the arrival
    // path (_enqueueMessage), BEFORE this handler is dequeued — rounds 7/8 finding 1 — so the
    // serialized queue is bounded by frames AND bytes. By the time this runs the frame has
    // already been admitted.
    const rawText = data.toString("utf8");
    let requestId;
    let requestType;
    let requestBody;
    try {
      const decoded = this._frameCodec.decodeFrame(rawText);
      requestId = decoded.id;
      requestType = decoded.type;
      requestBody = decoded.body;
      const version = decoded.version;
      if (version !== undefined && version !== CONTRACT_VERSION) {
        this._sendErrorRecord({
          id: requestId,
          code: "BAD_VERSION",
          message: `Unsupported contract version ${version}, expected ${CONTRACT_VERSION}`,
          retryable: false,
        });
        this.ws.close();
        return;
      }
    } catch {
      this._sendErrorRecord({
        id: null,
        code: "BAD_REQUEST",
        message: "Invalid JSON",
        retryable: false,
      });
      return;
    }

    try {
      // --- Session authentication ---
      if (requestType === T.SESSION_HELLO) {
        // Per-IP rate limit on session.hello — closes the LOW
        // observation in docs/SECURITY_AUDIT.md (pass 1) flagged as a
        // defense-in-depth gap LOW-4 only partially mitigated. An
        // attacker rotating keypairs to evade the deposit blocklist
        // still has to pay an IP-cost to even reach session.hello.
        if (this.peerIp && !SESSION_HELLO_RATE_LIMITER.record(this.peerIp, Date.now())) {
          this._sendErrorRecord({
            id: requestId,
            code: "RATE_LIMITED",
            message: "session.hello rate limit exceeded",
            retryable: true,
          });
          try {
            this.ws.close(1013, "rate_limited");
          } catch (closeErr) {
            console.error("[GatewaySession] ws close failed on rate-limited session.hello: " + (closeErr && closeErr.message ? closeErr.message : closeErr));
          }
          return;
        }
        const result = handleSessionHello({ body: requestBody });
        if (result.error) {
          this._sendErrorRecord({
            id: requestId,
            code: result.error.code,
            message: result.error.message,
            retryable: result.error.retryable ?? false,
          });
          if (result.error.close) {
            try {
              this.ws.close();
            } catch (closeErr) {
              console.error("[GatewaySession] ws close failed after rejected session.hello: " + (closeErr && closeErr.message ? closeErr.message : closeErr));
            }
          }
          return;
        }
        await this._beginSessionAuthentication(result.pendingAuthentication, requestId);
        return;
      }

      if (requestType === SESSION_AUTHENTICATE_TYPE) {
        await this._handleSessionAuthenticate(requestId, requestBody);
        return;
      }

      // --- Heartbeat (SDK WsTransport sends periodic pings for keepalive) ---
      // Reply with pong so the client can detect dead connections (e.g. cable pull
      // where no TCP FIN/RST arrives and socket events never fire).
      if (requestType === "ping") {
        if (this.ws.readyState === this.ws.OPEN) {
          try {
            this.ws.send(JSON.stringify({
              id: requestId,
              type: "pong",
              t: "pong",
              v: CONTRACT_VERSION,
              body: {},
            }));
          } catch (pingErr) {
            console.error("[GatewaySession] pong send failed, closing connection: " + (pingErr && pingErr.message ? pingErr.message : pingErr));
            try { this.ws.close(1011, "pong_send_failed"); } catch { /* already closing */ }
          }
        }
        return;
      }

      // --- Session guard for all other types ---
      if (!this.authenticated) {
        this._sendErrorRecord({
          id: requestId,
          code: ProtocolContext.SESSION_REQUIRED_CODE,
          message: ProtocolContext.SESSION_REQUIRED_MESSAGE,
          retryable: false,
        });
        return;
      }

      // --- Per-request authority guard for DELEGATED sessions (audit R4 F3-remediation
      // round-5 finding 1 + round-6 finding 1, L5) ---
      // The connect-time authority proof goes stale if the device OR any cert in its chain
      // is revoked WHILE this socket stays open; the dispatcher would otherwise keep
      // forwarding privileged ops (peerLink.create / deviceSet.publish / device mutations)
      // until reconnect. Re-check BOTH the terminal device status AND the cert chain against
      // the home's current revocation state on every delegated request. Direct (primary /
      // account-root) sessions are unrevocable and skip this.
      if (this.sessionAuthority && typeof this.sessionAuthority === "object" && this.sessionAuthority.mode === "delegated") {
        let stillAuthorized;
        try {
          stillAuthorized = await this._delegatedSessionStillAuthorized();
        } catch (guardErr) {
          // Review finding 4: the revocation backend is unavailable — we cannot PROVE the session
          // is still authorized, but this is not a revocation. Fail SOFT: reject THIS request as
          // retryable and leave the socket OPEN (no privileged op is dispatched, so nothing runs
          // unauthorized). A definitive revoked verdict below still closes the socket terminally.
          if (guardErr && guardErr.code === "REVOCATION_BACKEND_UNAVAILABLE") {
            this._sendErrorRecord({
              id: requestId,
              code: "SERVICE_UNAVAILABLE",
              message: "authority revocation state is temporarily unavailable",
              retryable: true,
            });
            return;
          }
          throw guardErr;
        }
        if (stillAuthorized !== true) {
          this._sendErrorRecord({
            id: requestId,
            code: "UNAUTHORIZED",
            message: "session authority has been revoked",
            retryable: false,
          });
          try {
            this.ws.close(1008, "authority_revoked");
          } catch (closeErr) {
            console.error("[GatewaySession] close after authority-dispatch-guard failed: " + (closeErr && closeErr.message ? closeErr.message : closeErr));
          }
          return;
        }
      }

      // --- HandlerRegistry dispatch ---
      await this._registry.dispatch(requestType, requestId, requestBody);
    } catch (err) {
      const errCode = err && typeof err.code === "string" ? err.code : "";
      const code = errCode === "UNKNOWN_TYPE" ? "UNKNOWN_TYPE"
        : errCode === "FORBIDDEN" ? "FORBIDDEN"
        : "INTERNAL";
      this._sendErrorRecord({
        id: requestId,
        code,
        message: (err && err.message) || "Internal error",
        retryable: false,
      });
    }
  }

  // --- Session auth ---

  async _adoptAuthenticatedSession(result, requestId) {
    this.sessionDeviceId = result.sessionDeviceId;
    this.ownerPublicKeyB64 = result.accountIdentityPublicKeyB64 || null;
    this.authenticated = true;
    this._bindOwnerSession(this.ownerPublicKeyB64);

    // After v1 cap rework the node is a verifier, not a signer — no session
    // capabilities are minted here. Operations are authorized via the
    // session-binding shortcut (inbox.claim) or via inbox-owner-signed cap
    // chains attached to requests (CapabilityMiddleware.resolveChain). See
    // docs/SECURITY_AUDIT.md MED-3 / HIGH-6.
    const middleware = new CapabilityMiddleware({
      validator: new CapabilityValidator({ crypto: SESSION_AUTH_CRYPTO }),
      inboxClaimRegistry: this.runtime && this.runtime.inboxClaimRegistry ? this.runtime.inboxClaimRegistry : null,
    });
    this._ctx.setCapabilityMiddleware(middleware);

    if (this.runtime.settlement) {
      const gate = new ServiceGate({
        capabilityMiddleware: middleware,
        pricingResolver: this.runtime.settlement.pricing,
        settlementProvider: this.runtime.settlement.provider,
      });
      this._ctx.setServiceGate(gate);
    }

    this._safeSendRecord(result.readyEvent, requestId);
  }

  async _beginSessionAuthentication(pending, requestId) {
    // Serialize against a concurrent authenticate/hello (round-7 + round-8 P2). Claim
    // the auth slot SYNCHRONOUSLY here, BEFORE the challenge-signing await inside
    // _issueSessionChallenge. A check-only guard goes stale across that await: a hello
    // could pass the check, yield during signing while an authenticate claims the old
    // challenge, then resume and publish a fresh challenge onto the now-authenticated
    // session (TOCTOU). Holding the slot across the whole operation makes hello and
    // authenticate mutually exclusive; released in the finally on every exit.
    if (this._sessionAuthInFlight) {
      this._sendErrorRecord({ id: requestId, code: "UNAUTHORIZED", message: "session authentication already in progress", retryable: false });
      return;
    }
    this._sessionAuthInFlight = true;
    try {
      await this._issueSessionChallenge(pending, requestId);
    } finally {
      this._sessionAuthInFlight = false;
    }
  }

  async _issueSessionChallenge(pending, requestId) {
    const identity = this.runtime && typeof this.runtime.getIdentity === "function" ? this.runtime.getIdentity() : null;
    const nodeKeyId = identity ? String(identity.nodeKeyId || "").trim() : "";
    const nodePublicKeyB64 = identity ? String(identity.nodePublicKeyB64 || "").trim() : "";
    const nodePrivateKeyB64 = identity ? String(identity.nodePrivateKeyB64 || "").trim() : "";
    const relayKeyId = identity ? String(identity.relayKeyId || "").trim() : "";
    if (!nodeKeyId || !nodePublicKeyB64 || !nodePrivateKeyB64 || !relayKeyId) {
      this._sendErrorRecord({
        id: requestId,
        code: "INTERNAL",
        message: "Session bootstrap unavailable",
        retryable: false,
      });
      this.ws.close(1011, "bootstrap_unavailable");
      return;
    }
    const issuedAtMs = Date.now();
    const wsPath = this._wsPath();
    const challengeId = this._eventId("session_challenge");
    const nonceB64 = bytesToBase64(randomBytes(32));
    const expiresAtMs = issuedAtMs + SESSION_AUTH_CHALLENGE_TTL_MS;

    // Sign the challenge with the node identity privkey so the client can
    // verify it actually came from a node holding nodeKeyId's privkey. Without
    // this, an attacker could relay another node's session-auth signature
    // back to this node — see docs/SECURITY_AUDIT.md CRITICAL-2.
    const challengePayloadBytes = signedPayloadBytes({
      kind: "session-challenge",
      challengeId,
      nonceB64,
      issuedAtMs,
      expiresAtMs,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      accountIdentityPublicKeyB64: pending.accountIdentityPublicKeyB64,
      sessionDeviceId: pending.sessionDeviceId,
      wsPath,
    });
    let nodePrivKeyBytes;
    try {
      nodePrivKeyBytes = base64ToBytes(nodePrivateKeyB64);
    } catch {
      this._sendErrorRecord({
        id: requestId,
        code: "INTERNAL",
        message: "Session bootstrap unavailable",
        retryable: false,
      });
      this.ws.close(1011, "bootstrap_unavailable");
      return;
    }
    const challengeSigBytes = await Promise.resolve(SESSION_AUTH_CRYPTO.sign({
      privateKey: nodePrivKeyBytes,
      msg: challengePayloadBytes,
    })).catch(() => null);
    if (!(challengeSigBytes instanceof Uint8Array)) {
      this._sendErrorRecord({
        id: requestId,
        code: "INTERNAL",
        message: "Session bootstrap unavailable",
        retryable: false,
      });
      this.ws.close(1011, "bootstrap_unavailable");
      return;
    }
    const challenge = {
      challengeId,
      nonceB64,
      issuedAtMs,
      expiresAtMs,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      wsPath,
      signatureB64: bytesToBase64(challengeSigBytes),
    };
    this._pendingSessionAuth = {
      ...pending,
      challengeId,
      nonceB64,
      issuedAtMs,
      expiresAtMs,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      wsPath,
    };
    this._safeSendRawRecord(SESSION_CHALLENGE_TYPE, {
      id: requestId || challengeId,
      body: challenge,
    });
  }

  async _handleSessionAuthenticate(requestId, body = {}) {
    // Serialize authentication (audit R4 L2c review round-7 P2). WS message callbacks
    // are not ordered, so two authenticate frames can arrive for one connection. If an
    // authentication is already running, refuse the competing frame outright — do NOT
    // touch the challenge or the socket (the in-flight attempt owns them).
    if (this._sessionAuthInFlight) {
      this._sendErrorRecord({ id: requestId, code: "UNAUTHORIZED", message: "session authentication already in progress", retryable: false });
      return;
    }
    const pending = this._pendingSessionAuth;
    if (!pending) {
      this._sendErrorRecord({ id: requestId, code: "UNAUTHORIZED", message: "No pending session authentication", retryable: false });
      this.ws.close(1008, "auth_required");
      return;
    }

    const challengeId = body && typeof body.challengeId === "string" ? body.challengeId.trim() : "";
    const signatureB64 = body && typeof body.signatureB64 === "string" ? body.signatureB64.trim() : "";
    if (!challengeId || !signatureB64 || challengeId !== pending.challengeId || Date.now() > pending.expiresAtMs) {
      this._pendingSessionAuth = null;
      this._sendErrorRecord({ id: requestId, code: "UNAUTHORIZED", message: "Session authentication failed", retryable: false });
      this.ws.close(1008, "auth_failed");
      return;
    }

    let signatureBytes;
    try {
      signatureBytes = base64ToBytes(signatureB64);
    } catch {
      this._pendingSessionAuth = null;
      this._sendErrorRecord({ id: requestId, code: "UNAUTHORIZED", message: "Session authentication failed", retryable: false });
      this.ws.close(1008, "auth_failed");
      return;
    }

    // ATOMIC consume: null the one-time challenge and claim the in-flight slot BEFORE
    // the first await. Everything above is synchronous, so a concurrent authenticate
    // frame runs either entirely before this point (and would fail its own challenge
    // match, since only one valid signature exists) or entirely after it — where it
    // sees _sessionAuthInFlight=true / _pendingSessionAuth=null and is refused. A
    // concurrent session.hello is likewise refused while in flight. The finally clears
    // the slot on every exit; no valid challenge is ever consumed twice.
    this._pendingSessionAuth = null;
    this._sessionAuthInFlight = true;
    try {
      // The signed payload includes nodeKeyId + nodePublicKeyB64 so the SDK's
      // signature is non-portable: a signature produced for one node cannot be
      // replayed to a different node (docs/SECURITY_AUDIT.md CRITICAL-2). Use
      // the wsPath captured when the challenge was issued so a malformed/changed
      // URL between hello and authenticate is rejected.
      const payloadBytes = signedPayloadBytes({
        kind: "session-auth",
        challengeId: pending.challengeId,
        nonceB64: pending.nonceB64,
        nodeKeyId: pending.nodeKeyId,
        nodePublicKeyB64: pending.nodePublicKeyB64,
        relayKeyId: pending.relayKeyId,
        publicKeyB64: pending.accountIdentityPublicKeyB64,
        deviceId: pending.sessionDeviceId,
        wsPath: pending.wsPath,
      });

      // Dual-mode session authentication (S2.5 S7 / audit F1). A PRIMARY device
      // signs this payload with its account root key (B-sign) — the unchanged
      // path. A DELEGATED device holds only its per-device key C (no B-sign
      // private key), so it signs with C and presents an account→device capability
      // chain; the node verifies the signature against C and anchors the chain to
      // the CLAIMED account (B) via verifyAccountAuthority.
      const certChain = body && Array.isArray(body.certChain) && body.certChain.length > 0 ? body.certChain : null;
      const authority = certChain
        ? await this._verifyDelegatedSessionAuth({ pending, body, payloadBytes, signatureBytes, certChain })
        : await this._verifyDirectSessionAuth({ pending, payloadBytes, signatureBytes });
      if (!authority || authority.ok !== true) {
        this._sendErrorRecord({ id: requestId, code: "UNAUTHORIZED", message: "Session authentication failed", retryable: false });
        this.ws.close(1008, "auth_failed");
        return;
      }
      // Build the ready payload BEFORE committing any authentication state. The build
      // can THROW (e.g. the fan-out readiness interlock rejecting a misconfigured
      // runtime); committing sessionAuthority first would strand a populated authority
      // on an unauthenticated, unrecoverable session while the generic dispatcher
      // returned a silent INTERNAL (round-6 P2). On ANY build failure, leave NO
      // authentication state behind, log the cause, send an explicit error, and close.
      let ready;
      try {
        ready = await buildAuthenticatedSession({
          runtime: this.runtime,
          deviceId: pending.sessionDeviceId,
          accountIdentityPublicKeyB64: pending.accountIdentityPublicKeyB64,
        });
      } catch (err) {
        this.sessionAuthority = null;
        console.error("[GatewaySession] session build failed after auth verify: " + (err && err.message ? err.message : err));
        this._sendErrorRecord({ id: requestId, code: "INTERNAL", message: "session could not be established", retryable: false });
        this.ws.close(1011, "session_build_failed");
        return;
      }
      if (ready.error) {
        this.sessionAuthority = null;
        this._sendErrorRecord({
          id: requestId,
          code: ready.error.code,
          message: ready.error.message,
          retryable: ready.error.retryable ?? false,
        });
        this.ws.close(1008, "auth_failed");
        return;
      }
      // The socket must still be open before adoption — an intervening close during
      // the awaits (client hangup, rate-limit close, teardown) must not adopt a dead
      // session or emit session.ready onto a closed socket.
      if (this.isOpen() !== true) {
        this.sessionAuthority = null;
        return;
      }
      // Success — only NOW commit the verified authority, then adopt.
      this.sessionAuthority = authority;
      // Seed the per-dispatch epoch fast-path watermark (review finding 1) with the epoch this
      // delegated admission was verified against. Direct sessions never consult it.
      this._admittedAuthorityEpoch = typeof authority.admittedAuthorityEpoch === "number"
        ? authority.admittedAuthorityEpoch
        : null;
      await this._adoptAuthenticatedSession(ready, requestId);
    } finally {
      this._sessionAuthInFlight = false;
    }
  }

  /**
   * PRIMARY-device session auth: the account root key (B-sign) signed the
   * session-auth payload directly. Byte-for-byte the pre-S7 verification — this
   * is the path every shipped client takes.
   */
  async _verifyDirectSessionAuth({ pending, payloadBytes, signatureBytes }) {
    let publicKeyBytes;
    try {
      publicKeyBytes = base64ToBytes(pending.accountIdentityPublicKeyB64);
    } catch {
      return { ok: false };
    }
    const verified = await Promise.resolve(SESSION_AUTH_CRYPTO.verify({
      publicKey: publicKeyBytes,
      msg: payloadBytes,
      sig: signatureBytes,
    })).catch(() => false);
    if (verified !== true) {
      return { ok: false };
    }
    return {
      ok: true,
      mode: "direct",
      accountIdentityPublicKeyB64: pending.accountIdentityPublicKeyB64,
      grantedCapabilities: null, // the account root holds every capability
      leafCertId: null,
      signerPublicKeyB64: pending.accountIdentityPublicKeyB64,
    };
  }

  /**
   * DELEGATED-device session auth (S2.5 S7 / audit F1). The device holds only its
   * per-device key C plus a capability chain C←…←B; it cannot sign with B-sign.
   * Three independent checks, all fail-closed:
   *   1. the session-auth payload signature verifies against C (the claimed signer);
   *   2. the claimed session deviceId IS C's self-certifying id (no arbitrary id);
   *   3. the capability chain anchors C→…→B to the CLAIMED account (membership is
   *      enough to authenticate; per-op authority is checked at each operation).
   * revocationState (S2.5 S11, F4): the CLAIMED account's current revoked-cert set
   * + issued-at cutoff, resolved from the authority home with bounded staleness.
   * A revoked leaf (or any revoked ancestor in the chain) fails the authority
   * check, so a revoked device can no longer authenticate. `null` when the account
   * has no revocations (byte-identical to the pre-S11 path) or when this node holds
   * no authority state (fs/desktop / non-home).
   */
  async _verifyDelegatedSessionAuth({ pending, body, payloadBytes, signatureBytes, certChain }) {
    const signerPublicKeyB64 = body && typeof body.signerPublicKeyB64 === "string" ? body.signerPublicKeyB64.trim() : "";
    if (!signerPublicKeyB64) {
      return { ok: false };
    }

    let signerKeyBytes;
    try {
      signerKeyBytes = base64ToBytes(signerPublicKeyB64);
    } catch {
      return { ok: false };
    }
    const sigOk = await Promise.resolve(SESSION_AUTH_CRYPTO.verify({
      publicKey: signerKeyBytes,
      msg: payloadBytes,
      sig: signatureBytes,
    })).catch(() => false);
    if (sigOk !== true) {
      return { ok: false };
    }

    let expectedDeviceId;
    try {
      expectedDeviceId = DeviceRegistrationV1.deviceIdFor(signerPublicKeyB64);
    } catch {
      return { ok: false };
    }
    if (expectedDeviceId !== pending.sessionDeviceId) {
      return { ok: false };
    }

    const revCache = this.runtime && this.runtime.accountAuthorityRevocationCache ? this.runtime.accountAuthorityRevocationCache : null;
    // Read the revocation state AND its coherent epoch in one snapshot. The epoch becomes this
    // delegated session's admitted watermark (review finding 1): the per-dispatch fast path treats
    // an unchanged epoch as "authority unchanged since we verified here". Capturing the epoch that
    // belongs to the very snapshot we verify against keeps the watermark sound — if a revoke
    // commits right after (bumping the epoch), the next dispatch sees the mismatch and re-verifies.
    const admissionResolve = revCache && typeof revCache.resolveFreshWithEpoch === "function"
      ? await revCache.resolveFreshWithEpoch(pending.accountIdentityPublicKeyB64)
      : { state: null, epoch: 0 };
    const revocationState = admissionResolve.state;
    const admittedAuthorityEpoch = admissionResolve.epoch;

    // Audit R4 F3-remediation round-6 finding 3 (+ round-7 finding 2): a delegated leaf is
    // only safe to accept where the home can FULLY resolve authoritative revocation — BOTH
    // the revoked-cert/cutoff state (revocation cache) AND the terminal device status
    // (registry). Requiring only one fails open on the other's split state (a tombstoned-
    // before-bind device has no revoked cert; a revoked leaf may still have an active device
    // row). If either is missing, FAIL CLOSED for delegated sessions (a pg home wires both;
    // fs/desktop have neither and are single-device, never presenting a delegated chain).
    // Off-home record verification that trusts a leaf without revocation state is the separate
    // registration-before-release / published-state work.
    const hasCache = revCache && typeof revCache.resolveFreshWithEpoch === "function";
    const hasRegistry = this.runtime && this.runtime.accountDeviceRegistry
      && typeof this.runtime.accountDeviceRegistry.isTerminallyRevoked === "function";
    if (!hasCache || !hasRegistry) {
      return { ok: false };
    }

    const result = await verifyAccountAuthority({
      expectedAccountIdentityPublicKeyB64: pending.accountIdentityPublicKeyB64,
      requiredCapability: null, // membership authenticates; per-op authority checked later
      opSignerPublicKeyB64: signerPublicKeyB64,
      certChain,
      crypto: SESSION_AUTH_CRYPTO,
      nowMs: Date.now(),
      revocationState,
    });
    if (!result || result.ok !== true) {
      return { ok: false };
    }

    // Audit R4 F3-remediation round-4 finding 1 (+ round-5 finding 3): verifyAccountAuthority
    // only consumes the revoked-CERT set. A delegated device revoked by DEVICE ID that never
    // bound its cert (cert_id was NULL, so Option A auto-revoked nothing) has NO cert in that
    // set and would still authenticate here — then exercise peerLink.create / deviceSet.publish
    // on its unrevoked leaf. Reject it at this consumption boundary using the AUTHORITATIVE
    // TERMINAL device predicate (`registry status='revoked' OR tombstoned`), NOT tombstone
    // alone — a historical revoked registry row (pre-0012, or a legacy cursor-only revoke)
    // may lack a tombstone. Home-only: the registry is the pg authority home's opt-in account
    // linkage; fs/desktop have no delegated device set (null ⇒ skip).
    const deviceRegistry = this.runtime && this.runtime.accountDeviceRegistry ? this.runtime.accountDeviceRegistry : null;
    if (deviceRegistry && typeof deviceRegistry.isTerminallyRevoked === "function") {
      const terminal = await deviceRegistry.isTerminallyRevoked(pending.accountIdentityPublicKeyB64, pending.sessionDeviceId);
      if (terminal === true) {
        return { ok: false };
      }
    }
    return {
      ok: true,
      mode: "delegated",
      accountIdentityPublicKeyB64: pending.accountIdentityPublicKeyB64,
      grantedCapabilities: Array.isArray(result.grantedCapabilities) ? result.grantedCapabilities : [],
      leafCertId: result.leafCertId || null,
      signerPublicKeyB64,
      // Retained so per-op handlers (e.g. AccountMutationHandler, audit F2) can
      // RE-validate the chain against the home's current revocation state instead
      // of trusting the connect-time capability snapshot until reconnect.
      certChain,
      // The authority epoch this admission was verified against — the initial watermark for the
      // per-dispatch epoch fast path (review finding 1).
      admittedAuthorityEpoch,
    };
  }

  /**
   * Re-check a DELEGATED session's authority against the home's CURRENT state — the
   * per-request dispatch guard (audit R4 F3-remediation round-5/6 finding 1, L5). Returns
   * false if the session device is terminally revoked OR its retained cert chain no longer
   * verifies against the current revocation state (a revoked leaf/ancestor cert or the
   * minValidIssuedAt cutoff — an independent revocation dimension the device-status check does
   * NOT cover). Direct (account-root) sessions are unrevocable ⇒ always true.
   *
   * EPOCH FAST PATH (review finding 1): the account authority epoch is monotonic and bumps on
   * EVERY add/revoke. So an epoch unchanged since this session was admitted (or since the last
   * full re-check) PROVES its authority is unchanged — no device could have been revoked, no cert
   * added to the revoked set, no cutoff moved. The hot path therefore reads only the cheap epoch
   * (one indexed int) and returns true. Only when the epoch ADVANCES does it pay the heavy path:
   * fresh terminal read + coherent revocation snapshot + full chain re-verify, then it advances
   * its watermark to that snapshot's epoch. This keeps the steady-state cost ~1 round-trip and no
   * per-frame crypto, while a mid-session revoke (which bumps the epoch) is still enforced on the
   * very next dispatch. A THROW from the backend is an AVAILABILITY failure (finding 4): it is
   * tagged REVOCATION_BACKEND_UNAVAILABLE so the caller answers SERVICE_UNAVAILABLE (retryable,
   * socket stays open) — never a definitive "revoked" that would close a legitimate socket.
   */
  async _delegatedSessionStillAuthorized() {
    const authority = this.sessionAuthority;
    if (!authority || typeof authority !== "object" || authority.mode !== "delegated") {
      return true;
    }
    const deviceRegistry = this.runtime && this.runtime.accountDeviceRegistry ? this.runtime.accountDeviceRegistry : null;
    const revCache = this.runtime && this.runtime.accountAuthorityRevocationCache ? this.runtime.accountAuthorityRevocationCache : null;
    // Round-7 finding 2: require BOTH revocation dimensions. The registry catches a device
    // TOMBSTONED with no revoked cert (tombstoned-before-bind); the cache catches a revoked
    // LEAF/ancestor cert with an active device row. Checking only whichever source is present
    // fails OPEN on the other's split state, so if EITHER source (or the retained chain) is
    // unavailable, revocation cannot be fully resolved → fail closed. The epoch fast path also
    // needs currentEpoch + resolveFreshWithEpoch, so require them too.
    if (!deviceRegistry || typeof deviceRegistry.isTerminallyRevoked !== "function"
        || !revCache || typeof revCache.currentEpoch !== "function" || typeof revCache.resolveFreshWithEpoch !== "function"
        || !Array.isArray(authority.certChain)
        || typeof authority.signerPublicKeyB64 !== "string" || authority.signerPublicKeyB64.length === 0) {
      return false;
    }
    try {
      // FAST PATH: epoch unchanged since admission / last re-check ⇒ authority unchanged.
      const epochNow = await revCache.currentEpoch(this.ownerPublicKeyB64);
      if (typeof this._admittedAuthorityEpoch === "number" && epochNow === this._admittedAuthorityEpoch) {
        return true;
      }
      // SLOW PATH: the epoch advanced (a mutation committed) — a revoke may target THIS device.
      // (1) terminal device status (fresh registry read).
      const terminal = await deviceRegistry.isTerminallyRevoked(this.ownerPublicKeyB64, this.sessionDeviceId);
      if (terminal === true) {
        return false;
      }
      // (2) cert-chain authority against the coherent fresh revocation snapshot + its epoch.
      const { state: revocationState, epoch: verifiedEpoch } = await revCache.resolveFreshWithEpoch(this.ownerPublicKeyB64);
      const result = await verifyAccountAuthority({
        expectedAccountIdentityPublicKeyB64: this.ownerPublicKeyB64,
        requiredCapability: null,
        opSignerPublicKeyB64: authority.signerPublicKeyB64,
        certChain: authority.certChain,
        crypto: SESSION_AUTH_CRYPTO,
        nowMs: Date.now(),
        revocationState,
      });
      if (!result || result.ok !== true) {
        return false;
      }
      // Still authorized despite the advance (the mutation touched some OTHER device). Advance the
      // watermark to the epoch we just verified against, so subsequent frames fast-path again until
      // the NEXT mutation. Only a coherent snapshot epoch is stored (never the fast-path read), so
      // the watermark can never run ahead of a state we have actually re-verified.
      this._admittedAuthorityEpoch = verifiedEpoch;
      return true;
    } catch (backendErr) {
      const wrapped = new Error("revocation backend unavailable: "
        + (backendErr && backendErr.message ? backendErr.message : String(backendErr)));
      wrapped.code = "REVOCATION_BACKEND_UNAVAILABLE";
      throw wrapped;
    }
  }

  _wsPath() {
    try {
      const rawUrl = this.request && typeof this.request.url === "string" ? this.request.url : "/ws";
      const reqHeaders = this.request && this.request.headers ? this.request.headers : {};
      const host = typeof reqHeaders.host === "string" && reqHeaders.host.trim()
        ? this.request.headers.host.trim()
        : "127.0.0.1";
      const url = new URL(rawUrl, `http://${host}`);
      return url.pathname || "/ws";
    } catch {
      return "/ws";
    }
  }

  // --- Send helpers ---

  _sendErrorRecord({ id, code, message, retryable = false, detail } = {}) {
    console.error("[GatewaySession] PRE-SANITIZE error:", { id, code, message, retryable });
    if (this.runtime.metrics && typeof this.runtime.metrics.increment === "function") this.runtime.metrics.increment("errorsTotal", 1);
    const safe = toClientSafeError({ code, retryable, detail, message });
    this._safeSendRecord(new WsErrorEvent({
      code: safe.code,
      message: safe.message,
      detail: new WsErrorDetail({
        retryable: safe.retryable === true,
        appContextId: safe.detail ? safe.detail.appContextId : undefined,
        messageId: safe.detail ? safe.detail.messageId : undefined,
      }),
    }), id || this._eventId("error"));
  }

  _eventId(prefix) {
    return `${prefix}:${Date.now()}:${randomHex()}`;
  }

  _safeSendRecord(record, id = null) {
    if (this.ws.readyState !== this.ws.OPEN) return;
    assertContractTree(record);
    const type = String(record && record.constructor && record.constructor.type ? record.constructor.type : "").trim();
    if (!type) return;

    if (this.queueLen >= QUEUE_MAX) {
      if (this.runtime.metrics && typeof this.runtime.metrics.increment === "function") this.runtime.metrics.increment("errorsTotal", 1);
      try {
        const overflowType = String(WsErrorEvent.type || "error");
        this.ws.send(JSON.stringify({
          id: this._eventId("error"),
          type: overflowType,
          t: overflowType,
          v: CONTRACT_VERSION,
          body: {
            code: "RATE_LIMITED",
            message: "Rate limited",
            detail: { retryable: false },
          },
        }));
      } catch (rlErr) {
        console.error("[GatewaySession] rate-limit notification send failed: " + (rlErr && rlErr.message ? rlErr.message : rlErr));
      }
      this.ws.close(1013, "backpressure");
      return;
    }

    this.queueLen += 1;
    try {
      const payload = {
        id: id || this._eventId(type),
        type,
        t: type,
        v: CONTRACT_VERSION,
        body: record,
      };
      this.ws.send(JSON.stringify(payload), () => {
        this.queueLen = Math.max(0, this.queueLen - 1);
      });
    } catch (sendErr) {
      this.queueLen = Math.max(0, this.queueLen - 1);
      console.error("[GatewaySession] send failed for type=" + type + ": " + (sendErr && sendErr.message ? sendErr.message : sendErr));
    }
  }

  _safeSendRawRecord(type, { id = null, body = {} } = {}) {
    if (this.ws.readyState !== this.ws.OPEN) return;
    const frame = { type, t: type, body };
    if (id !== null && id !== undefined) frame.id = id;
    try {
      this.ws.send(JSON.stringify(frame));
    } catch (rawErr) {
      console.error("[GatewaySession] _safeSendRawRecord failed for type=" + type + ": " + (rawErr && rawErr.message ? rawErr.message : rawErr));
    }
  }

  _safeSendRawFrame(frame) {
    if (this.ws.readyState !== this.ws.OPEN) return;
    this.ws.send(JSON.stringify(normalizeFrameShape(frame)));
  }

  // --- Session registry ---

  _emitToOwner(ownerPublicKeyB64, frame) {
    if (!frame || typeof frame !== "object") return;
    const owner = String(ownerPublicKeyB64 || "").trim();
    if (!owner || !this.sessionRegistry || typeof this.sessionRegistry.broadcastToOwner !== "function") {
      this._safeSendRawFrame(frame);
      return;
    }
    const delivered = this.sessionRegistry.broadcastToOwner(owner, frame);
    if (delivered === 0) {
      this._safeSendRawFrame(frame);
    }
  }

  _bindOwnerSession(ownerPublicKeyB64) {
    const owner = String(ownerPublicKeyB64 || "").trim();
    if (!owner) return;
    if (!this.sessionRegistry || typeof this.sessionRegistry.addSession !== "function") {
      this.ownerPublicKeyB64 = owner;
      return;
    }
    if (this._isRegistered && this.ownerPublicKeyB64 && this.ownerPublicKeyB64 !== owner) {
      this.sessionRegistry.removeSession({ ownerPublicKeyB64: this.ownerPublicKeyB64, session: this });
      this._isRegistered = false;
    }
    this.ownerPublicKeyB64 = owner;
    this.sessionRegistry.addSession({ ownerPublicKeyB64: owner, session: this });
    this._isRegistered = true;
  }

  /**
   * Register the session under a claimant pubkey so inbox deliveries
   * targeting that claimant reach this session even when the claimant
   * differs from the session-auth identity. Idempotent.
   */
  _bindClaimantSession(claimantPublicKeyB64) {
    const claimant = String(claimantPublicKeyB64 || "").trim();
    if (!claimant) return;
    if (this._boundClaimantRegistrations.has(claimant)) return;
    if (claimant !== this.ownerPublicKeyB64
        && this.sessionRegistry
        && typeof this.sessionRegistry.addSession === "function") {
      this.sessionRegistry.addSession({ ownerPublicKeyB64: claimant, session: this });
    }
    this._boundClaimantRegistrations.add(claimant);
  }

  _unbindOwnerSession() {
    // Drop the liveness-bus drain subscription in lockstep with leaving the
    // session registry — same lifecycle event, so the bus subscription and the
    // gate's local-socket signal can never disagree.
    this._unregisterLivenessInbox();
    // Tear down every per-claimant registration first. Hosted-session
    // entries are keyed by the claimant pubkey (not the session-auth
    // identity), so we must unregister each one explicitly.
    for (const claimant of this._boundClaimantRegistrations) {
      if (this.runtime && typeof this.runtime.unregisterHostedSession === "function") {
        Promise.resolve(this.runtime.unregisterHostedSession(claimant)).catch(() => {});
      }
      if (claimant !== this.ownerPublicKeyB64
          && this.sessionRegistry
          && typeof this.sessionRegistry.removeSession === "function") {
        this.sessionRegistry.removeSession({ ownerPublicKeyB64: claimant, session: this });
      }
    }
    this._boundClaimantRegistrations.clear();

    if (!this._isRegistered) return;
    if (this.sessionRegistry && typeof this.sessionRegistry.removeSession === "function") {
      this.sessionRegistry.removeSession({ ownerPublicKeyB64: this.ownerPublicKeyB64, session: this });
    }
    this._isRegistered = false;
  }

  /**
   * Subscribe this node to cross-node deposit pings for the bound inbox (pg +
   * redis only). On a ping (a deposit landed on ANOTHER node), drain the durable
   * log from this device's cursor and push the new events to the owner's socket —
   * the cross-node half of Option Y. Called from ProtocolContext.setSessionInbox
   * at the exact moment localInboxId is set, so the bus subscription and the
   * sessionRegistry membership flip together. Idempotent per inbox.
   */
  _registerLivenessInbox(inboxId) {
    const id = typeof inboxId === "string" ? inboxId.trim() : "";
    if (!id) return;
    const bus = this.runtime && this.runtime.livenessBus;
    const durableInbox = this.runtime && this.runtime.durableInbox;
    if (!bus || typeof bus.registerInbox !== "function" || !durableInbox) return;
    // Already subscribed for this inbox — don't stack a second handler on a
    // re-claim (which would double-drain every ping).
    if (this._livenessUnregister && this._livenessInboxId === id) return;
    if (this._livenessUnregister) this._unregisterLivenessInbox();

    this._livenessInboxId = id;
    this._livenessUnregister = bus.registerInbox(id, (payload) => this._drainDurableToSocket(id, payload));
  }

  _unregisterLivenessInbox() {
    if (typeof this._livenessUnregister === "function") {
      try {
        this._livenessUnregister();
      } catch (err) {
        console.error("[GatewaySession] liveness unregister failed for " + this._livenessInboxId
          + ": " + (err && err.message ? err.message : err));
      }
    }
    this._livenessUnregister = null;
    this._livenessInboxId = null;
  }

  /**
   * On a cross-node deposit ping, push the device's not-yet-pushed events to THIS
   * socket as evt.mailbox.deposited (the SAME shared frame the direct broadcast
   * builds — no drift).
   *
   * Reads via readUndelivered (events after the DELIVERED watermark), NOT
   * readAfterCursor (the consumed cursor): each event pushes exactly once, so a
   * repeated ping — or an un-acked/poison event that pins the consumed cursor —
   * cannot re-drain and amplify duplicate pushes. The consumed cursor still only
   * advances on the client's mailbox.cursorAck, and reconnect catch-up
   * (handleList) redelivers anything unconsumed, so no mail is lost.
   *
   * Sends DIRECTLY to this session — the socket that registered this bus
   * interest — never an owner-bucket broadcast: on the privacy path the inbox
   * claimant can differ from the session-auth owner, and another session under
   * that auth owner must not receive this claimed inbox's ciphertext.
   *
   * Drains in BOUNDED BATCHES until the triggering deposit (payload.seq) has been
   * pushed — a single fixed read could miss it when the delivered watermark is
   * far behind (e.g. after Redis downtime or a missed ping), stalling real-time
   * delivery of the very event that pinged. The MAX_BATCHES cap is backpressure:
   * any remaining backlog rides the next ping or reconnect catch-up.
   */
  async _drainDurableToSocket(inboxId, payload) {
    const durableInbox = this.runtime && this.runtime.durableInbox;
    if (!durableInbox || typeof durableInbox.readUndelivered !== "function") return;
    const deviceId = typeof this.sessionDeviceId === "string" ? this.sessionDeviceId.trim() : "";
    if (!deviceId) return;
    if (typeof this.send !== "function" || this.isOpen() !== true) return;

    const targetSeq = payload && Number.isFinite(Number(payload.seq)) ? Number(payload.seq) : null;
    const BATCH = 100;
    const MAX_BATCHES = 50; // cap: at most 5000 events live-pushed per ping
    let reachedTarget = false;
    for (let i = 0; i < MAX_BATCHES; i += 1) {
      if (this.isOpen() !== true) return; // socket closed mid-drain
      const events = await durableInbox.readUndelivered(inboxId, deviceId, BATCH);
      for (const e of events) {
        this.send(buildMailboxDepositedFrame({
          mailboxId: inboxId,
          eventId: String(e.seq),
          ciphertextB64: outerPacketBodyB64(e.body),
          seq: e.seq,
        }));
        if (targetSeq != null && e.seq >= targetSeq) reachedTarget = true;
      }
      if (events.length < BATCH) break;              // drained everything available
      if (targetSeq != null && reachedTarget) break; // delivered through the trigger
    }
  }

  /**
   * In-process (same-node) durable deposit notification — the LOCAL twin of a
   * cross-node bus ping, invoked by RelayDepositRouter when a deposit lands on
   * the node that already holds this inbox's socket. Routes through the SAME
   * drain as the bus path, so local and cross-node live delivery are identical:
   * `readUndelivered` advances `last_delivered` (so a later cursorAck(seq) is not
   * clamped to 0) and the event is direct-sent to THIS socket. Fire-and-forget;
   * never throws into the caller.
   */
  notifyLocalDeposit(inboxId, seq) {
    Promise.resolve(this._drainDurableToSocket(inboxId, { seq })).catch((err) => {
      console.error("[GatewaySession] local durable drain failed for " + inboxId
        + ": " + (err && err.message ? err.message : err));
    });
  }

  // --- Public API (used by WsGatewayServer, tests) ---

  send(frame) {
    this._safeSendRawFrame(frame);
  }

  isOpen() {
    return this.ws.readyState === this.ws.OPEN;
  }
}
