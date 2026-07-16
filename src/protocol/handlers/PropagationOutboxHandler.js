import { REZ_CONTRACT_TYPES, isCanonicalDeviceId } from "@rezprotocol/core";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";

const T = REZ_CONTRACT_TYPES;

// The capability a DELEGATED device must hold to drive its account's authority-state
// publication. SSOT for the string is rez-core accountCapabilityShared.js
// (ACCOUNT_CAPABILITY_ACTIONS) — deviceSet.publish authorizes publishing the
// AccountAuthorityStateV1 device set, which is exactly what draining this outbox does.
const DEVICE_SET_PUBLISH_CAPABILITY = "deviceSet.publish";

// req 1: bound the ONLY client-supplied field (the lease token). The server mints a
// 48-hex token; this cap matches the DB lease_token size CHECK (migration 0018) so a
// well-formed token always fits and an oversized body is rejected before any DB round-trip.
const MAX_LEASE_TOKEN_BYTES = 128;

// req 8: per-ACCOUNT lease-op budget (bounds one account's churn across reconnects/sessions —
// a per-session limit would reset every connect). Module-level + ephemeral, mirroring
// GatewaySession's SESSION_HELLO_RATE_LIMITER. The socket-level inbound flood limiter and the
// per-account serializer admission ceilings bound the rest.
export const OUTBOX_LEASE_MAX_PER_MINUTE = 240;
const OUTBOX_LEASE_RATE_LIMITER = new SlidingWindowRateLimiter({
  windowMs: 60_000,
  maxAttempts: OUTBOX_LEASE_MAX_PER_MINUTE,
  lruCap: 4096,
});

/**
 * PropagationOutboxHandler (P1#3 leaf 3b — the wire/auth surface for the head-advancing
 * account lease). A device DRAINS its account's authority-state publication obligations
 * (PgPropagationOutbox): claim the publishable head under a server lease, prepare (freeze)
 * the epoch to publish, and release / report failure. The signature-verifying completion
 * (ack) is the separate leaf-3c op — these four are crypto-FREE.
 *
 * Boundary invariants (audit leaf-3 reqs 1/2/3/8):
 *   - req 2: the account is ALWAYS the AUTHENTICATED session's own (ctx.ownerPublicKeyB64),
 *     and the lease OWNER is ALWAYS the authenticated session DEVICE (ctx.sessionDeviceId) —
 *     never a value from the request body (account-blindness + non-transferable lease).
 *   - req 3: a PRIMARY (direct) session holds all capabilities; a DELEGATED device must carry
 *     deviceSet.publish. The FRESH full-chain revocation re-check per op is enforced upstream
 *     by GatewaySession._delegatedSessionStillAuthorized() BEFORE this handler is dispatched
 *     (the per-dispatch L5 guard), so a mid-session revocation rejects the whole request.
 *   - req 1: the client-supplied lease token is size-bounded.
 *   - req 8: per-account rate limit; the lease token NEVER appears in a log line or error text.
 *
 * pg-cluster only: the outbox is null on fs/desktop ⇒ SERVICE_UNAVAILABLE.
 */
export class PropagationOutboxHandler {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
  }

  #outbox() {
    return this.#ctx.runtime && this.#ctx.runtime.propagationOutbox
      ? this.#ctx.runtime.propagationOutbox
      : null;
  }

  // Wall clock, injectable via ctx.now for deterministic tests; production → Date.now.
  #now() {
    return this.#ctx && typeof this.#ctx.now === "function" ? this.#ctx.now() : Date.now();
  }

  /**
   * Shared boundary spine for every lease op: authenticate, derive the account + owner
   * device FROM THE SESSION (never the body), enforce the delegated capability, and apply the
   * per-account rate limit. Returns { account, owner, outbox } on success, or null after
   * sending the appropriate error (the caller returns immediately on null).
   */
  #authorize(requestId) {
    if (!this.#ctx.requireSession(requestId)) return null;
    const outbox = this.#outbox();
    if (!outbox || typeof outbox.claim !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "authority-state propagation outbox unavailable", retryable: false });
      return null;
    }
    const account = typeof this.#ctx.ownerPublicKeyB64 === "string" ? this.#ctx.ownerPublicKeyB64.trim() : "";
    if (account.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session account identity required", retryable: false });
      return null;
    }
    // req 2: the lease owner is the AUTHENTICATED session device (canonical rez:dev:<64-hex>),
    // derived here — never accepted from the request body.
    const owner = typeof this.#ctx.sessionDeviceId === "string" ? this.#ctx.sessionDeviceId.trim() : "";
    if (!isCanonicalDeviceId(owner)) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session device identity required", retryable: false });
      return null;
    }
    // req 3: PRIMARY holds all; DELEGATED must carry deviceSet.publish. (The fresh per-op
    // full-chain revocation check runs upstream in GatewaySession before dispatch.)
    const authority = this.#ctx.sessionAuthority;
    const delegated = authority && typeof authority === "object" && authority.mode === "delegated";
    if (delegated) {
      const caps = Array.isArray(authority.grantedCapabilities) ? authority.grantedCapabilities : [];
      if (!caps.includes(DEVICE_SET_PUBLISH_CAPABILITY)) {
        this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "delegated device lacks the " + DEVICE_SET_PUBLISH_CAPABILITY + " capability", retryable: false });
        return null;
      }
    }
    // req 8: per-account rate limit, applied AFTER auth so unauthorized traffic spends no budget.
    if (!OUTBOX_LEASE_RATE_LIMITER.record(account, this.#now())) {
      this.#ctx.sendError({ id: requestId, code: "RATE_LIMITED", message: "too many outbox lease operations; retry shortly", retryable: true });
      return null;
    }
    return { account, owner, outbox };
  }

  // req 1: the lease token is the only client-supplied field — size-bound it. The error text
  // NEVER echoes the token value (req 8 token hygiene).
  #requireLeaseToken(requestId, body) {
    const token = body && typeof body.leaseToken === "string" ? body.leaseToken.trim() : "";
    if (token.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "leaseToken is required", retryable: false });
      return null;
    }
    if (Buffer.byteLength(token, "utf8") > MAX_LEASE_TOKEN_BYTES) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "leaseToken exceeds the " + MAX_LEASE_TOKEN_BYTES + "-byte limit", retryable: false });
      return null;
    }
    return token;
  }

  // req 8: a caught outbox/DB error is reported with a FIXED message — never err.message and
  // never the token — so a lease token can never surface in an error sent to the client or in
  // any upstream log of that error.
  #sendOpFailed(requestId) {
    this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: "outbox lease operation failed", retryable: false });
  }

  async handleClaim(requestId, body) {
    void body; // claim takes no client input — account + owner come from the session.
    const auth = this.#authorize(requestId);
    if (!auth) return;
    let result;
    try {
      result = await auth.outbox.claim(auth.account, auth.owner);
    } catch (err) {
      void err;
      this.#sendOpFailed(requestId);
      return;
    }
    // null ⇒ nothing publishable, the account is busy, or its head is backing off.
    const response = result === null
      ? { leased: false }
      : {
        leased: true,
        token: result.token,
        anchorEpoch: result.anchorEpoch,
        headEpoch: result.headEpoch,
        leaseExpiresAtMs: result.leaseExpiresAtMs,
        attempts: result.attempts,
      };
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES, response);
  }

  async handlePrepare(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireLeaseToken(requestId, body);
    if (!token) return;
    let result;
    try {
      result = await auth.outbox.preparePublication(auth.account, token, auth.owner);
    } catch (err) {
      void err;
      this.#sendOpFailed(requestId);
      return;
    }
    const response = result === null
      ? { prepared: false }
      : { prepared: true, anchorEpoch: result.anchorEpoch, headEpoch: result.headEpoch };
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_PREPARE_RES, response);
  }

  async handleRelease(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireLeaseToken(requestId, body);
    if (!token) return;
    let released;
    try {
      released = await auth.outbox.release(auth.account, token, auth.owner);
    } catch (err) {
      void err;
      this.#sendOpFailed(requestId);
      return;
    }
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_RELEASE_RES, { released: released === true });
  }

  async handleFail(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireLeaseToken(requestId, body);
    if (!token) return;
    let result;
    try {
      result = await auth.outbox.fail(auth.account, token, auth.owner);
    } catch (err) {
      void err;
      this.#sendOpFailed(requestId);
      return;
    }
    const response = result === null
      ? { recorded: false }
      : {
        recorded: true,
        attemptedEpoch: result.attemptedEpoch,
        anchorEpoch: result.anchorEpoch,
        attempts: result.attempts,
        backoffMs: result.backoffMs,
        blocked: result.blocked,
      };
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_FAIL_RES, response);
  }
}
