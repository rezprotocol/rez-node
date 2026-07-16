import { REZ_CONTRACT_TYPES, isCanonicalDeviceId, CAP_DEVICE_SET_PUBLISH } from "@rezprotocol/core";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";
import {
  OutboxLeaseClaimRequest,
  OutboxLeaseClaimResponse,
  OutboxLeasePrepareRequest,
  OutboxLeasePrepareResponse,
  OutboxLeaseReleaseRequest,
  OutboxLeaseReleaseResponse,
  OutboxLeaseFailRequest,
  OutboxLeaseFailResponse,
} from "../../contracts/records/OutboxLeaseRecords.js";

const T = REZ_CONTRACT_TYPES;

// req 8 (audit leaf-3b F4): a PER-NODE request-rate throttle keyed on the account. It is
// deliberately LOCAL (per-process, LRU-capped) — it bounds op frequency against a SINGLE node,
// not the cluster. The AUTHORITATIVE cluster-wide per-account limit is the DB one-leased partial
// unique index (migration 0019): at most ONE live authority-state lease per account across the
// ENTIRE cluster, no matter how many nodes a client connects to. So the durable resource (the
// lease) is cluster-serialized by Postgres; this limiter only smooths request churn per node.
export const OUTBOX_LEASE_MAX_PER_MINUTE = 240;
const OUTBOX_LEASE_RATE_LIMITER = new SlidingWindowRateLimiter({
  windowMs: 60_000,
  maxAttempts: OUTBOX_LEASE_MAX_PER_MINUTE,
  lruCap: 4096,
});

// pg SQLSTATE class prefixes that indicate a TRANSIENT / availability failure worth a client
// retry: 08 connection exception, 53 insufficient resources, 57 operator intervention (admin
// shutdown / cannot-connect-now), 58 system error. The SQLSTATE is a fixed code — never the
// lease token — so classifying on it leaks nothing (audit leaf-3b F6).
const TRANSIENT_SQLSTATE_CLASSES = new Set(["08", "53", "57", "58"]);

/**
 * PropagationOutboxHandler (P1#3 leaf 3b — the wire/auth surface for the head-advancing
 * account lease). A device DRAINS its account's authority-state publication obligations
 * (PgPropagationOutbox): claim the publishable head under a server lease, prepare (freeze) the
 * epoch to publish, and release / report failure. The signature-verifying completion (ack) is
 * the separate leaf-3c op — these four are crypto-FREE.
 *
 * Boundary invariants (audit leaf-3 reqs 1/2/3/8 + leaf-3b remediation):
 *   - req 2: the account is ALWAYS the AUTHENTICATED session's own (ctx.ownerPublicKeyB64,
 *     re-bound to the session authority object), and the lease OWNER is ALWAYS the authenticated
 *     session DEVICE (ctx.sessionDeviceId) — never a value from the request body.
 *   - F2 fail-closed authority: the session authority must be an EXPLICIT direct|delegated shape
 *     bound to THIS account; null / unknown / malformed authority is REJECTED (never implicitly
 *     treated as primary). A DELEGATED device must carry deviceSet.publish AND the full retained
 *     chain shape. The fresh full-chain revocation re-check per op runs upstream in
 *     GatewaySession._delegatedSessionStillAuthorized() before dispatch.
 *   - req 1: request/response bodies are RRecord contracts (OutboxLease*), so the lease token is
 *     size-bounded in the contract layer.
 *   - req 8: per-node rate limit; a lease token NEVER appears in a log line or error text; a
 *     transient backend failure is a retryable SERVICE_UNAVAILABLE (F6).
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
   * Shared boundary spine for every lease op: authenticate, derive the account + owner device
   * FROM THE SESSION (never the body), FAIL CLOSED on any non-explicit authority, enforce the
   * delegated capability, and apply the per-node rate limit. Returns { account, owner, outbox }
   * on success, or null after sending the appropriate error.
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
    // F2 (fail closed): the authority MUST be an explicit direct|delegated object bound to THIS
    // account. Anything else — null, unknown mode, malformed, or a mismatched account — is
    // rejected. These ops carry no independent signature, so a fall-through would grant the full
    // lease surface to malformed authority.
    const authority = this.#ctx.sessionAuthority;
    if (!authority || typeof authority !== "object" || (authority.mode !== "direct" && authority.mode !== "delegated")) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session authority is missing or malformed", retryable: false });
      return null;
    }
    if (typeof authority.accountIdentityPublicKeyB64 !== "string" || authority.accountIdentityPublicKeyB64 !== account) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session authority account does not match the authenticated account", retryable: false });
      return null;
    }
    if (authority.mode === "delegated") {
      // req 3: a delegated device must carry deviceSet.publish (SSOT constant, not a literal)...
      const caps = Array.isArray(authority.grantedCapabilities) ? authority.grantedCapabilities : null;
      if (!caps || !caps.includes(CAP_DEVICE_SET_PUBLISH)) {
        this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "delegated device lacks the " + CAP_DEVICE_SET_PUBLISH + " capability", retryable: false });
        return null;
      }
      // ...and the full retained chain shape the upstream per-dispatch guard re-verifies. A
      // delegated authority missing its signer key or cert chain is malformed → fail closed.
      const hasSigner = typeof authority.signerPublicKeyB64 === "string" && authority.signerPublicKeyB64.length > 0;
      const hasChain = Array.isArray(authority.certChain) && authority.certChain.length > 0;
      if (!hasSigner || !hasChain) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "delegated session authority is incomplete", retryable: false });
        return null;
      }
    }
    // req 8: per-node rate limit, applied AFTER auth so unauthorized traffic spends no budget.
    if (!OUTBOX_LEASE_RATE_LIMITER.record(account, this.#now())) {
      this.#ctx.sendError({ id: requestId, code: "RATE_LIMITED", message: "too many outbox lease operations; retry shortly", retryable: true });
      return null;
    }
    return { account, owner, outbox };
  }

  // Parse+validate a token-bearing request via its RRecord contract (req 1 size bound lives
  // there). Returns the trimmed token, or null after sending BAD_REQUEST. The record's assert
  // messages are token-free (they never interpolate the value), so surfacing err.message is safe.
  #requireToken(requestId, Ctor, body) {
    let req;
    try {
      req = new Ctor(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err && err.message ? err.message : "invalid lease request", retryable: false });
      return null;
    }
    return req.leaseToken.trim();
  }

  // req 8 (F6): a caught BACKEND error is reported WITHOUT err.message and WITHOUT the token. A
  // transient/availability SQLSTATE → retryable SERVICE_UNAVAILABLE; anything else → INTERNAL.
  #sendBackendError(requestId, err) {
    const code = err && typeof err.code === "string" ? err.code : "";
    const cls = code.length >= 2 ? code.slice(0, 2) : "";
    if (TRANSIENT_SQLSTATE_CLASSES.has(cls)) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "authority-state propagation outbox temporarily unavailable", retryable: true });
      return;
    }
    this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: "outbox lease operation failed", retryable: false });
  }

  async handleClaim(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    try {
      new OutboxLeaseClaimRequest(body); // contract-shape gate (claim carries no client input).
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err && err.message ? err.message : "invalid claim request", retryable: false });
      return;
    }
    let result;
    try {
      result = await auth.outbox.claim(auth.account, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, err);
      return;
    }
    // null ⇒ nothing publishable, another device holds the lease, or the head is backing off.
    const res = result === null
      ? new OutboxLeaseClaimResponse({ leased: false })
      : new OutboxLeaseClaimResponse({
        leased: true,
        token: result.token,
        anchorEpoch: result.anchorEpoch,
        headEpoch: result.headEpoch,
        leaseExpiresAtMs: result.leaseExpiresAtMs,
        attempts: result.attempts,
      });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES, res.toJSON());
  }

  async handlePrepare(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireToken(requestId, OutboxLeasePrepareRequest, body);
    if (token === null) return;
    let result;
    try {
      result = await auth.outbox.preparePublication(auth.account, token, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, err);
      return;
    }
    const res = result === null
      ? new OutboxLeasePrepareResponse({ prepared: false })
      : new OutboxLeasePrepareResponse({ prepared: true, anchorEpoch: result.anchorEpoch, headEpoch: result.headEpoch });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_PREPARE_RES, res.toJSON());
  }

  async handleRelease(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireToken(requestId, OutboxLeaseReleaseRequest, body);
    if (token === null) return;
    let released;
    try {
      released = await auth.outbox.release(auth.account, token, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, err);
      return;
    }
    const res = new OutboxLeaseReleaseResponse({ released: released === true });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_RELEASE_RES, res.toJSON());
  }

  async handleFail(requestId, body) {
    const auth = this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireToken(requestId, OutboxLeaseFailRequest, body);
    if (token === null) return;
    let result;
    try {
      result = await auth.outbox.fail(auth.account, token, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, err);
      return;
    }
    const res = result === null
      ? new OutboxLeaseFailResponse({ recorded: false })
      : new OutboxLeaseFailResponse({
        recorded: true,
        attemptedEpoch: result.attemptedEpoch,
        anchorEpoch: result.anchorEpoch,
        attempts: result.attempts,
        backoffMs: result.backoffMs,
        blocked: result.blocked,
      });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_FAIL_RES, res.toJSON());
  }
}
