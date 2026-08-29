import {
  REZ_CONTRACT_TYPES,
  isCanonicalDeviceId,
  CAP_DEVICE_SET_PUBLISH,
  DeviceRegistrationV1,
  verifyDurableRecordV2,
  AccountAuthorityStateV1,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  base64ToBytes,
} from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import { SlidingWindowRateLimiter } from "../../util/SlidingWindowRateLimiter.js";
import { isRetryableBackendError } from "../../util/backendRetryClassification.js";
import {
  OutboxLeaseClaimRequest,
  OutboxLeaseClaimResponse,
  OutboxLeasePrepareRequest,
  OutboxLeasePrepareResponse,
  OutboxLeaseReleaseRequest,
  OutboxLeaseReleaseResponse,
  OutboxLeaseFailRequest,
  OutboxLeaseFailResponse,
  OutboxLeaseCompleteRequest,
  OutboxLeaseCompleteResponse,
} from "../../contracts/records/OutboxLeaseRecords.js";

const T = REZ_CONTRACT_TYPES;

// req 8 (audit leaf-3b F4): a PER-NODE request-rate throttle keyed on the account. It is
// deliberately LOCAL (per-process, LRU-capped) — it bounds op frequency against a SINGLE node,
// not the cluster. The AUTHORITATIVE cluster-wide per-account limit is the DB one-leased partial
// unique index (migration 0019): at most ONE live authority-state lease per account across the
// ENTIRE cluster, no matter how many nodes a client connects to. So the durable resource (the
// lease) is cluster-serialized by Postgres; this limiter only smooths request churn per node.
export const OUTBOX_LEASE_MAX_PER_MINUTE = 240;
// F3 (audit leaf-3c, deferred then): the CLUSTER-WIDE ceiling. The per-node limiter below bounds
// one node; behind a non-sticky load balancer a device multiplies it by the node count. This
// budget is shared through Pg, so the ceiling no longer scales with the cluster. It is set ABOVE
// the per-node cap so a single-node deployment behaves exactly as before (the local limiter binds
// first) while a fleet cannot exceed roughly one node's worth of work per account.
export const OUTBOX_LEASE_CLUSTER_BUDGET_BUCKET = "outbox_lease";
export const OUTBOX_LEASE_CLUSTER_WINDOW_MS = 60_000;
export const OUTBOX_LEASE_CLUSTER_MAX_PER_MINUTE = 300;
const OUTBOX_LEASE_RATE_LIMITER = new SlidingWindowRateLimiter({
  windowMs: 60_000,
  maxAttempts: OUTBOX_LEASE_MAX_PER_MINUTE,
  lruCap: 4096,
});


/**
 * PropagationOutboxHandler (P1#3 leaf 3b/3c — the wire/auth surface for the head-advancing
 * account lease). A device DRAINS its account's authority-state publication obligations
 * (PgPropagationOutbox): claim the publishable head under a server lease, prepare (freeze) the
 * epoch to publish, release / report failure, and finally COMPLETE. claim/prepare/release/fail are
 * crypto-FREE; handleComplete (leaf 3c) is the ONE crypto-bearing op — it verifies the signed
 * AccountAuthorityStateV1 publication, stores it, then marks the drained obligations done.
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
  #crypto;

  constructor(ctx) {
    this.#ctx = ctx;
    // Only handleComplete (leaf 3c) is crypto-bearing — it verifies the submitted publication.
    this.#crypto = new NodeCryptoProvider();
  }

  #rateBudget() {
    return this.#ctx.runtime && this.#ctx.runtime.rateBudget
      ? this.#ctx.runtime.rateBudget
      : null;
  }

  #outbox() {
    return this.#ctx.runtime && this.#ctx.runtime.propagationOutbox
      ? this.#ctx.runtime.propagationOutbox
      : null;
  }

  #dht() {
    return this.#ctx.runtime && this.#ctx.runtime.recordDht ? this.#ctx.runtime.recordDht : null;
  }

  #serializer() {
    return this.#ctx.runtime && this.#ctx.runtime.accountMutationSerializer
      ? this.#ctx.runtime.accountMutationSerializer
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
  async #authorize(requestId) {
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
    if (authority.mode === "direct") {
      // F2 (audit leaf-3c): a DIRECT session is the account root signing the payload itself — its
      // signer MUST be the account key. Bind it explicitly here rather than assuming admission set it.
      if (typeof authority.signerPublicKeyB64 !== "string" || authority.signerPublicKeyB64 !== account) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "direct session signer must be the account identity", retryable: false });
        return null;
      }
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
      // F2 (audit leaf-3c): bind the delegated SIGNER to the lease OWNER device. The owner is the
      // authenticated session device (derived above); the signer's self-certifying device id MUST
      // equal it, or the lease would be owned by a device other than the one that signed the session.
      // Admission already enforces this, but the handler must not trust that invariant implicitly.
      let signerDeviceId;
      try {
        signerDeviceId = DeviceRegistrationV1.deviceIdFor(authority.signerPublicKeyB64);
      } catch (err) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "delegated session signer is malformed", retryable: false });
        return null;
      }
      if (signerDeviceId !== owner) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "delegated session signer is not bound to the session device", retryable: false });
        return null;
      }
    }
    // req 8: per-node rate limit, applied AFTER auth so unauthorized traffic spends no budget.
    if (!OUTBOX_LEASE_RATE_LIMITER.record(account, this.#now())) {
      this.#ctx.sendError({ id: requestId, code: "RATE_LIMITED", message: "too many outbox lease operations; retry shortly", retryable: true });
      return null;
    }
    // F3: the CLUSTER-WIDE budget, checked after the local one so a node that is already refusing
    // spends no shared round-trip. A backend failure here is NOT an allow — the ops this guards
    // need the same database anyway, so it surfaces as a retryable backend error rather than
    // opening the gate under load.
    const budget = this.#rateBudget();
    if (budget && typeof budget.consume === "function") {
      let verdict;
      try {
        verdict = await budget.consume({
          subject: account,
          bucket: OUTBOX_LEASE_CLUSTER_BUDGET_BUCKET,
          windowMs: OUTBOX_LEASE_CLUSTER_WINDOW_MS,
          maxPerWindow: OUTBOX_LEASE_CLUSTER_MAX_PER_MINUTE,
          nowMs: this.#now(),
        });
      } catch (err) {
        this.#sendBackendError(requestId, "rate-budget", err);
        return null;
      }
      if (verdict.allowed !== true) {
        this.#ctx.sendError({
          id: requestId,
          code: "RATE_LIMITED",
          message: "account exceeded the cluster-wide outbox lease budget; retry shortly",
          retryable: true,
        });
        return null;
      }
    }
    return { account, owner, outbox, mode: authority.mode };
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

  // req 8 (F6) + audit leaf-3c F4: a caught BACKEND error is reported WITHOUT err.message and WITHOUT
  // the token. Retry classification is CENTRALIZED (isRetryableBackendError) so every handler agrees
  // on which SQLSTATEs/transport codes are transient — a retryable failure → SERVICE_UNAVAILABLE;
  // anything else → INTERNAL. Token-free telemetry records the OP and error CODE only (never the
  // message or token) so operators can see backend-error rates without leaking secrets.
  #sendBackendError(requestId, op, err) {
    const code = err && typeof err.code === "string" ? err.code : "";
    const retryable = isRetryableBackendError(err);
    console.warn("[PropagationOutboxHandler] " + op + " backend error code=" + (code.length > 0 ? code : "unknown") + " retryable=" + retryable);
    if (retryable) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "authority-state propagation outbox temporarily unavailable", retryable: true });
      return;
    }
    this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: "outbox lease operation failed", retryable: false });
  }

  async handleClaim(requestId, body) {
    const auth = await this.#authorize(requestId);
    if (!auth) return;
    try {
      new OutboxLeaseClaimRequest(body); // contract-shape gate (claim carries no client input).
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err && err.message ? err.message : "invalid claim request", retryable: false });
      return;
    }
    // AWAITING ROOT SIGNATURE (Option A, 2026-07-26). Since the P0 fix the authority state is
    // root-signed only, so a DELEGATED session cannot author the publication this lease exists to
    // produce. Refuse the lease OUTRIGHT rather than handing one over: the outbox stores an
    // obligation, not a signed payload, so a delegated holder could only prepare, fail to sign,
    // and call fail() — burning an attempt, applying backoff, and eventually stamping the account
    // BLOCKED for revocations that were never actually broken. Nothing here touches the outbox, so
    // no attempt is recorded, no backoff applied, and the head stays immediately claimable by the
    // primary. The state is EXPLICIT so the client can say "your primary device needs to come
    // online" instead of silently reporting "nothing pending".
    if (auth.mode === "delegated") {
      const waiting = new OutboxLeaseClaimResponse({ leased: false, awaitingRootSignature: true });
      this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES, waiting.toJSON());
      return;
    }
    let result;
    try {
      result = await auth.outbox.claim(auth.account, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, "claim", err);
      return;
    }
    // null ⇒ nothing publishable, another device holds the lease, or the head is backing off.
    const res = result === null
      ? new OutboxLeaseClaimResponse({ leased: false, awaitingRootSignature: false })
      : new OutboxLeaseClaimResponse({
        leased: true,
        awaitingRootSignature: false,
        token: result.token,
        anchorEpoch: result.anchorEpoch,
        headEpoch: result.headEpoch,
        leaseExpiresAtMs: result.leaseExpiresAtMs,
        attempts: result.attempts,
      });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES, res.toJSON());
  }

  async handlePrepare(requestId, body) {
    const auth = await this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireToken(requestId, OutboxLeasePrepareRequest, body);
    if (token === null) return;
    let result;
    try {
      result = await auth.outbox.preparePublication(auth.account, token, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, "prepare", err);
      return;
    }
    const res = result === null
      ? new OutboxLeasePrepareResponse({ prepared: false })
      : new OutboxLeasePrepareResponse({ prepared: true, anchorEpoch: result.anchorEpoch, headEpoch: result.headEpoch });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_PREPARE_RES, res.toJSON());
  }

  async handleRelease(requestId, body) {
    const auth = await this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireToken(requestId, OutboxLeaseReleaseRequest, body);
    if (token === null) return;
    let released;
    try {
      released = await auth.outbox.release(auth.account, token, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, "release", err);
      return;
    }
    const res = new OutboxLeaseReleaseResponse({ released: released === true });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_RELEASE_RES, res.toJSON());
  }

  async handleFail(requestId, body) {
    const auth = await this.#authorize(requestId);
    if (!auth) return;
    const token = this.#requireToken(requestId, OutboxLeaseFailRequest, body);
    if (token === null) return;
    let result;
    try {
      result = await auth.outbox.fail(auth.account, token, auth.owner);
    } catch (err) {
      this.#sendBackendError(requestId, "fail", err);
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

  /**
   * leaf 3c — the VERIFIED completion (ack). The ONE crypto-bearing outbox op. Flow (the model the
   * contract fixes): authorize the session, VERIFY the submitted publication, STORE it, then mark
   * obligations done. Store-before-complete is deliberate: a 'done' watermark must imply the record
   * is retrievable by fan-out, so we never mark done a publication we failed to store.
   *
   * Verification (mirrors the peer-side open path): the DurableRecordV2 envelope must verify
   * (verifyDurableRecordV2 — envelope signature, time window, and — via the cert chain vs the
   * account's OWN current revocation state — that the signer holds deviceSet.publish for THIS
   * account); it must be an authority-state record OWNED BY the authenticated account; and the INNER
   * AccountAuthorityStateV1 must be bound to that same signer with its own valid signature. Only then
   * is its epoch M trusted. completePublication binds M to the lease's frozen prepared_epoch and
   * re-checks the token AFTER verification.
   */
  async handleComplete(requestId, body) {
    const auth = await this.#authorize(requestId);
    if (!auth) return;

    // Option A: only a ROOT session may complete. A delegated session cannot hold a lease (claim
    // refuses it above), so this is defense in depth — but it is stated here rather than left to
    // fail deep inside verification, because "publication verification failed" is the wrong answer
    // to give a device whose problem is structural, and because it keeps the node from spending an
    // Ed25519 verify on a submission that cannot pass by construction.
    if (auth.mode === "delegated") {
      this.#ctx.sendError({
        id: requestId,
        code: "FORBIDDEN",
        message: "the account authority state is root-signed only; a delegated session cannot complete a publication",
        retryable: false,
      });
      return;
    }

    let req;
    try {
      req = new OutboxLeaseCompleteRequest(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err && err.message ? err.message : "invalid complete request", retryable: false });
      return;
    }
    const token = req.leaseToken.trim();
    const record = req.record;

    // Completion needs BOTH the record store (to persist the publication) and the account's
    // authoritative revocation state (to verify a delegated signer's chain). Missing either ⇒ this
    // node cannot complete (fs/desktop wire neither) → SERVICE_UNAVAILABLE.
    const dht = this.#dht();
    if (!dht || typeof dht.putRecord !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable record store unavailable", retryable: false });
      return;
    }
    const serializer = this.#serializer();
    if (!serializer || typeof serializer.getAuthorityState !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "account mutation authority unavailable", retryable: false });
      return;
    }

    // The revocation state to verify a DELEGATED publication's cert chain against is the account's
    // OWN current authority (the home is authoritative for its own account). Projected strictly (F5)
    // — no coercion of a malformed backend shape into a plausible-but-wrong state.
    let revocationState;
    try {
      const current = await serializer.getAuthorityState(auth.account);
      if (!current || typeof current !== "object"
          || !Array.isArray(current.revokedCertIds)
          || typeof current.minValidIssuedAtMs !== "number" || !Number.isFinite(current.minValidIssuedAtMs)) {
        this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: "account authority state unavailable", retryable: false });
        return;
      }
      revocationState = { revokedCertIds: current.revokedCertIds, minValidIssuedAtMs: current.minValidIssuedAtMs };
    } catch (err) {
      this.#sendBackendError(requestId, "complete", err);
      return;
    }

    // VERIFY the envelope. A finite now is required; #now() supplies it (verifyDurableRecordV2 itself
    // rejects a non-finite now, so a bad clock fails closed to BAD_REQUEST, never bypasses).
    const verdict = await verifyDurableRecordV2({ record, crypto: this.#crypto, nowMs: this.#now(), revocationState });
    if (!verdict.ok) {
      // verdict.reason is token-free but may carry chain internals — answer generically.
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "publication verification failed", retryable: false });
      return;
    }
    if (verdict.recordKind !== ACCOUNT_AUTHORITY_STATE_RECORD_KIND || verdict.ownerPublicKeyB64 !== auth.account) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "publication is not this account's authority-state record", retryable: false });
      return;
    }

    // Open + bind the INNER AccountAuthorityStateV1: same signer as the verified envelope, same
    // account, and an independently valid inner signature. The envelope alone does not prove the
    // inner epoch is authentic (same-signer binding — the peer-side open path enforces this too).
    let authorityState;
    try {
      const stateJson = JSON.parse(new TextDecoder().decode(base64ToBytes(String(record.payloadB64 || ""))));
      authorityState = new AccountAuthorityStateV1(stateJson);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "publication payload is malformed", retryable: false });
      return;
    }
    if (authorityState.accountIdentityPublicKeyB64 !== auth.account || authorityState.signerPublicKeyB64 !== verdict.signerPublicKeyB64) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "publication payload is not bound to the verified envelope", retryable: false });
      return;
    }
    let innerOk;
    try {
      innerOk = await this.#crypto.verify({
        publicKey: base64ToBytes(authorityState.signerPublicKeyB64),
        msg: AccountAuthorityStateV1.signableBytes(authorityState.toJSON()),
        sig: base64ToBytes(authorityState.sig.sigB64),
      });
    } catch (err) {
      innerOk = false;
    }
    if (innerOk !== true) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "publication payload signature invalid", retryable: false });
      return;
    }
    const publishedEpoch = authorityState.epoch;
    // An obligation exists only for epoch >= 1 (a real mutation). A non-positive epoch acks nothing.
    if (!Number.isSafeInteger(publishedEpoch) || publishedEpoch < 1) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "publication epoch does not identify an obligation", retryable: false });
      return;
    }

    // STORE before completing — a 'done' watermark must imply the record is retrievable by fan-out.
    // An idempotent re-put of the same owner-keyed coordinate is safe.
    let putResult;
    try {
      putResult = await dht.putRecord(record);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_PUT_FAILED", message: err && err.message ? err.message : "publication store failed", retryable: true });
      return;
    }
    if (!putResult || putResult.storedLocally !== true) {
      this.#ctx.sendError({ id: requestId, code: "RECORD_REJECTED", message: "publication rejected by the record store", retryable: false });
      return;
    }

    // COMPLETE: re-check the token under the anchor lock (AFTER verification) and mark obligations
    // <= M done. The storage layer binds M to the lease's frozen prepared_epoch.
    let result;
    try {
      result = await auth.outbox.completePublication(auth.account, token, auth.owner, publishedEpoch);
    } catch (err) {
      this.#sendBackendError(requestId, "complete", err);
      return;
    }
    // null ⇒ the lease lapsed during verification (benign race): completed:false, no epoch. The
    // published record is stored + authentic, so the next lease's drain finds nothing new.
    if (result === null) {
      const res = new OutboxLeaseCompleteResponse({ completed: false });
      this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES, res.toJSON());
      return;
    }
    if (result.completed !== true) {
      // Live lease, but M != the frozen prepared_epoch: the device published an epoch other than the
      // one it prepared. A protocol violation → token-free CONFLICT.
      this.#ctx.sendError({ id: requestId, code: "CONFLICT", message: "publication epoch does not match the prepared epoch", retryable: false });
      return;
    }
    const res = new OutboxLeaseCompleteResponse({ completed: true, doneThroughEpoch: result.doneThroughEpoch });
    this.#ctx.sendResponse(requestId, T.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES, res.toJSON());
  }
}
