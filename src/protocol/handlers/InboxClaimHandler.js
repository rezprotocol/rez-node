import { REZ_CONTRACT_TYPES, base64ToBytes, canonicalJSONStringify , canonicalInboxClaimPayload, canonicalNodeDelegationPayload } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;
const NODE_DELEGATION_TTL_MAX_MS = 30 * 24 * 60 * 60 * 1000;

// Track 2 abuse quotas for OPEN registration. Two subjects, because they bound different attacks:
// the per-KEY budget stops one keypair churning claims, and the per-IP budget stops one source
// rotating through fresh keypairs to sidestep it (a keypair is free; an address is less so). Both
// are cluster-wide — a per-node limiter behind a load balancer just multiplies by the node count.
export const INBOX_CLAIM_BUDGET_BUCKET = "inbox_claim";
export const INBOX_CLAIM_WINDOW_MS = 60_000;
export const INBOX_CLAIM_MAX_PER_KEY_PER_MINUTE = 10;
export const INBOX_CLAIM_MAX_PER_IP_PER_MINUTE = 30;

/**
 * Handles inbox.claim — open-registration claim of an inbox at this node.
 *
 * The claim record is self-authenticating: the claimant signs canonical-JSON
 * of (inboxId, claimantPublicKeyB64, claimedAtMs) with the claimant privkey.
 * The handler verifies the signature against the supplied pubkey, then
 * persists the inbox → claimantPublicKey mapping in InboxClaimRegistry.
 *
 * The claim body ALSO carries a node-delegation signed by the same claimant
 * key, authorizing this node to advertise the inbox to the relay mesh. The
 * delegation binds (inboxId, claimantPublicKey, nodeKey, expiry) and is the
 * same proof every relay along the routing path will check.
 *
 * See docs/CAPABILITY_MODEL.md §6.
 */
export class InboxClaimHandler {
  #ctx;
  #crypto;

  constructor(ctx) {
    this.#ctx = ctx;
    this.#crypto = new NodeCryptoProvider();
  }

  #rateBudget() {
    return this.#ctx.runtime && this.#ctx.runtime.rateBudget ? this.#ctx.runtime.rateBudget : null;
  }

  /**
   * Cluster-wide claim budgets for open registration. Returns false after sending the error.
   *
   * A backend failure is NOT an allow: the claim it guards writes to the same database, so failing
   * closed here costs nothing that was going to work anyway, and a quota that opens under load is
   * not a quota.
   */
  async #withinClaimBudget(requestId, claimantPublicKeyB64) {
    const budget = this.#rateBudget();
    if (!budget || typeof budget.consume !== "function") return true;
    const nowMs = Date.now();
    const subjects = [
      { subject: "claim-key:" + claimantPublicKeyB64, max: INBOX_CLAIM_MAX_PER_KEY_PER_MINUTE },
    ];
    // peerIp is already normalized (IPv6 truncated to /64, SECURITY_AUDIT MED-14) and is empty for
    // synthetic sockets in tests, where there is no source to bound.
    const peerIp = typeof this.#ctx.peerIp === "string" ? this.#ctx.peerIp : "";
    if (peerIp.length > 0) {
      subjects.push({ subject: "claim-ip:" + peerIp, max: INBOX_CLAIM_MAX_PER_IP_PER_MINUTE });
    }
    for (const entry of subjects) {
      let verdict;
      try {
        verdict = await budget.consume({
          subject: entry.subject,
          bucket: INBOX_CLAIM_BUDGET_BUCKET,
          windowMs: INBOX_CLAIM_WINDOW_MS,
          maxPerWindow: entry.max,
          nowMs,
        });
      } catch (err) {
        console.warn("[InboxClaimHandler] claim budget unavailable: " + (err && err.code ? err.code : "unknown"));
        this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "claim quota unavailable; retry shortly", retryable: true });
        return false;
      }
      if (verdict.allowed !== true) {
        this.#ctx.sendError({ id: requestId, code: "RATE_LIMITED", message: "too many inbox claims; retry shortly", retryable: true });
        return false;
      }
    }
    return true;
  }

  async handleClaim(requestId, body) {
    const registry = this.#ctx.runtime && this.#ctx.runtime.inboxClaimRegistry;
    if (!registry) {
      this.#ctx.sendError({
        id: requestId,
        code: "SERVICE_UNAVAILABLE",
        message: "Inbox claim registry unavailable",
        retryable: false,
      });
      return;
    }

    const inboxId = typeof body.inboxId === "string" ? body.inboxId.trim() : "";
    const claimantPublicKeyB64 = typeof body.claimantPublicKeyB64 === "string" ? body.claimantPublicKeyB64.trim() : "";
    const claimedAtMs = Number(body.claimedAtMs);
    const signatureB64 = typeof body.signatureB64 === "string" ? body.signatureB64.trim() : "";
    // Portable inbox lease (plans/PORTABLE_INBOX_LEASE_SPEC.md §2): a claim
    // either CARRIES THE LEASE EXTENSION (closePublicKeyB64 + generation
    // INSIDE the signed payload) or it is legacy — a capability of the
    // record, not a version number. Both fields or neither; a partial pair
    // is malformed, never inferred around.
    const closePublicKeyB64 = typeof body.closePublicKeyB64 === "string" ? body.closePublicKeyB64.trim() : "";
    const generationRaw = body.generation;
    const hasClose = closePublicKeyB64.length > 0;
    const hasGeneration = generationRaw !== undefined && generationRaw !== null;
    if (hasClose !== hasGeneration) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "closePublicKeyB64 and generation must be supplied together or not at all", retryable: false });
      return;
    }
    const generation = hasGeneration ? Number(generationRaw) : null;
    if (hasGeneration && (!Number.isInteger(generation) || generation < 1)) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "generation must be a positive integer", retryable: false });
      return;
    }
    const claimCarriesLease = hasGeneration;

    if (!inboxId) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "inboxId required", retryable: false });
      return;
    }
    if (!claimantPublicKeyB64) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "claimantPublicKeyB64 required", retryable: false });
      return;
    }
    if (!Number.isFinite(claimedAtMs) || claimedAtMs <= 0) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "claimedAtMs must be a positive number", retryable: false });
      return;
    }
    if (!signatureB64) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "signatureB64 required", retryable: false });
      return;
    }

    // SESSION_AUTH_V5 §3 cardinality, fail-fast before any signature work:
    // the session principal decides which claimant roots it may bind. An
    // ACCOUNT (v4) principal admits any (legacy multi-key claim path); a
    // CLAIMANT(KA) principal admits only KA — one session, one claimant root,
    // which is what demotes the F1b co-residency disclosure to traffic
    // analysis. ProtocolContext.bindInboxToSession re-checks as the backstop.
    const principal = this.#ctx.principal;
    if (!principal || principal.admitsClaimantBinding(claimantPublicKeyB64) !== true) {
      this.#ctx.sendError({
        id: requestId,
        code: "FORBIDDEN",
        message: "session principal does not admit this claimant root",
        retryable: false,
      });
      return;
    }

    // F9 Option B is a configuration boundary, not a late cursor error. A
    // shared durable home remains the explicit ACCOUNT/device-bearing legacy
    // path, and Pg has no lease/generation storage surface. Reject both
    // incompatible shapes before signature budgets, registry writes, cursor
    // registration, or hosted-session publication can mutate anything.
    const durableInbox = this.#ctx.runtime && this.#ctx.runtime.durableInbox;
    const sharedDurableHome = Boolean(durableInbox && typeof durableInbox.registerDevice === "function");
    if (sharedDurableHome && typeof principal.isClaimant === "function" && principal.isClaimant()) {
      this.#ctx.sendError({
        id: requestId,
        code: "FORBIDDEN",
        message: "claimant sessions are not compatible with the shared durable home (F9 Option B)",
        retryable: false,
      });
      return;
    }
    if (sharedDurableHome && claimCarriesLease) {
      this.#ctx.sendError({
        id: requestId,
        code: "SERVICE_UNAVAILABLE",
        message: "portable lease claims are not supported by the shared durable home (F9 Option B)",
        retryable: false,
      });
      return;
    }

    // Proof-of-possession is the claim signature itself (verified below)
    // — NOT equality with the session-auth identity. A single session may
    // claim multiple inboxes under independent keypairs so that an
    // observer cannot link them via shared session identity (see
    // docs/CAPABILITY_MODEL.md §8). The session-registry binding for
    // delivery routing happens after a successful claim via
    // `ctx.bindInboxToSession`.
    let publicKey;
    let signature;
    try {
      publicKey = base64ToBytes(claimantPublicKeyB64);
      signature = base64ToBytes(signatureB64);
    } catch {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "claimantPublicKeyB64 or signatureB64 is not valid base64", retryable: false });
      return;
    }

    // The bytes-to-verify come from the SAME rez-core builder the SDK signs
    // with — signer and verifier cannot drift.
    const claimMsg = new TextEncoder().encode(canonicalJSONStringify(canonicalInboxClaimPayload({
      inboxId,
      claimantPublicKeyB64,
      claimedAtMs,
      closePublicKeyB64: claimCarriesLease ? closePublicKeyB64 : undefined,
      generation: claimCarriesLease ? generation : undefined,
    })));

    let claimVerified = false;
    try {
      claimVerified = await this.#crypto.verify({ publicKey, msg: claimMsg, sig: signature });
    } catch {
      claimVerified = false;
    }
    if (!claimVerified) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "claim signature did not verify", retryable: false });
      return;
    }

    // Budget AFTER the signature check, so unsigned junk costs an attacker a round-trip and costs
    // the budget nothing — spending it earlier would let anyone exhaust a victim key's allowance by
    // sending garbage in its name.
    if (!(await this.#withinClaimBudget(requestId, claimantPublicKeyB64))) return;

    // Lease L1 generation kill rule, M6-scoped by tombstone REASON
    // (rez-chat plans/MOBILE_LIFECYCLE_ADAPTER_PLAN.md §7e, frozen):
    //   "terminal"  — the close key killed the inboxId LINEAGE: EVERY future
    //                 claim is refused, any generation. Without this, a
    //                 malicious claimant could submit finalGeneration+1 over
    //                 a terminal tombstone and resurrect the inbox,
    //                 undermining the close key. (Legacy tombstones default
    //                 to "terminal" — unknown historical closure must never
    //                 permit resurrection.)
    //   "reclaimed" — expiry reclamation killed generations ≤
    //                 finalGeneration; a claim STRICTLY ABOVE it starts a
    //                 fresh lifetime (the M6 re-mint path).
    // The refusal carries typed detail — the client's re-mint policy needs
    // the authoritative reason + finalGeneration, never parsed error text.
    // Guarded on the method: the pg/hosted registry is the LEGACY path (F9)
    // and carries no lease surface yet.
    if (typeof registry.getTombstone === "function") {
      const tombstone = await registry.getTombstone(inboxId);
      if (tombstone) {
        const closeReason = tombstone.reason === "reclaimed" ? "reclaimed" : "terminal";
        const dead = closeReason === "terminal"
          || generation === null
          || generation <= tombstone.finalGeneration;
        if (dead) {
          this.#ctx.sendError({
            id: requestId,
            code: "INBOX_CLOSED",
            message: closeReason === "reclaimed"
              ? "inbox generation was reclaimed after lease expiry"
              : "inbox is terminally closed",
            retryable: false,
            detail: { closeReason, finalGeneration: tombstone.finalGeneration },
          });
          return;
        }
        // Fresh-lifetime admission over a reclaimed tombstone (M6 §7e pin 7):
        // PURGE any residual ciphertext of the dead generation BEFORE the new
        // claim can exist. The store is keyed by inboxId with no generation
        // namespace, so this is the one deterministic point where old bytes
        // and new bytes cannot be confused — after this claim is accepted the
        // two are indistinguishable. Normally a no-op (the reclamation sweep
        // already purged); it closes the crash window where the registry
        // recorded the reclamation but the process died before the purge
        // (the sweep never revisits a deleted claim). Purge failure fails
        // the CLAIM — admitting a fresh lifetime over unpurged bytes would
        // let the dead generation's mail surface under the new one.
        try {
          await this.#purgeResidualMailbox(inboxId);
        } catch (err) {
          this.#ctx.sendError({
            id: requestId,
            code: "INTERNAL",
            message: "residual mailbox purge failed; re-mint refused: " + (err && err.message ? err.message : "purge failed"),
            retryable: true,
          });
          return;
        }
      }
    }

    // Verify the node-delegation: the same claimant key signs an attestation
    // that this node may advertise the inbox to the relay mesh. The signed
    // payload must match the one every routing-layer relay will check.
    const nodeDelegation = body && typeof body.nodeDelegation === "object" && body.nodeDelegation !== null
      ? body.nodeDelegation
      : null;
    const delegationRecord = await this.#verifyNodeDelegation({
      inboxId,
      claimantPublicKeyB64,
      claimantPublicKey: publicKey,
      delegation: nodeDelegation,
      claimGeneration: claimCarriesLease ? generation : null,
    });
    if (!delegationRecord) {
      this.#ctx.sendError({
        id: requestId,
        code: "INVALID_SIGNATURE",
        message: "node delegation missing or invalid",
        retryable: false,
      });
      return;
    }

    // Idempotent re-claim path: same pubkey re-attesting an existing claim.
    // `await` works for both the in-memory registry (sync) and the cluster
    // PgInboxClaimRegistry (async, authoritative). The claim() below is still
    // race-safe: a stale null here is caught by claim()'s INBOX_ALREADY_CLAIMED.
    const existingClaimant = await registry.getClaimantPublicKey(inboxId);
    if (existingClaimant !== null && existingClaimant !== claimantPublicKeyB64) {
      this.#ctx.sendError({
        id: requestId,
        code: "INBOX_ALREADY_CLAIMED",
        message: "inbox already claimed by a different keypair",
        retryable: false,
      });
      return;
    }

    // Lease L2 lifecycle gate for EXISTING claims (derived purely from
    // durable state + now — a provider restarted mid-grace gives the same
    // answer): renewal is legal while ACTIVE or CLOSED_EXPIRED (the
    // "phone wakes five minutes late" path), and NEVER once the verdict is
    // RECLAIMABLE (grace over — the sweep owns it now, whether or not it has
    // run yet) or terminal.
    if (existingClaimant !== null && typeof registry.lifecycleFor === "function") {
      const verdict = registry.lifecycleFor(inboxId, Date.now());
      if (verdict.state === "RECLAIMABLE" || verdict.state === "CLOSED_TERMINAL") {
        // M6: a terminal refusal carries the tombstone's typed semantics so
        // the client can tell "intent death, never recover" from a
        // reclamation it may re-mint over. (The "expired" branch has no
        // tombstone yet — the sweep hasn't run — so there is no authority
        // for finalGeneration and no detail is attached.)
        let detail;
        if (verdict.reason !== "expired" && typeof registry.getTombstone === "function") {
          const tombstone = await registry.getTombstone(inboxId);
          if (tombstone) {
            detail = {
              closeReason: tombstone.reason === "reclaimed" ? "reclaimed" : "terminal",
              finalGeneration: tombstone.finalGeneration,
            };
          }
        }
        this.#ctx.sendError({
          id: requestId,
          code: verdict.reason === "expired" ? "LEASE_EXPIRED" : "INBOX_CLOSED",
          message: verdict.reason === "expired"
            ? "the lease grace window has lapsed; this inbox is due for reclamation"
            : "inbox is terminally closed",
          retryable: false,
          detail,
        });
        return;
      }
    }

    // Lease L1 reattestation consistency: a v2 claim re-attests with EXACTLY
    // its stored close key + generation. Neither a downgrade (legacy-shaped
    // reattest of a v2 claim) nor an in-place upgrade (v2 reattest of a
    // legacy claim — a stolen claim key must not be able to graft a close key
    // the account never minted) nor a substitution is accepted. Want v2
    // semantics for an old inbox: mint a new inbox.
    if (existingClaimant !== null && typeof registry.getClaim === "function") {
      const existingClaim = await registry.getClaim(inboxId);
      const storedCarriesLease = Boolean(existingClaim && Number.isInteger(existingClaim.generation));
      const mismatch = storedCarriesLease !== claimCarriesLease
        || (storedCarriesLease && (existingClaim.closePublicKeyB64 !== closePublicKeyB64
          || existingClaim.generation !== generation));
      if (mismatch) {
        this.#ctx.sendError({
          id: requestId,
          code: "CLAIM_RECORD_MISMATCH",
          message: "reattestation does not match the stored claim record",
          retryable: false,
        });
        return;
      }
    }

    let storedClaimedAtMs = claimedAtMs;
    if (existingClaimant === null) {
      try {
        const stored = await registry.claim({
          inboxId,
          claimantPublicKeyB64,
          claimedAtMs,
          closePublicKeyB64: claimCarriesLease ? closePublicKeyB64 : null,
          generation: claimCarriesLease ? generation : null,
          // L2: the verified lease's class + expiry become durable state so
          // the retention lifecycle is derivable across restarts.
          retentionClass: claimCarriesLease ? delegationRecord.retentionClass : null,
          leaseExpiresAtMs: claimCarriesLease ? delegationRecord.expiresAtMs : null,
        });
        storedClaimedAtMs = stored.claimedAtMs;
      } catch (err) {
        if (err && err.code === "INBOX_ALREADY_CLAIMED") {
          this.#ctx.sendError({
            id: requestId,
            code: "INBOX_ALREADY_CLAIMED",
            message: "inbox already claimed",
            retryable: false,
          });
          return;
        }
        if (err && err.code === "INBOX_CLAIM_QUOTA_EXCEEDED") {
          // A CEILING, not a rate: retrying later will not help, so this is NOT retryable. The
          // message deliberately states the limit without naming how many the claimant holds —
          // that count is not this caller's business to enumerate.
          this.#ctx.sendError({
            id: requestId,
            code: "INBOX_CLAIM_QUOTA_EXCEEDED",
            message: "this claimant already holds the maximum number of inboxes",
            retryable: false,
          });
          return;
        }
        this.#ctx.sendError({
          id: requestId,
          code: "INTERNAL_ERROR",
          message: err && err.message ? err.message : "claim failed",
          retryable: false,
        });
        return;
      }
    } else if (claimCarriesLease && typeof registry.renewLease === "function") {
      // L2 RENEWAL: a valid reattestation of an existing v2 claim extends the
      // stored lease expiry — this is the transition that restores ACTIVE
      // from CLOSED_EXPIRED during grace, with mail and bindings intact.
      try {
        await registry.renewLease({
          inboxId,
          retentionClass: delegationRecord.retentionClass,
          leaseExpiresAtMs: delegationRecord.expiresAtMs,
        });
      } catch (err) {
        this.#ctx.sendError({
          id: requestId,
          code: err && err.code === "CLAIM_RECORD_MISMATCH" ? "CLAIM_RECORD_MISMATCH" : "INTERNAL_ERROR",
          message: err && err.message ? err.message : "lease renewal failed",
          retryable: false,
        });
        return;
      }
    }

    // Durable home (pg cluster): register this session's device cursor so the
    // durable log is drainable from ANY node on reconnect — the client re-claims
    // (re-attests) on every (re)connect, so this is the "register on every bind"
    // point. registerDevice is idempotent: a reconnecting device is a no-op and
    // NEVER rewinds the shared (inbox, device) cursor (no split-brain). The
    // maxDevices=1 cap refuses a SECOND distinct device until per-device E2EE
    // (S2.5) — fanning one ciphertext to two devices on a shared ratchet breaks
    // it — surfaced as a clean refusal rather than binding an unusable device.
    if (durableInbox && typeof durableInbox.registerDevice === "function") {
      const deviceId = typeof this.#ctx.sessionDeviceId === "string" ? this.#ctx.sessionDeviceId.trim() : "";
      if (deviceId.length === 0) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session deviceId required", retryable: false });
        return;
      }
      try {
        await durableInbox.registerDevice(inboxId, deviceId);
      } catch (err) {
        if (err && err.code === "INBOX_CAP_EXCEEDED" && err.limitType === "devices") {
          this.#ctx.sendError({ id: requestId, code: "DEVICE_LIMIT", message: "additional devices are not yet supported (multi-device gated)", retryable: false });
          return;
        }
        this.#ctx.sendError({ id: requestId, code: "INTERNAL_ERROR", message: err && err.message ? err.message : "device registration failed", retryable: false });
        return;
      }
    }

    this.#ctx.bindInboxToSession(inboxId, claimantPublicKeyB64);
    this.#ctx.setSessionInbox(inboxId);

    if (typeof this.#ctx.runtime.registerHostedSession === "function") {
      try {
        const hostedRegistration = {
          inboxId,
          nodeKeyId: delegationRecord.nodeKeyId,
          nodePublicKeyB64: delegationRecord.nodePublicKeyB64,
          relayKeyId: delegationRecord.relayKeyId,
          issuedAtMs: delegationRecord.issuedAtMs,
          expiresAtMs: delegationRecord.expiresAtMs,
          delegationSigB64: delegationRecord.delegationSigB64,
        };
        // Lease L1: the fields are INSIDE the signed delegation bytes, so
        // every downstream verifier (relay registration) needs them to
        // reconstruct the payload — forward them wherever the sig travels.
        if (Number.isInteger(delegationRecord.generation)) {
          hostedRegistration.generation = delegationRecord.generation;
          hostedRegistration.retentionClass = delegationRecord.retentionClass;
        }
        await this.#ctx.runtime.registerHostedSession(claimantPublicKeyB64, hostedRegistration);
      } catch (err) {
        this.#ctx.sendError({
          id: requestId,
          code: "INTERNAL_ERROR",
          message: err && err.message ? err.message : "registerHostedSession failed",
          retryable: false,
        });
        return;
      }
    }

    this.#ctx.sendResponse(requestId, T.INBOX_CLAIM_RES, {
      inboxId,
      claimedAtMs: storedClaimedAtMs,
    });
  }

  async #verifyNodeDelegation({ inboxId, claimantPublicKeyB64, claimantPublicKey, delegation, claimGeneration = null } = {}) {
    const debug = process.env.REZ_INBOX_DEBUG === "1";
    const fail = (reason, extra) => {
      if (debug) console.warn("[INBOX-DEBUG] InboxClaimHandler.#verifyNodeDelegation reject: " + reason, extra || "");
      return null;
    };
    if (!delegation || typeof delegation !== "object") return fail("delegation-not-object", { inboxId });
    const identity = this.#ctx.runtime && typeof this.#ctx.runtime.getIdentity === "function"
      ? this.#ctx.runtime.getIdentity()
      : null;
    const expectedNodeKeyId = identity ? String(identity.nodeKeyId || "").trim() : "";
    const expectedNodePublicKeyB64 = identity ? String(identity.nodePublicKeyB64 || "").trim() : "";
    if (!expectedNodeKeyId || !expectedNodePublicKeyB64) return fail("runtime-identity-unavailable", { inboxId });

    const expectedRelayKeyId = identity ? String(identity.relayKeyId || "").trim() : "";
    if (!expectedRelayKeyId) return fail("runtime-relayKeyId-unavailable", { inboxId });

    const nodeKeyId = typeof delegation.nodeKeyId === "string" ? delegation.nodeKeyId.trim() : "";
    const nodePublicKeyB64 = typeof delegation.nodePublicKeyB64 === "string" ? delegation.nodePublicKeyB64.trim() : "";
    const relayKeyId = typeof delegation.relayKeyId === "string" ? delegation.relayKeyId.trim() : "";
    const delegationSigB64 = typeof delegation.delegationSigB64 === "string" ? delegation.delegationSigB64.trim() : "";
    const issuedAtMs = Number(delegation.issuedAtMs);
    const expiresAtMs = Number(delegation.expiresAtMs);
    if (nodeKeyId !== expectedNodeKeyId) return fail("nodeKeyId-mismatch", { inboxId, expected: expectedNodeKeyId, got: nodeKeyId });
    if (nodePublicKeyB64 !== expectedNodePublicKeyB64) return fail("nodePublicKeyB64-mismatch", { inboxId });
    if (relayKeyId !== expectedRelayKeyId) return fail("relayKeyId-mismatch", { inboxId, expected: expectedRelayKeyId, got: relayKeyId });
    if (!delegationSigB64) return fail("missing-delegationSigB64", { inboxId });
    if (!Number.isFinite(issuedAtMs)) return fail("invalid-issuedAtMs", { inboxId, raw: delegation.issuedAtMs });
    if (!Number.isFinite(expiresAtMs)) return fail("invalid-expiresAtMs", { inboxId, raw: delegation.expiresAtMs });
    if (expiresAtMs <= Date.now()) return fail("expired", { inboxId, expiresAtMs, nowMs: Date.now() });
    if (expiresAtMs <= issuedAtMs) return fail("expires-le-issued", { inboxId, issuedAtMs, expiresAtMs });
    if (expiresAtMs - issuedAtMs > NODE_DELEGATION_TTL_MAX_MS) return fail("ttl-too-long", { inboxId, ttlMs: expiresAtMs - issuedAtMs, maxMs: NODE_DELEGATION_TTL_MAX_MS });
    // Lease L1 (plans/PORTABLE_INBOX_LEASE_SPEC.md §2): for a v2 claim the
    // delegation IS the lease — its signed payload carries generation +
    // retentionClass. FAIL-CLOSED pairing: a v2 claim requires a v2 lease
    // whose generation EQUALS the claim's; a legacy claim must not carry
    // lease fields. An unknown retentionClass is refused, never silently
    // downgraded.
    const hasLeaseGen = delegation.generation !== undefined && delegation.generation !== null;
    const retentionClass = typeof delegation.retentionClass === "string" ? delegation.retentionClass.trim() : "";
    const leaseCarriesFields = hasLeaseGen || retentionClass.length > 0;
    if ((claimGeneration !== null) !== leaseCarriesFields) return fail("claim-lease-version-mismatch", { inboxId });
    let leaseGeneration = null;
    if (leaseCarriesFields) {
      if (!hasLeaseGen || retentionClass.length === 0) return fail("lease-fields-partial", { inboxId });
      leaseGeneration = Number(delegation.generation);
      if (!Number.isInteger(leaseGeneration) || leaseGeneration < 1) return fail("lease-generation-invalid", { inboxId });
      if (leaseGeneration !== claimGeneration) return fail("lease-generation-mismatch", { inboxId, leaseGeneration, claimGeneration });
      if (retentionClass !== "transient" && retentionClass !== "standard") return fail("retention-class-unknown", { inboxId, retentionClass });
    }
    let sigBytes;
    try {
      sigBytes = base64ToBytes(delegationSigB64);
    } catch (decodeErr) {
      return fail("base64-decode-failed", { inboxId, err: decodeErr && decodeErr.message ? decodeErr.message : decodeErr });
    }
    const payload = canonicalNodeDelegationPayload({
      inboxId,
      claimantPublicKeyB64,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      issuedAtMs,
      expiresAtMs,
      generation: leaseCarriesFields ? leaseGeneration : undefined,
      retentionClass: leaseCarriesFields ? retentionClass : undefined,
    });
    const msg = new TextEncoder().encode(canonicalJSONStringify(payload));
    let verified = false;
    let verifyErr = null;
    try {
      verified = await this.#crypto.verify({ publicKey: claimantPublicKey, msg, sig: sigBytes });
    } catch (err) {
      verified = false;
      verifyErr = err;
    }
    if (!verified) return fail("signature-verify-failed", { inboxId, err: verifyErr && verifyErr.message ? verifyErr.message : verifyErr });
    if (debug) console.log("[INBOX-DEBUG] InboxClaimHandler.#verifyNodeDelegation OK", { inboxId, nodeKeyId, relayKeyId });
    const out = { nodeKeyId, nodePublicKeyB64, relayKeyId, issuedAtMs, expiresAtMs, delegationSigB64 };
    if (leaseCarriesFields) {
      out.generation = leaseGeneration;
      out.retentionClass = retentionClass;
    }
    return out;
  }

  // M6: drain every residual event of a dead generation through the store's
  // own removal verb (caps/counters stay coherent) before a fresh-lifetime
  // claim is admitted. Missing store = nothing was ever stored here; missing
  // surface throws (the caller fails the claim — never admit over unknowns).
  async #purgeResidualMailbox(inboxId) {
    const inboxStore = this.#ctx.runtime && this.#ctx.runtime.inboxStore;
    if (!inboxStore) return;
    if (typeof inboxStore.list !== "function" || typeof inboxStore.ack !== "function") {
      throw new Error("inbox store lacks list/ack; cannot prove the dead generation's mail is gone");
    }
    for (;;) {
      const page = await inboxStore.list(inboxId, { limit: 100 });
      const items = page && Array.isArray(page.items) ? page.items : [];
      if (items.length === 0) return;
      for (const item of items) {
        await inboxStore.ack(inboxId, item.eventId);
      }
    }
  }
}
