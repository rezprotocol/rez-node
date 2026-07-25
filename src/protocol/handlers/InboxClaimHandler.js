import { REZ_CONTRACT_TYPES, base64ToBytes, canonicalJSONStringify } from "@rezprotocol/core";
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
    if (!this.#ctx.requireSession(requestId)) return;

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

    const claimMsg = new TextEncoder().encode(canonicalJSONStringify({
      inboxId,
      claimantPublicKeyB64,
      claimedAtMs,
    }));

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

    let storedClaimedAtMs = claimedAtMs;
    if (existingClaimant === null) {
      try {
        const stored = await registry.claim({
          inboxId,
          claimantPublicKeyB64,
          claimedAtMs,
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
    }

    // Durable home (pg cluster): register this session's device cursor so the
    // durable log is drainable from ANY node on reconnect — the client re-claims
    // (re-attests) on every (re)connect, so this is the "register on every bind"
    // point. registerDevice is idempotent: a reconnecting device is a no-op and
    // NEVER rewinds the shared (inbox, device) cursor (no split-brain). The
    // maxDevices=1 cap refuses a SECOND distinct device until per-device E2EE
    // (S2.5) — fanning one ciphertext to two devices on a shared ratchet breaks
    // it — surfaced as a clean refusal rather than binding an unusable device.
    const durableInbox = this.#ctx.runtime && this.#ctx.runtime.durableInbox;
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
        await this.#ctx.runtime.registerHostedSession(claimantPublicKeyB64, {
          inboxId,
          nodeKeyId: delegationRecord.nodeKeyId,
          nodePublicKeyB64: delegationRecord.nodePublicKeyB64,
          relayKeyId: delegationRecord.relayKeyId,
          issuedAtMs: delegationRecord.issuedAtMs,
          expiresAtMs: delegationRecord.expiresAtMs,
          delegationSigB64: delegationRecord.delegationSigB64,
        });
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

  async #verifyNodeDelegation({ inboxId, claimantPublicKeyB64, claimantPublicKey, delegation } = {}) {
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
    let sigBytes;
    try {
      sigBytes = base64ToBytes(delegationSigB64);
    } catch (decodeErr) {
      return fail("base64-decode-failed", { inboxId, err: decodeErr && decodeErr.message ? decodeErr.message : decodeErr });
    }
    const payload = {
      kind: "inbox-node-delegation",
      inboxId,
      claimantPublicKeyB64,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      issuedAtMs,
      expiresAtMs,
    };
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
    return { nodeKeyId, nodePublicKeyB64, relayKeyId, issuedAtMs, expiresAtMs, delegationSigB64 };
  }
}
