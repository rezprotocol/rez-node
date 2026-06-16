import { REZ_CONTRACT_TYPES, base64ToBytes, canonicalJSONStringify } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;
const NODE_DELEGATION_TTL_MAX_MS = 30 * 24 * 60 * 60 * 1000;

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
        this.#ctx.sendError({
          id: requestId,
          code: "INTERNAL_ERROR",
          message: err && err.message ? err.message : "claim failed",
          retryable: false,
        });
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
