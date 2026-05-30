import { DepositPolicyV1, verifyDepositPolicy, REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;

/**
 * Handles `inbox.setDepositPolicy` — claimant publishes/replaces the policy
 * that the relay enforces on incoming deposits to a given inbox.
 *
 * Verification chain:
 *   1. The claim registry must already have a claimant pubkey for the inbox.
 *   2. The policy must be signed by that pubkey (verifyDepositPolicy).
 *   3. The policy's `policyVersion` must strictly exceed the stored one
 *      (DepositPolicyStore.put enforces).
 *   4. The session presenting the policy must be bound to the same claimant
 *      pubkey — otherwise a stranger could submit a policy they happen to
 *      have a copy of, even if it's correctly signed.
 *
 * See docs/SECURITY_AUDIT.md HIGH-1.
 */
export class DepositPolicyHandler {
  #ctx;
  #crypto;

  constructor(ctx, { crypto = null } = {}) {
    this.#ctx = ctx;
    this.#crypto = crypto || new NodeCryptoProvider();
  }

  async handleSet(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const runtime = this.#ctx.runtime;
    const claimRegistry = runtime ? runtime.inboxClaimRegistry : null;
    const policyStore = runtime ? runtime.depositPolicyStore : null;
    if (!claimRegistry || !policyStore) {
      this.#ctx.sendError({
        id: requestId,
        code: "SERVICE_UNAVAILABLE",
        message: "deposit policy store unavailable",
        retryable: false,
      });
      return;
    }

    let policy;
    try {
      policy = DepositPolicyV1.fromJSON(body && body.policy);
    } catch (err) {
      this.#ctx.sendError({
        id: requestId,
        code: "BAD_REQUEST",
        message: err && err.message ? err.message : "invalid policy",
        retryable: false,
      });
      return;
    }

    const expectedClaimantPublicKeyB64 = claimRegistry.getClaimantPublicKey(policy.inboxId);
    if (!expectedClaimantPublicKeyB64) {
      this.#ctx.sendError({
        id: requestId,
        code: "INBOX_NOT_CLAIMED",
        message: "inbox is not claimed; cannot set deposit policy",
        retryable: false,
      });
      return;
    }
    // Session must have proven possession of the claimant key earlier in
    // the session (via inbox.claim). A session may have multiple bound
    // claimants — one per claimed inbox — so we don't compare against the
    // session-auth identity. See docs/CAPABILITY_MODEL.md §8.
    const bound = this.#ctx.boundClaimantPublicKeys;
    const isBound = bound && typeof bound.has === "function" && bound.has(expectedClaimantPublicKeyB64);
    if (!isBound) {
      this.#ctx.sendError({
        id: requestId,
        code: "UNAUTHORIZED",
        message: "session not bound to inbox claimant",
        retryable: false,
      });
      return;
    }
    const verified = await verifyDepositPolicy({
      policy,
      expectedClaimantPublicKeyB64,
      crypto: this.#crypto,
    });
    if (!verified) {
      this.#ctx.sendError({
        id: requestId,
        code: "INVALID_SIGNATURE",
        message: "deposit policy signature did not verify",
        retryable: false,
      });
      return;
    }

    let stored;
    try {
      stored = await policyStore.put(policy);
    } catch (err) {
      const code = err && err.code ? err.code : "INTERNAL_ERROR";
      this.#ctx.sendError({
        id: requestId,
        code,
        message: err && err.message ? err.message : "failed to store deposit policy",
        retryable: false,
      });
      return;
    }

    this.#ctx.sendResponse(requestId, T.INBOX_SET_DEPOSIT_POLICY_RES, {
      inboxId: stored.inboxId,
      policyVersion: stored.policyVersion,
      expiresAtMs: stored.expiresAtMs,
    });
  }
}
