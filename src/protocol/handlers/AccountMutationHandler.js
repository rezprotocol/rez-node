import { REZ_CONTRACT_TYPES, AccountDeviceMutationV1, DeviceInboxBindingV1 } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import { revalidateDelegatedAuthority } from "./revalidateDelegatedAuthority.js";

const T = REZ_CONTRACT_TYPES;

/**
 * Serialized account device-mutation authority (S2.5 S11). A device submits a
 * signed AccountDeviceMutationV1 to its account's HOME; the home serializes it
 * (PgAccountMutationSerializer) under a per-account lock, folds the canonical
 * device set, and bumps a monotonic epoch. A companion op serves the current
 * authority state ({epoch, revokedCertIds, minValidIssuedAtMs}) so a device can
 * fold + publish the signed AccountAuthorityStateV1 for off-home peers.
 *
 * Authority is proven by the AUTHENTICATED session (`ctx.sessionAuthority`),
 * exactly as DeviceHandler.handleRevoke does — the envelope carries no cert
 * chain. A delegated device needs the action's explicit capability (device.add /
 * device.revoke) in its granted set; a primary (direct) session holds all.
 *
 * Only on a pg cluster node (the serializer is null on fs/desktop ⇒
 * SERVICE_UNAVAILABLE). The account is always the AUTHENTICATED session's own
 * (ownerPublicKeyB64) — the account-blindness boundary is preserved.
 */
export class AccountMutationHandler {
  #ctx;
  #crypto;

  constructor(ctx) {
    this.#ctx = ctx;
    this.#crypto = new NodeCryptoProvider();
  }

  #serializer() {
    return this.#ctx.runtime && this.#ctx.runtime.accountMutationSerializer
      ? this.#ctx.runtime.accountMutationSerializer
      : null;
  }

  async #verifyEd25519(publicKeyB64, msgBytes, sigB64) {
    let publicKey;
    let sig;
    try {
      publicKey = Buffer.from(String(publicKeyB64), "base64");
      sig = Buffer.from(String(sigB64), "base64");
    } catch (err) {
      return false;
    }
    return Promise.resolve(this.#crypto.verify({ publicKey, msg: msgBytes, sig }))
      .then((ok) => ok === true)
      .catch(() => false);
  }

  async handleSubmit(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    const serializer = this.#serializer();
    if (!serializer || typeof serializer.submitMutation !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "account mutation authority unavailable", retryable: false });
      return;
    }
    const accountPubB64 = typeof this.#ctx.ownerPublicKeyB64 === "string" ? this.#ctx.ownerPublicKeyB64.trim() : "";
    if (accountPubB64.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session account identity required", retryable: false });
      return;
    }

    const mutationJson = body && typeof body.mutation === "object" && body.mutation !== null ? body.mutation : null;
    if (!mutationJson) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "mutation is required", retryable: false });
      return;
    }
    let mutation;
    try {
      mutation = new AccountDeviceMutationV1(mutationJson);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid mutation: " + (err && err.message ? err.message : "unknown"), retryable: false });
      return;
    }

    // You may only mutate YOUR OWN account.
    if (mutation.accountIdentityPublicKeyB64 !== accountPubB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "mutation account does not match the authenticated session account", retryable: false });
      return;
    }

    // Bind the envelope signer to the session + check per-op authority (dual-mode,
    // mirroring DeviceHandler.handleRevoke). A PRIMARY (direct) session holds all
    // capabilities and signs with the account key; a DELEGATED session signs with
    // its device key C and must carry the action's capability.
    const requiredCap = mutation.action; // "device.add" | "device.revoke"
    const authority = this.#ctx.sessionAuthority;
    const delegated = authority && typeof authority === "object" && authority.mode === "delegated";
    let expectedSignerB64;
    if (delegated) {
      const caps = Array.isArray(authority.grantedCapabilities) ? authority.grantedCapabilities : [];
      if (!caps.includes(requiredCap)) {
        this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "delegated device lacks the " + requiredCap + " capability", retryable: false });
        return;
      }
      expectedSignerB64 = typeof authority.signerPublicKeyB64 === "string" ? authority.signerPublicKeyB64.trim() : "";
      if (expectedSignerB64.length === 0) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "delegated session is missing its device signer key", retryable: false });
        return;
      }
    } else {
      expectedSignerB64 = accountPubB64;
    }
    if (mutation.signerPublicKeyB64 !== expectedSignerB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "mutation signer is not the authenticated session signer", retryable: false });
      return;
    }

    const nowMs = Date.now();
    if (nowMs < mutation.issuedAtMs || nowMs >= mutation.expiresAtMs) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "mutation is not currently valid", retryable: false });
      return;
    }
    const sigOk = await this.#verifyEd25519(mutation.signerPublicKeyB64, AccountDeviceMutationV1.signableBytes(mutation), mutation.sig.sigB64);
    if (!sigOk) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "mutation signature invalid", retryable: false });
      return;
    }

    // Audit 2026-07-09 (F2): the session's `sessionAuthority` (grantedCapabilities)
    // was fixed at connect time and consulted per-op above. Re-validate the
    // delegated chain against the home's CURRENT authority state on every
    // mutation via the shared revalidator (SSOT — DeviceHandler runs the same
    // check). Direct (primary) sessions sign with the account root, which holds
    // every capability and cannot be revoked, so they skip this.
    if (delegated) {
      const recheck = await revalidateDelegatedAuthority({
        serializer,
        crypto: this.#crypto,
        accountIdentityPublicKeyB64: accountPubB64,
        requiredCapability: requiredCap,
        opSignerPublicKeyB64: mutation.signerPublicKeyB64,
        certChain: authority.certChain,
        nowMs,
      });
      if (recheck.ok !== true) {
        this.#ctx.sendError({ id: requestId, code: recheck.code, message: recheck.message, retryable: recheck.retryable });
        return;
      }
    }

    // Unpack the action-tagged target into the serializer's flat shape.
    let target;
    if (mutation.action === "device.add") {
      let binding;
      try {
        binding = new DeviceInboxBindingV1(mutation.target.deviceInboxBinding);
      } catch (err) {
        this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid device.add binding: " + (err && err.message ? err.message : "unknown"), retryable: false });
        return;
      }
      // Audit 2026-07-09 (F3): the binding is a device-signed self-cert; enrolling
      // its deviceId/inboxId WITHOUT proving the device signed it (and that it is
      // in its validity window) lets an authorized mutator reserve/pollute an
      // arbitrary inbox binding — a DoS against another account's inbox enrollment.
      // Mirror DeviceHandler.handleBind checks (3) window + (4) signature; the
      // deviceId self-cert (rez:dev:sha256(devicePublicKeyB64)) is already enforced
      // by the DeviceInboxBindingV1 constructor.
      if (nowMs < binding.issuedAtMs || nowMs >= binding.expiresAtMs) {
        this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device.add binding is not currently valid", retryable: false });
        return;
      }
      const bindingSigOk = await this.#verifyEd25519(binding.devicePublicKeyB64, DeviceInboxBindingV1.signableBytes(binding), binding.sig.sigB64);
      if (!bindingSigOk) {
        this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device.add binding signature invalid", retryable: false });
        return;
      }
      target = { deviceId: binding.deviceId, inboxId: binding.inboxId, certId: null };
    } else {
      target = {
        revokedDeviceId: mutation.target.revokedDeviceId,
        revokedCertId: mutation.target.revokedCertId == null ? null : mutation.target.revokedCertId,
      };
    }

    let result;
    try {
      result = await serializer.submitMutation({
        accountIdentityPublicKeyB64: accountPubB64,
        opId: mutation.opId,
        expectedRevision: mutation.expectedRevision,
        action: mutation.action,
        target,
      });
    } catch (err) {
      let code = "INTERNAL";
      if (err && (err.code === "INBOX_ALREADY_ENROLLED" || err.code === "ACCOUNT_DEVICE_CONFLICT")) code = "CONFLICT";
      else if (err && err.code === "DEVICE_REVOKED") code = "FORBIDDEN";
      else if (err && (err.code === "BAD_TARGET" || err.code === "BAD_ACTION")) code = "BAD_REQUEST";
      // Audit R4 tombstone-DoS guard: a per-account revoked-device tombstone quota
      // hit is a hard, client-caused ceiling (retrying will not help).
      else if (err && err.code === "REVOKED_DEVICE_QUOTA_EXCEEDED") code = "RATE_LIMITED";
      this.#ctx.sendError({ id: requestId, code, message: err && err.message ? err.message : "mutation failed", retryable: false });
      return;
    }

    // Audit 2026-07-09 (F4): the serialized revoke fail-closes the target device's
    // HOME delivery cursor ATOMICALLY, inside the serializer's own transaction
    // (PgAccountMutationSerializer + PgDurableInbox.revokeDeviceInTx) — the authority
    // commit and the `device_cursors.revoked` close now succeed or roll back
    // together. There is no post-commit second phase here to split on a crash, and
    // no dependence on the caller replaying the exact opId to converge.
    this.#ctx.sendResponse(requestId, T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, result);
  }

  async handleGetAuthorityState(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    const serializer = this.#serializer();
    if (!serializer || typeof serializer.getAuthorityState !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "account mutation authority unavailable", retryable: false });
      return;
    }
    const accountPubB64 = typeof this.#ctx.ownerPublicKeyB64 === "string" ? this.#ctx.ownerPublicKeyB64.trim() : "";
    if (accountPubB64.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session account identity required", retryable: false });
      return;
    }
    // Own account only — off-home peers consult the published AccountAuthorityStateV1
    // durable record, not this op (blindness boundary).
    const requested = body && typeof body.accountIdentityPublicKeyB64 === "string" ? body.accountIdentityPublicKeyB64.trim() : accountPubB64;
    if (requested !== accountPubB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "authority state is served for the authenticated account only", retryable: false });
      return;
    }
    let state;
    try {
      state = await serializer.getAuthorityState(accountPubB64);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: err && err.message ? err.message : "authority state fetch failed", retryable: false });
      return;
    }
    this.#ctx.sendResponse(requestId, T.ACCOUNT_AUTHORITY_STATE_GET_RES, state);
  }
}
