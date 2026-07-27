import {
  REZ_CONTRACT_TYPES,
  AccountDeviceMutationV1,
  ACCOUNT_DEVICE_MUTATION_VERSION,
  AccountDeviceMutationV2,
  ACCOUNT_DEVICE_MUTATION_V2_VERSION,
  DeviceInboxBindingV1,
  AccountDeviceCapabilityV1,
} from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import { verifyDelegatedAuthorityAgainst } from "./revalidateDelegatedAuthority.js";

const T = REZ_CONTRACT_TYPES;

/**
 * Serialized account device-mutation authority (S2.5 S11). A device submits a
 * signed AccountDeviceMutationV2 (or a v1 device.revoke) to its account's HOME; the home serializes it
 * (PgAccountMutationSerializer) under a per-account lock, folds the canonical
 * device set, and bumps a monotonic epoch. A companion op serves the current
 * authority state ({epoch, revokedCertIds, minValidIssuedAtMs}) so a device can
 * fold + publish the signed AccountAuthorityStateV1 for off-home peers.
 *
 * Authority is proven by the AUTHENTICATED session (`ctx.sessionAuthority`) — the
 * envelope carries no cert chain. A delegated device needs the action's explicit
 * capability (device.add / device.revoke) in its granted set; a primary (direct)
 * session holds all. This is the SOLE revoke path (audit R4 L4 retired the legacy
 * per-inbox device.revoke directive).
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

  // The wall clock, injectable via ctx.now for deterministic tests (audit R4
  // F3-remediation finding 5). Production passes no ctx.now → real Date.now.
  #now() {
    return this.#ctx && typeof this.#ctx.now === "function" ? this.#ctx.now() : Date.now();
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
    // VERSION DISPATCH (audit #5). Two signed schemas exist and they are NOT interchangeable:
    //   v2 — the produced schema. device.add carries the leaf capability cert.
    //   v1 — FROZEN. device.revoke is still honoured (its target shape and meaning never changed,
    //        only its validation tightened, so old signatures still mean what they said), but
    //        device.add is REFUSED: it carries no certId for the home to bind, so a later revoke
    //        could not kill that device's leaf for off-home peers. Accepting it would re-open the
    //        completeness blocker the cert requirement exists to close.
    // Dispatching on the record's own `v` is what makes a v1 signature verify against v1 bytes and
    // a v2 signature against v2 bytes — the thing an in-place schema edit had made impossible.
    let mutation;
    let MutationClass;
    const submittedVersion = mutationJson.v;
    if (submittedVersion === ACCOUNT_DEVICE_MUTATION_V2_VERSION) {
      MutationClass = AccountDeviceMutationV2;
    } else if (submittedVersion === ACCOUNT_DEVICE_MUTATION_VERSION) {
      MutationClass = AccountDeviceMutationV1;
    } else {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "unsupported mutation version", retryable: false });
      return;
    }
    try {
      mutation = new MutationClass(mutationJson);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid mutation: " + (err && err.message ? err.message : "unknown"), retryable: false });
      return;
    }
    if (submittedVersion === ACCOUNT_DEVICE_MUTATION_VERSION && mutation.action === "device.add") {
      this.#ctx.sendError({
        id: requestId,
        code: "UPGRADE_REQUIRED",
        message: "device.add requires a v" + ACCOUNT_DEVICE_MUTATION_V2_VERSION + " mutation carrying"
          + " the device's leaf capability certificate; a v" + ACCOUNT_DEVICE_MUTATION_VERSION
          + " device.add cannot be revoked for off-home peers and is no longer accepted",
        retryable: false,
      });
      return;
    }

    // You may only mutate YOUR OWN account.
    if (mutation.accountIdentityPublicKeyB64 !== accountPubB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "mutation account does not match the authenticated session account", retryable: false });
      return;
    }

    // Bind the envelope signer to the session + check per-op authority (dual-mode).
    // A PRIMARY (direct) session holds all capabilities and signs with the account
    // key; a DELEGATED session signs with its device key C and must carry the
    // action's capability.
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
      // Audit 2026-07-14 (F3-remediation finding 1): the earlier "also require
      // capability.revoke to carry a revokedCertId" gate was the wrong model — it let a
      // device.revoke-only caller leave the target's OWN authority cert live (incomplete
      // revocation). The clean rule lives in the serializer under the account lock:
      // device.revoke AUTO-revokes the target's registry-bound cert, and a caller-supplied
      // revokedCertId is accepted only when it EQUALS that bound cert (else BAD_TARGET).
      // Arbitrary cert revocation is the separate capability.revoke operation.
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

    const nowMs = this.#now();
    if (nowMs < mutation.issuedAtMs || nowMs >= mutation.expiresAtMs) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "mutation is not currently valid", retryable: false });
      return;
    }
    const sigOk = await this.#verifyEd25519(mutation.signerPublicKeyB64, MutationClass.signableBytes(mutation), mutation.sig.sigB64);
    if (!sigOk) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "mutation signature invalid", retryable: false });
      return;
    }

    // Audit 2026-07-09 (F2) + R4 L3: the session's `sessionAuthority`
    // (grantedCapabilities) is fixed at connect time and consulted statically above.
    // The AUTHORITATIVE re-check of the delegated chain against the home's current
    // revocation set must run UNDER the serializer's per-account lock, against the
    // in-tx revocation state — otherwise a device.revoke committing between a pooled
    // read here and the serializer's fold would be a TOCTOU. So we hand the serializer
    // a `revalidate` closure (SSOT via verifyDelegatedAuthorityAgainst) that it invokes
    // under the lock. Direct (primary) sessions sign with the unrevocable account root
    // and pass no closure.
    let revalidate = null;
    if (delegated) {
      // Audit 2026-07-14 (F3-remediation finding 4): the recheck runs UNDER the account
      // lock, which may be acquired much later than this pre-lock point under contention.
      // Validate cert-chain expiry AND the mutation's own validity window against a FRESH
      // lock-time clock — otherwise a delegated cert (or the envelope) could expire while
      // queued on the lock yet still pass a check frozen at request-start. The pre-lock
      // window check above is the fast-reject; this is the authoritative one.
      const validFromMs = mutation.issuedAtMs;
      const validUntilMs = mutation.expiresAtMs;
      revalidate = (revocationState) => {
        const lockNowMs = this.#now();
        if (lockNowMs < validFromMs || lockNowMs >= validUntilMs) {
          return Promise.resolve(false);
        }
        return verifyDelegatedAuthorityAgainst({
          crypto: this.#crypto,
          accountIdentityPublicKeyB64: accountPubB64,
          requiredCapability: requiredCap,
          opSignerPublicKeyB64: mutation.signerPublicKeyB64,
          certChain: authority.certChain,
          nowMs: lockNowMs,
          revocationState,
        });
      };
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
      // Audit R4 completeness: device.add carries the device's leaf capability cert
      // (C←B). The mutation contract already bound it structurally to this device +
      // account; here we CRYPTO-verify it (B-signature, validity window, grantee = the
      // added device) via the canonical verifier, and store its certId so a later
      // device.revoke auto-revokes it into the account's revoked-cert set — the ONLY
      // revocation signal off-home peers get. A fresh cert is checked against no
      // revocations; a resurrected-device re-add is independently blocked by the
      // serializer (DEVICE_REVOKED on the tombstoned row). Without this the leaf would
      // be enrolled cert_id=NULL and stay valid to peers after revocation.
      let capability;
      try {
        capability = new AccountDeviceCapabilityV1(mutation.target.deviceCapability);
      } catch (err) {
        this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid device.add capability: " + (err && err.message ? err.message : "unknown"), retryable: false });
        return;
      }
      const capabilityOk = await verifyDelegatedAuthorityAgainst({
        crypto: this.#crypto,
        accountIdentityPublicKeyB64: accountPubB64,
        requiredCapability: null,
        opSignerPublicKeyB64: capability.granteeDevicePublicKeyB64,
        certChain: [capability.toJSON()],
        nowMs,
        revocationState: null,
      });
      if (!capabilityOk) {
        this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "device.add leaf capability cert did not verify against the account", retryable: false });
        return;
      }
      target = { deviceId: binding.deviceId, inboxId: binding.inboxId, certId: capability.certId };
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
        revalidate,
      });
    } catch (err) {
      let code = "INTERNAL";
      if (err && (err.code === "INBOX_ALREADY_ENROLLED" || err.code === "ACCOUNT_DEVICE_CONFLICT")) code = "CONFLICT";
      else if (err && err.code === "DEVICE_REVOKED") code = "FORBIDDEN";
      // Audit R4 L3: the under-lock delegated recheck rejected (leaf revoked mid-flight).
      else if (err && err.code === "DELEGATED_AUTHORITY_INVALID") code = "FORBIDDEN";
      else if (err && (err.code === "BAD_TARGET" || err.code === "BAD_ACTION" || err.code === "BAD_DEVICE_ID" || err.code === "BAD_CERT_ID")) code = "BAD_REQUEST";
      // Audit R4 F3 admission control: per-account active/lifetime device cap.
      else if (err && err.code === "DEVICE_LIMIT") code = "DEVICE_LIMIT";
      // Audit R4 F3 tombstone-DoS guard: the never-enrolled tombstone ceiling is a hard,
      // client-caused limit (retrying will not help). There is NO revoked-cert quota —
      // auto-revoked bound certs are lifetime-bounded (finding 1).
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

    // Round-7 finding 3 (+ round-8 finding 5) is now MOOT: the authority read path no longer caches.
    // The L5 review removed the warm TTL cache — connect-time auth and the per-dispatch guard read
    // the monotonic authority epoch fresh each time and re-verify against a fresh COHERENT snapshot
    // when it advances (AccountAuthorityRevocationCache is now a pass-through fresh reader). A
    // committed add/revoke bumps the epoch, so every session enforces it on its next dispatch, on
    // any cluster node, with no invalidation to broadcast.

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
