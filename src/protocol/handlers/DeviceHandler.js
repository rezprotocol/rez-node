import {
  REZ_CONTRACT_TYPES,
  base64ToBytes,
  DeviceInboxBindingV1,
  DeviceRevokeV1,
  verifyDeviceRegistrationV1,
} from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;

/**
 * Handles device.bind / device.revoke — per-device home binding (S2.5 Slice 4).
 *
 * The durable home keys a device cursor on a SIGNED, self-certifying deviceId
 * (rez:dev:sha256(devicePublicKeyB64)) rather than the unsigned SessionHello
 * string. device.bind presents the proof; device.revoke fail-closes it.
 *
 * Trust anchor: the session already proved possession of its account identity
 * key during session-auth (GatewaySession `_handleSessionAuthenticate`), so
 * `ctx.ownerPublicKeyB64` is a trustworthy account anchor. The two records carry
 * the rest:
 *   - DeviceRegistrationV1 (account-signed) ties the device key to THIS account.
 *   - DeviceInboxBindingV1 (device-signed) ties the device to the inbox it reads.
 *
 * The durable inbox itself is the enforcement point (append/read/cursorAck fail
 * closed for a revoked device) — this handler is the verified entry that creates
 * and revokes those device rows. No cap chain: authority is the authenticated
 * session plus the in-record signatures.
 */
export class DeviceHandler {
  #ctx;
  #crypto;

  constructor(ctx) {
    this.#ctx = ctx;
    this.#crypto = new NodeCryptoProvider();
  }

  #durableInbox() {
    const runtime = this.#ctx.runtime;
    const durableInbox = runtime && runtime.durableInbox ? runtime.durableInbox : null;
    if (!durableInbox || typeof durableInbox.registerDevice !== "function") {
      return null;
    }
    return durableInbox;
  }

  async #verifyEd25519(publicKeyB64, msgBytes, sigB64) {
    let publicKey;
    let sig;
    try {
      publicKey = base64ToBytes(publicKeyB64);
      sig = base64ToBytes(sigB64);
    } catch {
      return false;
    }
    if (!(sig instanceof Uint8Array) || sig.length === 0) return false;
    return Promise.resolve(this.#crypto.verify({ publicKey, msg: msgBytes, sig }))
      .catch(() => false);
  }

  /**
   * device.bind — register the proven device's cursor at the home, keyed on the
   * self-cert deviceId, persisting the bound device key (its DeviceInboxBindingV1).
   */
  async handleBind(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const durableInbox = this.#durableInbox();
    if (!durableInbox) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable inbox unavailable", retryable: false });
      return;
    }

    const accountPubB64 = typeof this.#ctx.ownerPublicKeyB64 === "string" ? this.#ctx.ownerPublicKeyB64.trim() : "";
    if (accountPubB64.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session account identity required", retryable: false });
      return;
    }
    const sessionDeviceId = typeof this.#ctx.sessionDeviceId === "string" ? this.#ctx.sessionDeviceId.trim() : "";
    if (sessionDeviceId.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session deviceId required", retryable: false });
      return;
    }

    const bindingJson = body && typeof body.deviceInboxBinding === "object" && body.deviceInboxBinding !== null
      ? body.deviceInboxBinding
      : null;
    if (!bindingJson) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "deviceInboxBinding is required", retryable: false });
      return;
    }

    // Structural validation of the binding (self-cert deviceId, SPKI, expiry
    // ordering, sig shape). Construction validates via _seal().
    let binding;
    try {
      binding = new DeviceInboxBindingV1(bindingJson);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid deviceInboxBinding: " + (err && err.message ? err.message : "unknown"), retryable: false });
      return;
    }

    const nowMs = Date.now();

    // (1) Establish that the binding's device key belongs to THIS account
    // (`provenDeviceKeyB64`). Dual-mode (S2.5 S8):
    //   - DELEGATED device: the session's cert chain C←…←B already proved C∈B at
    //     session-auth (S7, stashed on `sessionAuthority`). The leaf capability
    //     cert IS the registration (`device.register` was dropped) — a delegated
    //     device holds no B-sign key to produce a DeviceRegistrationV1, so none is
    //     required; the proven device key is the chain's leaf signer (C).
    //   - PRIMARY device: the account (B-sign == the session identity) vouches for
    //     the device DIRECTLY via an account-signed DeviceRegistrationV1. This is
    //     legacy-compat (resolves deviceId↔account; never delegated authority).
    const authority = this.#ctx.sessionAuthority;
    const delegated = authority && typeof authority === "object" && authority.mode === "delegated";
    let provenDeviceKeyB64;
    if (delegated) {
      provenDeviceKeyB64 = typeof authority.signerPublicKeyB64 === "string" ? authority.signerPublicKeyB64.trim() : "";
      if (provenDeviceKeyB64.length === 0) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "delegated session is missing its device signer key", retryable: false });
        return;
      }
    } else {
      const registrationJson = body && typeof body.deviceRegistration === "object" && body.deviceRegistration !== null
        ? body.deviceRegistration
        : null;
      if (!registrationJson) {
        this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "deviceRegistration is required for a primary device", retryable: false });
        return;
      }
      const regResult = await verifyDeviceRegistrationV1({
        registration: registrationJson,
        expectedAccountIdentityPublicKeyB64: accountPubB64,
        crypto: this.#crypto,
        nowMs,
      });
      if (!regResult.ok) {
        this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device registration invalid: " + regResult.reason, retryable: false });
        return;
      }
      // The binding must be for the SAME device the registration vouches for.
      if (binding.devicePublicKeyB64 !== registrationJson.devicePublicKeyB64) {
        this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "binding device key does not match the registration", retryable: false });
        return;
      }
      if (binding.deviceId !== regResult.deviceId) {
        this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "binding deviceId does not match the registration", retryable: false });
        return;
      }
      provenDeviceKeyB64 = registrationJson.devicePublicKeyB64;
    }

    // (2) Cross-checks binding ↔ proven device ↔ session: the binding must be for
    // the proven device key AND that device must be the one the session
    // authenticated as (you bind the device you ARE).
    if (binding.devicePublicKeyB64 !== provenDeviceKeyB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "binding device key does not match the authenticated session device", retryable: false });
      return;
    }
    if (binding.deviceId !== sessionDeviceId) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "binding deviceId does not match the authenticated session device", retryable: false });
      return;
    }

    // (3) The binding must target an inbox THIS session has claimed (proven
    // ownership via inbox.claim), and be within its validity window.
    if (!this.#ctx.isInboxBound(binding.inboxId)) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "session has not claimed the binding inbox", retryable: false });
      return;
    }
    if (nowMs < binding.issuedAtMs || nowMs >= binding.expiresAtMs) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device inbox binding is not currently valid", retryable: false });
      return;
    }

    // (4) Device-signed binding signature over its canonical body.
    const bindingOk = await this.#verifyEd25519(
      binding.devicePublicKeyB64,
      DeviceInboxBindingV1.signableBytes(binding),
      binding.sig.sigB64,
    );
    if (!bindingOk) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device inbox binding signature invalid", retryable: false });
      return;
    }

    // (5) Register the device cursor keyed on the SIGNED deviceId, persisting the
    // bound device key. Idempotent; the E6 gate (maxDevices) refuses a 2nd device
    // until Slice 8.
    try {
      await durableInbox.registerDevice(binding.inboxId, binding.deviceId, { devicePublicKeyB64: binding.devicePublicKeyB64 });
    } catch (err) {
      if (err && err.code === "INBOX_CAP_EXCEEDED" && err.limitType === "devices") {
        this.#ctx.sendError({ id: requestId, code: "DEVICE_LIMIT", message: "additional devices are not yet supported (multi-device gated)", retryable: false });
        return;
      }
      if (err && err.code === "DEVICE_KEY_MISMATCH") {
        this.#ctx.sendError({ id: requestId, code: "CONFLICT", message: "device id is already bound to a different key", retryable: false });
        return;
      }
      this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: err && err.message ? err.message : "device bind failed", retryable: false });
      return;
    }

    this.#ctx.sendResponse(requestId, T.DEVICE_BIND_RES, { inboxId: binding.inboxId, deviceId: binding.deviceId });
  }

  /**
   * device.revoke — fail-close the home for a device (plan fix P1a). Account-
   * signed; revokes the device for the session's claimed inbox.
   */
  async handleRevoke(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const durableInbox = this.#durableInbox();
    if (!durableInbox || typeof durableInbox.revokeDevice !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable inbox unavailable", retryable: false });
      return;
    }

    const accountPubB64 = typeof this.#ctx.ownerPublicKeyB64 === "string" ? this.#ctx.ownerPublicKeyB64.trim() : "";
    if (accountPubB64.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session account identity required", retryable: false });
      return;
    }
    const inboxId = typeof this.#ctx.localInboxId === "string" ? this.#ctx.localInboxId.trim() : "";
    if (inboxId.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "session has no claimed inbox to revoke a device from", retryable: false });
      return;
    }

    const revokeJson = body && typeof body.deviceRevoke === "object" && body.deviceRevoke !== null
      ? body.deviceRevoke
      : null;
    if (!revokeJson) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "deviceRevoke is required", retryable: false });
      return;
    }

    let revoke;
    try {
      revoke = new DeviceRevokeV1(revokeJson);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid deviceRevoke: " + (err && err.message ? err.message : "unknown"), retryable: false });
      return;
    }

    // You may only revoke devices of YOUR OWN account: the revoke names the
    // account the session authenticated as.
    if (revoke.accountIdentityPublicKeyB64 !== accountPubB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "revoke account does not match the authenticated session account", retryable: false });
      return;
    }

    // Dual-mode (S2.5 S8, V6): a PRIMARY device signs the revoke with the account
    // key (B-sign). A DELEGATED device holds no B-sign — it signs with its device
    // key C, authorized by the cert chain proven at session-auth (S7) AND holding
    // the `device.revoke` capability (per-op authority consumed from the session's
    // grantedCapabilities). `device.revoke` is a privileged action, so unlike
    // device.bind (membership) it requires the explicit capability.
    const authority = this.#ctx.sessionAuthority;
    const delegated = authority && typeof authority === "object" && authority.mode === "delegated";
    let revokeSignerB64;
    if (delegated) {
      const caps = Array.isArray(authority.grantedCapabilities) ? authority.grantedCapabilities : [];
      if (!caps.includes("device.revoke")) {
        this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "delegated device lacks the device.revoke capability", retryable: false });
        return;
      }
      revokeSignerB64 = typeof authority.signerPublicKeyB64 === "string" ? authority.signerPublicKeyB64.trim() : "";
      if (revokeSignerB64.length === 0) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "delegated session is missing its device signer key", retryable: false });
        return;
      }
    } else {
      revokeSignerB64 = accountPubB64;
    }

    const nowMs = Date.now();
    if (nowMs < revoke.issuedAtMs || nowMs >= revoke.expiresAtMs) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device revoke is not currently valid", retryable: false });
      return;
    }

    const revokeOk = await this.#verifyEd25519(
      revokeSignerB64,
      DeviceRevokeV1.signableBytes(revoke),
      revoke.sig.sigB64,
    );
    if (!revokeOk) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "device revoke signature invalid", retryable: false });
      return;
    }

    let revoked;
    try {
      revoked = await durableInbox.revokeDevice(inboxId, revoke.revokedDeviceId);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: err && err.message ? err.message : "device revoke failed", retryable: false });
      return;
    }

    this.#ctx.sendResponse(requestId, T.DEVICE_REVOKE_RES, {
      inboxId,
      revokedDeviceId: revoke.revokedDeviceId,
      revoked: revoked === true,
    });
  }
}
