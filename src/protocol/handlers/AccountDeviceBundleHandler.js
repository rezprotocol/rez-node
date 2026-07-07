import { REZ_CONTRACT_TYPES, DevicePrekeyBundleV1 } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";

const T = REZ_CONTRACT_TYPES;

/**
 * Home-aggregated per-device prekey bundle service (S2.5 S12, multi-device
 * fan-out). Each device self-publishes its DevicePrekeyBundleV1 (self-contained +
 * device-signed) to its account home; any device of the account then fetches the
 * WHOLE active device set (all siblings' bundles) so it can assemble the
 * multi-device DeviceSetRecordV1 it seals per peer.
 *
 * Publish authz: the bundle must (1) name the AUTHENTICATED session's account,
 * (2) be the SESSION's own device (you publish the bundle for the device you ARE),
 * (3) be an ACTIVE enrolled device, and (4) carry a valid device signature over
 * its own body. getDeviceSet serves the authenticated account only (the
 * account-blindness boundary — a peer learns another account's set from the sealed
 * DurableRecordV2, not here).
 *
 * Only on a pg cluster node (the bundle store is null on fs/desktop ⇒
 * SERVICE_UNAVAILABLE).
 */
export class AccountDeviceBundleHandler {
  #ctx;
  #crypto;

  constructor(ctx) {
    this.#ctx = ctx;
    this.#crypto = new NodeCryptoProvider();
  }

  #store() {
    return this.#ctx.runtime && this.#ctx.runtime.accountDeviceBundleStore
      ? this.#ctx.runtime.accountDeviceBundleStore
      : null;
  }

  #registry() {
    return this.#ctx.runtime && this.#ctx.runtime.accountDeviceRegistry
      ? this.#ctx.runtime.accountDeviceRegistry
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

  async handlePublish(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    const store = this.#store();
    const registry = this.#registry();
    if (!store || typeof store.putBundle !== "function" || !registry || typeof registry.getDevice !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "device bundle store unavailable", retryable: false });
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

    const bundleJson = body && typeof body.bundle === "object" && body.bundle !== null ? body.bundle : null;
    if (!bundleJson) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "bundle is required", retryable: false });
      return;
    }
    let bundle;
    try {
      bundle = new DevicePrekeyBundleV1(bundleJson);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "invalid bundle: " + (err && err.message ? err.message : "unknown"), retryable: false });
      return;
    }

    // (1) your own account; (2) the device you ARE.
    if (bundle.accountIdentityPublicKeyB64 !== accountPubB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "bundle account does not match the authenticated session account", retryable: false });
      return;
    }
    if (bundle.deviceId !== sessionDeviceId) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "bundle deviceId does not match the authenticated session device", retryable: false });
      return;
    }

    // (3) an ACTIVE enrolled device (the registry is the authoritative set).
    let enrolled;
    try {
      enrolled = await registry.getDevice(accountPubB64, bundle.deviceId);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: err && err.message ? err.message : "device lookup failed", retryable: false });
      return;
    }
    if (!enrolled || enrolled.status !== "active") {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "device is not an active enrolled device of this account", retryable: false });
      return;
    }

    // (4) time window + device signature.
    const nowMs = Date.now();
    if (nowMs < bundle.issuedAtMs || nowMs >= bundle.expiresAtMs) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "bundle is not currently valid", retryable: false });
      return;
    }
    const sigOk = await this.#verifyEd25519(bundle.devicePublicKeyB64, DevicePrekeyBundleV1.signableBytes(bundle), bundle.sig.sigB64);
    if (!sigOk) {
      this.#ctx.sendError({ id: requestId, code: "INVALID_SIGNATURE", message: "bundle signature invalid", retryable: false });
      return;
    }

    let result;
    try {
      result = await store.putBundle({
        accountIdentityPublicKeyB64: accountPubB64,
        deviceId: bundle.deviceId,
        prekeyVersion: bundle.prekeyVersion,
        bundleJson: bundle.toJSON(),
      });
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: err && err.message ? err.message : "bundle publish failed", retryable: false });
      return;
    }
    this.#ctx.sendResponse(requestId, T.ACCOUNT_DEVICE_BUNDLE_PUBLISH_RES, {
      deviceId: result.deviceId,
      prekeyVersion: result.prekeyVersion,
      applied: result.applied === true,
    });
  }

  async handleGetDeviceSet(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;
    const store = this.#store();
    if (!store || typeof store.listActiveBundles !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "device bundle store unavailable", retryable: false });
      return;
    }
    const accountPubB64 = typeof this.#ctx.ownerPublicKeyB64 === "string" ? this.#ctx.ownerPublicKeyB64.trim() : "";
    if (accountPubB64.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session account identity required", retryable: false });
      return;
    }
    // Own account only — a peer learns another account's set from the sealed record.
    const requested = body && typeof body.accountIdentityPublicKeyB64 === "string" ? body.accountIdentityPublicKeyB64.trim() : accountPubB64;
    if (requested !== accountPubB64) {
      this.#ctx.sendError({ id: requestId, code: "FORBIDDEN", message: "device set is served for the authenticated account only", retryable: false });
      return;
    }
    let bundles;
    try {
      bundles = await store.listActiveBundles(accountPubB64);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "INTERNAL", message: err && err.message ? err.message : "device set fetch failed", retryable: false });
      return;
    }
    this.#ctx.sendResponse(requestId, T.ACCOUNT_DEVICE_SET_GET_RES, {
      devices: bundles.map((b) => ({ deviceId: b.deviceId, prekeyVersion: b.prekeyVersion, bundle: b.bundleJson })),
    });
  }
}
