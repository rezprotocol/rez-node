import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * device.revoke — home-enforced, fail-closed revocation of a device from the
 * inbox the authenticated session has claimed (S2.5 Slice 4, plan fix P1a).
 *
 * The body carries a `deviceRevoke` (a DeviceRevokeV1 `toJSON()` object) signed
 * by the ACCOUNT identity key. The handler verifies the account signature
 * against the session's authenticated account (you may only revoke your own
 * devices) and the self-certifying revokedDeviceId, then marks the device's
 * cursor revoked at the home. Thereafter append/read/cursorAck fail closed for
 * that device — the backstop that makes a partial-propagation revoke safe even
 * when a lagging sender still tries to fan out to the revoked device.
 */
export class DeviceRevokeRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.DEVICE_REVOKE;

  constructor({ deviceRevoke } = {}) {
    super();
    this.deviceRevoke = deviceRevoke == null ? null : deviceRevoke;
    if (this.constructor === DeviceRevokeRequest) this._seal();
  }

  validate() {
    this.assert(
      this.deviceRevoke != null && typeof this.deviceRevoke === "object",
      "deviceRevoke must be an object",
    );
  }
}
