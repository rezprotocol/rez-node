import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * Response to a device.revoke. Echoes the inbox, the revoked self-cert deviceId,
 * and whether a matching device cursor existed at the home (`revoked` false means
 * the device was not registered here — the revoke is still honored idempotently).
 * On verification failure the node sends an error response instead.
 */
export class DeviceRevokeResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.DEVICE_REVOKE_RES;

  constructor({ inboxId, revokedDeviceId, revoked } = {}) {
    super();
    this.inboxId = inboxId == null ? "" : String(inboxId);
    this.revokedDeviceId = revokedDeviceId == null ? "" : String(revokedDeviceId);
    this.revoked = revoked === true;
    if (this.constructor === DeviceRevokeResponse) this._seal();
  }

  validate() {
    this.assert(this.inboxId.trim().length > 0, "inboxId must be non-empty");
    this.assert(this.revokedDeviceId.trim().length > 0, "revokedDeviceId must be non-empty");
  }
}
