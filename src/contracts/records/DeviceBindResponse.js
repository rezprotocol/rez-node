import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * Response to a successful device.bind. Echoes the inbox and the SIGNED
 * self-certifying deviceId the home now keys this device's cursor on. On failure
 * the node sends an error response instead (e.g. INVALID_SIGNATURE,
 * DEVICE_LIMIT, BAD_REQUEST).
 */
export class DeviceBindResponse extends RRecord {
  static type = REZ_CONTRACT_TYPES.DEVICE_BIND_RES;

  constructor({ inboxId, deviceId } = {}) {
    super();
    this.inboxId = inboxId == null ? "" : String(inboxId);
    this.deviceId = deviceId == null ? "" : String(deviceId);
    if (this.constructor === DeviceBindResponse) this._seal();
  }

  validate() {
    this.assert(this.inboxId.trim().length > 0, "inboxId must be non-empty");
    this.assert(this.deviceId.trim().length > 0, "deviceId must be non-empty");
  }
}
