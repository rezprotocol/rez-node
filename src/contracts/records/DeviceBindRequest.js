import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

/**
 * device.bind — bind a PROVEN device to the inbox the authenticated session has
 * claimed (S2.5 Slice 4).
 *
 * The body carries two rez-core records (as their `toJSON()` objects):
 *   - `deviceRegistration` — a DeviceRegistrationV1 signed by the ACCOUNT key
 *     (the device→account trust chain). The handler verifies it against the
 *     session's authenticated account identity (the trust anchor).
 *   - `deviceInboxBinding` — a DeviceInboxBindingV1 signed by the DEVICE key,
 *     asserting the device receives at this inbox. The handler verifies the
 *     device signature and cross-checks deviceId + inboxId against the
 *     registration and the session's claimed inbox.
 *
 * On success the durable home registers a device cursor keyed on the SIGNED
 * self-certifying deviceId (rez:dev:sha256(devicePublicKeyB64)), replacing the
 * unsigned SessionHello deviceId as the cursor authority. There is no cap chain:
 * authority is the authenticated session (it proved possession of the account
 * key) plus the two signatures inside the records.
 */
export class DeviceBindRequest extends RRecord {
  static type = REZ_CONTRACT_TYPES.DEVICE_BIND;

  constructor({ deviceRegistration, deviceInboxBinding } = {}) {
    super();
    this.deviceRegistration = deviceRegistration == null ? null : deviceRegistration;
    this.deviceInboxBinding = deviceInboxBinding == null ? null : deviceInboxBinding;
    if (this.constructor === DeviceBindRequest) this._seal();
  }

  validate() {
    this.assert(
      this.deviceRegistration != null && typeof this.deviceRegistration === "object",
      "deviceRegistration must be an object",
    );
    this.assert(
      this.deviceInboxBinding != null && typeof this.deviceInboxBinding === "object",
      "deviceInboxBinding must be an object",
    );
  }
}
