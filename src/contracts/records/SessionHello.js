import { RRecord, REZ_CONTRACT_TYPES, CONTRACT_VERSION } from "@rezprotocol/core";

/**
 * session.hello — the SDK's first frame after WS open.
 *
 * The protocol identifies the session by the SDK's public key, NOT by
 * account: `accountIdentityPublicKeyB64` is the binding the relay verifies
 * via challenge-response. Accounts are a chat-app concern; the protocol
 * does not see them.
 */
export class SessionHello extends RRecord {
  static type = REZ_CONTRACT_TYPES.SESSION_HELLO;

  constructor({ contractVersion, clientName, clientVersion, deviceId, accountIdentityPublicKeyB64 } = {}) {
    super();
    this.contractVersion = Number(contractVersion);
    this.clientName = clientName == null ? undefined : String(clientName);
    this.clientVersion = clientVersion == null ? undefined : String(clientVersion);
    this.deviceId = deviceId == null ? "" : String(deviceId);
    this.accountIdentityPublicKeyB64 = accountIdentityPublicKeyB64 == null ? "" : String(accountIdentityPublicKeyB64);
    if (this.constructor === SessionHello) this._seal();
  }

  validate() {
    this.assert(Number.isInteger(this.contractVersion), "contractVersion must be integer");
    this.assert(this.contractVersion === CONTRACT_VERSION, "contractVersion must match CONTRACT_VERSION");
    if (this.clientName != null) this.assert(this.clientName.length > 0, "clientName must be non-empty when provided");
    if (this.clientVersion != null) this.assert(this.clientVersion.length > 0, "clientVersion must be non-empty when provided");
    this.assert(this.deviceId.length > 0, "deviceId must be non-empty");
    this.assert(this.accountIdentityPublicKeyB64.length > 0, "accountIdentityPublicKeyB64 must be non-empty");
  }
}
