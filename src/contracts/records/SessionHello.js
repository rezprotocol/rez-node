import { RRecord, REZ_CONTRACT_TYPES, CONTRACT_VERSION, SUPPORTED_CONTRACT_VERSIONS } from "@rezprotocol/core";

export const SESSION_AUTH_MODES = Object.freeze({ ACCOUNT: "account", CLAIMANT: "claimant" });

/**
 * session.hello — the SDK's first frame after WS open.
 *
 * The protocol identifies the session by a public key, NOT by account:
 * the identity key in this record is the binding the relay verifies via
 * challenge-response. Accounts are a chat-app concern; the protocol does
 * not see them.
 *
 * SESSION_AUTH_V5 (plans/SESSION_AUTH_V5_SLICE2_PLAN.md §2A.2) — the shape is
 * version-gated and FAIL-CLOSED; ambiguous shapes are malformed, never
 * inferred around:
 *
 *   contractVersion 4 (legacy, byte-identical semantics):
 *     deviceId + accountIdentityPublicKeyB64 required; authMode FORBIDDEN
 *     (v4 has no modes — a v4 hello carrying one is malformed, so legacy
 *     inference can never silently consume a v5 concept).
 *
 *   contractVersion 5, authMode "account":
 *     deviceId + accountIdentityPublicKeyB64 required; claimant key forbidden.
 *
 *   contractVersion 5, authMode "claimant":
 *     claimantPublicKeyB64 required; accountIdentityPublicKeyB64 AND deviceId
 *     FORBIDDEN — a claimant session proves "I control claimant key K", not
 *     "I am device D of account A". A deviceId here would be pure correlation
 *     metadata smuggled back into the privacy-preserving path, so it is
 *     rejected rather than ignored.
 */
export class SessionHello extends RRecord {
  static type = REZ_CONTRACT_TYPES.SESSION_HELLO;

  constructor({ contractVersion, clientName, clientVersion, deviceId, accountIdentityPublicKeyB64, authMode, claimantPublicKeyB64 } = {}) {
    super();
    this.contractVersion = Number(contractVersion);
    this.clientName = clientName == null ? undefined : String(clientName);
    this.clientVersion = clientVersion == null ? undefined : String(clientVersion);
    this.deviceId = deviceId == null ? "" : String(deviceId);
    this.accountIdentityPublicKeyB64 = accountIdentityPublicKeyB64 == null ? "" : String(accountIdentityPublicKeyB64);
    this.authMode = authMode == null ? undefined : String(authMode);
    this.claimantPublicKeyB64 = claimantPublicKeyB64 == null ? "" : String(claimantPublicKeyB64);
    if (this.constructor === SessionHello) this._seal();
  }

  validate() {
    this.assert(Number.isInteger(this.contractVersion), "contractVersion must be integer");
    this.assert(
      SUPPORTED_CONTRACT_VERSIONS.includes(this.contractVersion),
      "contractVersion must be one of SUPPORTED_CONTRACT_VERSIONS",
    );
    if (this.clientName != null) this.assert(this.clientName.length > 0, "clientName must be non-empty when provided");
    if (this.clientVersion != null) this.assert(this.clientVersion.length > 0, "clientVersion must be non-empty when provided");

    if (this.contractVersion === CONTRACT_VERSION) {
      // v4 legacy shape — unchanged requirements, and no v5 concepts.
      this.assert(this.authMode === undefined, "authMode is not a v4 field");
      this.assert(this.claimantPublicKeyB64.length === 0, "claimantPublicKeyB64 is not a v4 field");
      this.assert(this.deviceId.length > 0, "deviceId must be non-empty");
      this.assert(this.accountIdentityPublicKeyB64.length > 0, "accountIdentityPublicKeyB64 must be non-empty");
      return;
    }

    // contractVersion 5 — explicit mode, exact mode-specific field rules.
    this.assert(
      this.authMode === SESSION_AUTH_MODES.ACCOUNT || this.authMode === SESSION_AUTH_MODES.CLAIMANT,
      "authMode must be \"account\" or \"claimant\"",
    );
    if (this.authMode === SESSION_AUTH_MODES.ACCOUNT) {
      this.assert(this.deviceId.length > 0, "deviceId must be non-empty");
      this.assert(this.accountIdentityPublicKeyB64.length > 0, "accountIdentityPublicKeyB64 must be non-empty");
      this.assert(this.claimantPublicKeyB64.length === 0, "account mode must not carry a claimant key");
    } else {
      this.assert(this.claimantPublicKeyB64.length > 0, "claimantPublicKeyB64 must be non-empty");
      this.assert(this.accountIdentityPublicKeyB64.length === 0, "claimant mode must not carry an account identity");
      this.assert(this.deviceId.length === 0, "claimant mode must not carry a deviceId");
    }
  }
}
