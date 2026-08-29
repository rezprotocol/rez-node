import { REZ_CONTRACT_TYPES, CAP_DEVICE_SET_PUBLISH } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

// SSOT (audit leaf-3c F2): the ACCOUNT CAPABILITY a DELEGATED session must hold to invoke a given
// wire op. Enforced per-dispatch in GatewaySession: after the revocation guard proves the session's
// (immutable) cert chain still verifies THIS dispatch, the op's required capability must be present in
// the chain-derived, frozen `grantedCapabilities`. That composition — "chain re-proven now" + "caps
// are the deterministic grant of that same chain" — means the capability is proven FROM THE CHAIN on
// every dispatch, not merely trusted from a mutable connect-time array.
//
// Keys are core wire-type constants; values are core capability constants — no string literals, so
// this map cannot drift from either vocabulary. An op ABSENT here requires only membership (the
// connect-time authentication proof), which is the pre-existing behavior for every op; ADDING an op
// to this map is a deliberate authorization decision.
const OP_REQUIRED_CAPABILITY = new Map([
  [T.ACCOUNT_OUTBOX_LEASE_CLAIM, CAP_DEVICE_SET_PUBLISH],
  [T.ACCOUNT_OUTBOX_LEASE_PREPARE, CAP_DEVICE_SET_PUBLISH],
  [T.ACCOUNT_OUTBOX_LEASE_RELEASE, CAP_DEVICE_SET_PUBLISH],
  [T.ACCOUNT_OUTBOX_LEASE_FAIL, CAP_DEVICE_SET_PUBLISH],
  [T.ACCOUNT_OUTBOX_LEASE_COMPLETE, CAP_DEVICE_SET_PUBLISH],
]);

/**
 * @param {string} type - the dispatched wire op type.
 * @returns {string|null} the required capability, or null when the op needs only membership.
 */
export function requiredCapabilityForOp(type) {
  const cap = OP_REQUIRED_CAPABILITY.get(type);
  return typeof cap === "string" ? cap : null;
}

/**
 * The wire-op types this map covers — consumed by the operation-authority
 * guardrail (test/architecture.operation-authority.test.js) to prove the map
 * never drifts from the registered operation surface.
 */
export const OP_REQUIRED_CAPABILITY_TYPES = Object.freeze([...OP_REQUIRED_CAPABILITY.keys()]);
