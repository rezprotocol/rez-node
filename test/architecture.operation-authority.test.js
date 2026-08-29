import test from "node:test";
import assert from "node:assert/strict";
import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { HandlerRegistry } from "../src/protocol/HandlerRegistry.js";
import { AuthorityRequirement } from "../src/protocol/AuthorityRequirement.js";
import { OP_REQUIRED_CAPABILITY_TYPES } from "../src/protocol/opRequiredCapability.js";

const T = REZ_CONTRACT_TYPES;

// SESSION_AUTH_V5 slice 1 — the operation → authority matrix guardrail
// (plans/SESSION_AUTH_V5_SLICE1_PLAN.md §5). The EXPECTED matrix is written
// out literally: changing any operation's classification means editing this
// table in the same diff, which is exactly the review moment the guardrail
// exists to force. Registration itself already fails at boot on a missing
// declaration; this test pins WHICH declaration each operation carries.

const ACCOUNT = AuthorityRequirement.ACCOUNT;
const ANY_PRINCIPAL = AuthorityRequirement.ANY_PRINCIPAL;

const EXPECTED_MATRIX = Object.freeze({
  // Mailbox — resource scope via authorize() (binding / cap chain)
  [T.MAILBOX_DEPOSIT]: ANY_PRINCIPAL,
  [T.MAILBOX_LIST]: ANY_PRINCIPAL,
  [T.MAILBOX_FETCH]: ANY_PRINCIPAL,
  [T.MAILBOX_ACK]: ANY_PRINCIPAL,
  [T.MAILBOX_CURSOR_ACK]: ANY_PRINCIPAL,
  // Claimant-proof-carrying ops
  [T.INBOX_CLAIM]: ANY_PRINCIPAL,
  [T.INBOX_SET_DEPOSIT_POLICY]: ANY_PRINCIPAL,
  // Lease L1: the TerminalInboxClose record AUTHORIZES ITSELF (close-key
  // signature vs the stored claim); session identity contributes no authority
  // — the kill switch never forces account identity onto the wire.
  [T.INBOX_CLOSE]: ANY_PRINCIPAL,
  // Account-control plane
  [T.DEVICE_BIND]: ACCOUNT,
  // Handle — ownership proofs + cap chains in the request
  [T.HANDLE_REGISTER]: ANY_PRINCIPAL,
  [T.HANDLE_RESOLVE]: ANY_PRINCIPAL,
  [T.HANDLE_RELEASE]: ANY_PRINCIPAL,
  // Durable records — root-signed, self-authenticating
  [T.RECORD_PUT]: ANY_PRINCIPAL,
  [T.RECORD_GET]: ANY_PRINCIPAL,
  // Node-only
  [T.NODE_STATUS]: ANY_PRINCIPAL,
  // Account namespace: ALL account-scoped, including both GETs — the handlers
  // enforce requested === session account (the blindness boundary; peers use
  // the published sealed records instead). Verified against the SDK
  // (DevicesCapability): both are documented "served for the authenticated
  // account only".
  [T.ACCOUNT_DEVICE_MUTATION_SUBMIT]: ACCOUNT,
  [T.ACCOUNT_AUTHORITY_STATE_GET]: ACCOUNT,
  [T.ACCOUNT_DEVICE_BUNDLE_PUBLISH]: ACCOUNT,
  [T.ACCOUNT_DEVICE_SET_GET]: ACCOUNT,
  [T.ACCOUNT_OUTBOX_LEASE_CLAIM]: ACCOUNT,
  [T.ACCOUNT_OUTBOX_LEASE_PREPARE]: ACCOUNT,
  [T.ACCOUNT_OUTBOX_LEASE_RELEASE]: ACCOUNT,
  [T.ACCOUNT_OUTBOX_LEASE_FAIL]: ACCOUNT,
  [T.ACCOUNT_OUTBOX_LEASE_COMPLETE]: ACCOUNT,
});

const NODE_ONLY_TYPES = new Set([
  T.NODE_STATUS,
  T.ACCOUNT_DEVICE_MUTATION_SUBMIT,
  T.ACCOUNT_AUTHORITY_STATE_GET,
  T.ACCOUNT_DEVICE_BUNDLE_PUBLISH,
  T.ACCOUNT_DEVICE_SET_GET,
  T.ACCOUNT_OUTBOX_LEASE_CLAIM,
  T.ACCOUNT_OUTBOX_LEASE_PREPARE,
  T.ACCOUNT_OUTBOX_LEASE_RELEASE,
  T.ACCOUNT_OUTBOX_LEASE_FAIL,
  T.ACCOUNT_OUTBOX_LEASE_COMPLETE,
]);

/** Drive the REAL _registerHandlers (same technique as architecture.wire-manifest.test.js). */
function realRegistry({ nodeEnabled }) {
  const registry = new HandlerRegistry();
  const stub = new Proxy({}, { get: () => () => {} });
  const session = Object.create(GatewaySession.prototype);
  for (const slot of [
    "_mailboxHandler", "_inboxClaimHandler", "_inboxCloseHandler", "_deviceHandler", "_depositPolicyHandler",
    "_handleHandler", "_recordHandler", "_meshStatusHandler",
    "_accountMutationHandler", "_accountDeviceBundleHandler", "_propagationOutboxHandler",
  ]) session[slot] = stub;
  session._nodeEnabled = nodeEnabled;
  session._registry = registry;
  session._registerHandlers();
  return registry;
}

test("TOTALITY — every registered operation appears in the expected matrix exactly once, with the expected authority; nothing is UNKNOWN", () => {
  const registry = realRegistry({ nodeEnabled: true });
  const registered = registry.listTypes();

  // Registry ⊆ matrix, with matching classification.
  for (const type of registered) {
    assert.ok(
      Object.prototype.hasOwnProperty.call(EXPECTED_MATRIX, type),
      `operation ${type} is registered but not classified in EXPECTED_MATRIX — classify it in this diff`,
    );
    assert.equal(
      registry.requiredAuthority(type),
      EXPECTED_MATRIX[type],
      `operation ${type} declares a different authority than the pinned matrix`,
    );
  }

  // Matrix ⊆ registry — a stale row is drift too.
  assert.deepEqual(
    Object.keys(EXPECTED_MATRIX).filter((t) => !registered.includes(t)).sort(),
    [],
    "the matrix classifies operations that are no longer registered",
  );

  // No UNKNOWN: every registered op resolves a valid requirement.
  for (const type of registered) {
    assert.ok(AuthorityRequirement.isValid(registry.requiredAuthority(type)), type + " has a valid declaration");
  }
});

test("relay mode (nodeEnabled: false) registers exactly the matrix minus the node-only rows — same classifications", () => {
  const registry = realRegistry({ nodeEnabled: false });
  const expected = Object.keys(EXPECTED_MATRIX).filter((t) => !NODE_ONLY_TYPES.has(t)).sort();
  assert.deepEqual(registry.listTypes(), expected);
  for (const type of registry.listTypes()) {
    assert.equal(registry.requiredAuthority(type), EXPECTED_MATRIX[type]);
  }
});

test("the delegated-capability map (opRequiredCapability) never drifts from the registered surface", () => {
  const registry = realRegistry({ nodeEnabled: true });
  for (const type of OP_REQUIRED_CAPABILITY_TYPES) {
    assert.ok(registry.has(type), `opRequiredCapability maps ${type}, which is not a registered operation`);
    assert.equal(
      registry.requiredAuthority(type),
      ACCOUNT,
      `${type} carries a delegated-ACCOUNT capability requirement, so its principal class must be ACCOUNT`,
    );
  }
});
