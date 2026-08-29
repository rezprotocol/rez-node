import test from "node:test";
import assert from "node:assert/strict";
import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { ContractRegistry, registerAllContracts } from "../src/contracts/index.js";
import {
  WIRE_MANIFEST,
  WIRE_DIRECTIONS,
  WIRE_VALIDATED_BY,
  wireManifestEntry,
  dispatchableRequestTypes,
} from "../src/contracts/wireManifest.js";
import { HandlerRegistry } from "../src/protocol/HandlerRegistry.js";
import { GatewaySession } from "../src/protocol/GatewaySession.js";

// AUDIT #6 — the registry SSOT guardrail.
//
// ContractRegistry (type → record shape) and HandlerRegistry (type → behaviour) each decided things
// about the same wire types with no knowledge of the other, so a type could be in either, both, or
// neither and nothing noticed. WIRE_MANIFEST is now the single declaration; these tests are what
// make it binding rather than documentation.
//
// The property that actually prevents recurrence is TOTALITY: every REZ_CONTRACT_TYPES value must
// appear in the manifest exactly once. A new op cannot be added without deciding its direction, its
// validation, and whether it is dispatched.

const T = REZ_CONTRACT_TYPES;

function contractTypes() {
  const registry = new ContractRegistry();
  registerAllContracts(registry);
  return new Set(registry.listTypes());
}

/** The types GatewaySession actually registers, read from a real session's HandlerRegistry. */
function dispatchedTypes({ nodeEnabled }) {
  const registry = new HandlerRegistry();
  // _registerHandlers is the single registration site; drive it exactly as the constructor does,
  // against a session whose handler slots are stubbed. Reading the REAL registrations (rather than
  // a hand-listed copy) is the point — a hand-copied list is how the two registries drifted.
  const session = Object.create(GatewaySession.prototype);
  const stub = new Proxy({}, { get: () => () => {} });
  for (const slot of [
    "_mailboxHandler", "_inboxClaimHandler", "_inboxCloseHandler", "_deviceHandler", "_depositPolicyHandler",
    "_handleHandler", "_recordHandler", "_meshStatusHandler",
    "_accountMutationHandler", "_accountDeviceBundleHandler", "_propagationOutboxHandler",
  ]) session[slot] = stub;
  session._nodeEnabled = nodeEnabled;
  session._registry = registry;
  session._registerHandlers();
  return new Set(registry.listTypes());
}

test("TOTALITY — every wire type is declared exactly once (the anti-drift property)", () => {
  const declared = WIRE_MANIFEST.map((e) => e.type);
  const duplicates = declared.filter((t, i) => declared.indexOf(t) !== i);
  assert.deepEqual(duplicates, [], "a wire type declared twice");

  const all = Object.values(T);
  const undeclared = all.filter((t) => !declared.includes(t)).sort();
  assert.deepEqual(undeclared, [], "wire types exist that the manifest does not classify");

  const unknown = declared.filter((t) => !all.includes(t)).sort();
  assert.deepEqual(unknown, [], "the manifest declares types that are not in REZ_CONTRACT_TYPES");
});

test("every manifest entry is well-formed", () => {
  const directions = new Set(Object.values(WIRE_DIRECTIONS));
  const validators = new Set(Object.values(WIRE_VALIDATED_BY));
  for (const entry of WIRE_MANIFEST) {
    assert.equal(typeof entry.type, "string");
    assert.ok(entry.type.length > 0, "type must be non-empty");
    assert.ok(directions.has(entry.direction), entry.type + " has an unknown direction");
    assert.ok(validators.has(entry.validatedBy), entry.type + " has an unknown validatedBy");
    if (entry.nodeOnly !== undefined) assert.equal(entry.nodeOnly, true, "nodeOnly is true-or-absent");
  }
});

test("CONTRACT REGISTRY agrees with the manifest, in both directions", () => {
  const registered = contractTypes();

  // Everything declared contract-backed IS registered...
  const missing = WIRE_MANIFEST
    .filter((e) => e.validatedBy === WIRE_VALIDATED_BY.CONTRACT && !registered.has(e.type))
    .map((e) => e.type).sort();
  assert.deepEqual(missing, [], "declared contract-backed but not registered");

  // ...and nothing is registered that the manifest calls handler-validated. This direction catches
  // the subtler drift: a record quietly added to the registry while the manifest still claims the
  // handler owns validation, leaving two answers to "who checks this".
  const unexpected = WIRE_MANIFEST
    .filter((e) => e.validatedBy === WIRE_VALIDATED_BY.HANDLER && registered.has(e.type))
    .map((e) => e.type).sort();
  assert.deepEqual(unexpected, [], "registered as a contract but declared handler-validated");

  // And the registry holds nothing undeclared at all.
  const declared = new Set(WIRE_MANIFEST.map((e) => e.type));
  assert.deepEqual([...registered].filter((t) => !declared.has(t)).sort(), []);
});

test("HANDLER REGISTRY dispatches exactly the manifest's request types (node enabled)", () => {
  const dispatched = dispatchedTypes({ nodeEnabled: true });
  const expected = dispatchableRequestTypes({ nodeEnabled: true });

  assert.deepEqual(
    expected.filter((t) => !dispatched.has(t)),
    [],
    "declared a request but nothing dispatches it — clients would get UNKNOWN_TYPE",
  );
  assert.deepEqual(
    [...dispatched].filter((t) => !expected.includes(t)).sort(),
    [],
    "dispatched but not declared a request in the manifest",
  );
});

test("HANDLER REGISTRY honours the nodeOnly split (node disabled)", () => {
  // A relay-only build must dispatch every non-nodeOnly request and NONE of the nodeOnly ones —
  // the manifest's nodeOnly flag has to mean the same thing GatewaySession's _nodeEnabled gate does.
  const dispatched = dispatchedTypes({ nodeEnabled: false });
  const expected = dispatchableRequestTypes({ nodeEnabled: false });
  assert.deepEqual([...dispatched].sort(), expected);

  const nodeOnly = WIRE_MANIFEST.filter((e) => e.nodeOnly === true).map((e) => e.type);
  for (const type of nodeOnly) {
    assert.equal(dispatched.has(type), false, type + " is nodeOnly but a relay-only build dispatched it");
  }
});

test("responses and events are NEVER dispatched as requests", () => {
  // A node→client type reaching HandlerRegistry would mean the node is willing to act on a frame it
  // is only ever supposed to send.
  const dispatched = dispatchedTypes({ nodeEnabled: true });
  for (const entry of WIRE_MANIFEST) {
    if (entry.direction === WIRE_DIRECTIONS.REQUEST) continue;
    assert.equal(dispatched.has(entry.type), false,
      entry.type + " is a " + entry.direction + " but is dispatched as a request");
  }
});

test("every request has a matching response declared", () => {
  // Not cosmetic: a request whose `.res` type is undeclared means the reply the node sends is not
  // covered by the manifest, so nothing checks it against the contract registry either.
  const declared = new Set(WIRE_MANIFEST.map((e) => e.type));
  const missing = [];
  for (const entry of WIRE_MANIFEST) {
    if (entry.direction !== WIRE_DIRECTIONS.REQUEST) continue;
    const res = entry.type + ".res";
    if (!declared.has(res)) missing.push(entry.type);
  }
  assert.deepEqual(missing, [], "request types with no declared .res");
});

test("the handler-validated set is DECLARED debt, and its size is pinned", () => {
  // These types validate by constructing a record inside the handler instead of through the
  // contract registry. That is legitimate but invisible to WS_CONTRACT_EXAMPLES, so no generic
  // layer can reject a malformed body before it reaches the handler.
  //
  // The count is pinned deliberately: it may go DOWN as types move onto the registry, and a rise
  // means a new op took the invisible path. Update this number only when reducing it, or when a
  // new type is added with a stated reason.
  const handlerValidated = WIRE_MANIFEST
    .filter((e) => e.validatedBy === WIRE_VALIDATED_BY.HANDLER)
    .map((e) => e.type).sort();
  // 26 (2026-08-24): +2 for lease L1's inbox.close/.res — the request body is
  // a rez-core TerminalInboxCloseV1 whose constructor performs the full
  // validation inside the handler (self-authorizing record), so it takes the
  // handler-validated path by design.
  assert.equal(handlerValidated.length, 26,
    "the handler-validated set changed: " + handlerValidated.join(", "));
  // The security-critical ones, named so they cannot quietly disappear from review.
  for (const type of [T.ACCOUNT_DEVICE_MUTATION_SUBMIT, T.ACCOUNT_OUTBOX_LEASE_COMPLETE, T.RECORD_PUT]) {
    assert.equal(wireManifestEntry(type).validatedBy, WIRE_VALIDATED_BY.HANDLER);
  }
});
