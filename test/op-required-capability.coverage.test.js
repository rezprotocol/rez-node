import test from "node:test";
import assert from "node:assert/strict";
import { REZ_CONTRACT_TYPES, CAP_DEVICE_SET_PUBLISH } from "@rezprotocol/core";
import { requiredCapabilityForOp } from "../src/protocol/opRequiredCapability.js";

// leaf-3c reviewer requirement: a COVERAGE test that fails the moment a new outbox-lease op is added
// to the wire vocabulary but NOT wired into OP_REQUIRED_CAPABILITY. Derived from REZ_CONTRACT_TYPES,
// so it needs no maintenance — adding ACCOUNT_OUTBOX_LEASE_<x> without mapping it turns this red.

const outboxRequestOps = Object.entries(REZ_CONTRACT_TYPES)
  .filter(([key]) => key.startsWith("ACCOUNT_OUTBOX_LEASE_") && !key.endsWith("_RES"))
  .map(([key, value]) => ({ key, value }));

test("coverage: every ACCOUNT_OUTBOX_LEASE_* request op is mapped to a required capability", () => {
  // Sanity: the five known request ops (claim/prepare/release/fail/complete) are actually present —
  // a mis-typed prefix that matched nothing would make the coverage assertion vacuously pass.
  assert.ok(outboxRequestOps.length >= 5, "expected >= 5 outbox-lease request ops, found " + outboxRequestOps.length);
  const unmapped = outboxRequestOps.filter((o) => requiredCapabilityForOp(o.value) === null).map((o) => o.key);
  assert.deepEqual(unmapped, [], "these outbox ops are unmapped — add them to OP_REQUIRED_CAPABILITY: " + unmapped.join(", "));
});

test("the completion op requires deviceSet.publish, same as the other lease ops", () => {
  assert.equal(requiredCapabilityForOp(REZ_CONTRACT_TYPES.ACCOUNT_OUTBOX_LEASE_COMPLETE), CAP_DEVICE_SET_PUBLISH);
  // All five map to the same capability today.
  for (const o of outboxRequestOps) {
    assert.equal(requiredCapabilityForOp(o.value), CAP_DEVICE_SET_PUBLISH, o.key + " should require deviceSet.publish");
  }
});

test("a _RES response type is not a dispatched op and maps to no capability", () => {
  assert.equal(requiredCapabilityForOp(REZ_CONTRACT_TYPES.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES), null);
});
