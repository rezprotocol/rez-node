import test from "node:test";
import assert from "node:assert/strict";
import {
  OutboxLeaseCompleteRequest,
  OutboxLeaseCompleteResponse,
  MAX_PUBLICATION_RECORD_BYTES,
} from "../src/contracts/records/index.js";

// leaf-3c: the COMPLETE request/response contracts. The request bounds SHAPE + SIZE only — the
// crypto (signature, cert chain, epoch match) is the handler's job. F5: values are preserved
// VERBATIM and validated strictly; malformed input fails LOUDLY at construction, never coerced.

const okRecord = { v: 2, recordKind: "account-authority-state", recordId: "v1", ownerPublicKeyB64: "O", signerPublicKeyB64: "S", sigB64: "sig", payloadB64: "p" };

test("complete request: a token + object record constructs and preserves both verbatim", () => {
  const req = new OutboxLeaseCompleteRequest({ leaseToken: "  tok  ", record: okRecord });
  assert.equal(req.leaseToken, "  tok  ", "leaseToken preserved verbatim (trim happens at the handler)");
  assert.deepEqual(req.record, okRecord, "record preserved verbatim");
});

test("complete request: a missing record fails loudly", () => {
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: "tok" }), /record is required/);
});

test("complete request: a null record fails loudly (not coerced)", () => {
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: "tok", record: null }), /record is required/);
});

test("complete request: an array record is rejected (must be an object)", () => {
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: "tok", record: [1, 2] }), /record must be an object/);
});

test("complete request: a non-object record is rejected", () => {
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: "tok", record: "a-string" }), /record must be an object/);
});

test("complete request: an oversized record is rejected at the contract layer", () => {
  const huge = { v: 2, payloadB64: "x".repeat(MAX_PUBLICATION_RECORD_BYTES + 100) };
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: "tok", record: huge }), new RegExp(MAX_PUBLICATION_RECORD_BYTES + "-byte limit"));
});

test("complete request: a record at exactly the limit is accepted", () => {
  // Construct a record whose JSON is exactly MAX bytes: overhead + a padded string field.
  const overhead = JSON.stringify({ p: "" }).length; // {"p":""}
  const pad = "y".repeat(MAX_PUBLICATION_RECORD_BYTES - overhead);
  const rec = { p: pad };
  assert.equal(JSON.stringify(rec).length, MAX_PUBLICATION_RECORD_BYTES);
  assert.doesNotThrow(() => new OutboxLeaseCompleteRequest({ leaseToken: "tok", record: rec }));
});

test("complete request: an absent/blank lease token still fails (inherited token rule)", () => {
  assert.throws(() => new OutboxLeaseCompleteRequest({ record: okRecord }), /leaseToken is required/);
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: "   ", record: okRecord }), /leaseToken is required/);
});

test("complete request: a non-string lease token fails loudly (no coercion)", () => {
  assert.throws(() => new OutboxLeaseCompleteRequest({ leaseToken: 123, record: okRecord }), /leaseToken must be a string/);
});

test("complete response: completed=true requires a positive integer doneThroughEpoch", () => {
  const res = new OutboxLeaseCompleteResponse({ completed: true, doneThroughEpoch: 7 });
  assert.equal(res.completed, true);
  assert.equal(res.doneThroughEpoch, 7);
  assert.throws(() => new OutboxLeaseCompleteResponse({ completed: true, doneThroughEpoch: 0 }), /positive integer/);
  assert.throws(() => new OutboxLeaseCompleteResponse({ completed: true }), /positive integer/);
});

test("complete response: completed=false carries no epoch and validates", () => {
  const res = new OutboxLeaseCompleteResponse({ completed: false });
  assert.equal(res.completed, false);
  assert.equal(res.doneThroughEpoch, undefined, "no epoch on the lease-lost race");
});

test("complete response: a missing/malformed completed never coerces to false", () => {
  assert.throws(() => new OutboxLeaseCompleteResponse({}), /completed must be a boolean/);
  assert.throws(() => new OutboxLeaseCompleteResponse({ completed: "true" }), /completed must be a boolean/);
});
