import test from "node:test";
import assert from "node:assert/strict";
import { isRetryableBackendError } from "../src/util/backendRetryClassification.js";

test("retryable: transient SQLSTATE classes 08/53/57/58", () => {
  for (const code of ["08006", "08003", "53300", "57P01", "58030"]) {
    assert.equal(isRetryableBackendError({ code }), true, code + " is transient");
  }
});

test("retryable: specific txn-conflict + lock SQLSTATEs outside those classes (40001/40P01/55P03)", () => {
  for (const code of ["40001", "40P01", "55P03"]) {
    assert.equal(isRetryableBackendError({ code }), true, code + " should retry");
  }
});

test("retryable: transient transport codes (ECONNRESET/ETIMEDOUT/ECONNREFUSED/EPIPE)", () => {
  for (const code of ["ECONNRESET", "ETIMEDOUT", "ECONNREFUSED", "EPIPE"]) {
    assert.equal(isRetryableBackendError({ code }), true, code + " should retry");
  }
});

test("NOT retryable: integrity/constraint + unknown/absent codes", () => {
  assert.equal(isRetryableBackendError({ code: "23505" }), false, "unique_violation is permanent");
  assert.equal(isRetryableBackendError({ code: "22P02" }), false, "invalid_text_representation is permanent");
  assert.equal(isRetryableBackendError({ code: "40003" }), false, "an unlisted class-40 code is not blindly retried");
  assert.equal(isRetryableBackendError({}), false, "no code → not retryable");
  assert.equal(isRetryableBackendError(null), false, "null → not retryable");
  assert.equal(isRetryableBackendError({ code: "" }), false, "empty code → not retryable");
});
