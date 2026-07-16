// Centralized classification of a caught BACKEND error (Postgres SQLSTATE or a Node socket/transport
// code) into "a retry may succeed" vs "permanent". Handlers use it to map a transient backend blip to
// a RETRYABLE response (SERVICE_UNAVAILABLE) instead of a terminal INTERNAL, so a healthy client backs
// off and retries rather than treating an availability wobble as a hard failure. (audit leaf-3c F4 —
// previously only the four transient SQLSTATE CLASSES were retryable, so serialization/deadlock/
// lock-timeout rollbacks and dropped connections were wrongly terminal.)
//
// Classification reads err.code ONLY. A SQLSTATE / errno is a FIXED code — never a parameter value or
// a secret/token — so deciding on it leaks nothing (the same token-hygiene property the outbox handler
// relies on when it declines to surface err.message).

// SQLSTATE class prefixes that indicate a TRANSIENT / availability failure:
//   08 connection exception, 53 insufficient resources, 57 operator intervention
//   (admin shutdown / cannot-connect-now), 58 system error.
const TRANSIENT_SQLSTATE_CLASSES = new Set(["08", "53", "57", "58"]);

// Specific retryable SQLSTATEs OUTSIDE those classes — a concurrent-transaction conflict the client
// should simply re-run:
//   40001 serialization_failure, 40P01 deadlock_detected (class 40 = transaction rollback),
//   55P03 lock_not_available (class 55 = object-not-in-prerequisite-state; e.g. NOWAIT lock contention).
const RETRYABLE_SQLSTATES = new Set(["40001", "40P01", "55P03"]);

// Node socket/transport errnos for a dropped or timed-out connection to the backend — the request
// never committed, so a retry is safe.
const RETRYABLE_TRANSPORT_CODES = new Set(["ECONNRESET", "ETIMEDOUT", "ECONNREFUSED", "EPIPE"]);

/**
 * @param {*} err - a caught error; only its `.code` is consulted.
 * @returns {boolean} true when the failure is transient and the client should retry.
 */
export function isRetryableBackendError(err) {
  const code = err && typeof err.code === "string" ? err.code : "";
  if (code.length === 0) return false;
  if (RETRYABLE_SQLSTATES.has(code)) return true;
  if (RETRYABLE_TRANSPORT_CODES.has(code)) return true;
  const cls = code.length >= 2 ? code.slice(0, 2) : "";
  return TRANSIENT_SQLSTATE_CLASSES.has(cls);
}
