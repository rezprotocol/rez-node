import test from "node:test";
import assert from "node:assert/strict";
import { AccountAuthorityRevocationCache } from "../src/protocol/AccountAuthorityRevocationCache.js";

// Audit R4 L5 review: the class is now a PASS-THROUGH FRESH READER over the home authority-state
// (the warm TTL cache was removed — it served no consumer and reintroduced a bounded-staleness
// window + an overlapping-read regression race). It exposes exactly two reads: currentEpoch (cheap
// fast-path int) and resolveDelegatedSnapshot (ONE coherent {state, epoch, terminal}). The
// byte-compat invariant — null-when-empty revocation state — is the load-bearing property.

function fakeSerializer({ epoch = 0, snapshot = null } = {}) {
  const calls = { getCurrentEpoch: 0, getDelegatedAuthoritySnapshot: 0 };
  let lastSnapshotArgs = null;
  return {
    calls,
    get lastSnapshotArgs() { return lastSnapshotArgs; },
    async getCurrentEpoch() { calls.getCurrentEpoch += 1; return epoch; },
    async getDelegatedAuthoritySnapshot(args) {
      calls.getDelegatedAuthoritySnapshot += 1;
      lastSnapshotArgs = args;
      return snapshot || { epoch, revokedCertIds: [], minValidIssuedAtMs: 0, terminal: false };
    },
  };
}

test("constructing without a capable serializer throws (fail loud)", () => {
  assert.throws(() => new AccountAuthorityRevocationCache({}), /requires a serializer/);
  assert.throws(() => new AccountAuthorityRevocationCache({ serializer: {} }), /requires a serializer/);
  assert.throws(
    () => new AccountAuthorityRevocationCache({ serializer: { async getCurrentEpoch() {} } }),
    /requires a serializer/,
    "must also have getDelegatedAuthoritySnapshot",
  );
});

test("currentEpoch passes through to the serializer (always fresh, no caching)", async () => {
  const serializer = fakeSerializer({ epoch: 5 });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.equal(await cache.currentEpoch("acct-A"), 5);
  assert.equal(await cache.currentEpoch("acct-A"), 5);
  assert.equal(serializer.calls.getCurrentEpoch, 2, "every call hits the home — nothing is cached");
});

test("currentEpoch of a blank/absent account is 0 and never touches the home", async () => {
  const serializer = fakeSerializer({ epoch: 9 });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.equal(await cache.currentEpoch(""), 0);
  assert.equal(await cache.currentEpoch(null), 0);
  assert.equal(serializer.calls.getCurrentEpoch, 0);
});

test("resolveDelegatedSnapshot returns null state for an account with no revocations (byte-compat primary path)", async () => {
  const serializer = fakeSerializer({ epoch: 3, snapshot: { epoch: 3, revokedCertIds: [], minValidIssuedAtMs: 0, terminal: false } });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  const snap = await cache.resolveDelegatedSnapshot("acct-B", "rez:dev:x");
  assert.deepEqual(snap, { state: null, epoch: 3, terminal: false });
});

test("resolveDelegatedSnapshot projects a revoked-cert set and carries epoch + terminal coherently", async () => {
  const serializer = fakeSerializer({
    snapshot: { epoch: 8, revokedCertIds: ["rez:cap:x", "rez:cap:y"], minValidIssuedAtMs: 1234, terminal: true },
  });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  const snap = await cache.resolveDelegatedSnapshot("acct-C", "rez:dev:x");
  assert.deepEqual(snap, {
    state: { revokedCertIds: ["rez:cap:x", "rez:cap:y"], minValidIssuedAtMs: 1234 },
    epoch: 8,
    terminal: true,
  });
});

test("resolveDelegatedSnapshot treats a bumped epoch with no revocations as null state (epoch alone is not revocation)", async () => {
  const serializer = fakeSerializer({ snapshot: { epoch: 7, revokedCertIds: [], minValidIssuedAtMs: 0, terminal: false } });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  const snap = await cache.resolveDelegatedSnapshot("acct-D", "rez:dev:x");
  assert.equal(snap.state, null);
  assert.equal(snap.epoch, 7);
});

test("resolveDelegatedSnapshot: a minValidIssuedAtMs cutoff alone still resolves non-null state", async () => {
  const serializer = fakeSerializer({ snapshot: { epoch: 2, revokedCertIds: [], minValidIssuedAtMs: 999, terminal: false } });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  const snap = await cache.resolveDelegatedSnapshot("acct-E", "rez:dev:x");
  assert.deepEqual(snap.state, { revokedCertIds: [], minValidIssuedAtMs: 999 });
});

test("resolveDelegatedSnapshot threads only (account, deviceId) — the façade no longer knows the InTx storage API (review-3 finding P2)", async () => {
  const serializer = fakeSerializer({ epoch: 1 });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  await cache.resolveDelegatedSnapshot("acct-F", "rez:dev:zzz");
  assert.equal(serializer.lastSnapshotArgs.accountIdentityPublicKeyB64, "acct-F");
  assert.equal(serializer.lastSnapshotArgs.deviceId, "rez:dev:zzz");
  assert.equal("deviceRegistry" in serializer.lastSnapshotArgs, false,
    "no per-call registry is threaded — the serializer resolves terminal via its OWN canonical registry");
});

test("resolveDelegatedSnapshot throws REVOCATION_BACKEND_UNAVAILABLE when the serializer returns an incomplete snapshot (review-4 finding P1)", async () => {
  // The canonical façade must NOT coerce a missing/malformed `terminal` to false (that dropped the
  // terminal-device revocation dimension and failed open downstream) — it fails loud instead.
  const missingTerminal = {
    async getCurrentEpoch() { return 1; },
    async getDelegatedAuthoritySnapshot() { return { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 }; }, // no `terminal`
  };
  const cache = new AccountAuthorityRevocationCache({ serializer: missingTerminal });
  await assert.rejects(
    () => cache.resolveDelegatedSnapshot("acct-Z", "rez:dev:x"),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
  );

  const malformedState = {
    async getCurrentEpoch() { return 1; },
    async getDelegatedAuthoritySnapshot() { return { epoch: 1, revokedCertIds: "nope", minValidIssuedAtMs: 0, terminal: false }; },
  };
  const cache2 = new AccountAuthorityRevocationCache({ serializer: malformedState });
  await assert.rejects(
    () => cache2.resolveDelegatedSnapshot("acct-Z", "rez:dev:x"),
    (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
  );
});

test("resolveDelegatedSnapshot rejects malformed NUMERIC fields with NO coercion (review-5 P1)", async () => {
  // The reported bypass: Number(null)===0 and Number("")===0, and numeric strings coerce too, so a
  // malformed epoch/minValidIssuedAtMs used to normalize to 0 — dropping the issued-at cutoff and
  // projecting state:null BEFORE the strict Gateway validator saw it. The RAW fields must be safe
  // nonnegative INTEGERS; anything else fails loud as an availability error.
  const base = { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 5, terminal: false };
  const badValues = [null, "", "1", 1.5, -1, NaN, Infinity, -Infinity, undefined, {}, true];
  for (const field of ["epoch", "minValidIssuedAtMs"]) {
    for (const bad of badValues) {
      const snapshot = { ...base, [field]: bad };
      const serializer = {
        async getCurrentEpoch() { return 1; },
        async getDelegatedAuthoritySnapshot() { return snapshot; },
      };
      const cache = new AccountAuthorityRevocationCache({ serializer });
      await assert.rejects(
        () => cache.resolveDelegatedSnapshot("acct-Z", "rez:dev:x"),
        (err) => err && err.code === "REVOCATION_BACKEND_UNAVAILABLE",
        `field=${field} value=${String(bad)} must fail loud, not coerce`,
      );
    }
  }
});

test("resolveDelegatedSnapshot preserves a real issued-at cutoff as non-null state (no coercion drops it)", async () => {
  // The counterpart to the coercion bug: a VALID integer cutoff must survive as non-null state.
  const serializer = {
    async getCurrentEpoch() { return 4; },
    async getDelegatedAuthoritySnapshot() { return { epoch: 4, revokedCertIds: [], minValidIssuedAtMs: 1700, terminal: false }; },
  };
  const cache = new AccountAuthorityRevocationCache({ serializer });
  const snap = await cache.resolveDelegatedSnapshot("acct-Z", "rez:dev:x");
  assert.deepEqual(snap, { state: { revokedCertIds: [], minValidIssuedAtMs: 1700 }, epoch: 4, terminal: false });
});

test("resolveDelegatedSnapshot of a blank account is null/0/false and never touches the home", async () => {
  const serializer = fakeSerializer({ epoch: 4 });
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.deepEqual(await cache.resolveDelegatedSnapshot("", "rez:dev:x"), { state: null, epoch: 0, terminal: false });
  assert.deepEqual(await cache.resolveDelegatedSnapshot(null, "rez:dev:x"), { state: null, epoch: 0, terminal: false });
  assert.equal(serializer.calls.getDelegatedAuthoritySnapshot, 0);
});
