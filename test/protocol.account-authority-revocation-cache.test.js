import test from "node:test";
import assert from "node:assert/strict";
import { AccountAuthorityRevocationCache } from "../src/protocol/AccountAuthorityRevocationCache.js";

// S2.5 S11 (F4): the bounded-staleness cache over the home authority-state that
// feeds the verify hot paths' revocationState. The byte-compat invariant —
// null-when-empty — is the load-bearing property: a never-revoked account must
// resolve to `null`, not `{}`, so the primary verify path is untouched.

function fakeSerializer(stateMap) {
  let calls = 0;
  return {
    get calls() { return calls; },
    async getAuthorityState(account) {
      calls += 1;
      return stateMap.get(account) || { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 };
    },
  };
}

test("resolve returns null for an account with no revocations (byte-compat primary path)", async () => {
  const serializer = fakeSerializer(new Map());
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.equal(await cache.resolve("acct-A"), null);
});

test("resolve returns null for a blank/absent account", async () => {
  const serializer = fakeSerializer(new Map());
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.equal(await cache.resolve(""), null);
  assert.equal(await cache.resolve(null), null);
  assert.equal(serializer.calls, 0, "a blank account never touches the home");
});

test("resolve projects a revoked-cert set to {revokedCertIds, minValidIssuedAtMs}", async () => {
  const serializer = fakeSerializer(new Map([
    ["acct-B", { epoch: 3, revokedCertIds: ["rez:cap:x", "rez:cap:y"], minValidIssuedAtMs: 1234 }],
  ]));
  const cache = new AccountAuthorityRevocationCache({ serializer });
  const state = await cache.resolve("acct-B");
  assert.deepEqual(state, { revokedCertIds: ["rez:cap:x", "rez:cap:y"], minValidIssuedAtMs: 1234 });
});

test("resolve treats a bumped epoch with no revocations as null (epoch alone is not revocation)", async () => {
  const serializer = fakeSerializer(new Map([
    ["acct-C", { epoch: 7, revokedCertIds: [], minValidIssuedAtMs: 0 }],
  ]));
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.equal(await cache.resolve("acct-C"), null);
});

test("a minValidIssuedAtMs cutoff alone (no revoked certs) still resolves non-null", async () => {
  const serializer = fakeSerializer(new Map([
    ["acct-D", { epoch: 2, revokedCertIds: [], minValidIssuedAtMs: 999 }],
  ]));
  const cache = new AccountAuthorityRevocationCache({ serializer });
  assert.deepEqual(await cache.resolve("acct-D"), { revokedCertIds: [], minValidIssuedAtMs: 999 });
});

test("within the TTL the home is read once; after expiry it re-reads", async () => {
  let clock = 1000;
  const serializer = fakeSerializer(new Map([
    ["acct-E", { epoch: 1, revokedCertIds: ["rez:cap:z"], minValidIssuedAtMs: 0 }],
  ]));
  const cache = new AccountAuthorityRevocationCache({ serializer, ttlMs: 100, nowMs: () => clock });
  await cache.resolve("acct-E");
  await cache.resolve("acct-E");
  assert.equal(serializer.calls, 1, "second resolve within TTL is served from cache");
  clock += 101;
  await cache.resolve("acct-E");
  assert.equal(serializer.calls, 2, "resolve after TTL re-reads the home");
});

test("invalidate forces the next resolve to re-read the home", async () => {
  const serializer = fakeSerializer(new Map([
    ["acct-F", { epoch: 1, revokedCertIds: ["rez:cap:z"], minValidIssuedAtMs: 0 }],
  ]));
  const cache = new AccountAuthorityRevocationCache({ serializer, ttlMs: 100000 });
  await cache.resolve("acct-F");
  cache.invalidate("acct-F");
  await cache.resolve("acct-F");
  assert.equal(serializer.calls, 2);
});

test("the cache is bounded — a flood of distinct accounts evicts oldest entries", async () => {
  const serializer = fakeSerializer(new Map());
  const cache = new AccountAuthorityRevocationCache({ serializer, maxEntries: 2, ttlMs: 100000 });
  await cache.resolve("a1");
  await cache.resolve("a2");
  await cache.resolve("a3"); // evicts a1
  const before = serializer.calls;
  await cache.resolve("a1"); // must re-read (was evicted)
  assert.equal(serializer.calls, before + 1, "evicted entry re-reads the home");
  await cache.resolve("a3"); // still cached
  assert.equal(serializer.calls, before + 1, "a3 stayed cached");
});

test("constructing without a serializer throws (fail loud)", () => {
  assert.throws(() => new AccountAuthorityRevocationCache({}), /requires a serializer/);
  assert.throws(() => new AccountAuthorityRevocationCache({ serializer: {} }), /requires a serializer/);
});
