import test from "node:test";
import assert from "node:assert/strict";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";

/**
 * docs/SECURITY_AUDIT.md MED-6 — `InboxClaimRegistry.claim()` previously
 * mutated `#claims` BEFORE awaiting persistence. Two exploitable
 * properties followed:
 *
 *   (a) During the persist await, readers (`getClaimantPublicKey`,
 *       `hasInbox`, …) observed transient state. Authz decisions could
 *       anchor against a claim that was about to roll back if persist
 *       failed.
 *
 *   (b) Two concurrent claims for DIFFERENT inboxIds — using the prior
 *       fix sketch ("persist-first, then swap in") naively — would each
 *       persist a snapshot computed from `#claims` BEFORE the other's
 *       commit, and the second persist would silently drop the first
 *       entry from the KV.
 *
 * The remediation moves all writes through a promise-chain mutex
 * (`#writeQueue`) AND persists before swapping in `#claims`. This suite
 * exercises both failure modes against a controllable mock KV.
 */

/**
 * Mock storage provider whose `set` calls block on an external gate.
 * Each `set` call records a deferred entry; the test drives them.
 */
function makeGatedStorageProvider() {
  const gates = [];
  const store = new Map();
  const kv = {
    async get(key) {
      return store.has(key) ? structuredClone(store.get(key)) : null;
    },
    async set(key, value) {
      const deferred = makeDeferred();
      gates.push({ key, value: structuredClone(value), deferred });
      await deferred.promise;
      store.set(key, structuredClone(value));
    },
    async delete(key) { store.delete(key); },
    async keys() { return Array.from(store.keys()); },
  };
  return {
    storageProvider: { getKeyValueStore: () => kv },
    gates,
    store,
  };
}

function makeDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

const PK_A = "cHVibGljLWtleS1hbGljZQ==";
const PK_B = "cHVibGljLWtleS1ib2I=";
const PK_C = "cHVibGljLWtleS1jaGFybGll";

test("MED-6: readers do NOT see a pending claim mid-persist", async () => {
  const { storageProvider, gates } = makeGatedStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();

  // Start a claim — it will await the gated kv.set.
  const claimPromise = registry.claim({
    inboxId: "inbox:in-flight",
    claimantPublicKeyB64: PK_A,
    claimedAtMs: 1,
  });

  // Wait one tick so the claim() coroutine reaches the kv.set await.
  await new Promise((r) => setImmediate(r));

  // Reader during the in-flight persist must NOT see the claim. This is
  // the load-bearing assertion: authz anchored against this view would
  // have been against transient state under the old code.
  assert.equal(registry.hasInbox("inbox:in-flight"), false, "reader saw transient state");
  assert.equal(registry.getClaimantPublicKey("inbox:in-flight"), null, "reader saw transient pubkey");

  // Release the gate, await completion.
  assert.equal(gates.length, 1, "exactly one persist in flight");
  gates[0].deferred.resolve();
  await claimPromise;

  // Post-resolution the reader sees the durable claim.
  assert.equal(registry.hasInbox("inbox:in-flight"), true);
  assert.equal(registry.getClaimantPublicKey("inbox:in-flight"), PK_A);
});

test("MED-6: a persist failure leaves no in-memory entry behind", async () => {
  const { storageProvider, gates } = makeGatedStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();

  const claimPromise = registry.claim({
    inboxId: "inbox:fails",
    claimantPublicKeyB64: PK_A,
    claimedAtMs: 1,
  });
  await new Promise((r) => setImmediate(r));

  assert.equal(gates.length, 1);
  gates[0].deferred.reject(new Error("disk full"));

  await assert.rejects(() => claimPromise, /disk full/);

  // No phantom entry survived the failed persist.
  assert.equal(registry.hasInbox("inbox:fails"), false);
  assert.equal(registry.size(), 0);

  // Subsequent claim of the same id succeeds — the slot wasn't burned.
  const nextPromise = registry.claim({
    inboxId: "inbox:fails",
    claimantPublicKeyB64: PK_B,
    claimedAtMs: 2,
  });
  await new Promise((r) => setImmediate(r));
  // Second persist is in flight — release it.
  gates[1].deferred.resolve();
  const next = await nextPromise;
  assert.equal(next.claimantPublicKeyB64, PK_B);
  assert.equal(registry.getClaimantPublicKey("inbox:fails"), PK_B);
});

test("MED-6: two concurrent claims for DIFFERENT inboxIds both persist (no last-writer-wins)", async () => {
  // Direct test of the naive "persist-first" failure mode: without the
  // write-queue serialization, claim(A) and claim(B) each build their
  // proposed map from the same starting state and one's persist would
  // overwrite the other in KV.
  const { storageProvider, gates, store } = makeGatedStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();

  const p1 = registry.claim({ inboxId: "inbox:one", claimantPublicKeyB64: PK_A, claimedAtMs: 1 });
  const p2 = registry.claim({ inboxId: "inbox:two", claimantPublicKeyB64: PK_B, claimedAtMs: 2 });

  // Drain the queue. With serialization there is exactly ONE persist in
  // flight at a time, so we release them one by one.
  await new Promise((r) => setImmediate(r));
  assert.equal(gates.length, 1, "writes are serialized — only one persist at a time");
  gates[0].deferred.resolve();
  await p1;

  await new Promise((r) => setImmediate(r));
  assert.equal(gates.length, 2, "second persist now in flight");
  gates[1].deferred.resolve();
  await p2;

  // Both claims survived in the durable store.
  const persisted = store.get("node:inbox:claims:v1");
  assert.equal(persisted.claims.length, 2);
  const ids = persisted.claims.map((c) => c.inboxId).sort();
  assert.deepEqual(ids, ["inbox:one", "inbox:two"]);
  assert.equal(registry.size(), 2);
});

test("MED-6: a duplicate concurrent claim of the SAME id is rejected (in-flight collision)", async () => {
  const { storageProvider, gates } = makeGatedStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();

  const first = registry.claim({ inboxId: "inbox:dup", claimantPublicKeyB64: PK_A, claimedAtMs: 1 });

  // Let first claim enter its kv.set await.
  await new Promise((r) => setImmediate(r));

  // Queue a second claim for the same id while the first is in flight.
  const second = registry.claim({ inboxId: "inbox:dup", claimantPublicKeyB64: PK_B, claimedAtMs: 2 });

  // Release the first claim's persist.
  gates[0].deferred.resolve();
  const firstResult = await first;
  assert.equal(firstResult.claimantPublicKeyB64, PK_A);

  // The second claim, once it acquires the mutex, sees the durable
  // entry and rejects with INBOX_ALREADY_CLAIMED.
  await assert.rejects(() => second, (err) => err.code === "INBOX_ALREADY_CLAIMED");
  assert.equal(registry.getClaimantPublicKey("inbox:dup"), PK_A);
});

test("MED-6: a failed claim does not block subsequent claims (mutex always releases)", async () => {
  const { storageProvider, gates } = makeGatedStorageProvider();
  const registry = new InboxClaimRegistry({ storageProvider });
  await registry.hydrate();

  const bad = registry.claim({ inboxId: "inbox:err", claimantPublicKeyB64: PK_A, claimedAtMs: 1 });
  await new Promise((r) => setImmediate(r));
  gates[0].deferred.reject(new Error("transient"));
  await assert.rejects(() => bad, /transient/);

  // The mutex must have released so this next claim proceeds normally.
  const okPromise = registry.claim({ inboxId: "inbox:ok", claimantPublicKeyB64: PK_C, claimedAtMs: 5 });
  await new Promise((r) => setImmediate(r));
  assert.equal(gates.length, 2, "second persist proceeded — mutex released after failure");
  gates[1].deferred.resolve();
  await okPromise;
  assert.equal(registry.getClaimantPublicKey("inbox:ok"), PK_C);
});
