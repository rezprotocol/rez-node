import test from "node:test";
import assert from "node:assert/strict";
import { HandleClaimV1 } from "@rezprotocol/core";
import { HandleRegistry } from "../src/handle/HandleRegistry.js";
import { ReceiptSigner } from "../src/settlement/ReceiptSigner.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

const KV_PREFIX = "handle:claim:";
const RELAY_KEY_ID = "relay-key-test-1";
const KEY_ALICE = "pubkey:alice";
const KEY_BOB = "pubkey:bob";

function makeMemKv() {
  const m = new Map();
  return {
    async set(key, value) {
      m.set(key, JSON.parse(JSON.stringify(value)));
    },
    async get(key) {
      if (!m.has(key)) return undefined;
      return JSON.parse(JSON.stringify(m.get(key)));
    },
    async delete(key) {
      const had = m.has(key);
      m.delete(key);
      return had;
    },
    async keys(prefix = "") {
      const out = [];
      for (const k of m.keys()) {
        if (k.startsWith(prefix)) out.push(k);
      }
      return out;
    },
    _raw: m,
  };
}

function makeRegistry() {
  const crypto = new NodeCryptoProvider();
  const keyPair = crypto.generateSigningKeyPair();
  const signer = new ReceiptSigner({
    relayKeyId: RELAY_KEY_ID,
    signFn: async (msg) => crypto.sign({ privateKey: keyPair.privateKey, msg }),
  });
  const kvStore = makeMemKv();
  const registry = new HandleRegistry({
    kvStore,
    receiptSigner: signer,
    selfRelayKeyId: RELAY_KEY_ID,
  });
  return { registry, kvStore, signer };
}

// --- register ---

test("register creates a signed claim with correct fields", async () => {
  const { registry } = makeRegistry();
  const claim = await registry.register("alice", KEY_ALICE);

  assert.equal(claim.handle, "alice");
  assert.equal(claim.keyId, KEY_ALICE);
  assert.equal(claim.relayKeyId, RELAY_KEY_ID);
  assert.equal(claim.previousKeyId, null);
  assert.ok(claim.expiresAtMs > claim.createdAtMs);
  assert.equal(claim.sig.alg, "ed25519");
  assert.equal(claim.sig.relayKeyId, RELAY_KEY_ID);
  assert.ok(claim.sig.sig instanceof Uint8Array);
  assert.ok(claim.sig.sig.length > 0);
});

test("register normalizes handle to lowercase + trimmed", async () => {
  const { registry, kvStore } = makeRegistry();
  const claim = await registry.register("  Alice  ", KEY_ALICE);
  assert.equal(claim.handle, "alice");
  // Stored under normalized key
  const keys = await kvStore.keys(KV_PREFIX);
  assert.deepEqual(keys, [KV_PREFIX + "alice"]);
});

test("register throws when handle is held by a different key (not expired)", async () => {
  const { registry } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  await assert.rejects(
    () => registry.register("alice", KEY_BOB),
    /Handle already claimed: @alice/,
  );
});

test("register with same keyId on existing claim succeeds (re-claim) and records previousKeyId", async () => {
  const { registry } = makeRegistry();
  const first = await registry.register("alice", KEY_ALICE);
  const second = await registry.register("alice", KEY_ALICE);
  assert.equal(second.keyId, KEY_ALICE);
  // previousKeyId records the prior owner's keyId (here, same as own — reassignment ledger)
  assert.equal(second.previousKeyId, KEY_ALICE);
  assert.ok(second.createdAtMs >= first.createdAtMs);
});

test("register can take over an expired claim (different key) and records previous owner", async () => {
  const { registry, kvStore, signer } = makeRegistry();
  const past = Date.now() - 10_000;
  const expiredBody = {
    v: 1,
    handle: "alice",
    keyId: KEY_BOB,
    relayKeyId: RELAY_KEY_ID,
    createdAtMs: past - 1000,
    expiresAtMs: past, // already expired
    previousKeyId: null,
  };
  const sig = await signer.sign(expiredBody);
  const expiredClaim = new HandleClaimV1({ ...expiredBody, sig });
  await kvStore.set(KV_PREFIX + "alice", expiredClaim.toJSON());

  const fresh = await registry.register("alice", KEY_ALICE);
  assert.equal(fresh.keyId, KEY_ALICE);
  assert.equal(fresh.previousKeyId, KEY_BOB);
  assert.ok(!fresh.isExpired());
});

// --- resolve ---

test("resolve returns null for unknown handle", async () => {
  const { registry } = makeRegistry();
  assert.equal(await registry.resolve("ghost"), null);
});

test("resolve returns the claim for an existing fresh handle", async () => {
  const { registry } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  const got = await registry.resolve("alice");
  assert.ok(got);
  assert.equal(got.handle, "alice");
  assert.equal(got.keyId, KEY_ALICE);
});

test("resolve normalizes handle case", async () => {
  const { registry } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  const got = await registry.resolve("ALICE");
  assert.ok(got);
  assert.equal(got.handle, "alice");
});

test("resolve returns null for expired claim", async () => {
  const { registry, kvStore, signer } = makeRegistry();
  const past = Date.now() - 10_000;
  const expiredBody = {
    v: 1,
    handle: "alice",
    keyId: KEY_ALICE,
    relayKeyId: RELAY_KEY_ID,
    createdAtMs: past - 1000,
    expiresAtMs: past,
    previousKeyId: null,
  };
  const sig = await signer.sign(expiredBody);
  const expiredClaim = new HandleClaimV1({ ...expiredBody, sig });
  await kvStore.set(KV_PREFIX + "alice", expiredClaim.toJSON());

  assert.equal(await registry.resolve("alice"), null);
});

// --- renew ---

test("renew by owner extends expiresAtMs", async () => {
  const { registry } = makeRegistry();
  const first = await registry.register("alice", KEY_ALICE);
  // Force a measurable time gap
  await new Promise((r) => setTimeout(r, 5));
  const renewed = await registry.renew("alice", KEY_ALICE);
  assert.equal(renewed.handle, "alice");
  assert.equal(renewed.keyId, KEY_ALICE);
  assert.ok(renewed.createdAtMs >= first.createdAtMs);
  assert.ok(renewed.expiresAtMs >= first.expiresAtMs);
});

test("renew by non-owner throws", async () => {
  const { registry } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  await assert.rejects(
    () => registry.renew("alice", KEY_BOB),
    /not owned by this key/,
  );
});

test("renew of unknown handle throws", async () => {
  const { registry } = makeRegistry();
  await assert.rejects(
    () => registry.renew("ghost", KEY_ALICE),
    /Handle not found/,
  );
});

// --- release ---

test("release by owner returns true and removes the claim", async () => {
  const { registry, kvStore } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  assert.equal(await registry.release("alice", KEY_ALICE), true);
  assert.equal(await registry.resolve("alice"), null);
  const keys = await kvStore.keys(KV_PREFIX);
  assert.deepEqual(keys, []);
});

test("release by non-owner returns false and leaves claim intact", async () => {
  const { registry } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  assert.equal(await registry.release("alice", KEY_BOB), false);
  const still = await registry.resolve("alice");
  assert.ok(still);
  assert.equal(still.keyId, KEY_ALICE);
});

test("release of unknown handle returns false", async () => {
  const { registry } = makeRegistry();
  assert.equal(await registry.release("ghost", KEY_ALICE), false);
});

// --- acceptGossipedClaim (FCFS conflict resolution) ---

async function makeClaim(signer, fields) {
  const body = {
    v: 1,
    handle: fields.handle,
    keyId: fields.keyId,
    relayKeyId: fields.relayKeyId || RELAY_KEY_ID,
    createdAtMs: fields.createdAtMs,
    expiresAtMs: fields.expiresAtMs,
    previousKeyId: fields.previousKeyId || null,
  };
  const sig = await signer.sign(body);
  return new HandleClaimV1({ ...body, sig });
}

test("acceptGossipedClaim accepts a fresh claim when registry is empty", async () => {
  const { registry, signer } = makeRegistry();
  const now = Date.now();
  const claim = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_ALICE,
    createdAtMs: now,
    expiresAtMs: now + 60_000,
  });
  assert.equal(await registry.acceptGossipedClaim(claim), true);
  const got = await registry.resolve("alice");
  assert.ok(got);
  assert.equal(got.keyId, KEY_ALICE);
});

test("acceptGossipedClaim rejects when local claim is older (FCFS keeps the older claim)", async () => {
  const { registry, signer } = makeRegistry();
  const now = Date.now();
  const older = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_ALICE,
    createdAtMs: now - 1_000,
    expiresAtMs: now + 60_000,
  });
  await registry.acceptGossipedClaim(older);

  const newer = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_BOB,
    createdAtMs: now,
    expiresAtMs: now + 60_000,
  });
  assert.equal(await registry.acceptGossipedClaim(newer), false);

  const got = await registry.resolve("alice");
  assert.equal(got.keyId, KEY_ALICE, "older claim must win");
});

test("acceptGossipedClaim accepts when incoming claim is older than local (FCFS replaces with older)", async () => {
  const { registry, signer } = makeRegistry();
  const now = Date.now();
  const local = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_BOB,
    createdAtMs: now,
    expiresAtMs: now + 60_000,
  });
  await registry.acceptGossipedClaim(local);

  const olderIncoming = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_ALICE,
    createdAtMs: now - 5_000,
    expiresAtMs: now + 60_000,
  });
  assert.equal(await registry.acceptGossipedClaim(olderIncoming), true);

  const got = await registry.resolve("alice");
  assert.equal(got.keyId, KEY_ALICE, "older incoming claim must replace newer local");
});

test("acceptGossipedClaim accepts when local exists but is expired", async () => {
  const { registry, signer, kvStore } = makeRegistry();
  const past = Date.now() - 10_000;
  const expired = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_BOB,
    createdAtMs: past - 1_000,
    expiresAtMs: past,
  });
  await kvStore.set(KV_PREFIX + "alice", expired.toJSON());

  const now = Date.now();
  const fresh = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_ALICE,
    createdAtMs: now,
    expiresAtMs: now + 60_000,
  });
  assert.equal(await registry.acceptGossipedClaim(fresh), true);
  const got = await registry.resolve("alice");
  assert.equal(got.keyId, KEY_ALICE);
});

test("acceptGossipedClaim rejects an already-expired incoming claim", async () => {
  const { registry, signer } = makeRegistry();
  const past = Date.now() - 10_000;
  const expired = await makeClaim(signer, {
    handle: "alice",
    keyId: KEY_ALICE,
    createdAtMs: past - 1_000,
    expiresAtMs: past,
  });
  assert.equal(await registry.acceptGossipedClaim(expired), false);
  assert.equal(await registry.resolve("alice"), null);
});

test("acceptGossipedClaim rejects non-HandleClaimV1 inputs", async () => {
  const { registry } = makeRegistry();
  assert.equal(await registry.acceptGossipedClaim(null), false);
  assert.equal(await registry.acceptGossipedClaim({}), false);
  assert.equal(await registry.acceptGossipedClaim({ type: "Other", handle: "alice" }), false);
});

// --- listClaims ---

test("listClaims returns all non-expired claims", async () => {
  const { registry } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  await registry.register("bob", KEY_BOB);
  const claims = await registry.listClaims();
  const handles = claims.map((c) => c.handle).sort();
  assert.deepEqual(handles, ["alice", "bob"]);
});

test("listClaims skips expired entries", async () => {
  const { registry, signer, kvStore } = makeRegistry();
  await registry.register("alice", KEY_ALICE);

  const past = Date.now() - 10_000;
  const expired = await makeClaim(signer, {
    handle: "bob",
    keyId: KEY_BOB,
    createdAtMs: past - 1_000,
    expiresAtMs: past,
  });
  await kvStore.set(KV_PREFIX + "bob", expired.toJSON());

  const claims = await registry.listClaims();
  assert.equal(claims.length, 1);
  assert.equal(claims[0].handle, "alice");
});

test("listClaims skips corrupted entries without throwing", async () => {
  const { registry, kvStore } = makeRegistry();
  await registry.register("alice", KEY_ALICE);
  await kvStore.set(KV_PREFIX + "corrupt", { not: "a claim" });

  const claims = await registry.listClaims();
  assert.equal(claims.length, 1);
  assert.equal(claims[0].handle, "alice");
});
