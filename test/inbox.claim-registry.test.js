import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";

function makeRegistry() {
  const storageProvider = new MemoryStorageProvider();
  return { registry: new InboxClaimRegistry({ storageProvider }), storageProvider };
}

const SAMPLE_PUBKEY_A = "cHVibGljLWtleS1hbGljZQ==";
const SAMPLE_PUBKEY_B = "cHVibGljLWtleS1ib2I=";

test("InboxClaimRegistry requires storageProvider", () => {
  assert.throws(() => new InboxClaimRegistry({}), /storageProvider/);
  assert.throws(() => new InboxClaimRegistry({ storageProvider: {} }), /storageProvider/);
});

test("hydrate is idempotent and required before reads", async () => {
  const { registry } = makeRegistry();
  assert.throws(() => registry.hasInbox("inbox:x"), /hydrate/);
  await registry.hydrate();
  assert.equal(registry.size(), 0);
  await registry.hydrate();
  assert.equal(registry.size(), 0);
});

test("claim persists and is readable", async () => {
  const { registry } = makeRegistry();
  await registry.hydrate();
  const claimedAtMs = 1700000000000;
  const result = await registry.claim({
    inboxId: "inbox:abc",
    claimantPublicKeyB64: SAMPLE_PUBKEY_A,
    claimedAtMs,
  });
  assert.equal(result.inboxId, "inbox:abc");
  assert.equal(result.claimantPublicKeyB64, SAMPLE_PUBKEY_A);
  assert.equal(result.claimedAtMs, claimedAtMs);
  assert.equal(registry.hasInbox("inbox:abc"), true);
  assert.equal(registry.getClaimantPublicKey("inbox:abc"), SAMPLE_PUBKEY_A);
  assert.equal(registry.size(), 1);
});

test("getClaimantPublicKey returns null for unclaimed inbox", async () => {
  const { registry } = makeRegistry();
  await registry.hydrate();
  assert.equal(registry.getClaimantPublicKey("inbox:never-claimed"), null);
  assert.equal(registry.hasInbox("inbox:never-claimed"), false);
});

test("duplicate claim is rejected with INBOX_ALREADY_CLAIMED", async () => {
  const { registry } = makeRegistry();
  await registry.hydrate();
  await registry.claim({
    inboxId: "inbox:abc",
    claimantPublicKeyB64: SAMPLE_PUBKEY_A,
    claimedAtMs: 1700000000000,
  });
  await assert.rejects(
    () => registry.claim({
      inboxId: "inbox:abc",
      claimantPublicKeyB64: SAMPLE_PUBKEY_B,
      claimedAtMs: 1700000001000,
    }),
    (err) => err.code === "INBOX_ALREADY_CLAIMED",
  );
  // Original claim unchanged
  assert.equal(registry.getClaimantPublicKey("inbox:abc"), SAMPLE_PUBKEY_A);
  assert.equal(registry.size(), 1);
});

test("claim validates inputs", async () => {
  const { registry } = makeRegistry();
  await registry.hydrate();
  await assert.rejects(
    () => registry.claim({ inboxId: "", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 1 }),
    /inboxId/,
  );
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox:x", claimantPublicKeyB64: "", claimedAtMs: 1 }),
    /claimantPublicKeyB64/,
  );
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox:x", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 0 }),
    /claimedAtMs/,
  );
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox:x", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: -1 }),
    /claimedAtMs/,
  );
});

test("claim is methods reject calls before hydrate", async () => {
  const { registry } = makeRegistry();
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox:x", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 1 }),
    /hydrate/,
  );
});

test("listInboxIds returns all claimed", async () => {
  const { registry } = makeRegistry();
  await registry.hydrate();
  await registry.claim({ inboxId: "inbox:a", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 1 });
  await registry.claim({ inboxId: "inbox:b", claimantPublicKeyB64: SAMPLE_PUBKEY_B, claimedAtMs: 2 });
  const ids = registry.listInboxIds();
  assert.equal(ids.length, 2);
  assert.ok(ids.includes("inbox:a"));
  assert.ok(ids.includes("inbox:b"));
});

test("claims survive a new registry instance against the same storage", async () => {
  const storageProvider = new MemoryStorageProvider();
  const first = new InboxClaimRegistry({ storageProvider });
  await first.hydrate();
  await first.claim({ inboxId: "inbox:abc", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 1700000000000 });
  await first.claim({ inboxId: "inbox:def", claimantPublicKeyB64: SAMPLE_PUBKEY_B, claimedAtMs: 1700000001000 });

  const second = new InboxClaimRegistry({ storageProvider });
  await second.hydrate();
  assert.equal(second.size(), 2);
  assert.equal(second.getClaimantPublicKey("inbox:abc"), SAMPLE_PUBKEY_A);
  assert.equal(second.getClaimantPublicKey("inbox:def"), SAMPLE_PUBKEY_B);
});

test("registry rejects collision across instances", async () => {
  const storageProvider = new MemoryStorageProvider();
  const first = new InboxClaimRegistry({ storageProvider });
  await first.hydrate();
  await first.claim({ inboxId: "inbox:abc", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 1 });

  const second = new InboxClaimRegistry({ storageProvider });
  await second.hydrate();
  await assert.rejects(
    () => second.claim({ inboxId: "inbox:abc", claimantPublicKeyB64: SAMPLE_PUBKEY_B, claimedAtMs: 2 }),
    (err) => err.code === "INBOX_ALREADY_CLAIMED",
  );
});

test("whitespace-only inboxIds and pubkeys are treated as missing", async () => {
  const { registry } = makeRegistry();
  await registry.hydrate();
  await assert.rejects(
    () => registry.claim({ inboxId: "   ", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 1 }),
    /inboxId/,
  );
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox:x", claimantPublicKeyB64: "   ", claimedAtMs: 1 }),
    /claimantPublicKeyB64/,
  );
});

test("malformed persisted entries fail closed on hydrate", async () => {
  const storageProvider = new MemoryStorageProvider();
  // Directly seed storage with a mix of valid and malformed entries
  const kv = storageProvider.getKeyValueStore(null);
  await kv.set("node:inbox:claims:v1", {
    claims: [
      { inboxId: "inbox:good", claimantPublicKeyB64: SAMPLE_PUBKEY_A, claimedAtMs: 100 },
      { inboxId: "", claimantPublicKeyB64: SAMPLE_PUBKEY_B, claimedAtMs: 100 },
      { inboxId: "inbox:no-pubkey", claimantPublicKeyB64: "", claimedAtMs: 100 },
      { inboxId: "inbox:bad-ts", claimantPublicKeyB64: SAMPLE_PUBKEY_B, claimedAtMs: -1 },
      "not-an-object",
      null,
    ],
  });
  const registry = new InboxClaimRegistry({ storageProvider });
  await assert.rejects(() => registry.hydrate(), /durable claim entry is malformed/);
  assert.throws(() => registry.size(), /hydrate/, "a failed hydrate never exposes a partial trust root");
});

test("a malformed whole registry snapshot cannot become an empty claim namespace", async () => {
  const storageProvider = new MemoryStorageProvider();
  await storageProvider.getKeyValueStore(null).set("node:inbox:claims:v1", "corrupt");
  const registry = new InboxClaimRegistry({ storageProvider });
  await assert.rejects(() => registry.hydrate(), /durable snapshot is malformed/);
  await assert.rejects(
    () => registry.claim({ inboxId: "inbox:victim", claimantPublicKeyB64: SAMPLE_PUBKEY_B, claimedAtMs: 2 }),
    /hydrate/,
  );
});
