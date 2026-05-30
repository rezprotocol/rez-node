import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";

import { HostedInboxRegistry } from "../src/app/HostedInboxRegistry.js";

test("HostedInboxRegistry persists hosted inbox mappings across instances", async () => {
  const storageProvider = new MemoryStorageProvider();
  const claimantPublicKeyB64 = "claimant-pub-key";
  const inboxId = "inbox:owner";
  const registration = {
    inboxId,
    nodeKeyId: "node-key",
    nodePublicKeyB64: "node-pub",
    relayKeyId: "relay-key",
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    delegationSigB64: "sig",
  };

  const registry1 = new HostedInboxRegistry({ storageProvider });
  await registry1.hydrate();
  await registry1.add(claimantPublicKeyB64, registration);

  const registry2 = new HostedInboxRegistry({ storageProvider });
  await registry2.hydrate();

  assert.deepEqual(
    Array.from(registry2.getOwnerPublicKeysForInbox(inboxId)),
    [claimantPublicKeyB64],
  );
  assert.deepEqual(registry2.getInboxIds(), [inboxId]);
  assert.deepEqual(registry2.getRegistrations(), [{
    claimantPublicKeyB64,
    inboxId,
    nodeKeyId: "node-key",
    nodePublicKeyB64: "node-pub",
    relayKeyId: "relay-key",
    issuedAtMs: registration.issuedAtMs,
    expiresAtMs: registration.expiresAtMs,
    delegationSigB64: "sig",
  }]);
});

test("HostedInboxRegistry.remove unregisters a claimant", async () => {
  const storageProvider = new MemoryStorageProvider();
  const registry = new HostedInboxRegistry({ storageProvider });
  await registry.hydrate();
  await registry.add("pubkey-A", {
    inboxId: "inbox:A",
    nodeKeyId: "node",
    nodePublicKeyB64: "pub",
    relayKeyId: "relay",
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    delegationSigB64: "sig",
  });
  assert.equal(registry.getInboxIds().length, 1);
  await registry.remove("pubkey-A");
  assert.equal(registry.getInboxIds().length, 0);
  assert.equal(registry.getOwnerPublicKeysForInbox("inbox:A").size, 0);
});

test("HostedInboxRegistry filters expired registrations from getRegistrations", async () => {
  const storageProvider = new MemoryStorageProvider();
  const registry = new HostedInboxRegistry({ storageProvider });
  await registry.hydrate();
  await registry.add("pubkey-A", {
    inboxId: "inbox:A",
    nodeKeyId: "node",
    nodePublicKeyB64: "pub",
    relayKeyId: "relay",
    issuedAtMs: Date.now() - 100,
    expiresAtMs: Date.now() - 1, // already expired
    delegationSigB64: "sig",
  });
  assert.deepEqual(registry.getRegistrations(), []);
});
