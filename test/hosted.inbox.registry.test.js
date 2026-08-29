import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";

import { HostedInboxRegistry } from "../src/app/HostedInboxRegistry.js";
import { makeRelayIdentity } from "./support/relayIdentity.js";

test("HostedInboxRegistry persists hosted inbox mappings across instances", async () => {
  const storageProvider = new MemoryStorageProvider();
  const claimantPublicKeyB64 = "claimant-pub-key";
  const inboxId = "inbox:owner";
  const relay = makeRelayIdentity();
  const registration = {
    inboxId,
    nodeKeyId: relay.nodeKeyId,
    nodePublicKeyB64: relay.nodePublicKeyB64,
    relayKeyId: relay.relayKeyId,
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
    nodeKeyId: relay.nodeKeyId,
    nodePublicKeyB64: relay.nodePublicKeyB64,
    relayKeyId: relay.relayKeyId,
    issuedAtMs: registration.issuedAtMs,
    expiresAtMs: registration.expiresAtMs,
    delegationSigB64: "sig",
  }]);
});

test("HostedInboxRegistry.remove unregisters a claimant", async () => {
  const storageProvider = new MemoryStorageProvider();
  const registry = new HostedInboxRegistry({ storageProvider });
  await registry.hydrate();
  const relay = makeRelayIdentity();
  await registry.add("pubkey-A", {
    inboxId: "inbox:A",
    nodeKeyId: relay.nodeKeyId,
    nodePublicKeyB64: relay.nodePublicKeyB64,
    relayKeyId: relay.relayKeyId,
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
  const relay = makeRelayIdentity();
  await registry.add("pubkey-A", {
    inboxId: "inbox:A",
    nodeKeyId: relay.nodeKeyId,
    nodePublicKeyB64: relay.nodePublicKeyB64,
    relayKeyId: relay.relayKeyId,
    issuedAtMs: Date.now() - 100,
    expiresAtMs: Date.now() - 1, // already expired
    delegationSigB64: "sig",
  });
  assert.deepEqual(registry.getRegistrations(), []);
});

test("P1.3d fix: getRegistrations preserves the lease pair (generation + retentionClass) — the fields are INSIDE the claimant's signed bytes", async () => {
  const storageProvider = new MemoryStorageProvider();
  const relay = makeRelayIdentity();
  const registration = {
    inboxId: "inbox:leased",
    nodeKeyId: relay.nodeKeyId,
    nodePublicKeyB64: relay.nodePublicKeyB64,
    relayKeyId: relay.relayKeyId,
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    delegationSigB64: "sig",
    generation: 3,
    retentionClass: "standard",
  };
  const registry = new HostedInboxRegistry({ storageProvider });
  await registry.hydrate();
  await registry.add("pubkey-leased", registration);

  // The DEFECT this pins: this projection used to strip the pair, so the
  // announced registration failed signature reconstruction at every
  // receiving relay (the payload was rebuilt without the signed fields) and
  // fresh v2 claims were unroutable across nodes ("no route to target").
  const projected = registry.getRegistrations();
  assert.equal(projected.length, 1);
  assert.equal(projected[0].generation, 3, "generation survives the announce projection");
  assert.equal(projected[0].retentionClass, "standard", "retentionClass survives the announce projection");

  // Round-trips persistence too (a restarted node must announce the same
  // signed shape).
  const reborn = new HostedInboxRegistry({ storageProvider });
  await reborn.hydrate();
  const reprojected = reborn.getRegistrations();
  assert.equal(reprojected[0].generation, 3);
  assert.equal(reprojected[0].retentionClass, "standard");

  // A LEGACY registration (no lease pair) keeps the legacy projection —
  // no fabricated fields.
  await reborn.add("pubkey-legacy", {
    inboxId: "inbox:legacy",
    nodeKeyId: relay.nodeKeyId,
    nodePublicKeyB64: relay.nodePublicKeyB64,
    relayKeyId: relay.relayKeyId,
    issuedAtMs: Date.now(),
    expiresAtMs: Date.now() + 60_000,
    delegationSigB64: "sig",
  });
  const legacy = reborn.getRegistrations().find((r) => r.inboxId === "inbox:legacy");
  assert.equal("generation" in legacy, false, "legacy claims announce the legacy shape");
  assert.equal("retentionClass" in legacy, false);
});
