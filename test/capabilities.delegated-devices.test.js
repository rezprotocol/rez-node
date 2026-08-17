import test from "node:test";
import assert from "node:assert/strict";
import { buildAuthenticatedSession } from "../src/protocol/sessionBootstrap.js";

/**
 * The `delegatedDevices` capability answers ONE question: can this home carry a
 * second device at all?
 *
 * It exists because rez-chat offered "Link a new device" unconditionally, so on
 * the default fs/desktop home a user could start a ceremony that could never
 * finish (rez-chat#3, rez-node#2). An fs node has no account-mutation
 * serializer to commit device.add and no authority resolver to later admit the
 * delegated session — GatewaySession fails delegated admission CLOSED without
 * the latter, deliberately.
 *
 * It is derived from the WIRING rather than a config flag: a flag can advertise
 * a capability the node does not actually have, which is the exact failure mode
 * being closed here.
 */

function runtimeWith({ serializer = false, revocationCache = false } = {}) {
  const runtime = {
    getIdentity: () => ({ nodeKeyId: "node", nodePublicKeyB64: "pub", relayKeyId: "relay" }),
  };
  if (serializer) runtime.accountMutationSerializer = { submitMutation: async () => ({}) };
  if (revocationCache) runtime.accountAuthorityRevocationCache = { resolveDelegatedSnapshot: async () => ({}) };
  return runtime;
}

async function advertised(runtime) {
  const result = await buildAuthenticatedSession({ runtime, deviceId: "rez:dev:cap" });
  return result.readyEvent.capabilities.delegatedDevices;
}

test("a pg-shaped home (serializer + resolver) advertises delegatedDevices", async () => {
  assert.equal(await advertised(runtimeWith({ serializer: true, revocationCache: true })), true);
});

test("an fs/desktop home advertises false", async () => {
  // The shipped default: neither collaborator is constructed.
  assert.equal(await advertised(runtimeWith()), false);
});

test("the serializer ALONE is not enough", async () => {
  // The dangerous middle state. With device.add committable but no authority
  // resolver, the ceremony would succeed and the new device would then fail
  // every session.authenticate — a linked device that can never connect is
  // worse than a refused link.
  assert.equal(await advertised(runtimeWith({ serializer: true })), false);
});

test("the resolver ALONE is not enough", async () => {
  assert.equal(await advertised(runtimeWith({ revocationCache: true })), false);
});

test("a collaborator present but not FUNCTIONAL does not count", async () => {
  // Presence is not capability: a truthy object missing the method it is relied
  // on for would advertise a capability the node cannot perform. Same lesson as
  // the repo's "presence != contract" fail-open findings.
  const runtime = runtimeWith({ revocationCache: true });
  runtime.accountMutationSerializer = { notSubmitMutation: () => {} };
  assert.equal(await advertised(runtime), false);

  const other = runtimeWith({ serializer: true });
  other.accountAuthorityRevocationCache = { notResolveDelegatedSnapshot: () => {} };
  assert.equal(await advertised(other), false);
});

test("the capability is independent of the multiDeviceFanout gate", async () => {
  // Different questions: delegatedDevices = "can this home hold a second
  // device", multiDeviceFanout = "cursor semantics for a home that already
  // does". A pg home with fan-out off can still link.
  const runtime = runtimeWith({ serializer: true, revocationCache: true });
  const result = await buildAuthenticatedSession({ runtime, deviceId: "rez:dev:cap" });
  assert.equal(result.readyEvent.capabilities.delegatedDevices, true);
  assert.equal(result.readyEvent.capabilities.multiDeviceFanout, false);
});
