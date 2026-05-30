import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageProvider } from "@rezprotocol/core";

import { HostedInboxRegistry } from "../src/app/HostedInboxRegistry.js";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RouteTable } from "../src/routing/RouteTable.js";
import { RouteAnnouncer } from "../src/routing/RouteAnnouncer.js";

/**
 * When a hosted-inbox claimant is added to `HostedInboxRegistry`, that
 * inbox must also land in `InboxRouter._routeTable` as a direct,
 * registration-anchored route, and the configured route announcer must
 * be invoked. Without this wiring the DHT announcer never STOREs the
 * route on k-closest peers, so cross-mesh `FIND_VALUE` lookups for this
 * inbox return nothing — which is what the live DO-relay run on
 * 2026-05-15 surfaced.
 *
 * The contract under test is exactly the helper bootstrapRelay wires
 * onto `HostedInboxRegistry.setOnChange` and the matching boot-time
 * call in startRezNode. We build the real components here rather than
 * mocking, because the bug is in the wiring, not in any single class.
 */

class CapturingRouteAnnouncer extends RouteAnnouncer {
  constructor() {
    super();
    this.calls = [];
  }
  announceRoutes(inboxIds, hops, ctx) {
    this.calls.push({ method: "announceRoutes", inboxIds: [...inboxIds], hops, hasCtx: !!ctx });
  }
  announceRoutesExcept(_excludeSocket, _entries, _ctx) {}
  announceAllToPeer(_peerSocket, _ctx) {}
  announceWithdraw(_inboxIds, _ctx) {}
  reannounceAll(_ctx) {}
}

function makeFixture() {
  const storageProvider = new MemoryStorageProvider();
  const registry = new HostedInboxRegistry({ storageProvider });
  const routeTable = new RouteTable();
  const routeAnnouncer = new CapturingRouteAnnouncer();
  const router = new InboxRouter({
    selfRelayKeyId: "self-relay",
    routeTable,
    routeAnnouncer,
  });
  const syncHostedInboxesToRouter = () => {
    const registrations = registry.getRegistrations();
    for (const reg of registrations) {
      router.registerLocal([reg.inboxId], null, {
        announce: true,
        registrations: [reg],
      });
    }
  };
  registry.setOnChange(syncHostedInboxesToRouter);
  return { storageProvider, registry, routeTable, routeAnnouncer, router, syncHostedInboxesToRouter };
}

function makeRegistration(inboxId, overrides = {}) {
  const now = Date.now();
  return {
    inboxId,
    nodeKeyId: "node-key-1",
    nodePublicKeyB64: "node-pub-1",
    relayKeyId: "relay-1",
    issuedAtMs: now,
    expiresAtMs: now + 60_000,
    delegationSigB64: "sig-1",
    ...overrides,
  };
}

test("hosted-inbox add → InboxRouter has direct route with registration + announcer invoked", async () => {
  const { registry, routeTable, routeAnnouncer } = makeFixture();
  const inboxId = "inbox:hosted:alice";

  await registry.add("claimant-alice", makeRegistration(inboxId));

  const route = routeTable.get(inboxId);
  assert.ok(route, "route should be present after hosted-inbox add");
  assert.equal(route.direct, true, "route must be direct (this node is authoritative)");
  assert.equal(route.hops, 0);
  assert.ok(route.registration, "route must carry the signed registration for HIGH-8 validation");
  assert.equal(route.registration.inboxId, inboxId);
  assert.equal(route.registration.delegationSigB64, "sig-1");

  assert.equal(routeAnnouncer.calls.length, 1, "announcer should be invoked exactly once");
  assert.deepEqual(routeAnnouncer.calls[0].inboxIds, [inboxId]);
});

test("hosted-inbox boot-time rehydrate → sync places route in router without setOnChange firing", async () => {
  // First instance: persist a claimant delegation, but DO NOT run the
  // setOnChange wiring through it — simulate a fresh-process boot.
  const storageProvider = new MemoryStorageProvider();
  const registry0 = new HostedInboxRegistry({ storageProvider });
  await registry0.hydrate();
  await registry0.add("claimant-bob", makeRegistration("inbox:hosted:bob"));

  // Second instance: simulate a node restart. hydrate() repopulates the
  // map but does NOT fire setOnChange. The boot-time syncHostedInboxesToRouter
  // call is what has to bridge this gap.
  const registry = new HostedInboxRegistry({ storageProvider });
  await registry.hydrate();
  const routeTable = new RouteTable();
  const routeAnnouncer = new CapturingRouteAnnouncer();
  const router = new InboxRouter({
    selfRelayKeyId: "self-relay",
    routeTable,
    routeAnnouncer,
  });
  const sync = () => {
    for (const reg of registry.getRegistrations()) {
      router.registerLocal([reg.inboxId], null, { announce: true, registrations: [reg] });
    }
  };

  // Before sync: route table is empty.
  assert.equal(routeTable.get("inbox:hosted:bob"), null);

  // After sync: route is present.
  sync();
  const route = routeTable.get("inbox:hosted:bob");
  assert.ok(route, "rehydrated hosted inbox must reach the router via boot-time sync");
  assert.equal(route.direct, true);
  assert.ok(route.registration);
});

test("hosted-inbox add is idempotent — re-adding same registration does not duplicate routes", async () => {
  const { registry, routeTable, routeAnnouncer } = makeFixture();
  const inboxId = "inbox:hosted:idempotent";
  const reg = makeRegistration(inboxId);

  await registry.add("claimant-idem", reg);
  // Second add with the same record is a no-op inside HostedInboxRegistry,
  // so setOnChange should not fire again.
  await registry.add("claimant-idem", reg);

  assert.equal(routeAnnouncer.calls.length, 1, "announcer should only fire on the first add");
  const route = routeTable.get(inboxId);
  assert.ok(route);
  assert.equal(route.direct, true);
});

test("hosted-inbox add with a different registration for the same claimant re-syncs", async () => {
  const { registry, routeTable, routeAnnouncer } = makeFixture();
  const inboxId = "inbox:hosted:rotated";

  await registry.add("claimant-r", makeRegistration(inboxId, { delegationSigB64: "sig-original" }));
  await registry.add("claimant-r", makeRegistration(inboxId, { delegationSigB64: "sig-rotated" }));

  const route = routeTable.get(inboxId);
  assert.ok(route);
  assert.equal(route.registration.delegationSigB64, "sig-rotated",
    "route's registration must reflect the most recent claimant delegation");
  assert.equal(routeAnnouncer.calls.length, 2, "announcer fires on each material change");
});
