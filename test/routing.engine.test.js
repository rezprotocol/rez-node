import test from "node:test";
import assert from "node:assert/strict";
import { RMailbox, MemoryDataStore, encodeOuterPacket, newRoutingKey, createDefaultRegistry, RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";
import { RouteEnvelopeV1 } from "../src/contracts/records/RouteEnvelopeV1.js";
import { RoutingEngine } from "../src/routing/index.js";
import { RouteTable } from "../src/routing/RouteTable.js";
import { RelayStore } from "../src/network/RelayStore.js";

function makeValidOuterBytes(body) {
  return encodeOuterPacket({
    bodyBytes: body || new Uint8Array([1, 2, 3, 4]),
  });
}

test("RoutingEngine local delivery deposits into inbox store", async () => {
  const inboxStore = new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() });
  const engine = new RoutingEngine({
    nodeId: "rez:node:test-local",
    localInboxId: "inbox:local:test:123456",
    inboxStore,
    relaySources: [],
  });

  const payload = makeValidOuterBytes(new Uint8Array([1, 2, 3, 4]));
  const result = await engine.routePayload({
    targetHandle: "inbox:local:test:123456",
    payloadBytes: payload,
  });
  assert.equal(result.mode, "local");

  const listed = await inboxStore.list("inbox:local:test:123456", { limit: 10 });
  assert.equal(listed.items.length, 1);
  const fetched = await inboxStore.fetch("inbox:local:test:123456", listed.items[0].eventId);
  assert.ok(fetched.bytes instanceof Uint8Array || Array.isArray(fetched.bytes));
});

test("RoutingEngine falls back to gateway when no route exists", async () => {
  let called = 0;
  const gatewayLoop = {
    async sendToInbox(args) {
      called += 1;
      assert.equal(args.deliverInboxId, "inbox:remote:test:123456");
      assert.ok(args.innerBytes instanceof Uint8Array);
    },
  };
  const engine = new RoutingEngine({
    nodeId: "rez:node:test-fallback",
    localInboxId: "inbox:local:test:abc999",
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    gatewayLoop,
  });

  const result = await engine.routePayload({
    targetHandle: "inbox:remote:test:123456",
    payloadBytes: new Uint8Array([9, 8, 7]),
  });
  assert.equal(result.mode, "fallback-gateway");
  assert.equal(called, 1);
});

test("RoutingEngine suppresses duplicate packet forwards", async () => {
  let called = 0;
  const gatewayLoop = {
    async sendToInbox() {
      called += 1;
    },
  };
  const engine = new RoutingEngine({
    nodeId: "rez:node:test-dup",
    localInboxId: "inbox:local:test:dup",
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    gatewayLoop,
  });

  const envelope = new RouteEnvelopeV1({
    packetId: "packet:dup:test:12345678",
    targetHandle: "inbox:remote:test:dup:1234",
    payloadB64: Buffer.from(new Uint8Array([1, 1, 2])).toString("base64"),
    ttl: 5,
    originNodeId: "rez:node:origin",
    hops: ["rez:node:origin"],
  });

  const first = await engine.forwardEnvelope({ envelope });
  const second = await engine.forwardEnvelope({ envelope });
  assert.equal(first.mode, "fallback-gateway");
  assert.equal(second.mode, "duplicate");
  assert.equal(called, 1);
});

// P0.4 regression: this shortcut was permanently dead for months because the
// engine called a nonexistent relayStore.getDescriptorByKeyId behind a typeof
// guard and silently fell back to HTTP peer queries.
test("RoutingEngine resolveNextHop derives next hop from RouteTable + RelayStore descriptor without HTTP", async () => {
  const nowMs = Date.now();
  const routeTable = new RouteTable();
  routeTable.addRemote("inbox:remote:shortcut:1234", {
    hops: 1,
    nextHopRelayKeyId: "relay-shortcut",
    deliveryRelayKeyId: "relay-shortcut",
    nowMs,
  });

  const relayStore = new RelayStore();
  const descriptor = new RelayDescriptorV1({
    relayKeyId: "relay-shortcut",
    endpoints: [{ host: "127.0.0.1", port: 4567 }],
    onionKeys: [
      new OnionKeyRecordV1({
        onionKeyId: "relay-shortcut-onion",
        publicKeyBytes: new Uint8Array(32).fill(3),
        format: "raw",
        createdAt: nowMs - 1000,
        notBefore: nowMs - 1000,
        notAfter: nowMs + 60_000,
        status: "active",
      }),
    ],
    expiresAt: nowMs + 60_000,
    nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  });
  relayStore.upsertDescriptor(descriptor.toJSON(), { source: "config", receivedAtMs: nowMs });

  const engine = new RoutingEngine({
    nodeId: "rez:node:test-shortcut",
    localInboxId: "inbox:local:test:shortcut",
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    routeTable,
    relayStore,
    fetchImpl: async () => {
      throw new Error("HTTP query must not run when the RouteTable shortcut applies");
    },
  });

  const peer = await engine.resolveNextHop({ targetHandle: "inbox:remote:shortcut:1234" });
  assert.ok(peer, "shortcut must resolve a peer");
  assert.equal(peer.nodeId, "relay-shortcut");
  assert.equal(peer.routeBaseUrl, "http://127.0.0.1:4567");
});

test("RoutingEngine resolveNextHop fails loudly when the relay store lacks getDescriptor", async () => {
  const routeTable = new RouteTable();
  routeTable.addRemote("inbox:remote:loud:1234", {
    hops: 1,
    nextHopRelayKeyId: "relay-x",
    deliveryRelayKeyId: "relay-x",
    nowMs: Date.now(),
  });
  const engine = new RoutingEngine({
    nodeId: "rez:node:test-loud",
    localInboxId: "inbox:local:test:loud",
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    routeTable,
    relayStore: {},
  });
  await assert.rejects(
    engine.resolveNextHop({ targetHandle: "inbox:remote:loud:1234" }),
    TypeError,
    "a renamed/missing descriptor accessor must throw, not silently disable the shortcut",
  );
});
