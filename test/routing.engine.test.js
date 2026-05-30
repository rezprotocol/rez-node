import test from "node:test";
import assert from "node:assert/strict";
import { RMailbox, MemoryDataStore, encodeOuterPacket, newRoutingKey, createDefaultRegistry } from "@rezprotocol/core";
import { RouteEnvelopeV1 } from "../src/contracts/records/RouteEnvelopeV1.js";
import { RoutingEngine } from "../src/routing/index.js";

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
