import test from "node:test";
import assert from "node:assert/strict";
import { E2eeDeliveryAckV1 } from "@rezprotocol/core";

test("E2eeDeliveryAckV1 round-trip toJSON/fromJSON", () => {
  const ack = new E2eeDeliveryAckV1({
    senderAccountId: "rez:acct:alice",
    messageIds: ["msg-1", "msg-2"],
  });
  assert.equal(ack.kind, "rez.delivery.ack");
  assert.equal(ack.senderAccountId, "rez:acct:alice");
  assert.deepEqual(ack.messageIds, ["msg-1", "msg-2"]);

  const json = ack.toJSON();
  assert.equal(Object.hasOwn(json, "threadId"), false);
  const restored = E2eeDeliveryAckV1.fromJSON(json);
  assert.equal(restored.senderAccountId, "rez:acct:alice");
  assert.deepEqual(restored.messageIds, ["msg-1", "msg-2"]);
});

test("E2eeDeliveryAckV1 round-trip toBytes/fromBytes", () => {
  const ack = new E2eeDeliveryAckV1({
    senderAccountId: "rez:acct:bob",
    messageIds: ["pkt-99"],
  });
  const bytes = ack.toBytes();
  assert.ok(bytes instanceof Uint8Array);
  assert.ok(bytes.length > 0);

  const restored = E2eeDeliveryAckV1.fromBytes(bytes);
  assert.equal(restored.senderAccountId, "rez:acct:bob");
  assert.deepEqual(restored.messageIds, ["pkt-99"]);
});

test("E2eeDeliveryAckV1 rejects invalid input", () => {
  assert.throws(() => new E2eeDeliveryAckV1({}), /senderAccountId/);
  assert.throws(() => new E2eeDeliveryAckV1({ senderAccountId: "a" }), /messageIds/);
  assert.throws(
    () => new E2eeDeliveryAckV1({ senderAccountId: "a", messageIds: [] }),
    /messageIds/,
  );
});

test("E2eeDeliveryAckV1 payload is detectable after E2EE decrypt simulation", () => {
  // Simulates the ServerPeerLinkProtocolService flow: after E2EE decrypt, the
  // plaintext bytes are the E2eeDeliveryAckV1 payload. The dispatcher must
  // detect kind="rez.delivery.ack" and handle it as a protocol message.
  const ack = new E2eeDeliveryAckV1({
    senderAccountId: "rez:acct:alice",
    messageIds: ["msg-1"],
  });
  const plaintextBytes = ack.toBytes();
  const text = new TextDecoder().decode(plaintextBytes);
  const parsed = JSON.parse(text);

  assert.equal(parsed.kind, "rez.delivery.ack");
  assert.equal(parsed.senderAccountId, "rez:acct:alice");
  assert.equal(Object.hasOwn(parsed, "threadId"), false);
  assert.deepEqual(parsed.messageIds, ["msg-1"]);
});

