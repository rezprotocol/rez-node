import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { SecureChannelManager } from "@rezprotocol/core";
import { SecureChannelManager as CompatibilityManager } from "../../rez-sdk/src/e2ee/SecureChannelManager.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { LivenessBus } from "../src/relay/LivenessBus.js";
import { verifyClaimantNodeDelegation } from "../src/relay/InboxRouter.js";
import { GossipRouteResolver } from "../src/routing/GossipRouteResolver.js";

test("secure-channel diagnostics omit peer identity and the SDK uses the canonical implementation", (t) => {
  const logged = [];
  t.mock.method(console, "info", (...args) => logged.push(args));
  const manager = new SecureChannelManager({ crypto: new NodeCryptoProvider() });
  assert.equal(manager.hasSession("rez:acct:private-sentinel"), false);
  assert.ok(logged.length > 0);
  assert.doesNotMatch(JSON.stringify(logged), /private-sentinel|No session for peerId/);
  assert.equal(CompatibilityManager, SecureChannelManager);
});

test("gateway errors never log raw request identifiers or backend messages", (t) => {
  const logged = [];
  t.mock.method(console, "error", (...args) => logged.push(args));
  let sent;
  GatewaySession.prototype._sendErrorRecord.call({ runtime: {}, _safeSendRecord(record) { sent = record; }, _eventId() { return "error-id"; } },
    { id: "private-request-id", code: "INTERNAL", message: "private database path and account" });
  assert.doesNotMatch(JSON.stringify(logged), /private/);
  assert.equal(sent.message, "Internal error");
});

test("liveness reconnect resynchronizes all registered inboxes and failures omit identifiers", async (t) => {
  const logged = [];
  t.mock.method(console, "error", (...args) => logged.push(args));
  const subscriber = new EventEmitter();
  subscriber.subscribe = async () => {};
  subscriber.unsubscribe = async () => {};
  const bus = new LivenessBus({ publisher: { publish() {} }, subscriber });
  await bus.start();
  const received = [];
  bus.registerInbox("inbox:private-one", (payload) => { received.push(payload.seq); });
  bus.registerInbox("inbox:private-two", () => { throw new Error("private backend details"); });
  bus.resync();
  subscriber.emit("message", "unused", '{"private malformed data');
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(received, [null]);
  assert.ok(logged.length >= 2);
  assert.doesNotMatch(JSON.stringify(logged), /private/);
  await bus.close();
});

test("rejected delegation and failed route lookup omit identifiers even with debug enabled", async (t) => {
  const priorInboxDebug = process.env.REZ_INBOX_DEBUG;
  const priorGatewayDebug = process.env.REZ_GW_DEBUG;
  process.env.REZ_INBOX_DEBUG = "1";
  process.env.REZ_GW_DEBUG = "1";
  t.after(() => {
    if (priorInboxDebug === undefined) delete process.env.REZ_INBOX_DEBUG;
    else process.env.REZ_INBOX_DEBUG = priorInboxDebug;
    if (priorGatewayDebug === undefined) delete process.env.REZ_GW_DEBUG;
    else process.env.REZ_GW_DEBUG = priorGatewayDebug;
  });
  const logged = [];
  t.mock.method(console, "warn", (...args) => logged.push(args));
  t.mock.method(console, "log", (...args) => logged.push(args));
  assert.equal(verifyClaimantNodeDelegation({ inboxId: "inbox:private-sentinel" }), null);
  assert.equal(await new GossipRouteResolver().resolve("inbox:private-sentinel", {
    routeTable: null,
    relayConnectionPool: { async queryRoute() { throw new Error("private backend account details"); } },
  }), null);
  assert.ok(logged.length >= 3);
  assert.doesNotMatch(JSON.stringify(logged), /private/);
});
