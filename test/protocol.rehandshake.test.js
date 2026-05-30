import test from "node:test";
import assert from "node:assert/strict";
import { E2eeRehandshakeRequestV1 } from "@rezprotocol/core";

test("E2eeRehandshakeRequestV1 round-trip toJSON/fromJSON", () => {
  const req = new E2eeRehandshakeRequestV1({
    requestId: "rh-001",
    senderAccountId: "rez:acct:alice",
    senderInboxId: "inbox:alice",
    bundleJson: {
      receiverId: "rez:acct:alice",
      signedPreKeyPublicB64: "AAAA",
      identitySigningPublicKeyB64: "BBBB",
    },
  });
  assert.equal(req.e2ee, 1);
  assert.equal(req.type, "x3dh.rehandshake.v1");
  assert.equal(req.requestId, "rh-001");
  assert.equal(req.senderAccountId, "rez:acct:alice");
  assert.equal(req.senderInboxId, "inbox:alice");
  assert.equal(req.bundleJson.receiverId, "rez:acct:alice");

  const json = req.toJSON();
  assert.equal(json.e2ee, 1);
  assert.equal(json.type, "x3dh.rehandshake.v1");
  assert.equal(json.rehandshake.requestId, "rh-001");

  const restored = E2eeRehandshakeRequestV1.fromJSON(json);
  assert.equal(restored.requestId, "rh-001");
  assert.equal(restored.senderAccountId, "rez:acct:alice");
  assert.equal(restored.bundleJson.signedPreKeyPublicB64, "AAAA");
});

test("E2eeRehandshakeRequestV1 round-trip toBytes/fromBytes", () => {
  const req = new E2eeRehandshakeRequestV1({
    requestId: "rh-002",
    senderAccountId: "rez:acct:bob",
    senderInboxId: "inbox:bob",
    bundleJson: {
      receiverId: "rez:acct:bob",
      signedPreKeyPublicB64: "CCCC",
    },
  });
  const bytes = req.toBytes();
  assert.ok(bytes instanceof Uint8Array);
  assert.ok(bytes.length > 0);

  const restored = E2eeRehandshakeRequestV1.fromBytes(bytes);
  assert.equal(restored.requestId, "rh-002");
  assert.equal(restored.senderAccountId, "rez:acct:bob");
  assert.equal(restored.senderInboxId, "inbox:bob");
});

test("E2eeRehandshakeRequestV1 rejects invalid input", () => {
  assert.throws(() => new E2eeRehandshakeRequestV1({}), /requestId/);
  assert.throws(() => new E2eeRehandshakeRequestV1({ requestId: "a" }), /senderAccountId/);
  assert.throws(
    () => new E2eeRehandshakeRequestV1({ requestId: "a", senderAccountId: "b" }),
    /senderInboxId/,
  );
  assert.throws(
    () => new E2eeRehandshakeRequestV1({ requestId: "a", senderAccountId: "b", senderInboxId: "c" }),
    /bundleJson/,
  );
  assert.throws(
    () => new E2eeRehandshakeRequestV1({
      requestId: "a",
      senderAccountId: "b",
      senderInboxId: "c",
      bundleJson: { signedPreKeyPublicB64: "x" },
    }),
    /receiverId/,
  );
});

test("E2eeRehandshakeRequestV1 wire format is detectable as re-handshake", () => {
  const req = new E2eeRehandshakeRequestV1({
    requestId: "rh-003",
    senderAccountId: "rez:acct:charlie",
    senderInboxId: "inbox:charlie",
    bundleJson: {
      receiverId: "rez:acct:charlie",
      signedPreKeyPublicB64: "DDDD",
    },
  });
  const bytes = req.toBytes();
  const parsed = JSON.parse(new TextDecoder().decode(bytes));

  // ServerPeerLinkProtocolService dispatches on these fields after E2EE decrypt.
  assert.equal(parsed.e2ee, 1);
  assert.equal(parsed.type, "x3dh.rehandshake.v1");
  assert.ok(parsed.rehandshake && typeof parsed.rehandshake === "object");
  assert.equal(parsed.rehandshake.requestId, "rh-003");
  assert.equal(parsed.rehandshake.senderAccountId, "rez:acct:charlie");
  assert.ok(parsed.rehandshake.bundleJson && typeof parsed.rehandshake.bundleJson === "object");
});

test("fromJSON handles both wrapped and unwrapped forms", () => {
  // Wrapped (standard wire format)
  const wrapped = E2eeRehandshakeRequestV1.fromJSON({
    e2ee: 1,
    type: "x3dh.rehandshake.v1",
    rehandshake: {
      requestId: "rh-w",
      senderAccountId: "rez:acct:a",
      senderInboxId: "inbox:a",
      bundleJson: { receiverId: "rez:acct:a", signedPreKeyPublicB64: "X" },
    },
  });
  assert.equal(wrapped.requestId, "rh-w");

  // Unwrapped (direct fields)
  const unwrapped = E2eeRehandshakeRequestV1.fromJSON({
    requestId: "rh-u",
    senderAccountId: "rez:acct:b",
    senderInboxId: "inbox:b",
    bundleJson: { receiverId: "rez:acct:b", signedPreKeyPublicB64: "Y" },
  });
  assert.equal(unwrapped.requestId, "rh-u");
});
