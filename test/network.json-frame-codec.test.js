import test from "node:test";
import assert from "node:assert/strict";
import { createJsonFrameCodec } from "../src/network/ws/JsonFrameCodec.js";

// SDK-1's twin on the server side (rez-core/docs/SECURITY_FINDINGS_CONSOLIDATED.md).
// This codec runs BEFORE session authentication, so anyone who can open a socket
// reaches it — it is the more exposed of the two near-identical files, and fixing
// only the SDK one would have left the worse half open.

test("a frame body carrying a prototype-poisoning key is refused at the boundary", () => {
  const codec = createJsonFrameCodec();
  for (const key of ["__proto__", "constructor", "prototype"]) {
    const raw = `{"id":"1","t":"session.hello","v":1,"body":{${JSON.stringify(key)}:{"isAdmin":true}}}`;
    assert.throws(() => codec.decodeFrame(raw), (err) => {
      assert.equal(err.code, "UNSAFE_FRAME", key);
      assert.equal(err.retryable, false);
      assert.equal(err.unsafeKey, key, "the key is carried for the operator log");
      return true;
    }, key);
  }
});

test("the poison key is caught wherever it sits, not only at the body root", () => {
  const codec = createJsonFrameCodec();
  const nested = String.raw`{"id":"1","t":"x","v":1,"body":{"caps":[{"__proto__":{"admin":1}}]}}`;
  assert.throws(() => codec.decodeFrame(nested), (err) => err.code === "UNSAFE_FRAME");
  // ...including outside `body`, since the whole frame is parsed as one value.
  const topLevel = String.raw`{"id":"1","t":"x","v":1,"constructor":{"x":1},"body":{}}`;
  assert.throws(() => codec.decodeFrame(topLevel), (err) => err.code === "UNSAFE_FRAME");
});

test("hostile frames stay distinguishable from merely malformed ones", () => {
  // Flattening both into BAD_FRAME would leave an operator hunting an encoding
  // bug while the node is being probed.
  const codec = createJsonFrameCodec();
  assert.throws(() => codec.decodeFrame("{not json"), (err) => {
    assert.equal(err.code, "BAD_FRAME");
    assert.equal(err.unsafeKey, undefined);
    return true;
  });
  assert.throws(() => codec.decodeFrame("null"), (err) => err.code === "BAD_FRAME");
});

test("the error carries no attacker-chosen text for the operator log", () => {
  // `unsafeKey` is one of three constants. The full path is built from keys the
  // peer chose, and a log line is not the place to interpolate those.
  const codec = createJsonFrameCodec();
  const raw = `{"body":{"a\\nFAKE LOG LINE":{"__proto__":{}}}}`;
  assert.throws(() => codec.decodeFrame(raw), (err) => {
    assert.equal(err.unsafeKey, "__proto__");
    assert.equal(err.path, undefined, "the attacker-controlled path is not forwarded");
    assert.equal(/FAKE LOG LINE/.test(err.message), false);
    return true;
  });
});

test("ordinary frames still decode, and round-trip through the encoder", () => {
  const codec = createJsonFrameCodec();
  assert.deepEqual(
    codec.decodeFrame(String.raw`{"id":"abc","t":"session.hello","v":1,"body":{"deviceId":"d1"}}`),
    { id: "abc", type: "session.hello", version: 1, body: { deviceId: "d1" } },
  );
  assert.deepEqual(
    codec.decodeFrame(codec.encodeFrame({ id: "x", type: "t", body: { a: [1, { b: 2 }] } })),
    { id: "x", type: "t", version: 1, body: { a: [1, { b: 2 }] } },
  );
  // Frames with no id / no body still normalize exactly as before.
  assert.deepEqual(
    codec.decodeFrame(String.raw`{"t":"evt.x","v":1}`),
    { id: null, type: "evt.x", version: 1, body: {} },
  );
});
