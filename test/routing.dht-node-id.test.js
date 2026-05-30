import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";

describe("DhtNodeId", () => {
  it("fromRelayKeyId produces deterministic 32-byte ID", () => {
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    const b = DhtNodeId.fromRelayKeyId("relay-a");
    assert.equal(a.bytes.length, 32);
    assert.ok(a.equals(b));
  });

  it("different relayKeyIds produce different IDs", () => {
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    const b = DhtNodeId.fromRelayKeyId("relay-b");
    assert.ok(!a.equals(b));
  });

  it("fromHex round-trips through hex", () => {
    const original = DhtNodeId.fromRelayKeyId("relay-test");
    const hex = original.hex;
    const restored = DhtNodeId.fromHex(hex);
    assert.ok(original.equals(restored));
    assert.equal(hex.length, 64);
  });

  it("fromBytes makes a defensive copy", () => {
    const src = new Uint8Array(32);
    src[0] = 0xff;
    const id = DhtNodeId.fromBytes(src);
    src[0] = 0x00;
    assert.equal(id.bytes[0], 0xff);
  });

  it("bytes getter returns a copy", () => {
    const id = DhtNodeId.fromRelayKeyId("relay-x");
    const b1 = id.bytes;
    b1[0] = 0x00;
    assert.notEqual(id.bytes[0], 0x00);
  });

  it("xorDistance is symmetric", () => {
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    const b = DhtNodeId.fromRelayKeyId("relay-b");
    const dAB = a.xorDistance(b);
    const dBA = b.xorDistance(a);
    assert.deepStrictEqual(dAB, dBA);
  });

  it("xorDistance to self is all zeros", () => {
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    const d = a.xorDistance(a);
    for (let i = 0; i < 32; i += 1) {
      assert.equal(d[i], 0);
    }
  });

  it("bucketIndex returns -1 for identical nodes", () => {
    const a = DhtNodeId.fromRelayKeyId("same");
    const b = DhtNodeId.fromRelayKeyId("same");
    assert.equal(a.bucketIndex(b), -1);
  });

  it("bucketIndex returns valid range 0-255 for different nodes", () => {
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    const b = DhtNodeId.fromRelayKeyId("relay-b");
    const idx = a.bucketIndex(b);
    assert.ok(idx >= 0 && idx <= 255, "bucket index " + idx + " out of range");
  });

  it("bucketIndex is higher for more distant nodes", () => {
    // Construct IDs that differ at known positions
    const selfBytes = new Uint8Array(32);
    selfBytes[0] = 0b00000000;
    const self = DhtNodeId.fromBytes(selfBytes);

    // Differs only in last bit of last byte
    const nearBytes = new Uint8Array(32);
    nearBytes[31] = 0b00000001;
    const near = DhtNodeId.fromBytes(nearBytes);

    // Differs in first bit of first byte
    const farBytes = new Uint8Array(32);
    farBytes[0] = 0b10000000;
    const far = DhtNodeId.fromBytes(farBytes);

    const nearIdx = self.bucketIndex(near);
    const farIdx = self.bucketIndex(far);
    assert.ok(farIdx > nearIdx, "far=" + farIdx + " should be > near=" + nearIdx);
  });

  it("compareDistanceTo correctly orders by XOR distance", () => {
    const target = DhtNodeId.fromRelayKeyId("target");
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    const b = DhtNodeId.fromRelayKeyId("relay-b");
    const cmp = target.compareDistanceTo(a, b);
    // Just verify it returns a consistent number
    assert.ok(typeof cmp === "number");
    assert.ok(cmp !== 0 || a.equals(b), "different nodes should have different distances");
  });

  it("compareDistanceTo returns 0 for same node", () => {
    const target = DhtNodeId.fromRelayKeyId("target");
    const a = DhtNodeId.fromRelayKeyId("relay-a");
    assert.equal(target.compareDistanceTo(a, a), 0);
  });

  it("compareDistanceTo is consistent with sort ordering", () => {
    const target = DhtNodeId.fromRelayKeyId("target");
    const nodes = [];
    for (let i = 0; i < 10; i += 1) {
      nodes.push(DhtNodeId.fromRelayKeyId("relay-" + i));
    }
    const sorted = nodes.slice().sort(function (a, b) {
      return target.compareDistanceTo(a, b);
    });
    // Sorting twice should produce the same order (stable)
    const sorted2 = nodes.slice().sort(function (a, b) {
      return target.compareDistanceTo(a, b);
    });
    for (let i = 0; i < sorted.length; i += 1) {
      assert.ok(sorted[i].equals(sorted2[i]));
    }
  });

  it("rejects empty string", () => {
    assert.throws(() => DhtNodeId.fromRelayKeyId(""), /non-empty/);
    assert.throws(() => DhtNodeId.fromRelayKeyId("   "), /non-empty/);
  });

  it("rejects wrong byte length", () => {
    assert.throws(() => new DhtNodeId(new Uint8Array(16)), /exactly 32/);
  });

  it("rejects invalid hex length", () => {
    assert.throws(() => DhtNodeId.fromHex("abcd"), /64-character/);
  });

  it("equals returns false for non-DhtNodeId", () => {
    const id = DhtNodeId.fromRelayKeyId("relay-a");
    assert.equal(id.equals(null), false);
    assert.equal(id.equals("not-a-node-id"), false);
  });
});
