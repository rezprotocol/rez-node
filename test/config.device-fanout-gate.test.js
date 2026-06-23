import test from "node:test";
import assert from "node:assert/strict";

import { validateConfig } from "../src/app/NodeConfigValidator.js";

// S2.5 Slice 4 leaf B + review P2: the E6 fan-out gate. node.device.multiDeviceFanout
// is the operator's INTENT, but a code-level FANOUT_READY constant (false until the
// S12 multi-device suite is green) is the real interlock — the config flag ALONE
// cannot open the gate. Until FANOUT_READY flips in code, the effective maxDevices
// stays 1 (single active device, shipped behaviour) and the advertised capability
// stays false even when multiDeviceFanout=true. No DB needed.
const baseNode = (device) => ({
  node: {
    ws: { host: "127.0.0.1", port: 0, path: "/ws" },
    network: { knownRelays: [] },
    storage: { dataDir: "/tmp/x" },
    ...(device === undefined ? {} : { device }),
  },
});

test("fan-out gate defaults closed: maxDevices=1, multiDeviceFanout=false", () => {
  const resolved = validateConfig(baseNode());
  assert.equal(resolved.device.multiDeviceFanout, false);
  assert.equal(resolved.device.maxDevices, 1);
});

test("an empty device config is still gate-closed", () => {
  const resolved = validateConfig(baseNode({}));
  assert.equal(resolved.device.multiDeviceFanout, false);
  assert.equal(resolved.device.maxDevices, 1);
});

test("multiDeviceFanout=true does NOT open the gate while FANOUT_READY is false (review P2)", () => {
  // The config flag is operator intent, not the interlock. With the code-level
  // FANOUT_READY constant still false (it flips only at S12), an operator config
  // flip leaves the EFFECTIVE gate closed: maxDevices stays 1 and the effective
  // multiDeviceFanout is false, so the advertised capability (derived from
  // maxDevices > 1) never opens. This is what makes E6 un-flippable by config.
  const resolved = validateConfig(baseNode({ multiDeviceFanout: true }));
  assert.equal(resolved.device.multiDeviceFanout, false);
  assert.equal(resolved.device.maxDevices, 1);
});

test("multiDeviceFanout=false is explicitly gate-closed", () => {
  const resolved = validateConfig(baseNode({ multiDeviceFanout: false }));
  assert.equal(resolved.device.maxDevices, 1);
});

test("a non-boolean multiDeviceFanout is rejected (fail loud, no silent coercion)", () => {
  assert.throws(
    () => validateConfig(baseNode({ multiDeviceFanout: "yes" })),
    /config\.node\.device\.multiDeviceFanout/,
  );
});
