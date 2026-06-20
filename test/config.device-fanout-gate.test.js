import test from "node:test";
import assert from "node:assert/strict";

import { validateConfig } from "../src/app/NodeConfigValidator.js";

// S2.5 Slice 4 leaf B: the E6 fan-out gate. node.device.multiDeviceFanout
// controls the per-inbox durable device cap — default false ⇒ 1 (single active
// device, shipped behaviour); true ⇒ DEVICE_FANOUT_MAX. No DB needed.
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

test("multiDeviceFanout=true lifts the cap to DEVICE_FANOUT_MAX (8)", () => {
  const resolved = validateConfig(baseNode({ multiDeviceFanout: true }));
  assert.equal(resolved.device.multiDeviceFanout, true);
  assert.equal(resolved.device.maxDevices, 8);
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
