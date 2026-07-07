import test from "node:test";
import assert from "node:assert/strict";

import { validateConfig } from "../src/app/NodeConfigValidator.js";

// S2.5 Slice 4 leaf B + review P2 + S12 flip: the E6 fan-out gate.
// node.device.multiDeviceFanout is the operator's INTENT; a code-level
// FANOUT_READY constant was the interlock that kept it closed until the S12
// multi-device suite was green. S12 flipped FANOUT_READY to true, so the operator
// flag now takes effect: multiDeviceFanout=true ⇒ maxDevices=8 and the advertised
// capability opens. The flag still DEFAULTS closed, so a node that does not set it
// (desktop/fs and every unconfigured pg node) stays single-device, byte-identical.
// No DB needed.
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

test("S12: multiDeviceFanout=true OPENS the gate now that FANOUT_READY is true (operator opt-in)", () => {
  // FANOUT_READY flipped at S12 (the multi-device suite is green), so the operator
  // flag now takes effect: the EFFECTIVE gate opens — maxDevices rises to the
  // fan-out cap and the advertised multiDeviceFanout is true.
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
