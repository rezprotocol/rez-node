import test from "node:test";
import assert from "node:assert/strict";

import { validateConfig } from "../src/app/NodeConfigValidator.js";

// S2.5 Slice 4 leaf B + review P2 + S12 flip + audit-R4 interlock: the E6 fan-out
// gate. node.device.multiDeviceFanout is the operator's INTENT; code-level readiness
// constants are the interlock. S12 flipped the multi-device-suite flag true, but the
// audit-R4 review made legacy-cursor migration (F2) and durable admission control
// (F3) RELEASE BLOCKERS that must land first — each with its own readiness constant,
// still false. So requesting fan-out today FAILS LOUD (no silent downgrade). The flag
// still DEFAULTS closed, so a node that does not set it (desktop/fs and every
// unconfigured pg node) stays single-device, byte-identical. No DB needed.
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

test("audit-R4 interlock: multiDeviceFanout=true FAILS LOUD while F2/F3 are unbuilt (no silent open)", () => {
  // The multi-device suite (S12) is green, but the legacy-cursor migration (F2) and
  // durable admission control (F3) release blockers are not built, so their readiness
  // constants are false. Requesting fan-out must throw and NAME the unmet blockers —
  // it must never silently downgrade to single-device and hide the misconfiguration.
  assert.throws(
    () => validateConfig(baseNode({ multiDeviceFanout: true })),
    (err) => /release blockers/.test(err.message)
      && /audit R4 F2/.test(err.message)
      && /audit R4 F3/.test(err.message),
  );
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
