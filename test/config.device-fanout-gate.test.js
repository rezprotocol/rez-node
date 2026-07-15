import test from "node:test";
import assert from "node:assert/strict";

import { validateConfig } from "../src/app/NodeConfigValidator.js";
import { createRelayRuntime } from "../src/app/createRelayRuntime.js";
import { assertMultiDeviceFanoutReady, MULTI_DEVICE_FANOUT_READY } from "../src/app/deviceFanoutReadiness.js";
import { buildAuthenticatedSession } from "../src/protocol/sessionBootstrap.js";
import { GatewaySession } from "../src/protocol/GatewaySession.js";
// Pin the PUBLIC package-root exports too (an embedder imports from here, not the
// internal module paths) — audit R4 L2c review round-5 P3.
import {
  createRelayRuntime as rootCreateRelayRuntime,
  bootstrapRelayInfrastructure as rootBootstrapRelayInfrastructure,
} from "../src/index.js";

// S2.5 Slice 4 leaf B + review P2 + S12 flip + audit-R4 interlock: the E6 fan-out
// gate. node.device.multiDeviceFanout is the operator's INTENT; code-level readiness
// constants are the interlock. Every audit-R4 release blocker has now SHIPPED (S12
// suite, F2 legacy-cursor fail-close, F3 admission control, L4 revoke consolidation,
// L5 fresh-revocation dispatch guard, and the delegated-revocation completeness leaf),
// so MULTI_DEVICE_FANOUT_READY is TRUE and requesting fan-out now OPENS it (resolves
// maxDevices = DEVICE_FANOUT_MAX, advertises the capability). The flag still DEFAULTS
// closed, so a node that does not set it (desktop/fs and every unconfigured pg node)
// stays single-device, byte-identical — the opt-in is the sole remaining runtime
// backstop. No DB needed for these config/factory-level assertions.
const DEVICE_FANOUT_MAX = 8; // NodeConfigValidator's maxDevices when fan-out opens.
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

test("audit-R4 interlock: multiDeviceFanout=true now OPENS fan-out (every release blocker shipped)", () => {
  // F2 (legacy-cursor fail-close) was the last unbuilt blocker; it has now shipped, so every
  // readiness constant is true and requesting fan-out resolves OPEN — no throw, no silent
  // downgrade. maxDevices lifts to DEVICE_FANOUT_MAX and the resolved intent is true.
  const resolved = validateConfig(baseNode({ multiDeviceFanout: true }));
  assert.equal(resolved.device.multiDeviceFanout, true, "fan-out requested + ready ⇒ open");
  assert.equal(resolved.device.maxDevices, DEVICE_FANOUT_MAX);
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

// audit R4 L2c review P1: the interlock must not be bypassable through the runtime
// factories the package exports directly (an embedding app that skips validateConfig).
// deviceFanoutReadiness.js is the ONE SSOT every construction path consults.
test("readiness SSOT: fan-out is READY; the assert reports readiness and never throws now", () => {
  assert.equal(MULTI_DEVICE_FANOUT_READY, true, "every release blocker shipped ⇒ ready");
  // assertMultiDeviceFanoutReady returns SYSTEM readiness (true) and only throws on a true
  // request while NOT ready — which can no longer happen. Each construction path then ANDs
  // this with the caller's own `requested` intent, so a false (default) request stays closed
  // despite readiness being true. Neither call throws.
  assert.equal(assertMultiDeviceFanoutReady(true), true, "a true request passes the interlock (no throw)");
  assert.equal(assertMultiDeviceFanoutReady(false), true, "readiness is reported; the caller AND-gates on intent");
});

test("createRelayRuntime opens fan-out on multiDeviceFanout:true (every blocker shipped)", () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: true });
  assert.equal(runtime.multiDeviceFanout, true, "requested + ready ⇒ the runtime advertises fan-out");
});

test("createRelayRuntime with the default (fan-out off) builds a gate-closed runtime", () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: false });
  assert.equal(runtime.multiDeviceFanout, false);
});

// audit R4 L2c review round-5 P1: the consumption boundary (buildAuthenticatedSession,
// which builds the advertised SessionCapabilities) re-asserts readiness rather than
// trusting the mutable runtime flag. With every blocker shipped that re-assertion now
// PASSES, so a runtime with multiDeviceFanout=true legitimately advertises fan-out — the
// interlock is satisfied, not bypassed. (While a blocker was unbuilt this same call threw;
// that guard is still wired and would fail loud again if any readiness constant regressed.)
test("consumption boundary: buildAuthenticatedSession advertises fan-out for a multiDeviceFanout=true runtime", async () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: true });
  assert.equal(runtime.multiDeviceFanout, true, "the factory opened fan-out");
  const ready = await buildAuthenticatedSession({ runtime, deviceId: "rez:dev:fanout" });
  assert.equal(ready.readyEvent.capabilities.multiDeviceFanout, true, "the advertised capability reflects the open gate");
});

// P3: pin the PUBLIC package-root exports, not just the internal module paths.
test("package-root createRelayRuntime opens fan-out on multiDeviceFanout:true", () => {
  const runtime = rootCreateRelayRuntime({ multiDeviceFanout: true });
  assert.equal(runtime.multiDeviceFanout, true);
});

test("package-root bootstrapRelayInfrastructure no longer trips the interlock on maxDevices>1", async () => {
  // The readiness interlock (assertMultiDeviceFanoutReady) is the FIRST thing bootstrap
  // checks; it now PASSES, so a hand-built resolved with maxDevices>1 gets past the fan-out
  // gate and fails later on an unrelated missing field (this skeletal resolved has no
  // network/storage). The point: the failure is NO LONGER a "release blockers" rejection —
  // the interlock opened without opening a bypass, since the other invariants still run.
  await assert.rejects(
    () => rootBootstrapRelayInfrastructure({ resolved: { device: { maxDevices: 8 } } }),
    (err) => !/release blockers/.test(err.message),
  );
});

// audit R4 L2c review round-6 P2: if the session build throws (e.g. the fan-out
// interlock rejecting a mutated/misconfigured runtime) AFTER auth verification, the
// session must leave NO authentication state behind — the pending challenge cleared,
// sessionAuthority not committed, never authenticated — and close the socket with an
// explicit error, not strand a populated authority on a silent, unrecoverable session.
test("GatewaySession: a session-build failure leaves clean auth state and closes the socket", async () => {
  const closes = [];
  const errors = [];
  const ws = {
    OPEN: 1, readyState: 1, send() {}, on() {}, once() {}, off() {}, removeListener() {},
    close(code, reason) { closes.push({ code, reason }); },
  };
  // Force buildAuthenticatedSession to throw AFTER auth verification (a getIdentity that
  // blows up) so we exercise the same post-auth build-failure cleanup path. (The readiness
  // interlock no longer throws now that every blocker shipped, so an unrelated build fault
  // stands in for it; the cleanup contract under test is identical.)
  const session = new GatewaySession({
    runtime: { getIdentity() { throw new Error("identity backend down"); } },
    ws,
  });
  session._sendErrorRecord = (rec) => errors.push(rec);
  session._safeSendRawRecord = () => {};
  // Auth verification passes (stubbed) so control reaches the session-build step.
  session._verifyDirectSessionAuth = async () => ({ ok: true, mode: "direct", accountIdentityPublicKeyB64: "acct" });
  session._pendingSessionAuth = {
    challengeId: "c1", nonceB64: "AA", nodeKeyId: "nk", nodePublicKeyB64: "np",
    relayKeyId: "rk", accountIdentityPublicKeyB64: "acct", sessionDeviceId: "rez:dev:x",
    wsPath: "/ws", expiresAtMs: Date.now() + 60_000,
  };

  await session._handleSessionAuthenticate("r1", { challengeId: "c1", signatureB64: "AAAA" });

  assert.equal(session.authenticated, false, "never authenticated");
  assert.equal(session.sessionAuthority, null, "authority was NOT committed on a build failure");
  assert.equal(session._pendingSessionAuth, null, "the one-time challenge was cleared (no half-open pending state)");
  assert.equal(errors.length, 1, "an explicit error was sent (not a silent dispatcher INTERNAL)");
  assert.equal(errors[0].code, "INTERNAL");
  assert.equal(closes.length, 1, "the socket was closed, not left open");
  assert.equal(closes[0].code, 1011);
});
