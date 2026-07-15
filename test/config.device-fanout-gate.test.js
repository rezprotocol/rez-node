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
// constants are the interlock. Most audit-R4 blockers have shipped (S12 suite, F2
// legacy-cursor fail-close incl. the mailbox.fetch surface, F3 admission control, L4
// revoke consolidation, L5 fresh-revocation dispatch guard), BUT the No-Go audit reverted
// the delegated-revocation COMPLETENESS blocker (round-3 finding 2): the device-link
// ceremony releases a leaf cert before the home binds its certId (registration-before-
// release not implemented). So MULTI_DEVICE_FANOUT_READY is FALSE and requesting fan-out
// FAILS LOUD naming ONLY completeness — never a silent downgrade. The flag still DEFAULTS
// closed, so an unconfigured node stays single-device, byte-identical. No DB needed.
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

test("audit-R4 interlock: multiDeviceFanout=true FAILS LOUD while completeness is unbuilt (no silent open)", () => {
  // The No-Go audit reverted the delegated-revocation completeness blocker (round-3 finding 2);
  // F2/F3/L4/L5 have shipped. Requesting fan-out must throw and NAME ONLY the remaining unmet
  // blocker (completeness) — never a silent downgrade, and never re-naming the shipped ones.
  assert.throws(
    () => validateConfig(baseNode({ multiDeviceFanout: true })),
    (err) => /release blockers/.test(err.message)
      && /round-3 finding 2/.test(err.message)
      && !/audit R4 F2/.test(err.message)
      && !/audit R4 F3/.test(err.message)
      && !/audit R4 L4/.test(err.message)
      && !/audit R4 L5/.test(err.message),
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

// audit R4 L2c review P1: the interlock must not be bypassable through the runtime
// factories the package exports directly (an embedding app that skips validateConfig).
// deviceFanoutReadiness.js is the ONE SSOT every construction path consults.
test("readiness SSOT: fan-out is NOT ready and the assert fails loud on a true request", () => {
  assert.equal(MULTI_DEVICE_FANOUT_READY, false, "completeness reverted ⇒ not ready");
  assert.equal(assertMultiDeviceFanoutReady(false), false, "a false request is a no-op");
  assert.throws(
    () => assertMultiDeviceFanoutReady(true),
    // Only completeness (round-3 finding 2) remains named; F2/F3/L4/L5 shipped.
    (err) => /release blockers/.test(err.message)
      && /round-3 finding 2/.test(err.message)
      && !/audit R4 F2/.test(err.message)
      && !/audit R4 F3/.test(err.message)
      && !/audit R4 L4/.test(err.message)
      && !/audit R4 L5/.test(err.message),
  );
});

test("createRelayRuntime FAILS LOUD on multiDeviceFanout:true (public factory bypass closed)", () => {
  assert.throws(
    () => createRelayRuntime({ multiDeviceFanout: true }),
    (err) => /release blockers/.test(err.message),
  );
});

test("createRelayRuntime with the default (fan-out off) builds a gate-closed runtime", () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: false });
  assert.equal(runtime.multiDeviceFanout, false);
});

// audit R4 L2c review round-5 P1: the runtime object is mutable and GatewaySession
// accepts an arbitrary runtime, so the construction-time gate can be bypassed by
// MUTATING runtime.multiDeviceFanout after the fact. The FINAL consumption boundary
// (buildAuthenticatedSession, which builds the advertised SessionCapabilities) must
// re-assert readiness — else a tampered runtime advertises fan-out to rez-chat.
test("consumption boundary: a runtime MUTATED to multiDeviceFanout=true after construction FAILS LOUD at session build", async () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: false });
  assert.equal(runtime.multiDeviceFanout, false, "the factory built it gate-closed");
  runtime.multiDeviceFanout = true; // post-construction tamper
  await assert.rejects(
    () => buildAuthenticatedSession({ runtime, deviceId: "rez:dev:tamper" }),
    (err) => /release blockers/.test(err.message),
  );
});

// P3: pin the PUBLIC package-root exports, not just the internal module paths.
test("package-root createRelayRuntime FAILS LOUD on multiDeviceFanout:true", () => {
  assert.throws(
    () => rootCreateRelayRuntime({ multiDeviceFanout: true }),
    (err) => /release blockers/.test(err.message),
  );
});

test("package-root bootstrapRelayInfrastructure FAILS LOUD on a hand-built resolved.device.maxDevices>1", async () => {
  await assert.rejects(
    () => rootBootstrapRelayInfrastructure({ resolved: { device: { maxDevices: 8 } } }),
    (err) => /release blockers/.test(err.message),
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
  // A runtime whose (mutated) multiDeviceFanout makes buildAuthenticatedSession throw
  // at the readiness interlock — the exact round-5 mutation scenario, now reaching
  // the live GatewaySession lifecycle rather than buildAuthenticatedSession directly.
  const session = new GatewaySession({ runtime: { multiDeviceFanout: true }, ws });
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
