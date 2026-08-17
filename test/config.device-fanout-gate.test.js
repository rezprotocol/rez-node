import test from "node:test";
import assert from "node:assert/strict";

import { validateConfig } from "../src/app/NodeConfigValidator.js";
import { createRelayRuntime } from "../src/app/createRelayRuntime.js";
import {
  assertMultiDeviceFanoutReady,
  MULTI_DEVICE_FANOUT_READY,
  FANOUT_SUITE_READY,
  LEGACY_CURSOR_MIGRATION_READY,
  DEVICE_ADMISSION_CONTROL_READY,
  LEGACY_REVOKE_SERIALIZATION_READY,
  DELEGATED_SESSION_FRESH_REVOCATION_READY,
  DELEGATED_REVOCATION_COMPLETE_READY,
} from "../src/app/deviceFanoutReadiness.js";
import { buildAuthenticatedSession } from "../src/protocol/sessionBootstrap.js";
import { GatewaySession } from "../src/protocol/GatewaySession.js";
// Pin the PUBLIC package-root exports too (an embedder imports from here, not the
// internal module paths) — audit R4 L2c review round-5 P3.
import {
  createRelayRuntime as rootCreateRelayRuntime,
} from "../src/index.js";

// S2.5 Slice 4 leaf B + review P2 + S12 flip + audit-R4 interlock: the E6 fan-out gate.
// node.device.multiDeviceFanout is the operator's INTENT; the code-level readiness constants
// are the interlock.
//
// EVERY blocker has now shipped (S12 suite, F2 legacy-cursor fail-close incl. mailbox.fetch,
// F3 admission control, L4 revoke consolidation, L5 fresh-revocation dispatch guard, and —
// as of P1#2 — complete delegated-device revocation, proven end-to-end by
// e2e.pg.registration-before-release and e2e.pg.revoke-propagation). So requesting fan-out now
// OPENS it rather than failing loud.
//
// The interlock itself is UNCHANGED and still wired at every construction boundary: if any
// readiness constant regresses to false, these same paths fail loud again. The operator flag
// still DEFAULTS closed, so an unconfigured node stays single-device, byte-identical. No DB needed.
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

// rez-node#4: fan-out is a PG-HOME capability. A filesystem home wires no
// account-mutation serializer and no authority resolver, so it can never admit a
// second device — asking for fan-out there is a misconfiguration, not a
// preference. Hence the pg base for every "gate open" case below.
const basePgNode = (device) => ({
  node: {
    ws: { host: "127.0.0.1", port: 0, path: "/ws" },
    network: { knownRelays: [] },
    storage: {
      dataDir: "/tmp/x",
      backend: "pg",
      pg: { connectionString: "postgres://rez:rez@127.0.0.1:5432/rez" },
      // 32 bytes, base64 — the explicit cluster key a pg home requires.
      encryptionKeyB64: Buffer.alloc(32, 7).toString("base64"),
    },
    ...(device === undefined ? {} : { device }),
  },
});

test("audit-R4 interlock: multiDeviceFanout=true opens only after every blocker is ready", () => {
  const resolved = validateConfig(basePgNode({ multiDeviceFanout: true }));
  assert.equal(resolved.device.multiDeviceFanout, true);
  assert.equal(resolved.device.maxDevices, 8);
});

test("rez-node#4: multiDeviceFanout=true on an fs home fails loud", () => {
  // The flag alone would still set maxDevices > 1, opening the cursor-readability
  // gate that the single-device path does NOT share byte-for-byte — so the operator
  // would get a changed code path, no fan-out, and no sign either had happened.
  assert.throws(
    () => validateConfig(baseNode({ multiDeviceFanout: true })),
    /requires storage\.backend=pg/,
    "a silent downgrade here is exactly the failure mode the readiness interlock exists to prevent",
  );
});

test("rez-node#4: the fs default is untouched — no flag, no error, still single-device", () => {
  const resolved = validateConfig(baseNode());
  assert.equal(resolved.device.multiDeviceFanout, false);
  assert.equal(resolved.device.maxDevices, 1);
});

test("rez-node#4: explicitly disabling fan-out on fs is fine, not an error", () => {
  const resolved = validateConfig(baseNode({ multiDeviceFanout: false }));
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

// audit R4 L2c review P1: the interlock must not be bypassable through the runtime
// factories the package exports directly (an embedding app that skips validateConfig).
// deviceFanoutReadiness.js is the ONE SSOT every construction path consults.
test("readiness SSOT: fan-out readiness stays the AND of every blocker", () => {
  assert.equal(MULTI_DEVICE_FANOUT_READY, true, "all six release blockers are closed");
  assert.equal(assertMultiDeviceFanoutReady(false), true, "readiness is independent of operator intent");
  assert.equal(assertMultiDeviceFanoutReady(true), true);

  // The structural guard survives the revert: readiness is the conjunction, so restoring the one
  // false constant reopens the gate without further edits — and any FUTURE regression closes it.
  assert.equal(
    MULTI_DEVICE_FANOUT_READY,
    FANOUT_SUITE_READY
      && LEGACY_CURSOR_MIGRATION_READY
      && DEVICE_ADMISSION_CONTROL_READY
      && LEGACY_REVOKE_SERIALIZATION_READY
      && DELEGATED_SESSION_FRESH_REVOCATION_READY
      && DELEGATED_REVOCATION_COMPLETE_READY,
    "readiness must remain the AND of all six blockers",
  );
});

test("createRelayRuntime accepts multiDeviceFanout:true only through the readiness SSOT", () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: true });
  assert.equal(runtime.multiDeviceFanout, true);
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
test("consumption boundary re-asserts the now-open readiness SSOT", async () => {
  const runtime = createRelayRuntime({ multiDeviceFanout: true });
  runtime.getIdentity = () => ({ nodeKeyId: "node", nodePublicKeyB64: "pub", relayKeyId: "relay" });
  const result = await buildAuthenticatedSession({ runtime, deviceId: "rez:dev:ready" });
  assert.equal(result.readyEvent.capabilities.multiDeviceFanout, true);
});

// P3: pin the PUBLIC package-root exports, not just the internal module paths.
test("package-root createRelayRuntime shares the open readiness SSOT", () => {
  const runtime = rootCreateRelayRuntime({ multiDeviceFanout: true });
  assert.equal(runtime.multiDeviceFanout, true);
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
  // A runtime whose session build THROWS. This used to be triggered by the fan-out readiness
  // interlock, which no longer refuses anything now that every blocker has shipped — but the
  // behaviour under test was never about fan-out: it is that a build failure occurring AFTER auth
  // verification leaves no authentication state behind. Triggered here by getIdentity throwing,
  // which reaches the same code path.
  const session = new GatewaySession({
    runtime: {
      getIdentity() { throw new Error("identity unavailable during session build"); },
    },
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
