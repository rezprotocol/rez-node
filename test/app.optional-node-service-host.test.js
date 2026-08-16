/**
 * P6 — optional subsystem lifecycle and failure isolation
 * (ATLAS_PREREQUISITES). A generic test-double service can start/stop with
 * the node; every failure mode is surfaced in namespaced status and none of
 * them touches core mesh readiness.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { OptionalNodeServiceHost, OPTIONAL_SERVICE_STATES } from "../src/app/OptionalNodeServiceHost.js";
import { MeshCoordinator } from "../src/gateway/MeshCoordinator.js";
import { RelayStore } from "../src/network/RelayStore.js";

function makeService(name, overrides = {}) {
  const calls = [];
  return {
    name,
    calls,
    async start() { calls.push("start"); if (overrides.startError) throw new Error(overrides.startError); },
    async stop() {
      calls.push("stop");
      if (overrides.stopError) throw new Error(overrides.stopError);
      if (overrides.hangOnStop) return new Promise(() => {});
    },
    getStatus() {
      if (overrides.statusThrows) throw new Error("status boom");
      return overrides.status || {};
    },
    ...(overrides.isEnabled !== undefined ? { isEnabled: () => overrides.isEnabled } : {}),
  };
}

test("start/ready/stop lifecycle with reverse-order stop", async () => {
  const a = makeService("svc-a");
  const b = makeService("svc-b");
  const order = [];
  a.stop = async () => order.push("stop-a");
  b.stop = async () => order.push("stop-b");
  const host = new OptionalNodeServiceHost({ services: [a, b] });
  await host.startAll();
  assert.equal(host.getStatuses()["svc-a"].state, "ready");
  assert.equal(host.getStatuses()["svc-b"].state, "ready");
  await host.stopAll();
  assert.deepEqual(order, ["stop-b", "stop-a"], "reverse start order");
  assert.equal(host.getStatuses()["svc-a"].state, "stopped");
});

test("start failure, degraded status, stop hang, and stop error are isolated per service", async () => {
  const failing = makeService("svc-fails", { startError: "cannot bind" });
  const degraded = makeService("svc-degraded", { status: { degraded: true, error: "backlog" } });
  const hangs = makeService("svc-hangs", { hangOnStop: true });
  const stopErr = makeService("svc-stop-error", { stopError: "flush failed" });
  const healthy = makeService("svc-healthy");
  const host = new OptionalNodeServiceHost({
    services: [failing, degraded, hangs, stopErr, healthy],
    stopTimeoutMs: 50,
  });
  await host.startAll();

  const statuses = host.getStatuses();
  assert.equal(statuses["svc-fails"].state, "failed");
  assert.match(statuses["svc-fails"].error, /cannot bind/);
  assert.equal(statuses["svc-degraded"].state, "degraded");
  assert.equal(statuses["svc-healthy"].state, "ready");

  await host.stopAll();
  const after = host.getStatuses();
  assert.equal(after["svc-hangs"].state, "failed");
  assert.match(after["svc-hangs"].error, /timed out/);
  assert.equal(after["svc-stop-error"].state, "failed");
  assert.match(after["svc-stop-error"].error, /flush failed/);
  assert.equal(after["svc-healthy"].state, "stopped", "other services stop cleanly around failures");
});

test("disabled services never start; no services configured is a clean no-op", async () => {
  const disabled = makeService("svc-off", { isEnabled: false });
  const host = new OptionalNodeServiceHost({ services: [disabled] });
  await host.startAll();
  assert.equal(host.getStatuses()["svc-off"].state, "disabled");
  assert.deepEqual(disabled.calls, [], "start never called");

  const empty = new OptionalNodeServiceHost();
  await empty.startAll();
  await empty.stopAll();
  assert.deepEqual(empty.getStatuses(), {});
});

test("the host is not a plugin loader: instances only, stable names, no duplicates", () => {
  const host = new OptionalNodeServiceHost();
  assert.throws(() => host.register({ name: "Bad Name!", start() {}, stop() {}, getStatus() {} }), Error);
  assert.throws(() => host.register("module-path-string"), Error);
  host.register(makeService("svc-x"));
  assert.throws(() => host.register(makeService("svc-x")), Error, "duplicate name rejected");
  assert.deepEqual([...OPTIONAL_SERVICE_STATES],
    ["disabled", "starting", "ready", "degraded", "failed", "stopped"]);
});

test("optional-service status is namespaced — MeshCoordinator status shape is untouched", () => {
  // R5 exit: mesh truth and optional truth never merge. The coordinator's
  // status keys are pinned; an optional-service host adds nothing to them.
  const coordinator = new MeshCoordinator({
    relayStore: new RelayStore(),
    metrics: { setGauge() {}, getGauge() { return 0; } },
    meshConfig: {},
  });
  const status = coordinator.getStatus();
  for (const key of Object.keys(status)) {
    assert.ok(!/optional|service|atlas/i.test(key), "mesh status must not absorb optional-service fields: " + key);
  }
});

test("re-audit R5: non-Error throws (strings, objects, null) are isolated at every lifecycle hook", async () => {
  const stringThrow = makeService("svc-string");
  stringThrow.start = async () => { throw "plain string failure"; }; // eslint-disable-line no-throw-literal
  const nullReject = makeService("svc-null");
  nullReject.start = () => Promise.reject(null);
  const objectStop = makeService("svc-object-stop");
  objectStop.stop = () => Promise.reject({ code: "E_WEIRD" });
  const badEnabled = makeService("svc-bad-enabled");
  badEnabled.isEnabled = () => { throw { toJSON() { throw new Error("cyclic"); } }; };
  const badStatus = makeService("svc-bad-status");
  badStatus.getStatus = () => { throw "status string"; }; // eslint-disable-line no-throw-literal
  const healthy = makeService("svc-healthy");

  const host = new OptionalNodeServiceHost({
    services: [stringThrow, nullReject, objectStop, badEnabled, badStatus, healthy],
  });
  await host.startAll(); // must resolve — nothing escapes
  const statuses = host.getStatuses();
  assert.equal(statuses["svc-string"].state, "failed");
  assert.match(statuses["svc-string"].error, /plain string failure/);
  assert.equal(statuses["svc-null"].state, "failed");
  assert.equal(statuses["svc-bad-enabled"].state, "failed");
  assert.equal(statuses["svc-bad-status"].state, "degraded");
  assert.equal(statuses["svc-healthy"].state, "ready",
    "one service's garbage throw never blocks the others from starting");

  await host.stopAll(); // must resolve — object rejection isolated
  assert.equal(host.getStatuses()["svc-object-stop"].state, "failed");
  assert.equal(host.getStatuses()["svc-healthy"].state, "stopped");
});

test("re-audit R5: a hung start() is bounded — startAll resolves and later services still start", async () => {
  const hung = makeService("svc-hung");
  hung.start = () => new Promise(() => {});
  const after = makeService("svc-after");
  const host = new OptionalNodeServiceHost({ services: [hung, after], startTimeoutMs: 50 });

  const startedAt = Date.now();
  await host.startAll();
  const elapsedMs = Date.now() - startedAt;

  assert.ok(elapsedMs < 1000, "startAll returned at the start timeout (took " + elapsedMs + "ms)");
  const statuses = host.getStatuses();
  assert.equal(statuses["svc-hung"].state, "failed");
  assert.match(statuses["svc-hung"].error, /timed out/);
  assert.equal(statuses["svc-after"].state, "ready", "the hung service did not block the next one");

  // Shutdown still attempts bounded cleanup on the maybe-started service.
  await host.stopAll();
  assert.ok(hung.calls.includes("stop"), "a start-timeout service is still stopped at shutdown");
});
