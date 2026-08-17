import test from "node:test";
import assert from "node:assert/strict";
import { raceWithDeadline } from "../src/util/raceWithDeadline.js";

test("resolves the work's value when the work wins", async () => {
  const result = await raceWithDeadline(Promise.resolve("done"), 1000, "timeout");
  assert.equal(result, "done");
});

test("resolves the caller's sentinel when the clock wins", async () => {
  const sentinel = Symbol("expired");
  const never = new Promise(() => {});
  assert.equal(await raceWithDeadline(never, 5, sentinel), sentinel);
});

test("a never-settling operation is still bounded by the deadline", async () => {
  // The property rez-node#5 violated: work that never settles must still
  // resolve to the sentinel.
  //
  // MEASURED CAVEAT — this test does NOT detect the unref regression, and it
  // was checked rather than assumed: reintroducing `timer.unref()` in the
  // helper leaves this test PASSING, both in the full file and run alone via
  // --test-name-pattern. node:test holds its own handles, so the loop never
  // actually drains inside a test process. The condition that exposed #5 was
  // the full suite in CI, which is not reproducible on demand here.
  //
  // The deterministic protection is architecture.deadline-timers.test.js
  // rule 1, which is verified negatively. Do not treat this test as the guard.
  const sentinel = Symbol("idle-loop-deadline");
  const never = new Promise(() => {});
  assert.equal(await raceWithDeadline(never, 10, sentinel), sentinel);
});

test("propagates rejection: a deadline bounds duration, not failure", async () => {
  const boom = new Error("work failed");
  await assert.rejects(
    () => raceWithDeadline(Promise.reject(boom), 1000, "timeout"),
    (err) => err === boom,
  );
});

test("a 0ms budget still lets already-resolved work be counted", async () => {
  // DhtNode's ack window depends on this: it clamps to 0 rather than skipping
  // so acks that already resolved can flush their microtask. A 0ms deadline
  // must not preempt work that is already settled.
  assert.equal(await raceWithDeadline(Promise.resolve("counted"), 0, "timeout"), "counted");
});

test("the timer is cleared once the work settles", async () => {
  // A ref'd timer that was never cleared would keep the loop alive for the
  // full window. Prove the handle is gone well before a long deadline elapses
  // rather than inferring it from the returned value.
  const before = process.getActiveResourcesInfo().filter((r) => r === "Timeout").length;
  await raceWithDeadline(Promise.resolve("fast"), 60_000, "timeout");
  await new Promise((r) => setImmediate(r));
  const after = process.getActiveResourcesInfo().filter((r) => r === "Timeout").length;
  assert.ok(
    after <= before,
    "a settled race must leave no live Timeout behind (before=" + before + " after=" + after + ")",
  );
});

test("rejects a non-promise instead of silently never settling", () => {
  for (const bad of [null, undefined, 42, "nope", {}]) {
    assert.throws(() => raceWithDeadline(bad, 10, "timeout"), TypeError);
  }
});

test("rejects a non-finite budget instead of coercing it to an instant timeout", () => {
  // setTimeout treats NaN as 0. Coercing would turn a broken clock reading
  // into a silent instant expiry -- a wrong answer that looks like a deadline.
  for (const bad of [NaN, Infinity, -1, "1000", null, undefined]) {
    assert.throws(() => raceWithDeadline(Promise.resolve(1), bad, "timeout"), RangeError);
  }
});
