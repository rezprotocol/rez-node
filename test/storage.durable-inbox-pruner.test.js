import test from "node:test";
import assert from "node:assert/strict";
import { DurableInboxPruner } from "../src/storage/DurableInboxPruner.js";

// Lifecycle + resilience of the periodic durable-inbox prune sweep. The actual
// prune SQL is proven against real Pg in storage.pg.durable-inbox.test.js; here
// we prove the scheduler contract with a fake durableInbox: it sweeps with the
// configured retention args, never re-enters an in-flight sweep, swallows (logs)
// a sweep error instead of throwing, and is a clean no-op after stop().

function makeDurable() {
  const calls = [];
  let next = { inboxesSwept: 1, deleted: 0 };
  let fail = null;
  let gate = null; // optional promise to hold a sweep in-flight
  return {
    calls,
    setNext(v) { next = v; },
    setFail(err) { fail = err; },
    holdNext() { let release; gate = new Promise((r) => { release = r; }); return release; },
    async pruneAll(opts) {
      calls.push(opts);
      if (gate) { await gate; gate = null; }
      if (fail) { const e = fail; fail = null; throw e; }
      return next;
    },
  };
}

test("DurableInboxPruner requires a durableInbox with pruneAll()", () => {
  assert.throws(() => new DurableInboxPruner({}), /requires a durableInbox/);
  assert.throws(() => new DurableInboxPruner({ durableInbox: {} }), /requires a durableInbox/);
});

test("sweep() forwards the configured ttl + staleGrace retention args", async () => {
  const durable = makeDurable();
  const pruner = new DurableInboxPruner({ durableInbox: durable, ttlMs: 111, staleGraceMs: 222 });
  const res = await pruner.sweep();
  assert.deepEqual(res, { inboxesSwept: 1, deleted: 0 });
  assert.deepEqual(durable.calls, [{ ttlMs: 111, staleGraceMs: 222 }]);
});

test("an in-flight sweep is not re-entered", async () => {
  const durable = makeDurable();
  const pruner = new DurableInboxPruner({ durableInbox: durable });
  const release = durable.holdNext();
  const first = pruner.sweep();            // starts, blocks on the gate
  const second = await pruner.sweep();     // must short-circuit (null), not call again
  assert.equal(second, null, "concurrent sweep returns null without a second pruneAll");
  assert.equal(durable.calls.length, 1, "pruneAll called exactly once while one is in flight");
  release();
  await first;
});

test("a sweep error is logged, never thrown (the timer tick must survive)", async () => {
  const durable = makeDurable();
  const errors = [];
  const logger = { log() {}, error: (m) => errors.push(m) };
  const pruner = new DurableInboxPruner({ durableInbox: durable, intervalMs: 5, logger });
  durable.setFail(new Error("pg down"));

  pruner.start();
  // Give the unref'd timer a tick or two to fire and hit the failure path.
  await new Promise((r) => setTimeout(r, 40));
  pruner.stop();

  assert.ok(durable.calls.length >= 1, "the timer fired the sweep");
  assert.ok(errors.some((m) => /sweep failed/.test(m) && /pg down/.test(m)), "the error was logged");
});

test("stop() halts further sweeps; double start/stop is safe", async () => {
  const durable = makeDurable();
  const pruner = new DurableInboxPruner({ durableInbox: durable, intervalMs: 5 });
  pruner.start();
  pruner.start(); // idempotent
  await new Promise((r) => setTimeout(r, 30));
  pruner.stop();
  const after = durable.calls.length;
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(durable.calls.length, after, "no sweeps after stop()");
  pruner.stop(); // idempotent
});
