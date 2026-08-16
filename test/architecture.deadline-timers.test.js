import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Files whose timers BOUND AN AWAITED OPERATION. A deadline of this kind must
 * hold the event loop open: an unref'd timer stops firing the moment the loop
 * would otherwise drain, so the bound silently becomes conditional on
 * unrelated work existing. That is the same "waits forever" hole the re-audit
 * R2/R5 remediation closed by making deadlines RACE instead of checking the
 * clock between awaits — just reachable only when the node is idle.
 *
 * It reached main once (rez-node 61cbc2c) and was caught by CI, not locally:
 * a test process is precisely the idle-loop condition, so the whole DHT /
 * advisor / optional-service suite failed with "Promise resolution is still
 * pending but the event loop has already resolved" while passing on a busier
 * developer machine.
 *
 * This is NOT a blanket ban on unref. Background and recurring timers
 * (DurableInboxPruner, RetryScheduler, TCP idle timers, the control ticker)
 * are unref'd on purpose — they must never keep the process alive. The rule
 * applies only where a caller is awaiting the deadline.
 */
const DEADLINE_BEARING_FILES = [
  "src/routing/dht/DhtLookup.js",
  "src/routing/dht/DhtCandidateResolver.js",
  "src/routing/dht/DhtRecordStoreAckWaiter.js",
  "src/routing/dht/DhtNode.js",
  "src/gateway/RouteAdvisor.js",
  "src/app/OptionalNodeServiceHost.js",
];

const UNREF_CALL = /\.unref\s*\(/;

test("deadline timers are never unref'd", () => {
  const offenders = [];
  for (const relative of DEADLINE_BEARING_FILES) {
    const source = readFileSync(join(ROOT, relative), "utf8");
    source.split("\n").forEach((line, index) => {
      const code = line.split("//")[0];
      if (UNREF_CALL.test(code)) {
        offenders.push(relative + ":" + (index + 1) + "  " + line.trim());
      }
    });
  }
  assert.deepEqual(
    offenders,
    [],
    "A deadline that bounds an awaited operation must hold the event loop open.\n" +
      "Unref'ing it means it will not fire on an otherwise idle node, so the\n" +
      "operation waits forever — exactly what the deadline exists to prevent.\n" +
      "Clear the timer when the work settles instead; that bounds how long it\n" +
      "can hold the loop to the deadline window itself.\n\n" +
      "Offending lines:\n  " + offenders.join("\n  "),
  );
});

test("the guard actually detects an unref'd deadline", () => {
  // Negative control: the rule is worthless if the matcher does not fire.
  // Proves the regex catches the exact shape that shipped, and that a
  // mention inside a comment does not produce a false positive.
  const shipped = '      const timer = setTimeout(fn, ms);\n      if (typeof timer.unref === "function") timer.unref();';
  const commentOnly = "      // The timer is deliberately NOT unref'd — see above.";

  const hits = shipped.split("\n").filter((l) => UNREF_CALL.test(l.split("//")[0]));
  assert.equal(hits.length, 1, "matcher must flag the shipped unref'd-deadline shape");
  assert.equal(
    commentOnly.split("\n").filter((l) => UNREF_CALL.test(l.split("//")[0])).length,
    0,
    "a comment mentioning unref must not trip the guard",
  );
});
