import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * A deadline that BOUNDS AN AWAITED OPERATION must hold the event loop open.
 * An unref'd timer stops firing the moment the loop would otherwise drain, so
 * the bound silently becomes conditional on unrelated work existing — the same
 * "waits forever" hole the re-audit R2/R5 remediation closed by making
 * deadlines RACE instead of checking the clock between awaits.
 *
 * It reached main once (rez-node 61cbc2c, issue #5) and CI caught it, not the
 * local suite: a test process is precisely the idle-loop condition, so the DHT
 * / advisor / optional-service suites all died with "Promise resolution is
 * still pending but the event loop has already resolved" while passing on a
 * busier developer machine.
 *
 * The mechanism now lives in ONE place. These two rules keep it there.
 */
const HELPER = "src/util/raceWithDeadline.js";

/**
 * Files that bound awaited work and must route through the helper rather than
 * hand-rolling the race. This list is the anti-drift property: an eighth copy
 * of the pattern in any of them fails rule 2.
 */
const MUST_USE_HELPER = [
  "src/routing/dht/DhtLookup.js",
  "src/routing/dht/DhtCandidateResolver.js",
  "src/routing/dht/DhtNode.js",
  "src/gateway/RouteAdvisor.js",
  "src/app/OptionalNodeServiceHost.js",
];

/**
 * DhtRecordStoreAckWaiter is deliberately NOT in MUST_USE_HELPER. It is not a
 * race over a promise: its timer is parked in a #pending map and cleared by an
 * unrelated code path when the matching ack arrives off the socket. Forcing it
 * through raceWithDeadline would mean inventing a promise just to satisfy the
 * shape. It still may not unref — rule 1 covers it.
 */
const NO_UNREF = [HELPER, ...MUST_USE_HELPER, "src/routing/dht/DhtRecordStoreAckWaiter.js"];

const UNREF_CALL = /\.unref\s*\(/;
const SET_TIMEOUT_CALL = /\bsetTimeout\s*\(/;

function codeLines(relative) {
  return readFileSync(join(ROOT, relative), "utf8")
    .split("\n")
    .map((line, index) => ({ n: index + 1, code: line.split("//")[0], raw: line.trim() }));
}

test("rule 1: deadline-bearing code never unrefs a timer", () => {
  const offenders = [];
  for (const relative of NO_UNREF) {
    for (const { n, code, raw } of codeLines(relative)) {
      if (UNREF_CALL.test(code)) offenders.push(relative + ":" + n + "  " + raw);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    "A deadline that bounds an awaited operation must hold the event loop open.\n" +
      "Unref'd, it will not fire on an otherwise idle node and the operation\n" +
      "waits forever — exactly what the deadline exists to prevent. Clear the\n" +
      "timer when the work settles instead.\n\nOffending lines:\n  " + offenders.join("\n  "),
  );
});

test("rule 2: deadline-bearing code races through the shared helper", () => {
  const offenders = [];
  for (const relative of MUST_USE_HELPER) {
    for (const { n, code, raw } of codeLines(relative)) {
      if (SET_TIMEOUT_CALL.test(code)) offenders.push(relative + ":" + n + "  " + raw);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    "Use raceWithDeadline() instead of hand-rolling a setTimeout race.\n" +
      "Six hand-copied versions of this pattern all carried the same unref\n" +
      "defect (#5) — one mistake became systemic because the mechanism was\n" +
      "duplicated. If a new call site genuinely cannot use the helper, remove\n" +
      "it from MUST_USE_HELPER and say why, as DhtRecordStoreAckWaiter does.\n\n" +
      "Offending lines:\n  " + offenders.join("\n  "),
  );
});

test("the helper is actually imported by every file the rules cover", () => {
  // Rule 2 passes trivially if a file simply stopped bounding anything.
  // Assert the positive: each listed file really does reach the helper.
  for (const relative of MUST_USE_HELPER) {
    const source = readFileSync(join(ROOT, relative), "utf8");
    assert.match(
      source,
      /import \{ raceWithDeadline \} from ".*raceWithDeadline\.js";/,
      relative + " is listed as deadline-bearing but does not import raceWithDeadline",
    );
  }
});

test("the guards actually detect the shapes they forbid", () => {
  // Negative control. A rule never seen to fail is not a rule.
  const shippedUnref = '  if (typeof timer.unref === "function") timer.unref();';
  const handRolled = "  const timer = setTimeout(() => resolve(SENTINEL), ms);";
  const commentOnly = "  // The timer is deliberately NOT unref'd, and no setTimeout here.";

  assert.ok(UNREF_CALL.test(shippedUnref.split("//")[0]), "rule 1 must flag the shipped unref shape");
  assert.ok(SET_TIMEOUT_CALL.test(handRolled.split("//")[0]), "rule 2 must flag a hand-rolled race");
  assert.ok(!UNREF_CALL.test(commentOnly.split("//")[0]), "a comment must not trip rule 1");
  assert.ok(!SET_TIMEOUT_CALL.test(commentOnly.split("//")[0]), "a comment must not trip rule 2");
});
