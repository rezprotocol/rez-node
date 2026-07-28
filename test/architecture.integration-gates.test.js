import test from "node:test";
import assert from "node:assert/strict";
import {
  pgTestUrl,
  redisTestUrl,
  probePostgres,
  probeRedis,
  integrationSkipRequested,
  backendSetupHint,
  SKIP_ENV,
} from "./support/integrationBackends.js";

// AUDIT #7 — integration backends are MANDATORY by default.
//
// 32 test files gate on REZ_PG_TEST_URL and 4 on REZ_REDIS_TEST_URL, skipping when unset. A bare
// `npm test` therefore exited 0 having skipped every test that touches a real backend: the
// serialized mutation fold, the durable inbox, the propagation outbox, cross-node delivery. The
// output said "pass" and a skip count, and nothing distinguished that from a run where the
// integration story had actually been exercised.
//
// This inverts the polarity. Backends are required; running without them is an explicit act that
// announces itself. The suites keep their own skip guards — a suite that truly cannot run should
// say so — but a run where they ALL skip can no longer be mistaken for a passing run.

test("Postgres is reachable, or the run explicitly opted out", async () => {
  if (integrationSkipRequested()) {
    console.warn(
      "\n[integration] " + SKIP_ENV + "=1 — running WITHOUT real backends.\n"
        + "[integration] Every Postgres/Redis-backed suite is skipped. This run does NOT exercise\n"
        + "[integration] the serialized mutation fold, durable inbox, propagation outbox, or\n"
        + "[integration] cross-node delivery. Do not read it as integration coverage.\n",
    );
    return;
  }
  const url = pgTestUrl();
  const { ok, reason } = await probePostgres(url);
  assert.ok(
    ok,
    "Postgres is NOT reachable, so every Pg-backed suite would silently skip.\n"
      + "  tried:  " + url + "\n"
      + "  reason: " + reason + "\n\n" + backendSetupHint(),
  );
});

test("Redis is reachable, or the run explicitly opted out", async () => {
  if (integrationSkipRequested()) return; // announced by the Postgres case above
  const url = redisTestUrl();
  const { ok, reason } = await probeRedis(url);
  assert.ok(
    ok,
    "Redis is NOT reachable, so the cross-node suites would silently skip.\n"
      + "  tried:  " + url + "\n"
      + "  reason: " + reason + "\n\n" + backendSetupHint(),
  );
});

test("the opt-out is a single, greppable switch — not a family of ad-hoc variables", () => {
  // The failure mode being closed is a run that LOOKS complete. That only holds if there is exactly
  // one way to disable integration coverage; a second, differently-named escape hatch would restore
  // the original problem under a new name.
  assert.equal(SKIP_ENV, "REZ_SKIP_INTEGRATION_BACKENDS");
  assert.equal(integrationSkipRequested(), String(process.env[SKIP_ENV] || "").trim() === "1",
    "only the exact value \"1\" opts out — a stray truthy value must not silently disable coverage");
});

test("resolution falls back to the conventional local endpoints", () => {
  // Defaulting to "" was a large part of why these suites were so easy to leave un-run: a developer
  // with the standard container already running still got skips unless they remembered an env var.
  if (integrationSkipRequested()) {
    assert.equal(pgTestUrl(), "", "opting out yields no URL, so suites skip rather than dial a stranger");
    assert.equal(redisTestUrl(), "");
    return;
  }
  assert.ok(pgTestUrl().length > 0, "a URL is always resolved when not opted out");
  assert.ok(redisTestUrl().length > 0);
});
