/**
 * Integration-backend resolution — MANDATORY by default (audit #7, 2026-07-28).
 *
 * THE FINDING. 32 test files gate themselves on REZ_PG_TEST_URL and 4 on REZ_REDIS_TEST_URL, each
 * SKIPPING when the variable is absent. A bare `npm test` therefore exits 0 having skipped every
 * test that touches a real backend — the serialized mutation fold, the durable inbox, the
 * propagation outbox, cross-node delivery. "Green" meant "the unit tests pass and the integration
 * story was not exercised", and nothing in the output distinguished that from a real pass.
 *
 * Skipping is not wrong; skipping SILENTLY, BY DEFAULT is. So the polarity is inverted: the
 * backends are required, and opting out is an explicit, loud act (REZ_SKIP_INTEGRATION_BACKENDS=1)
 * that announces itself in the output rather than hiding in a skip count.
 *
 * The individual suites keep their own `skip:` guards — they still need a URL to connect with, and
 * a suite that skips because it genuinely cannot run should say so. What changed is that a run
 * where they ALL skip can no longer be mistaken for a passing run.
 */

/** The conventional local endpoints, used when the env vars are unset. */
export const DEFAULT_PG_URL = "postgres://postgres:rez@127.0.0.1:55432/reztest";
export const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379";

/** Set to "1" to run without real backends. Deliberately verbose to type and easy to grep for. */
export const SKIP_ENV = "REZ_SKIP_INTEGRATION_BACKENDS";

export function integrationSkipRequested() {
  return String(process.env[SKIP_ENV] || "").trim() === "1";
}

/**
 * The Postgres URL these tests should use: the explicit env var, else the conventional local
 * endpoint. Returning a default rather than "" is what lets a developer with the standard docker
 * container running get the integration suites WITHOUT remembering an env var — the previous
 * default-to-empty is a large part of why they were so easy to leave un-run.
 */
export function pgTestUrl() {
  const explicit = String(process.env.REZ_PG_TEST_URL || "").trim();
  if (explicit) return explicit;
  if (integrationSkipRequested()) return "";
  return DEFAULT_PG_URL;
}

export function redisTestUrl() {
  const explicit = String(process.env.REZ_REDIS_TEST_URL || "").trim();
  if (explicit) return explicit;
  if (integrationSkipRequested()) return "";
  return DEFAULT_REDIS_URL;
}

/**
 * Can we actually reach Postgres? Reachability, not configuration — a URL pointing at nothing is
 * exactly the state that used to read as "configured, therefore fine" right up until the suite
 * failed for unrelated-looking reasons.
 * @returns {Promise<{ok: boolean, reason: string}>}
 */
export async function probePostgres(url) {
  if (!url) return { ok: false, reason: "no URL configured" };
  let pg;
  try {
    pg = await import("pg");
  } catch (err) {
    return { ok: false, reason: "the pg driver is not installed: " + (err && err.message ? err.message : err) };
  }
  const Client = pg.default && pg.default.Client ? pg.default.Client : pg.Client;
  const client = new Client({ connectionString: url, connectionTimeoutMillis: 3000 });
  try {
    await client.connect();
    await client.query("SELECT 1");
    return { ok: true, reason: "" };
  } catch (err) {
    return { ok: false, reason: err && err.message ? err.message : String(err) };
  } finally {
    try {
      await client.end();
    } catch (err) {
      // The probe already has its verdict; a failure to close a socket we are discarding tells us
      // nothing further. Recorded rather than ignored so it is not an empty catch.
      void err;
    }
  }
}

/**
 * Can we reach Redis? Uses a raw socket + inline PING so the probe does not depend on a client
 * library the node may not ship.
 * @returns {Promise<{ok: boolean, reason: string}>}
 */
export async function probeRedis(url) {
  if (!url) return { ok: false, reason: "no URL configured" };
  let parsed;
  try {
    parsed = new URL(url);
  } catch (err) {
    return { ok: false, reason: "unparseable URL: " + (err && err.message ? err.message : err) };
  }
  const net = await import("node:net");
  return new Promise((resolve) => {
    const socket = net.createConnection({
      host: parsed.hostname || "127.0.0.1",
      port: Number(parsed.port || 6379),
    });
    let settled = false;
    const done = (ok, reason) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve({ ok, reason });
    };
    socket.setTimeout(3000, () => done(false, "connection timed out"));
    socket.on("error", (err) => done(false, err && err.message ? err.message : String(err)));
    socket.on("connect", () => socket.write("PING\r\n"));
    socket.on("data", (buf) => {
      const reply = buf.toString("utf8");
      done(reply.startsWith("+PONG"), reply.startsWith("+PONG") ? "" : "unexpected reply: " + reply.trim());
    });
  });
}

/** The command that gets a developer unblocked, printed with any failure so it is actionable. */
export function backendSetupHint() {
  return [
    "Start the integration backends:",
    "  docker run -d --name rez-pg-test -e POSTGRES_PASSWORD=rez -e POSTGRES_DB=reztest -p 55432:5432 postgres:16",
    "  docker run -d --name rez-redis-test -p 6379:6379 redis:7",
    "",
    "Or point at your own:  REZ_PG_TEST_URL=... REZ_REDIS_TEST_URL=... npm test",
    "Or run WITHOUT them (integration coverage is skipped):  " + SKIP_ENV + "=1 npm test",
  ].join("\n");
}
