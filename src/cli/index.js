import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { newRoutingKey } from "@rezprotocol/core";
import { ControlServer, defaultControlSocketPath } from "../control/ControlServer.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PACKAGE_JSON = path.resolve(HERE, "../../package.json");

function usage() {
  return [
    "Usage: rez-node <command> [options]",
    "",
    "Commands:",
    "  version                  Print rez-node package version",
    "  init [--config <path>] [--data-dir <path>] [--force]",
    "                           Create config file and data directory",
    "  doctor [--config <path>] Validate config, data dir, ws port, and control socket",
    "  start [--config <path>] [--no-control] Start rez-node using config",
    "  migrate [--pg <url>]     Apply Postgres schema migrations (or set REZ_PG_URL)",
  ].join("\n");
}

function getVersion() {
  const pkg = JSON.parse(fs.readFileSync(PACKAGE_JSON, "utf8"));
  return String(pkg.version || "0.0.0");
}

function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      out._.push(token);
      continue;
    }
    const key = token.slice(2);
    if (key === "force") {
      out.force = true;
      continue;
    }
    if (key === "no-control" || key === "noControl") {
      out["no-control"] = true;
      continue;
    }
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    out[key] = next;
    i += 1;
  }
  return out;
}

function defaultConfigPath() {
  return path.resolve(process.cwd(), "rez-node.config.json");
}

function buildDefaultConfig({ dataDir }) {
  const entropy = Math.random().toString(16).slice(2, 10);
  return {
    node: {
      mode: "full",
      ws: {
        host: "127.0.0.1",
        port: 8787,
        path: "/ws",
      },
      network: {
        knownRelays: [],
      },
      mesh: {
        mode: "seeded-gossip",
        seeds: [],
        minPeers: 3,
        maxPeers: 32,
        discoveryIntervalMs: 30000,
        policy: {
          rateLimit: 120,
          payloadMaxBytes: 1048576,
          failureThreshold: 8,
        },
      },
      storage: {
        dataDir,
        defaultThreadId: newRoutingKey(),
        controlSocketPath: defaultControlSocketPath(dataDir),
      },
      backup: {
        retentionDays: 90,
      },
      identity: {
        accountId: `rez:node:${os.hostname() || "local"}:${entropy}`,
        deviceId: `dev:${os.hostname() || "local"}:${entropy}`,
        localInboxId: `inbox:${entropy}`,
      },
    },
  };
}

async function readConfig(configPath) {
  const raw = await fsp.readFile(configPath, "utf8");
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(`Config parse failed for ${configPath}: ${err.message}`);
  }
  if (!parsed || typeof parsed !== "object") {
    throw new Error(`Config must be an object: ${configPath}`);
  }
  return parsed;
}

/**
 * Apply deployment env overrides onto a loaded config (12-factor: env wins over
 * the file, so a container can switch backends without re-templating the file).
 *
 * - REZ_STORAGE_BACKEND: "postgres" (operator alias) | "pg" | "fs"
 * - REZ_PG_URL: Postgres connection string
 * - REZ_STORAGE_ENCRYPTION_KEY: base64 32-byte at-rest cluster key (pg mode).
 *   SECRET — never logged here; keep it in a secret manager.
 * - REZ_REDIS_URL: Redis connection string for the liveness bus (optional, pg
 *   clusters; unset = no real-time cross-node push, reconnect-drain still works).
 */
export function applyStorageEnvOverrides(config, env) {
  const backendRaw = typeof env.REZ_STORAGE_BACKEND === "string" ? env.REZ_STORAGE_BACKEND.trim().toLowerCase() : "";
  const pgUrl = typeof env.REZ_PG_URL === "string" ? env.REZ_PG_URL.trim() : "";
  const storageKey = typeof env.REZ_STORAGE_ENCRYPTION_KEY === "string" ? env.REZ_STORAGE_ENCRYPTION_KEY.trim() : "";
  const redisUrl = typeof env.REZ_REDIS_URL === "string" ? env.REZ_REDIS_URL.trim() : "";
  if (backendRaw === "" && pgUrl === "" && storageKey === "" && redisUrl === "") {
    return;
  }
  if (!config.node || typeof config.node !== "object") {
    config.node = {};
  }
  if (redisUrl !== "") {
    if (!config.node.redis || typeof config.node.redis !== "object") {
      config.node.redis = {};
    }
    config.node.redis.url = redisUrl;
  }
  if (!config.node.storage || typeof config.node.storage !== "object") {
    config.node.storage = {};
  }
  const storage = config.node.storage;
  if (backendRaw !== "") {
    // "postgres" is the operator-facing alias for the canonical "pg".
    storage.backend = (backendRaw === "postgres" || backendRaw === "pg") ? "pg" : backendRaw;
  }
  if (pgUrl !== "") {
    if (!storage.pg || typeof storage.pg !== "object") {
      storage.pg = {};
    }
    storage.pg.connectionString = pgUrl;
  }
  if (storageKey !== "") {
    storage.encryptionKeyB64 = storageKey;
  }
}

async function checkPortAvailable(host, port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.unref();
    server.once("error", (err) => {
      resolve({ ok: false, error: err });
    });
    server.listen({ host, port }, () => {
      server.close(() => resolve({ ok: true }));
    });
  });
}

async function cmdMigrate(args, io) {
  const pgUrl = typeof args.pg === "string" && args.pg.length > 0
    ? args.pg
    : (process.env.REZ_PG_URL || "");
  if (!pgUrl) {
    io.stderr.write("migrate requires --pg <url> or REZ_PG_URL\n");
    return 1;
  }
  // Lazy import so `version`/`init`/`doctor` never require the pg dependency.
  const { PgConnection } = await import("../storage/pg/PgConnection.js");
  const { MigrationRunner } = await import("../storage/pg/MigrationRunner.js");
  const conn = new PgConnection({ connectionString: pgUrl });
  try {
    const result = await new MigrationRunner({ connection: conn }).migrate();
    if (result.appliedNow.length === 0) {
      io.stdout.write(`rez-node migrate: up to date at version ${result.shipped}\n`);
    } else {
      io.stdout.write(`rez-node migrate: applied ${result.appliedNow.join(", ")} (now at ${result.shipped})\n`);
    }
    return 0;
  } finally {
    await conn.close();
  }
}

async function cmdVersion(io) {
  io.stdout.write(`${getVersion()}\n`);
  return 0;
}

async function cmdInit(args, io) {
  const configPath = path.resolve(String(args.config || defaultConfigPath()));
  const dataDir = path.resolve(String(args["data-dir"] || path.join(process.cwd(), ".local", "rez-node-data")));
  const force = Boolean(args.force);

  await fsp.mkdir(dataDir, { recursive: true });
  const config = buildDefaultConfig({ dataDir });

  if (!force && fs.existsSync(configPath)) {
    io.stderr.write(`ERR config already exists: ${configPath} (use --force to overwrite)\n`);
    return 1;
  }

  await fsp.mkdir(path.dirname(configPath), { recursive: true });
  await fsp.writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`, "utf8");
  io.stdout.write(`OK config=${configPath}\n`);
  io.stdout.write(`OK dataDir=${dataDir}\n`);
  return 0;
}

async function cmdDoctor(args, io) {
  const configPath = path.resolve(String(args.config || defaultConfigPath()));
  const failures = [];

  if (!fs.existsSync(configPath)) {
    failures.push(`missing config file: ${configPath}`);
  }

  let config = null;
  if (failures.length === 0) {
    try {
      config = await readConfig(configPath);
    } catch (err) {
      failures.push(err.message);
    }
  }

  if (config) {
    // One explicit unwrap of the config shape, reused by everything below — the CLI reads a
    // user-supplied file, so every level genuinely can be absent.
    const cfgNode = config && config.node ? config.node : null;
    const cfgStorage = cfgNode && cfgNode.storage ? cfgNode.storage : null;
    const cfgWs = cfgNode && cfgNode.ws ? cfgNode.ws : null;
    const nodeMode = cfgNode && cfgNode.mode === "relay-only" ? "relay-only" : "full";
    const dataDirRaw = cfgStorage ? cfgStorage.dataDir : undefined;
    const dataDir = path.resolve(typeof dataDirRaw === "string" ? dataDirRaw : path.join(process.cwd(), ".local", "rez-node-data"));
    const controlSocketPath = cfgStorage && typeof cfgStorage.controlSocketPath === "string" && cfgStorage.controlSocketPath.trim().length > 0
      ? config.node.storage.controlSocketPath.trim()
      : defaultControlSocketPath(dataDir);
    try {
      await fsp.mkdir(dataDir, { recursive: true });
      await fsp.access(dataDir, fs.constants.R_OK | fs.constants.W_OK);
      io.stdout.write(`OK dataDir rw: ${dataDir}\n`);
    } catch (err) {
      failures.push(`data dir not writable: ${dataDir} (${err.message})`);
    }

    if (nodeMode !== "relay-only") {
      const host = cfgWs && typeof cfgWs.host === "string" ? cfgWs.host : "127.0.0.1";
      const port = Number(cfgWs ? cfgWs.port : undefined);
      if (!Number.isInteger(port) || port < 0 || port > 65535) {
        failures.push(`invalid ws port: ${String(cfgWs ? cfgWs.port : undefined)}`);
      } else {
        const availability = await checkPortAvailable(host, port);
        if (!availability.ok) {
          failures.push(`ws port unavailable ${host}:${port} (${availability.error && availability.error.message ? availability.error.message : "unknown error"})`);
        } else {
          io.stdout.write(`OK ws port available: ${host}:${port}\n`);
        }
      }
    } else {
      io.stdout.write("OK ws skipped: relay-only mode\n");
    }

    const probe = new ControlServer({
      metrics: { startTimeMs: Date.now(), snapshot: () => ({}), on: () => {}, off: () => {}, setGauge: () => {} },
      dataDir,
      socketPath: controlSocketPath,
      version: getVersion(),
      metricsIntervalMs: 5000,
    });
    try {
      await probe.start();
      io.stdout.write(`OK control bind: ${controlSocketPath}\n`);
      if (process.platform !== "win32") {
        const stat = await fsp.stat(controlSocketPath);
        const mode = stat.mode & 0o777;
        if (mode !== 0o600) {
          failures.push(`control socket permissions must be 600, got ${mode.toString(8)}`);
        } else {
          io.stdout.write(`OK control perms: ${mode.toString(8)}\n`);
        }
      }
    } catch (err) {
      failures.push(`control bind failed: ${controlSocketPath} (${err.message})`);
    } finally {
      await probe.stop().catch(() => {});
    }
  }

  if (failures.length > 0) {
    io.stderr.write("DOCTOR_FAIL\n");
    for (const failure of failures) {
      io.stderr.write(`ERR ${failure}\n`);
    }
    return 1;
  }
  io.stdout.write("DOCTOR_OK\n");
  return 0;
}

async function cmdStart(args, io) {
  const configPath = path.resolve(String(args.config || defaultConfigPath()));
  const config = await readConfig(configPath);
  if (!config.node) config.node = {};
  applyStorageEnvOverrides(config, process.env);
  const nodeMode = config.node.mode === "relay-only" ? "relay-only" : "full";
  if (nodeMode !== "relay-only" && (!config.node.serverServicesFactory || !config.node.serviceCacheFactory)) {
    try {
      const chat = await import("../../../rez-chat/src/server/index.js");
      if (!config.node.serverServicesFactory) config.node.serverServicesFactory = chat.createServerServices;
      if (!config.node.serviceCacheFactory) config.node.serviceCacheFactory = chat.createPerAccountServices;
    } catch {
      throw new Error("rez-node CLI requires chat server services. Install rez-chat or provide serverServicesFactory in config.");
    }
  }
  const { startRezNode } = await import("../app/startRezNode.js");
  const app = await startRezNode(config);
  const enableControl = !Boolean(args["no-control"] || args.noControl);
  let control = null;
  if (enableControl) {
    control = new ControlServer({
      metrics: app.metrics,
      dataDir: app.config.storage.dataDir,
      socketPath: app.config.storage.controlSocketPath,
      version: getVersion(),
    });
    await control.start();
  }
  io.stdout.write(`rez-node version=${getVersion()}\n`);
  io.stdout.write(`rez-node config=${configPath}\n`);
  io.stdout.write(`rez-node dataDir=${app.config.storage.dataDir}\n`);
  if (app.gateway && typeof app.gateway.address === "function" && app.config.ws) {
    const address = app.gateway.address();
    const wsUrl = `ws://${address.address}:${address.port}${app.config.ws.path}`;
    io.stdout.write(`rez-node wsUrl=${wsUrl}\n`);
  } else if (app.relayAddress) {
    io.stdout.write(`rez-node relay=tcp://${app.relayAddress.host}:${app.relayAddress.port}\n`);
  }
  io.stdout.write(`rez-node control=${control ? control.address() : "disabled"}\n`);

  const shutdown = async (signal) => {
    io.stdout.write(`rez-node shutdown signal=${signal}\n`);
    if (control) await control.stop().catch(() => {});
    await app.stop();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  return new Promise(() => {});
}

export async function runCli(argv, io = { stdout: process.stdout, stderr: process.stderr }) {
  const parsed = parseArgs(argv);
  const command = parsed._[0];

  if (!command || command === "help" || command === "--help" || command === "-h") {
    io.stdout.write(`${usage()}\n`);
    return 0;
  }

  if (command === "version") return cmdVersion(io);
  if (command === "init") return cmdInit(parsed, io);
  if (command === "doctor") return cmdDoctor(parsed, io);
  if (command === "start") return cmdStart(parsed, io);
  if (command === "migrate") return cmdMigrate(parsed, io);

  io.stderr.write(`Unknown command: ${command}\n`);
  io.stderr.write(`${usage()}\n`);
  return 1;
}

async function main() {
  try {
    const code = await runCli(process.argv.slice(2));
    process.exit(code);
  } catch (err) {
    process.stderr.write(`ERR ${err && err.message ? err.message : String(err)}\n`);
    process.exit(1);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  void main();
}
