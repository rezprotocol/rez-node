import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";
import { newRoutingKey } from "@rezprotocol/core";
import { createServerServices, createPerAccountServices, createProtocolFactory, createDepositHandler } from "./helpers/nodeTestServices.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

import { ControlServer } from "../src/control/ControlServer.js";
import { NodeMetrics } from "../src/metrics/NodeMetrics.js";

function createUnixSocketPath(name) {
  return path.join(os.tmpdir(), `${name}-${Date.now()}-${Math.random().toString(16).slice(2)}.sock`);
}

function connect(socketPath) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    socket.once("error", reject);
    socket.once("connect", () => {
      socket.off("error", reject);
      resolve(socket);
    });
  });
}

function collectFrames(socket, out) {
  let buffer = "";
  socket.setEncoding("utf8");
  socket.on("data", (chunk) => {
    buffer += chunk;
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";
    for (const line of lines) {
      const text = String(line || "").trim();
      if (!text) continue;
      try {
        out.push(JSON.parse(text));
      } catch {
        // ignore malformed frames in test harness
      }
    }
  });
}

async function waitFor(predicate, timeoutMs = 1200) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error("timed out waiting for condition");
}

test("ControlServer handles partial frames, invalid JSON, and burst ops", async (t) => {
  if (process.platform === "win32") {
    t.skip("unix socket framing test is unix-only");
    return;
  }

  const metrics = new NodeMetrics();
  const socketPath = createUnixSocketPath("rez-control");
  const server = new ControlServer({ metrics, dataDir: os.tmpdir(), socketPath, version: "0.1.0", metricsIntervalMs: 1000 });
  try {
    await server.start();
  } catch (err) {
    if (["EPERM", "EACCES"].includes(err?.code)) {
      t.skip("unix socket listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
  t.after(async () => {
    await server.stop();
  });

  const socket = await connect(socketPath);
  t.after(() => socket.destroy());
  const frames = [];
  collectFrames(socket, frames);

  socket.write('{"op":"get","name":"metrics"}\n');
  socket.write('{"op":"get","name":"metrics"');
  socket.write('}\n');
  socket.write('{bad json}\n');
  socket.write('{"op":"subscribe","streams":["metrics"]}\n{"op":"get","name":"metrics"}\n');

  await waitFor(() => frames.some((f) => f.type === "hello"));
  await waitFor(() => frames.filter((f) => f.type === "metrics").length >= 3);
  await waitFor(() => frames.some((f) => f.type === "error" && f.code === "BAD_JSON"));
});

test("control subscribe smoke: hello then metrics frames", async (t) => {
  if (process.platform === "win32") {
    t.skip("unix socket smoke test is unix-only");
    return;
  }

  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-node-control-smoke-"));
  const config = {
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws" },
      network: { participateInRouting: true, knownRelays: [] },
      storage: {
        dataDir,
        defaultThreadId: newRoutingKey(),
        controlSocketPath: path.join(dataDir, "control.sock"),
      },
      identity: {
        accountId: "rez:node:test",
        deviceId: "dev:test",
        localInboxId: "inbox:test",
      },
      serverServicesFactory: createServerServices,
      serviceCacheFactory: createPerAccountServices,
      protocolFactory: createProtocolFactory(),
      onInboundDeposit: createDepositHandler({ crypto: new NodeCryptoProvider() }),
    },
  };

  let app;
  let startRezNode;
  try {
    ({ startRezNode } = await import("../src/app/startRezNode.js"));
  } catch (err) {
    if (err?.code === "ERR_MODULE_NOT_FOUND") {
      t.skip("ws dependency unavailable in this environment");
      return;
    }
    throw err;
  }
  try {
    app = await startRezNode(config);
  } catch (err) {
    if (["EPERM", "EACCES"].includes(err?.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => {
    await app.stop();
  });

  const server = new ControlServer({
    metrics: app.metrics,
    dataDir,
    socketPath: config.node.storage.controlSocketPath,
    version: "0.1.0",
    metricsIntervalMs: 200,
  });
  await server.start();
  t.after(async () => {
    await server.stop();
  });

  const socket = await connect(config.node.storage.controlSocketPath);
  t.after(() => socket.destroy());
  const frames = [];
  collectFrames(socket, frames);
  socket.write('{"op":"subscribe","streams":["metrics"]}\n');

  app.metrics.increment("packetsRoutedTotal", 1);
  app.metrics.addTraffic({ packets: 1, bytes: 128 });

  await waitFor(() => frames.some((f) => f.type === "hello"));
  await waitFor(() => frames.some((f) => f.type === "metrics"));

  const metricsFrame = frames.find((f) => f.type === "metrics");
  assert.equal(typeof metricsFrame.data.uptimeMs, "number");
  assert.equal(typeof metricsFrame.data.packetsRoutedTotal, "number");
});
