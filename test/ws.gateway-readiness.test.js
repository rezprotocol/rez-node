import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { WebSocket } from "ws";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";

function get(port, path) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString("utf8")) }));
    });
    req.on("error", reject);
  });
}

async function start(t, checkReadiness, metrics = null, readinessCacheMs = 1000) {
  const runtime = {
    inboxStore: null,
    checkReadiness,
    getMeshStatus() { return null; },
  };
  const server = new WsGatewayServer({
    runtime,
    host: "127.0.0.1",
    port: 0,
    metrics,
    readinessCacheMs,
    protocolFactory() { throw new Error("not used"); },
  });
  await server.start();
  t.after(() => server.stop());
  return server.address().port;
}

test("/ready reports dependency health and fails closed", async (t) => {
  const port = await start(t, async () => ({ ok: false, components: { storage: false, redis: true } }));
  const response = await get(port, "/ready");
  assert.equal(response.status, 503);
  assert.deepEqual(response.body.components, { storage: false, redis: true });
});

test("/ready keeps durable service available while reporting Redis degradation", async (t) => {
  const port = await start(t, async () => ({ ok: true, degraded: true, components: { storage: true, redis: false } }));
  const response = await get(port, "/ready");
  assert.equal(response.status, 200);
  assert.equal(response.body.degraded, true);
});

test("/ready returns 200 only after every configured dependency is ready", async (t) => {
  const port = await start(t, async () => ({ ok: true, components: { storage: true, redis: true } }));
  const response = await get(port, "/ready");
  assert.equal(response.status, 200);
  assert.equal(response.body.ok, true);
});

test("/health remains a process liveness check when dependencies are down", async (t) => {
  const port = await start(t, async () => ({ ok: false, components: { storage: false } }));
  const response = await get(port, "/health");
  assert.equal(response.status, 200);
  assert.equal(response.body.ok, true);
});

test("an unready hosted runtime refuses WebSocket admission with retryable 503", async (t) => {
  const port = await start(t, async () => ({ ok: false, components: { storage: false } }));
  const ws = new WebSocket("ws://127.0.0.1:" + port + "/ws");
  t.after(() => ws.terminate());
  const status = await new Promise((resolve, reject) => {
    ws.once("unexpected-response", (request, response) => {
      void request;
      response.resume();
      resolve(response.statusCode);
    });
    ws.once("open", () => reject(new Error("unready gateway accepted a WebSocket")));
    ws.once("error", reject);
  });
  assert.equal(status, 503);
});

test("concurrent unauthenticated readiness requests coalesce onto one dependency probe", async (t) => {
  let probes = 0;
  const port = await start(t, async () => {
    probes += 1;
    await new Promise((resolve) => setTimeout(resolve, 10));
    return { ok: true, components: { storage: true } };
  });
  const responses = await Promise.all(new Array(20).fill(null).map(() => get(port, "/ready")));
  assert.ok(responses.every((response) => response.status === 200));
  assert.equal(probes, 1, "health traffic must not amplify into one Postgres query per request");
});

test("/metrics exports aggregate Prometheus values without labels", async (t) => {
  const metrics = {
    setGauge() {},
    snapshot() { return { packetsRoutedTotal: 7, activeConnections: 2, ignored: "tenant" }; },
  };
  const port = await start(t, async () => ({ ok: true, components: { storage: true } }), metrics);
  const response = await new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path: "/metrics" }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
  });
  assert.equal(response.status, 200);
  assert.match(response.body, /rez_packets_routed_total 7/);
  assert.match(response.body, /rez_active_connections 2/);
  assert.doesNotMatch(response.body, /tenant/);
});
