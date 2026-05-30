import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { RezNode } from "../src/app/RezNode.js";

function makeConfig({ rootDir, wsPort = 0 } = {}) {
  return {
    node: {
      ws: { host: "127.0.0.1", port: wsPort, path: "/ws" },
      storage: { dataDir: rootDir },
      network: {
        knownRelays: [],
      },
      mesh: { seeds: [] },
      identity: {
        accountId: "rez:test:acct:reznode-class",
        deviceId: "dev:reznode-class",
        localInboxId: "inbox:reznode-class",
      },
    },
  };
}

test("RezNode constructor is synchronous and inert", () => {
  const node = new RezNode({ storage: { dataDir: "/tmp/x" } });
  assert.equal(node.started, false);
  assert.equal(node.runtime, null);
  assert.equal(node.gateway, null);
  assert.equal(node.storageProvider, null);
});

test("RezNode constructor rejects missing config", () => {
  assert.throws(() => new RezNode(), /config/);
  assert.throws(() => new RezNode(null), /config/);
  assert.throws(() => new RezNode("hi"), /config/);
});

test("RezNode start() initializes runtime + storage; stop() tears down", async (t) => {
  const rootDir = mkdtempSync(path.join(tmpdir(), "rez-node-class-"));
  t.after(() => { try { rmSync(rootDir, { recursive: true, force: true }); } catch { /* ignore */ } });

  const node = new RezNode(makeConfig({ rootDir }));
  try {
    await node.start();
  } catch (err) {
    if (err && (err.code === "EACCES" || err.code === "EPERM")) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  assert.equal(node.started, true);
  assert.ok(node.runtime, "runtime should be populated after start");
  assert.ok(node.storageProvider, "storageProvider should be populated after start");

  await node.stop();
  assert.equal(node.started, false);
  assert.equal(node.runtime, null);
  assert.equal(node.gateway, null);
});

test("RezNode start() is idempotent", async (t) => {
  const rootDir = mkdtempSync(path.join(tmpdir(), "rez-node-idem-"));
  t.after(() => { try { rmSync(rootDir, { recursive: true, force: true }); } catch { /* ignore */ } });

  const node = new RezNode(makeConfig({ rootDir }));
  try { await node.start(); } catch (err) {
    if (err && (err.code === "EACCES" || err.code === "EPERM")) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await node.stop(); });
  const runtimeBefore = node.runtime;
  await node.start(); // second call should be a no-op
  assert.equal(node.runtime, runtimeBefore);
});

test("RezNode listen() implies start() when not yet started", async (t) => {
  const rootDir = mkdtempSync(path.join(tmpdir(), "rez-node-listen-"));
  t.after(() => { try { rmSync(rootDir, { recursive: true, force: true }); } catch { /* ignore */ } });

  const node = new RezNode(makeConfig({ rootDir }));
  try { await node.listen(); } catch (err) {
    if (err && (err.code === "EACCES" || err.code === "EPERM")) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  t.after(async () => { await node.stop(); });
  assert.equal(node.started, true);
});

test("RezNode stop() before start() is a no-op", async () => {
  const node = new RezNode(makeConfig({ rootDir: "/tmp/never-created" }));
  await node.stop(); // should not throw
  assert.equal(node.started, false);
});
