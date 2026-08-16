import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";
import { generateKeyPairSync } from "node:crypto";

import { newRoutingKey } from "@rezprotocol/core";
import { startRezNode } from "../src/app/startRezNode.js";
import { validateConfig } from "../src/app/NodeConfigValidator.js";
import { createServerServices, createPerAccountServices, createProtocolFactory, createDepositHandler } from "./helpers/nodeTestServices.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

test("validateConfig normalizes known relays and relay TLS settings", () => {
  const secureResolved = validateConfig({
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws" },
      network: {
        knownRelays: [{ id: "relay:secure", host: "r1.rezprotocol.io", port: 8081, tls: true }],
      },
      mesh: {
        seeds: ["https://r1.rezprotocol.io:8081"],
      },
      relay: {
        tls: {
          enabled: true,
          certPath: "./certs/relay.crt",
          keyPath: "./certs/relay.key",
        },
      },
    },
  });
  assert.equal(secureResolved.network.knownRelays[0].insecure, false);
  assert.equal(secureResolved.network.knownRelays[0].tls, true);
  assert.deepEqual(secureResolved.mesh.seeds, ["https://r1.rezprotocol.io:8081"]);
  assert.equal(secureResolved.relay.tls.enabled, true);
  assert.ok(secureResolved.relay.tls.certPath.endsWith("/certs/relay.crt"));
  assert.ok(secureResolved.relay.tls.keyPath.endsWith("/certs/relay.key"));

  const insecureResolved = validateConfig({
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws" },
      network: {
        knownRelays: [{ id: "relay:local", host: "127.0.0.1", port: 8788, insecure: true }],
      },
      mesh: {
        seeds: ["http://127.0.0.1:8788"],
      },
    },
  });
  assert.equal(insecureResolved.network.knownRelays[0].insecure, true);
  assert.equal(insecureResolved.network.knownRelays[0].tls, false);
  assert.deepEqual(insecureResolved.mesh.seeds, ["http://127.0.0.1:8788"]);
  assert.equal(insecureResolved.relay.tls.enabled, false);
});

test("public mesh mode refuses to boot as an isolated island", () => {
  assert.throws(
    () => validateConfig({
      node: {
        ws: { host: "127.0.0.1", port: 0, path: "/ws" },
        network: { knownRelays: [], requireKnownRelays: true },
      },
    }),
    /requires at least one.*knownRelays/,
  );
  const isolated = validateConfig({
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws" },
      network: { knownRelays: [], requireKnownRelays: false },
    },
  });
  assert.equal(isolated.network.requireKnownRelays, false);
});

test("validateConfig accepts relay-only mode without ws and preserves relay settings", () => {
  const pair = generateKeyPairSync("x25519", {
    publicKeyEncoding: { format: "der", type: "spki" },
    privateKeyEncoding: { format: "der", type: "pkcs8" },
  });

  const resolved = validateConfig({
    node: {
      mode: "relay-only",
      network: {
        knownRelays: [
          { id: "relay:tcp", relayKeyId: "ws:relay2", host: "127.0.0.1", port: 8082, transport: "tcp" },
        ],
      },
      mesh: {
        mode: "seeded-gossip",
        seeds: ["http://127.0.0.1:9082"],
      },
      relay: {
        listenHost: "0.0.0.0",
        listenPort: 8081,
        advertisedHost: "127.0.0.1",
        relayKeyId: "ws:relay1",
        onion: {
          v2: {
            keys: [{
              onionKeyId: "relay-key-1",
              publicKeyBytes: Buffer.from(pair.publicKey).toString("base64"),
              privateKeyBytes: Buffer.from(pair.privateKey).toString("base64"),
              notBefore: 1,
              notAfter: 2,
              status: "active",
            }],
          },
        },
      },
    },
  });

  assert.equal(resolved.node.mode, "relay-only");
  assert.equal(resolved.ws, null);
  assert.equal(resolved.relay.relayKeyId, "ws:relay1");
  assert.equal(resolved.relay.onion.v2.keys.length, 1);
});

test("validateConfig rejects legacy mesh disable flags", () => {
  assert.throws(
    () => validateConfig({
      node: {
        ws: { host: "127.0.0.1", port: 0, path: "/ws" },
        network: {
          participateInRouting: false,
          knownRelays: [],
        },
      },
    }),
    /full mesh is always enabled/,
  );

  assert.throws(
    () => validateConfig({
      node: {
        ws: { host: "127.0.0.1", port: 0, path: "/ws" },
        network: { knownRelays: [] },
        mesh: { enabled: false },
      },
    }),
    /full mesh is always enabled/,
  );

  assert.throws(
    () => validateConfig({
      node: {
        ws: { host: "127.0.0.1", port: 0, path: "/ws" },
        network: { knownRelays: [] },
        mesh: { participateInRouting: false },
      },
    }),
    /always participates in routing/,
  );
});

test("startRezNode uses explicit mesh seeds and keeps known relay peers", async (t) => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-node-mesh-bootstrap-"));
  t.after(async () => {
    await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
  });

  const config = {
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws" },
      storage: { dataDir: tempRoot, defaultThreadId: newRoutingKey() },
      network: {
        knownRelays: [
          { id: "ws:relay1", url: "ws://134.209.119.210:8081/rez", host: "134.209.119.210", port: 8081, transport: "ws" },
          { id: "ws:relay2", url: "ws://147.182.162.134:8082/rez", host: "147.182.162.134", port: 8082, transport: "ws" },
          { id: "ws:relay3", url: "ws://157.230.213.181:8083/rez", host: "157.230.213.181", port: 8083, transport: "ws" },
        ],
      },
      mesh: {
        seeds: [
          "http://134.209.119.210:8081",
          "http://147.182.162.134:8082",
          "http://157.230.213.181:8083",
        ],
      },
      identity: {
        accountId: "rez:test:acct",
        deviceId: "dev:test",
        localInboxId: "inbox:test",
      },
      serverServicesFactory: createServerServices,
      serviceCacheFactory: createPerAccountServices,
      protocolFactory: createProtocolFactory(),
      onInboundDeposit: createDepositHandler({ crypto: new NodeCryptoProvider() }),
    },
  };

  let app = null;
  try {
    app = await startRezNode(config);
  } catch (err) {
    if (["EACCES", "EPERM"].includes(err?.code)) {
      t.skip("WebSocket bind not permitted in this environment");
      return;
    }
    throw err;
  }
  try {
    assert.deepEqual(
      app.config.mesh.seeds.slice().sort(),
      [
        "http://134.209.119.210:8081",
        "http://147.182.162.134:8082",
        "http://157.230.213.181:8083",
      ],
    );

    const status = app.runtime.getMeshStatus();
    // No self descriptor is published here because the test does not configure a public relay host.
    assert.equal(status.peerCount, 3);
    assert.equal(status.peers.length, 3);
    assert.deepEqual(
      Object.keys(status.seedReachable).sort(),
      [
        "http://134.209.119.210:8081",
        "http://147.182.162.134:8082",
        "http://157.230.213.181:8083",
      ],
    );
  } finally {
    await app?.stop?.().catch(() => {});
  }
});

test("startRezNode starts relay-only mode with relay listener", async (t) => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-node-relay-only-"));
  t.after(async () => {
    await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
  });

  const config = {
    node: {
      mode: "relay-only",
      storage: { dataDir: tempRoot },
      network: {
        knownRelays: [],
      },
      mesh: {
        mode: "seeded-gossip",
        seeds: [],
      },
      relay: {
        listenHost: "127.0.0.1",
        listenPort: 0,
        advertisedHost: "127.0.0.1",
      },
    },
  };

  const app = await startRezNode(config);
  try {
    assert.equal(app.config.node.mode, "relay-only");
    assert.equal(app.gateway, null);
    assert.equal(app.config.ws, null);
    assert.ok(app.relayAddress && app.relayAddress.port > 0);
    assert.equal(app.runtime.getMeshStatus().peerCount, 1);
    assert.equal(app.optionalServices, null, "no optional services configured → exact base composition");
  } finally {
    await app.stop();
  }
});

// P6.2 — optional services compose at the root; their failures never touch
// core startup, mesh status, readiness, or shutdown.
test("startRezNode with optional services: failures are isolated and status stays namespaced", async (t) => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-node-optsvc-"));
  t.after(async () => {
    await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
  });

  const events = [];
  const healthy = {
    name: "double-healthy",
    async start() { events.push("healthy-start"); },
    async stop() { events.push("healthy-stop"); },
    getStatus() { return {}; },
  };
  const broken = {
    name: "double-broken",
    async start() { events.push("broken-start"); throw new Error("optional start boom"); },
    async stop() { events.push("broken-stop"); },
    getStatus() { return {}; },
  };

  const app = await startRezNode({
    node: {
      mode: "relay-only",
      storage: { dataDir: tempRoot },
      network: { knownRelays: [] },
      mesh: { mode: "seeded-gossip", seeds: [] },
      relay: { listenHost: "127.0.0.1", listenPort: 0, advertisedHost: "127.0.0.1" },
      optionalServices: [healthy, broken],
    },
  });
  try {
    // Core startup succeeded despite the broken optional service.
    assert.ok(app.relayAddress && app.relayAddress.port > 0);
    assert.equal(app.runtime.getMeshStatus().peerCount, 1, "mesh routing unaffected");
    assert.deepEqual(events, ["healthy-start", "broken-start"]);

    const statuses = app.optionalServices.getStatuses();
    assert.equal(statuses["double-healthy"].state, "ready");
    assert.equal(statuses["double-broken"].state, "failed");
    assert.match(statuses["double-broken"].error, /optional start boom/);

    // Mesh status never absorbs optional-service truth.
    for (const key of Object.keys(app.runtime.getMeshStatus())) {
      assert.ok(!/optional|service/i.test(key), "mesh status leak: " + key);
    }

    // /ready remains governed by mandatory storage/runtime deps only.
    const readiness = await app.runtime.checkReadiness();
    assert.equal(readiness.ok, true, "a failed optional service must not flip readiness");
  } finally {
    await app.stop();
  }
  // Only successfully-started services are stopped, before core teardown.
  assert.ok(events.includes("healthy-stop"));
  assert.equal(events.includes("broken-stop"), false, "a service that never started is not stopped");
});
