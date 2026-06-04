import { test } from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";

import { startRezNode } from "../src/app/startRezNode.js";
import { makeSignedRecord } from "./support/durableRecord.js";

/**
 * LIVE local mesh — four REAL rez-node instances over REAL TCP sockets (not the
 * in-process fake-socket harness that hid the original bug). Topology:
 *
 *   leafA ── relayR1 ═══ relayR2 ── leafB
 *
 * built purely from `knownRelays` config, which reproduces the production
 * relay/leaf auth asymmetry over the wire: each relay knows the other (mutual
 * relay-verified → they peer each other's record DHT), but neither relay knows
 * the leaves (relay-provisional → the relay does NOT add the leaf to its
 * k-buckets). So leafA publishes to R1 only, and leafB — whose sole DHT peer is
 * R2 — must resolve the record by delegating to R2, which crosses the core to
 * R1. This is the exact scenario that produced `acceptInvite: invite envelope
 * not found`.
 *
 * Gated behind RUN_LOCAL_MESH_E2E=1 because it binds real loopback ports and
 * waits on real mesh formation.
 */

const RUN = process.env.RUN_LOCAL_MESH_E2E === "1";

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = addr && typeof addr === "object" ? addr.port : 0;
      server.close((err) => (err ? reject(err) : resolve(port)));
    });
  });
}

async function waitFor(label, predicate, { timeoutMs = 25_000, intervalMs = 200 } = {}) {
  const start = Date.now();
  let lastErr = null;
  while (Date.now() - start < timeoutMs) {
    try {
      if (await predicate()) return;
    } catch (err) {
      lastErr = err;
    }
    await delay(intervalMs);
  }
  throw new Error("timeout waiting for: " + label + (lastErr ? " (" + lastErr.message + ")" : ""));
}

function relayOnlyConfig({ dataDir, listenPort, relayKeyId, knownRelays }) {
  return {
    node: {
      mode: "relay-only",
      storage: { dataDir },
      network: { knownRelays },
      mesh: { mode: "seed-only", seeds: [] },
      relay: {
        listenHost: "127.0.0.1",
        listenPort,
        advertisedHost: "127.0.0.1",
        relayKeyId,
      },
    },
  };
}

const knownRelay = (relayKeyId, port) => ({ id: relayKeyId, relayKeyId, host: "127.0.0.1", port, transport: "tcp", insecure: true, tls: false });

test("live local mesh: a record published at one leaf resolves at another leaf across the relay core", { skip: !RUN, timeout: 90_000 }, async () => {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "rez-local-mesh-"));
  const dirs = ["r1", "r2", "leafA", "leafB"].map((n) => path.join(tmp, n));
  await Promise.all(dirs.map((d) => fs.mkdir(d, { recursive: true })));

  // Pre-allocate the two relay ports so the relays can cross-reference each
  // other at start (their links retry until both listeners are up).
  const r1Port = await getFreePort();
  const r2Port = await getFreePort();

  const started = [];
  try {
    const r1 = await startRezNode(relayOnlyConfig({
      dataDir: dirs[0], listenPort: r1Port, relayKeyId: "relay-core-1",
      knownRelays: [knownRelay("relay-core-2", r2Port)],
    }));
    started.push(r1);
    const r2 = await startRezNode(relayOnlyConfig({
      dataDir: dirs[1], listenPort: r2Port, relayKeyId: "relay-core-2",
      knownRelays: [knownRelay("relay-core-1", r1Port)],
    }));
    started.push(r2);

    // Leaves: each lists ONLY its entry relay, and runs seed-only so it never
    // discovers/connects to the other relay.
    const leafA = await startRezNode(relayOnlyConfig({
      dataDir: dirs[2], listenPort: 0, relayKeyId: "leaf-A",
      knownRelays: [knownRelay("relay-core-1", r1Port)],
    }));
    started.push(leafA);
    const leafB = await startRezNode(relayOnlyConfig({
      dataDir: dirs[3], listenPort: 0, relayKeyId: "leaf-B",
      knownRelays: [knownRelay("relay-core-2", r2Port)],
    }));
    started.push(leafB);

    const dht = (n) => n.runtime.recordDht;
    assert.ok(dht(r1) && dht(r2) && dht(leafA) && dht(leafB), "all four nodes expose a record DHT");

    // Wait for the mesh to form over real TCP: the core peers each other and
    // each leaf peers exactly its entry relay.
    await waitFor("relay core connected", () => dht(r1).kBuckets.size >= 1 && dht(r2).kBuckets.size >= 1);
    await waitFor("leafA peers R1", () => dht(leafA).kBuckets.size >= 1);
    await waitFor("leafB peers R2", () => dht(leafB).kBuckets.size >= 1);

    // The asymmetry, asserted live: leaves know exactly one relay; the relays do
    // NOT carry the leaves as routing peers.
    assert.equal(dht(leafA).kBuckets.size, 1, "leafA's only DHT peer is its entry relay");
    assert.equal(dht(leafB).kBuckets.size, 1, "leafB's only DHT peer is its entry relay");

    // leafA (inviter, ONLINE) publishes a signed record.
    const now = Date.now();
    const { record, publicKeyB64, localId } = makeSignedRecord({
      recordKind: "peerlink-invite", recordId: "live-" + r1Port, issuedAtMs: now, expiresAtMs: now + 3_600_000,
    });
    const put = await dht(leafA).putRecord(record);
    assert.equal(put.stored, true, "publish stored locally: " + put.reason);

    // Confirm over the wire that the record reached R1 (leafA's entry relay)…
    await waitFor("record lands on R1", () => Boolean(dht(r1).recordStore.get(localId, Date.now())));
    // …and is NOT on R2 (leafB's entry relay) — so resolving it genuinely
    // requires crossing the core.
    assert.equal(dht(r2).recordStore.get(localId, Date.now()), null, "R2 does not hold the record");
    assert.equal(dht(leafB).recordStore.get(localId, Date.now()), null, "leafB has no local copy");

    // The acceptor fetches across the live mesh. Allow a moment for the
    // first-attempt round trips.
    let got = null;
    await waitFor("leafB resolves across the core", async () => {
      got = await dht(leafB).getRecord({ recordKind: "peerlink-invite", recordId: "live-" + r1Port, publisherPublicKeyB64: publicKeyB64 });
      return Boolean(got);
    }, { timeoutMs: 10_000 });

    assert.ok(got, "leafB resolved the record over the real mesh");
    assert.equal(got.sigB64, record.sigB64, "resolved record is the one leafA published");
  } finally {
    for (const app of started.reverse()) {
      await app.stop().catch(() => {});
    }
    await fs.rm(tmp, { recursive: true, force: true }).catch(() => {});
  }
});
