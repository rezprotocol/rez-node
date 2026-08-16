/**
 * P7.3 — final no-extension compatibility gate (ATLAS_PREREQUISITES).
 *
 * Two-configuration authority: base Rez with every optional seam absent must
 * behave identically whether the deterministic test doubles are installed,
 * disabled, timing out, or broken. Scenario coverage lives in the focused
 * suites (start/ready/stop + optional-service isolation in
 * startRezNode.mesh-bootstrap; advisor fallback in gateway.route-advisor;
 * durable-record put/get/restart/churn in routing.durable-record-mesh;
 * onion selection + route failure in the gateway suites). This file holds the
 * cross-cutting equivalence checks and the final repository searches.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { RelayDescriptorV1, OnionKeyRecordV1 } from "@rezprotocol/core";
import { GatewayRelaySelector } from "../src/gateway/GatewayRelaySelector.js";
import { RouteAdvisor } from "../src/gateway/RouteAdvisor.js";
import { MeshCoordinator } from "../src/gateway/MeshCoordinator.js";

const REPO_ROOT = fileURLToPath(new URL("../..", import.meta.url));
const SRC_DIRS = ["rez-core", "rez-node", "rez-sdk", "rez-chat", "rez-ui"]
  .map((pkg) => join(REPO_ROOT, pkg, "src"))
  .filter((dir) => existsSync(dir));

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) out.push(...walk(full));
    else if (name.endsWith(".js") || name.endsWith(".jsx")) out.push(full);
  }
  return out;
}

function allSourceFiles() {
  const files = [];
  for (const dir of SRC_DIRS) files.push(...walk(dir));
  return files;
}

test("final search: no Atlas production symbol exists in any package src", () => {
  // Spec rule: no class, record, constant, status field, metric, or config
  // key containing Atlas. Comments citing the ATLAS_PREREQUISITES document
  // are documentation, not symbols — strip comments before searching.
  const offenders = [];
  for (const file of allSourceFiles()) {
    const code = readFileSync(file, "utf8")
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/\/\/.*$/gm, "");
    if (/atlas/i.test(code)) offenders.push(file.slice(REPO_ROOT.length));
  }
  assert.deepEqual(offenders, [], "the completed work is generic Rez hardening — no Atlas naming");
});

test("final search: no optional chaining in any package src (?? remains allowed)", () => {
  const offenders = [];
  for (const file of allSourceFiles()) {
    const text = readFileSync(file, "utf8");
    for (const [index, line] of text.split("\n").entries()) {
      const code = line.replace(/\/\/.*$/, "");
      if (code.includes("?.") && !/["'`][^"'`]*\?\.[^"'`]*["'`]/.test(code)) {
        offenders.push(file.slice(REPO_ROOT.length) + ":" + (index + 1));
      }
    }
  }
  assert.deepEqual(offenders, []);
});

test("final search: the fake future record kind never grew transport- or kind-specific handling", () => {
  const offenders = [];
  for (const file of allSourceFiles()) {
    if (readFileSync(file, "utf8").includes("future-test-public-fact-v1")) {
      offenders.push(file.slice(REPO_ROOT.length));
    }
  }
  assert.deepEqual(offenders, [], "unknown kinds ride the generic path only — tests may name them, src may not");
});

test("final search: relay-ID derivation has one canonical implementation (rez-core)", () => {
  const offenders = [];
  for (const file of allSourceFiles()) {
    if (file.includes(join("rez-core", "src", "identity"))) continue;
    const text = readFileSync(file, "utf8");
    // Constructing a rez:relay: identity anywhere else is a second derivation.
    if (/["'`]rez:relay:["'`]\s*\+|rez:relay:\$\{/.test(text)) {
      offenders.push(file.slice(REPO_ROOT.length));
    }
  }
  assert.deepEqual(offenders, []);
});

test("final search: descriptor schema validation has one canonical owner", () => {
  // The compatibility adapter must stay schema-free (also asserted in
  // rez-core's corpus test); no OTHER file may grow a descriptor field
  // allowlist.
  const offenders = [];
  for (const file of allSourceFiles()) {
    if (file.endsWith(join("objects", "relay", "RelayDescriptorV1.js"))) continue;
    const text = readFileSync(file, "utf8");
    if (text.includes("allowedNodeKeys") || text.includes("allowedMetaKeys")
      || (text.includes('"nickname"') && text.includes("allowedKeys"))) {
      offenders.push(file.slice(REPO_ROOT.length));
    }
  }
  assert.deepEqual(offenders, []);
});

test("final search: rez-core remains dependency-free; no package gained a dependency", () => {
  const expectations = {
    "rez-core": [],
    "rez-sdk": ["@rezprotocol/core"],
    "rez-node": ["@rezprotocol/core", "@rezprotocol/sdk", "ioredis", "pg", "ws"],
  };
  for (const [pkg, expected] of Object.entries(expectations)) {
    const manifest = JSON.parse(readFileSync(join(REPO_ROOT, pkg, "package.json"), "utf8"));
    const deps = Object.keys(manifest.dependencies || {}).sort();
    assert.deepEqual(deps, expected.sort(), pkg + " dependencies changed");
  }
});

// ---------------------------------------------------------------------------
// Two-configuration equivalence
// ---------------------------------------------------------------------------

function makeDescriptor(relayKeyId, nowMs) {
  return new RelayDescriptorV1({
    relayKeyId,
    endpoints: [{ host: "127.0.0.1", port: 4900 }],
    onionKeys: [new OnionKeyRecordV1({
      onionKeyId: relayKeyId + "-onion",
      publicKeyBytes: new Uint8Array(32).fill(7),
      format: "raw",
      createdAt: nowMs - 1000, notBefore: nowMs - 1000, notAfter: nowMs + 60_000, status: "active",
    })],
    expiresAt: nowMs + 60_000,
    nowMs,
    meta: { v: 1, capabilities: { transports: ["tcp"] } },
  });
}

test("base configuration is the authority: a broken, hanging, or absent advisor yields identical selection", async () => {
  const nowMs = Date.now();
  const descriptors = ["r-a", "r-b", "r-c"].map((id) => makeDescriptor(id, nowMs));
  const seq = () => { let i = 0; const s = [1, 0]; return () => s[(i += 1) - 1] || 0; };

  const base = await new GatewayRelaySelector({ rng: seq() })
    .selectRanked({ descriptors, minHops: 2, maxHops: 2, nowMs });

  class BrokenAdvisor extends RouteAdvisor {
    async adviseOrder() { throw new Error("boom"); }
  }
  class HangingAdvisor extends RouteAdvisor {
    async adviseOrder() { return new Promise(() => {}); }
  }
  for (const advisor of [new BrokenAdvisor(), new HangingAdvisor()]) {
    const withDouble = await new GatewayRelaySelector({
      rng: seq(), advisor, advisorMode: "advisory", advisorDeadlineMs: 30,
    }).selectRanked({ descriptors, minHops: 2, maxHops: 2, nowMs });
    assert.deepEqual(
      withDouble.map((d) => d.relayKeyId),
      base.map((d) => d.relayKeyId),
      "double must not change a base scenario",
    );
  }
});

test("MeshCoordinator retained only mesh lifecycle responsibilities (final review)", () => {
  // Same pinned surface as the P0.1 baseline: any acquired advisor/work-
  // scheduling/settlement/trust/storage-repair method would change this list.
  assert.deepEqual(Object.getOwnPropertyNames(MeshCoordinator.prototype).sort(), [
    "_clearStartupRetry",
    "_emitStatusChanged",
    "_needsStartupRetry",
    "_scheduleStartupRetryIfNeeded",
    "_syncRouteState",
    "connectNewPeers",
    "constructor",
    "getStatus",
    "onStatusChanged",
    "refresh",
    "refreshSeedReachabilityFromConnections",
    "refreshSeedReachabilityFromStore",
    "setDescriptorExchange",
    "setOnSyncTick",
    "start",
    "stop",
  ]);
});
