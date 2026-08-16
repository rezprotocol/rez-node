/**
 * P4.1 — DHT control-registry totality (ATLAS_PREREQUISITES).
 *
 * The WS wire-manifest test does not cover ControlMessageRegistry types, so
 * DHT control messages need their own guardrail: every `dht.*` control type
 * that appears in DHT protocol source (registered or emitted) must be
 * declared in DHT_CONTROL_MANIFEST exactly once, and vice versa — no silent
 * new wire surface, no dead manifest rows.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { DHT_CONTROL_MANIFEST } from "../src/contracts/dhtControlManifest.js";

// Control-type literals live in the DHT protocol classes and in the node-owned
// wire-record definitions they import their constants from.
const SCAN_DIRS = [
  fileURLToPath(new URL("../src/routing/dht", import.meta.url)),
  fileURLToPath(new URL("../src/contracts/wireRecords", import.meta.url)),
];

function dhtControlTypesInSource() {
  const found = new Set();
  for (const dir of SCAN_DIRS) {
    for (const name of readdirSync(dir)) {
      if (!name.endsWith(".js")) continue;
      const text = readFileSync(join(dir, name), "utf8");
      for (const match of text.matchAll(/"(dht\.[a-z_.]+)"/g)) {
        found.add(match[1]);
      }
    }
  }
  return found;
}

test("every dht.* control type in source is declared in DHT_CONTROL_MANIFEST", () => {
  const inSource = dhtControlTypesInSource();
  const declared = new Set(Object.keys(DHT_CONTROL_MANIFEST));
  const undeclared = [...inSource].filter((t) => !declared.has(t));
  assert.deepEqual(undeclared, [], "undeclared DHT control types (add to dhtControlManifest.js): " + undeclared.join(", "));
});

test("every manifest row corresponds to a control type actually present in source", () => {
  const inSource = dhtControlTypesInSource();
  const dead = Object.keys(DHT_CONTROL_MANIFEST).filter((t) => !inSource.has(t));
  assert.deepEqual(dead, [], "manifest rows with no source usage: " + dead.join(", "));
});

test("manifest rows are well-formed", () => {
  for (const [type, row] of Object.entries(DHT_CONTROL_MANIFEST)) {
    assert.match(type, /^dht\.[a-z_.]+$/, type);
    assert.ok(["request", "response"].includes(row.direction), type + " direction");
    assert.ok(typeof row.validatedBy === "string" && row.validatedBy.length > 0, type + " validatedBy");
    assert.ok(["DhtProtocol", "DurableRecordProtocol"].includes(row.owner), type + " owner");
  }
});
