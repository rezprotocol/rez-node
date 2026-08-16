import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// DT-003 boundary guardrail (frozen delivery-transports plan, Phase 0).
// WHAT THIS ENFORCES: rez-node never holds foreign-carrier credentials or
// carrier protocol code. The node is the RezNet control/delivery plane;
// carrier adapters (and their credentials) are an rez-sdk client concern.
// This also enforces the scope-lock non-goal: no global email-address/DHT
// index — an email address must never become a DHT key or node-side lookup.
// WHAT IS DELIBERATELY NOT ENFORCED: the word "credential" itself (TLS
// credentials for the WS gateway are legitimate node configuration).
// See rez-core/docs/adr/ADR-DELIVERY-TRANSPORT-LAYERS.md.

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const NODE_ROOT = path.resolve(__dirname, "..");

const CARRIER_PATTERN = /smtp|imap|nodemailer|mailparser|\bpop3\b|(^|[^a-z])e-?mail/i;

function walk(dir, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full, out);
    else if (entry.isFile() && full.endsWith(".js")) out.push(full);
  }
  return out;
}

test("rez-node src contains no foreign-carrier vocabulary or credential surface", () => {
  const violations = [];
  for (const file of walk(path.join(NODE_ROOT, "src"))) {
    const src = fs.readFileSync(file, "utf8");
    const lines = src.split("\n");
    for (let i = 0; i < lines.length; i++) {
      if (CARRIER_PATTERN.test(lines[i])) {
        violations.push(path.relative(NODE_ROOT, file) + ":" + (i + 1) + "  " + lines[i].trim());
      }
    }
  }
  assert.deepEqual(violations, [],
    "Foreign-carrier vocabulary appeared in rez-node. Carrier adapters and their "
    + "credentials belong in rez-sdk behind RDeliveryTransport; the node stays "
    + "carrier-blind. See rez-core/docs/adr/ADR-DELIVERY-TRANSPORT-LAYERS.md.\n"
    + violations.join("\n"));
});
