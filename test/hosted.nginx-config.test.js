import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const config = fs.readFileSync(path.join(REPO_ROOT, "deploy", "nginx.conf"), "utf8");

test("hosted edge serves the PWA manifest with its registered media type", () => {
  const manifestLocation = config.match(/location = \/manifest\.webmanifest \{[\s\S]*?\n    \}/);
  assert.ok(manifestLocation, "manifest has an exact Nginx location");
  assert.match(manifestLocation[0], /default_type application\/manifest\+json;/);
  assert.match(manifestLocation[0], /X-Content-Type-Options "nosniff"/);
});

test("static cache locations retain the hosted security headers", () => {
  for (const marker of ["location = /config {", "location /assets/ {", "location / {"]) {
    const start = config.indexOf(marker);
    assert.notEqual(start, -1, marker + " exists");
    const end = config.indexOf("\n    }", start);
    const block = config.slice(start, end);
    assert.match(block, /Content-Security-Policy/, marker + " keeps CSP after adding Cache-Control");
    assert.match(block, /X-Content-Type-Options/, marker + " keeps nosniff after adding Cache-Control");
  }
});
