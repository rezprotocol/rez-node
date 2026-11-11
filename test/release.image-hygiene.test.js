import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readTar } from "../scripts/lib/ReleaseArchive.js";

const SCRIPT = fileURLToPath(new URL("../scripts/verify-release-image.mjs", import.meta.url));
function tar(entries) {
  const blocks = [];
  for (const [name, content] of entries) {
    const bytes = Buffer.from(content);
    const header = Buffer.alloc(512);
    header.write(name, 0, 100);
    header.write("0000644\0", 100);
    header.write("0000000\0", 108);
    header.write("0000000\0", 116);
    header.write(bytes.length.toString(8).padStart(11, "0") + "\0", 124);
    header.write("00000000000\0", 136);
    header.fill(32, 148, 156);
    header.write("0", 156);
    header.write("ustar\0", 257);
    header.write("00", 263);
    header.write(header.reduce((sum, byte) => sum + byte, 0).toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) % 512));
  }
  return Buffer.concat([...blocks, Buffer.alloc(1024)]);
}
function image(extraLayers = [], environment = []) {
  const names = ["usr/local/bin/node", "usr/local/bin/docker-entrypoint.sh", "bin/sh", "usr/bin/wget",
    "app/rez-node/src/cli/index.js", "app/rez-node/src/storage/pg/migrations/0001_init.sql"];
  for (const repo of ["core", "sdk", "node"]) names.push("app/rez-" + repo + "/LICENSE", "app/rez-" + repo + "/NOTICE");
  const layers = [tar(names.map((name) => [name, "public runtime fixture"])), ...extraLayers];
  const config = JSON.stringify({ config: { User: "node", Env: ["PATH=/usr/bin", "REZ_NODE_CONFIG=/etc/rez/rez-node.config.json", ...environment] } });
  return tar([["config.json", config], ["manifest.json", JSON.stringify([{ Config: "config.json", Layers: layers.map((_, i) => "layer" + i + ".tar") }])], ...layers.map((bytes, i) => ["layer" + i + ".tar", bytes])]);
}
function verify(bytes) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "rez-image-hygiene-"));
  try {
    const file = path.join(root, "image.tar");
    fs.writeFileSync(file, bytes);
    return spawnSync(process.execPath, [SCRIPT, file], { encoding: "utf8", timeout: 15_000 });
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}

test("image inspection accepts a complete runtime-only archive", () => {
  const result = verify(image());
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /PASS release image hygiene/);
});

test("image inspection rejects cache and credentials hidden beneath deletion layers without printing values", () => {
  const secret = "npm_" + "x".repeat(36);
  const result = verify(image([tar([["root/.npm/debug.log", secret]]), tar([["root/.wh..npm", ""]])]));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /forbidden path root\/\.npm\/debug.log/);
  assert.match(result.stderr, /credential material/);
  assert.ok(!result.stderr.includes(secret));
});

test("image inspection rejects secret environment metadata without printing values", () => {
  const secret = "private-test-value-never-print";
  const result = verify(image([], ["PRIVATE_TOKEN=" + secret]));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /unexpected image environment name PRIVATE_TOKEN/);
  assert.ok(!result.stderr.includes(secret));
});

test("archive reader rejects corruption, traversal and truncated payloads", () => {
  const corrupt = tar([["ok.txt", "hello"]]);
  corrupt[0] ^= 1;
  assert.throws(() => [...readTar(corrupt)], /checksum/);
  assert.throws(() => [...readTar(tar([["../escape.txt", "hello"]]))], /Unsafe archive path/);
  assert.throws(() => [...readTar(tar([["ok.txt", "hello"]]).subarray(0, 514))], /Truncated tar entry/);
});

function packageArchive(extra = []) {
  return tar([
    ["package/package.json", JSON.stringify({ name: "@rezprotocol/core", version: "0.6.0-rc.13" })],
    ["package/LICENSE", "license fixture"], ["package/NOTICE", "notice fixture"],
    ["package/README.md", "public documentation"], ["package/src/index.js", "export const ready = true;"],
    ...extra,
  ]);
}

test("package inspection accepts public source and legal notices", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "rez-package-hygiene-"));
  try {
    const file = path.join(root, "package.tar");
    fs.writeFileSync(file, packageArchive());
    const script = fileURLToPath(new URL("../scripts/verify-release-package.mjs", import.meta.url));
    const result = spawnSync(process.execPath, [script, file], { encoding: "utf8", timeout: 15_000 });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /PASS release package hygiene/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test("package inspection rejects reintroduced helper scripts", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "rez-package-hygiene-"));
  try {
    const file = path.join(root, "package.tar");
    fs.writeFileSync(file, packageArchive([["package/scripts/test-support/proof.mjs", "public test-only code"]]));
    const script = fileURLToPath(new URL("../scripts/verify-release-package.mjs", import.meta.url));
    const result = spawnSync(process.execPath, [script, file], { encoding: "utf8", timeout: 15_000 });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /forbidden package entry package\/scripts\/test-support\/proof.mjs/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
