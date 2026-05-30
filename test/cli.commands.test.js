import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";

import { runCli } from "../src/cli/index.js";

function ioCapture() {
  const stdout = [];
  const stderr = [];
  return {
    io: {
      stdout: { write: (s) => void stdout.push(String(s)) },
      stderr: { write: (s) => void stderr.push(String(s)) },
    },
    out: () => stdout.join(""),
    err: () => stderr.join(""),
  };
}

test("rez-node cli version prints semver", async () => {
  const cap = ioCapture();
  const code = await runCli(["version"], cap.io);
  assert.equal(code, 0);
  assert.match(cap.out().trim(), /^\d+\.\d+\.\d+/);
});

test("rez-node cli init creates config and doctor validates", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-node-cli-"));
  const configPath = path.join(dir, "rez-node.config.json");
  const dataDir = path.join(dir, "data");

  const initCap = ioCapture();
  const initCode = await runCli(["init", "--config", configPath, "--data-dir", dataDir], initCap.io);
  assert.equal(initCode, 0);
  assert.match(initCap.out(), /OK config=/);
  assert.match(initCap.out(), /OK dataDir=/);

  const raw = JSON.parse(await fs.readFile(configPath, "utf8"));
  raw.node.ws.port = "bad";
  await fs.writeFile(configPath, `${JSON.stringify(raw, null, 2)}\n`, "utf8");

  const doctorCap = ioCapture();
  const doctorCode = await runCli(["doctor", "--config", configPath], doctorCap.io);
  assert.equal(doctorCode, 1);
  assert.match(doctorCap.err(), /DOCTOR_FAIL/);
  assert.match(doctorCap.err(), /invalid ws port/);
});
