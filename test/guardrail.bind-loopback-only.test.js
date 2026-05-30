import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(TEST_DIR, "../..");
const GUARDRAILS_SCRIPT = path.join(REPO_ROOT, "scripts", "guardrails.mjs");

test("guardrails wrapper: use canonical tools/guardrails engine", () => {
  const result = spawnSync(process.execPath, [GUARDRAILS_SCRIPT], {
    cwd: REPO_ROOT,
    encoding: "utf8",
  });

  assert.equal(
    result.status,
    0,
    [
      "Root guardrails failed.",
      result.stdout || "",
      result.stderr || "",
    ].join("\n")
  );
});
