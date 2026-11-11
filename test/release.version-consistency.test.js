import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

test("release gate rejects mismatched sibling versions and dependency ranges", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "rez-release-gate-"));
  const version = "0.6.0-rc.11";
  try {
    for (const repo of ["rez-core", "rez-sdk", "rez-node"]) await fs.mkdir(path.join(root, repo), { recursive: true });
    await fs.mkdir(path.join(root, "rez-node/scripts"));
    const script = path.join(root, "rez-node/scripts/ensure-tag-version.mjs");
    await fs.copyFile(new URL("../scripts/ensure-tag-version.mjs", import.meta.url), script);
    const write = (repo, value) => fs.writeFile(path.join(root, repo, "package.json"), JSON.stringify(value));
    const core = { name: "@rezprotocol/core", version };
    const sdk = { name: "@rezprotocol/sdk", version, dependencies: { "@rezprotocol/core": version } };
    const node = { name: "@rezprotocol/node", version, dependencies: { "@rezprotocol/core": version, "@rezprotocol/sdk": version } };
    await Promise.all([write("rez-core", core), write("rez-sdk", sdk), write("rez-node", node)]);
    const run = (refName = "v" + version) => spawnSync(process.execPath, [script], { encoding: "utf8", env: { ...process.env, GITHUB_REF_NAME: refName } });
    assert.equal(run().status, 0);
    assert.equal(run("npm/v" + version).status, 0);
    assert.match(run("npm/v0.6.0-rc.10").stderr, /Version mismatch/);
    assert.match(run("release/" + version).stderr, /Tag must start with v or npm\/v/);
    await write("rez-core", { ...core, version: "0.6.0-rc.10" });
    assert.match(run().stderr, /Coordinated release mismatch/);
    await write("rez-core", core);
    await write("rez-sdk", { ...sdk, dependencies: { "@rezprotocol/core": "0.6.0-rc.10" } });
    assert.match(run().stderr, /SDK must depend on Core/);
    await write("rez-sdk", sdk);
    await write("rez-node", { ...node, dependencies: { ...node.dependencies, "@rezprotocol/sdk": "0.6.0-rc.10" } });
    assert.match(run().stderr, /Coordinated release mismatch/);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
