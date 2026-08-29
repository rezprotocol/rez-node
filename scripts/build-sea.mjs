import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DIST = path.join(ROOT, "dist");
const SEA_DIR = path.join(DIST, "sea");

function run(cmd, args, opts = {}) {
  const result = spawnSync(cmd, args, {
    cwd: ROOT,
    stdio: "inherit",
    ...opts,
  });
  if (result.status !== 0) {
    throw new Error(`Command failed: ${cmd} ${args.join(" ")}`);
  }
}

function currentNodeBinary() {
  return process.execPath;
}

function targetBinaryName() {
  return process.platform === "win32" ? "rez-node.exe" : "rez-node";
}

function outFolder(version) {
  return path.join(DIST, `rez-node-${version}-${process.platform}-${process.arch}`);
}

async function readVersion() {
  const pkgPath = path.join(ROOT, "package.json");
  const pkg = JSON.parse(await fs.readFile(pkgPath, "utf8"));
  return String(pkg.version || "0.0.0");
}

async function writeSeaConfig(mainPath, blobPath) {
  const cfg = {
    main: mainPath,
    output: blobPath,
    disableExperimentalSEAWarning: true,
  };
  const cfgPath = path.join(SEA_DIR, "sea-config.json");
  await fs.writeFile(cfgPath, `${JSON.stringify(cfg, null, 2)}\n`, "utf8");
  return cfgPath;
}

async function main() {
  const version = await readVersion();
  const outDir = outFolder(version);
  await fs.rm(SEA_DIR, { recursive: true, force: true });
  await fs.rm(outDir, { recursive: true, force: true });
  await fs.mkdir(SEA_DIR, { recursive: true });
  await fs.mkdir(outDir, { recursive: true });

  const bundlePath = path.join(SEA_DIR, "rez-node.bundle.cjs");
  const blobPath = path.join(SEA_DIR, "rez-node.blob");

  run("npx", [
    "esbuild",
    "./src/cli/index.js",
    "--bundle",
    "--platform=node",
    "--format=cjs",
    `--banner:js=globalThis.__REZ_NODE_SEA_VERSION__=${JSON.stringify(version)};globalThis.__REZ_NODE_MODULE_URL__=require("node:url").pathToFileURL(process.execPath).href;`,
    "--define:import.meta.url=globalThis.__REZ_NODE_MODULE_URL__",
    `--outfile=${bundlePath}`,
  ]);

  const cfgPath = await writeSeaConfig(bundlePath, blobPath);
  run(process.execPath, ["--experimental-sea-config", cfgPath]);

  const targetPath = path.join(outDir, targetBinaryName());
  await fs.copyFile(currentNodeBinary(), targetPath);
  if (process.platform !== "win32") {
    // Node installations may expose the source executable as 0555. postject
    // must be able to rewrite the copied image before it is returned to 0755.
    await fs.chmod(targetPath, 0o755);
  }

  if (process.platform === "darwin") {
    try {
      run("codesign", ["--remove-signature", targetPath]);
    } catch {
      // best effort; unsigned binaries are acceptable for local artifacts.
    }
  }

  const postjectArgs = [
    "postject",
    targetPath,
    "NODE_SEA_BLOB",
    blobPath,
    "--sentinel-fuse",
    "NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2",
  ];
  if (process.platform === "darwin") {
    postjectArgs.push("--macho-segment-name", "NODE_SEA");
  }
  run("npx", postjectArgs);

  if (process.platform === "darwin") {
    try {
      run("codesign", ["--sign", "-", targetPath]);
    } catch {
      // best effort for CI portability.
    }
  }

  if (process.platform !== "win32") {
    await fs.chmod(targetPath, 0o755);
  }

  await fs.cp(
    path.join(ROOT, "src", "storage", "pg", "migrations"),
    path.join(outDir, "migrations"),
    { recursive: true },
  );

  const readme = [
    `rez-node ${version}`,
    `platform=${process.platform}`,
    `arch=${process.arch}`,
    "",
    "Run:",
    `  ./${targetBinaryName()} version`,
  ].join("\n");
  await fs.writeFile(path.join(outDir, "README.txt"), `${readme}\n`, "utf8");

  // Match the Homebrew release smoke test locally so a non-runnable image is
  // never archived merely because injection itself returned success.
  run(targetPath, ["version"]);

  console.log(`Built SEA binary: ${targetPath}`);
}

main().catch((err) => {
  console.error((err && (err.stack || err.message)) || String(err));
  process.exit(1);
});
