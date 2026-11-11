import fs from "node:fs";
import { createHash } from "node:crypto";
import { readTar, unpackGzip } from "./lib/ReleaseArchive.js";
import { hasCredentialMaterial, hasPrivateFilename } from "./lib/ReleaseContent.js";

const file = process.argv[2];
if (!file) throw new Error("Usage: node scripts/verify-release-package.mjs <package.tgz>");
const archive = fs.readFileSync(file);
const entries = [...readTar(unpackGzip(archive))];
const manifest = entries.find(([name]) => name === "package/package.json");
if (!manifest) throw new Error("Package manifest is missing");
const pkg = JSON.parse(manifest[2]);
if (!["@rezprotocol/core", "@rezprotocol/sdk", "@rezprotocol/node"].includes(pkg.name)) throw new Error("Unexpected release package");
const failures = new Set();
const required = new Set(["package/package.json", "package/README.md", "package/LICENSE", "package/NOTICE"]);
let files = 0;
for (const [name, type, bytes] of entries) {
  if (type === "5") continue;
  files += 1;
  const local = name.replace(/^package\//, "");
  const allowed = ["package.json", "README.md", "LICENSE", "NOTICE"].includes(local)
    || local.startsWith("src/")
    || pkg.name === "@rezprotocol/node" && local.startsWith("bin/")
    || pkg.name === "@rezprotocol/sdk" && local.startsWith("examples/");
  if (!name.startsWith("package/") || type !== "0" || !allowed || hasPrivateFilename(local) || /\.(?:pem|key)$/.test(local)) {
    failures.add("forbidden package entry " + name);
  }
  if (hasCredentialMaterial(bytes)) failures.add("credential material in " + name);
  required.delete(name);
}
for (const name of required) failures.add("missing package file " + name);
if (failures.size) {
  console.error("FAIL release package hygiene\n" + [...failures].sort().join("\n"));
  process.exitCode = 1;
} else {
  console.log("PASS release package hygiene " + pkg.name + "@" + pkg.version + " files=" + files);
  console.log("sha512=" + createHash("sha512").update(archive).digest("base64"));
}
