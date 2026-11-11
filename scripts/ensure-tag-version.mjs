import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pkgPath = path.join(ROOT, "package.json");
const refName = process.env.GITHUB_REF_NAME || "";

if (!refName) {
  console.error("GITHUB_REF_NAME is required");
  process.exit(1);
}
if (!refName.startsWith("v")) {
  console.error(`Tag must start with v: ${refName}`);
  process.exit(1);
}

const expected = refName.slice(1);
const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
const actual = String(pkg.version || "");

if (actual !== expected) {
  console.error(`Version mismatch: tag=${expected} package.json=${actual}`);
  process.exit(1);
}

// The version tag selects the same immutable release cut in all three repos.
// Refuse a mixed checkout even if Node's own manifest matches the tag.
for (const name of ["rez-core", "rez-sdk"]) {
  const sibling = JSON.parse(fs.readFileSync(path.join(ROOT, "..", name, "package.json"), "utf8"));
  if (sibling.version !== expected || pkg.dependencies[sibling.name] !== expected) {
    console.error(`Coordinated release mismatch: ${name} must be ${expected}, including Node's dependency`);
    process.exit(1);
  }
  if (name === "rez-sdk" && sibling.dependencies["@rezprotocol/core"] !== expected) {
    console.error(`Coordinated release mismatch: SDK must depend on Core ${expected}`);
    process.exit(1);
  }
}

console.log(`Version check passed: ${actual}`);
