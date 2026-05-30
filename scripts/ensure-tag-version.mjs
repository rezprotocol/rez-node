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

console.log(`Version check passed: ${actual}`);
