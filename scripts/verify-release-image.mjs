import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { isTar, readTar, unpackGzip } from "./lib/ReleaseArchive.js";
import { hasCredentialMaterial, hasPrivateFilename, isBinary } from "./lib/ReleaseContent.js";

const file = process.argv[2];
if (!file) throw new Error("Usage: node scripts/verify-release-image.mjs <docker-save.tar[.gz]>");
const archive = fs.readFileSync(file);
const files = new Map();
for (const [name, type, data] of readTar(unpackGzip(archive))) if (type === "0") files.set(name, data);
const failures = new Set();
let scannedText = 0;
let binaryFiles = 0;
let layerBlobs = 0;
let fileEntries = 0;
const runtimeConfigs = new Set();
const required = new Set([
  "usr/local/bin/node", "usr/local/bin/docker-entrypoint.sh", "bin/sh", "usr/bin/wget",
  "app/rez-node/src/cli/index.js", "app/rez-node/src/storage/pg/migrations/0001_init.sql",
  ...["core", "sdk", "node"].flatMap((repo) => ["app/rez-" + repo + "/LICENSE", "app/rez-" + repo + "/NOTICE"]),
]);
const applicationRoots = new Set(["", "node_modules", "package.json", "package-lock.json", "rez-core", "rez-sdk", "rez-node"]);

function forbidden(name) {
  return name.startsWith("app/") && !applicationRoots.has(name.split("/")[1])
    || /^(?:root|home\/[^/]+)\/(?:\.npm|\.cache|\.ssh|\.aws)(?:\/|$)/.test(name)
    || /^(?:usr\/local\/lib\/node_modules|opt\/yarn[^/]*)(?:\/|$)/.test(name)
    || /^usr\/local\/bin\/(?:npm|npx|corepack|yarn|yarnpkg)$/.test(name)
    || /^app\/rez-(?:core|sdk|node)\/(?:scripts|test|tests|docs|deploy|\.github|\.git|\.local)(?:\/|$)/.test(name)
    || /^app\/node_modules\/(?:esbuild|postject|@esbuild)(?:\/|$)/.test(name)
    || hasPrivateFilename(name)
    || /^app\/.*\.(?:pem|key)$/.test(name)
    || /^data\/.+/.test(name);
}

function inspectText(name, data) {
  if (isBinary(data)) { binaryFiles += 1; return; }
  scannedText += 1;
  if (hasCredentialMaterial(data)) failures.add("credential material in " + name);
}

for (const [name, raw] of files) {
  const data = unpackGzip(raw);
  if (isTar(data)) {
    layerBlobs += 1;
    for (const [entry, type, bytes, link] of readTar(data)) {
      if (!entry || entry === ".") continue;
      if (forbidden(entry)) failures.add("forbidden path " + entry);
      if (type === "2" && forbidden(path.posix.resolve("/", path.posix.dirname(entry), link).slice(1))) {
        failures.add("forbidden symlink target at " + entry);
      }
      required.delete(entry);
      if (type === "0" || type === "7") { fileEntries += 1; inspectText(entry, bytes); }
    }
  } else {
    inspectText("image metadata " + name, data);
    let metadata;
    try { metadata = JSON.parse(data); } catch { throw new Error("Unrecognized image payload: " + name); }
    if (metadata.config && Array.isArray(metadata.config.Env)) {
      // Docker save can include both OCI and legacy configurations. Compare
      // runtime settings rather than legacy-only empty/default fields.
      runtimeConfigs.add(createHash("sha256").update(JSON.stringify([
        metadata.config.User, metadata.config.Env.slice().sort(), metadata.config.WorkingDir,
        metadata.config.Entrypoint, metadata.config.Cmd,
        Object.keys(metadata.config.Volumes || {}).sort(), Object.keys(metadata.config.ExposedPorts || {}).sort(),
        Object.entries(metadata.config.Labels || {}).sort(),
      ])).digest("hex"));
      if (metadata.config.User !== "node") failures.add("runtime user must be node");
      for (const entry of metadata.config.Env) {
        if (!/^(?:PATH|REZ_NODE_CONFIG)=/.test(entry)) failures.add("unexpected image environment name " + entry.split("=")[0]);
      }
    }
  }
}
if (layerBlobs === 0) failures.add("no filesystem layers inspected");
if (runtimeConfigs.size !== 1) failures.add("expected exactly one distinct runtime image configuration");
for (const entry of required) failures.add("missing runtime file " + entry);
if (failures.size) {
  console.error("FAIL release image hygiene\n" + [...failures].sort().join("\n"));
  process.exitCode = 1;
} else {
  console.log("PASS release image hygiene");
  console.log("sha256=" + createHash("sha256").update(archive).digest("hex"));
  console.log("layerBlobs=" + layerBlobs + " fileEntries=" + fileEntries + " textEntries=" + scannedText + " binaryEntries=" + binaryFiles);
  console.log("Every archived filesystem layer and metadata payload inspected; no forbidden helper/cache/credential paths or token/key patterns found.");
  console.log("Binary contents are not exhaustively classified; this gate complements vulnerability and clean-source checks.");
}
