import { FsStorageProvider } from "../../src/storage/fs/FsStorageProvider.js";
const provider = new FsStorageProvider({ rootDir: process.argv[2] });
try {
  const grant = await provider.acquireRuntimeOwnership();
  process.send({ ready: true, epoch: grant.runtimeEpoch });
  process.on("message", async (message) => {
    if (message === "release") { await grant.release(); process.exit(0); }
  });
} catch (err) { process.send({ error: err.code || err.message }); process.exitCode = 1; process.disconnect(); }
