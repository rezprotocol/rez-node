#!/usr/bin/env node
import { runCli } from "../src/cli/index.js";

runCli(process.argv.slice(2))
  .then((code) => process.exit(code))
  .catch((err) => {
    process.stderr.write(`ERR ${err && err.message ? err.message : String(err)}\n`);
    process.exit(1);
  });
