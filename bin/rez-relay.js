#!/usr/bin/env node

process.stderr.write(
  "rez-relay is no longer a standalone runtime. Use `rez-node start --config <relay-only-config> --no-control`.\n",
);
process.exit(1);
