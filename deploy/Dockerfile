# Reference image for a hosted rez-node (build context = polyrepo root).
# rez-node depends on rez-core + rez-sdk via npm workspaces, so the build copies
# those three packages from the workspace root.
FROM node:24-alpine

WORKDIR /app

# Workspace manifests first for better layer caching.
COPY package.json package-lock.json ./
COPY rez-core/package.json rez-core/package.json
COPY rez-sdk/package.json rez-sdk/package.json
COPY rez-node/package.json rez-node/package.json

RUN npm install --omit=dev

# Source.
COPY rez-core rez-core
COPY rez-sdk rez-sdk
COPY rez-node rez-node

WORKDIR /app/rez-node

# Default: start the node. The compose overrides this for the `migrate` one-shot.
CMD ["node", "src/cli/index.js", "start"]
