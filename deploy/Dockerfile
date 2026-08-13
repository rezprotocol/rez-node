FROM node:24-alpine

WORKDIR /app

# The hosted image is built from the polyrepo root. Copy every workspace manifest so npm can
# validate the root lock, but install only the node and its core/sdk runtime dependencies.
COPY package.json package-lock.json ./
COPY rez-core/package.json rez-core/package.json
COPY rez-sdk/package.json rez-sdk/package.json
COPY rez-node/package.json rez-node/package.json
COPY rez-chat/package.json rez-chat/package.json
COPY rez-ui/package.json rez-ui/package.json

RUN npm ci --omit=dev --workspace @rezprotocol/node --include-workspace-root=false

COPY rez-core rez-core
COPY rez-sdk rez-sdk
COPY rez-node rez-node

WORKDIR /app/rez-node
RUN mkdir -p /data && chown -R node:node /app /data
USER node

ENV REZ_NODE_CONFIG=/etc/rez/rez-node.config.json
EXPOSE 8787 4201

CMD ["node", "src/cli/index.js", "start", "--no-control"]
