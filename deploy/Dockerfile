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

COPY rez-core/src rez-core/src
COPY rez-sdk/src rez-sdk/src
COPY rez-node/src rez-node/src
COPY rez-node/bin rez-node/bin
COPY rez-node/scripts rez-node/scripts

WORKDIR /app/rez-node
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
    /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack \
    /usr/local/bin/yarn /usr/local/bin/yarnpkg /opt/yarn-* /root/.npm \
    && mkdir -p /data && chown -R node:node /app /data
USER node

ENV REZ_NODE_CONFIG=/etc/rez/rez-node.config.json
EXPOSE 8787 4201

CMD ["node", "src/cli/index.js", "start", "--no-control"]
