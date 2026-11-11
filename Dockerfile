FROM node:24-alpine@sha256:a34ab6d7f6f4813f4d0c5469d74685604b962fbdbf5793a7bc7b4998ecb867c8

WORKDIR /app

COPY rez-node/release/workspace-package.json ./package.json
COPY rez-node/release/workspace-package-lock.json ./package-lock.json
COPY rez-core/package.json ./rez-core/package.json
COPY rez-sdk/package.json ./rez-sdk/package.json
COPY rez-node/package.json ./rez-node/package.json
RUN npm ci --omit=dev --ignore-scripts

COPY rez-core/src ./rez-core/src
COPY rez-sdk/src ./rez-sdk/src
COPY rez-node/bin ./rez-node/bin
COPY rez-node/src ./rez-node/src
COPY rez-node/scripts ./rez-node/scripts
COPY rez-node/README.md ./rez-node/README.md

WORKDIR /app/rez-node
RUN chmod +x ./bin/rez-node.js ./bin/rez-relay.js \
    && rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
    /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack \
    /usr/local/bin/yarn /usr/local/bin/yarnpkg /opt/yarn-* /root/.npm \
    && mkdir -p /data && chown -R node:node /app /data

ENV REZ_NODE_CONFIG=/etc/rez/rez-node.config.json
VOLUME ["/data"]
EXPOSE 8787 4201

USER node

CMD ["node", "src/cli/index.js", "start", "--no-control"]
