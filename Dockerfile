FROM node:24-alpine

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
COPY rez-node/README.md ./rez-node/README.md

RUN chmod +x ./rez-node/bin/rez-node.js ./rez-node/bin/rez-relay.js ./rez-node/bin/rez-directory.js

ENV REZ_NODE_CONFIG=/data/rez-node.config.json
VOLUME ["/data"]
EXPOSE 8787

USER node

ENTRYPOINT ["node", "./rez-node/bin/rez-node.js", "start"]
