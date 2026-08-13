FROM node:24-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi

COPY bin ./bin
COPY src ./src
COPY README.md ./README.md

RUN chmod +x ./bin/rez-node.js ./bin/rez-relay.js ./bin/rez-directory.js

ENV REZ_NODE_CONFIG=/data/rez-node.config.json
VOLUME ["/data"]
EXPOSE 8787

USER node

ENTRYPOINT ["node", "./bin/rez-node.js", "start"]
