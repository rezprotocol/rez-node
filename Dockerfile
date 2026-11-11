FROM node:24-alpine@sha256:a34ab6d7f6f4813f4d0c5469d74685604b962fbdbf5793a7bc7b4998ecb867c8 AS runtime-files

WORKDIR /app

COPY rez-node/release/workspace-package.json ./package.json
COPY rez-node/release/workspace-package-lock.json ./package-lock.json
COPY rez-core/package.json ./rez-core/package.json
COPY rez-sdk/package.json ./rez-sdk/package.json
COPY rez-node/package.json ./rez-node/package.json
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund

COPY rez-core/src ./rez-core/src
COPY rez-sdk/src ./rez-sdk/src
COPY rez-node/bin ./rez-node/bin
COPY rez-node/src ./rez-node/src
COPY rez-node/README.md ./rez-node/README.md
COPY rez-core/LICENSE rez-core/NOTICE ./rez-core/
COPY rez-sdk/LICENSE rez-sdk/NOTICE ./rez-sdk/
COPY rez-node/LICENSE rez-node/NOTICE ./rez-node/

WORKDIR /app/rez-node
RUN chmod +x ./bin/rez-node.js ./bin/rez-relay.js \
    && mkdir -p /data && chown -R node:node /app /data \
    && rm -rf /usr/local/lib/node_modules \
    /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack \
    /usr/local/bin/yarn /usr/local/bin/yarnpkg /opt/yarn-* \
    /root/.npm /root/.cache /home/node/.npm /home/node/.cache /tmp/* /var/cache/apk/*

# Copy only the cleaned filesystem. The final image inherits none of the
# installation layers, including deleted npm caches and package-manager files.
FROM scratch
COPY --from=runtime-files / /

WORKDIR /app/rez-node
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    REZ_NODE_CONFIG=/etc/rez/rez-node.config.json
VOLUME ["/data"]
EXPOSE 8787 4201

USER node

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["node", "src/cli/index.js", "start", "--no-control"]
