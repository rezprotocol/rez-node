import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const root = process.env.REZ_RELEASE_PROOF_ROOT || "/tmp/rez-network-proof-" + Date.now();
const image = process.env.REZ_RELEASE_IMAGE;
const port = Number(process.env.REZ_RELEASE_PROOF_PORT_BASE || 58540);
if (!/^\/tmp\/rez-network-proof-[0-9]+$/.test(root) || fs.existsSync(root)) throw new Error("A new isolated /tmp/rez-network-proof-N directory is required");
if (!image) throw new Error("REZ_RELEASE_IMAGE is required");
if (!Number.isInteger(port) || port < 1024 || port > 65530) throw new Error("Invalid proof port base");
const deploy = path.join(root, "rez-node/deploy");
fs.mkdirSync(deploy, { recursive: true, mode: 0o700 });
execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost",
  "-addext", "subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1", "-keyout", root + "/key.pem", "-out", root + "/cert.pem"], { stdio: "ignore" });
// These are disposable test-only keys. Read-only file mounts let the non-root
// image read them without changing production TLS permissions or credentials.
fs.chmodSync(root + "/key.pem", 0o644);
fs.copyFileSync(source + "/deploy/rez-node.config.json", deploy + "/rez-node.config.json");
fs.copyFileSync(source + "/deploy/nginx.conf", deploy + "/nginx.conf");
const node = {
  image, platform: "linux/amd64",
  environment: {
    REZ_NODE_CONFIG: "/etc/rez/rez-node.config.json", REZ_STORAGE_BACKEND: "postgres",
    REZ_PG_URL: "postgres://rez:rez-proof-only@postgres:5432/rez",
    REZ_STORAGE_ENCRYPTION_KEY: "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=",
    REZ_REDIS_URL: "redis://redis:6379", REZ_ADVERTISED_HOST: "localhost",
    REZ_ADVERTISED_PORT: String(port + 4), REZ_REQUIRE_KNOWN_RELAYS: "0",
  },
  volumes: [deploy + "/rez-node.config.json:/etc/rez/rez-node.config.json:ro", root + "/cert.pem:/etc/rez/tls/fullchain.pem:ro", root + "/key.pem:/etc/rez/tls/privkey.pem:ro"],
  networks: ["edge", "data"],
  depends_on: { migrate: { condition: "service_completed_successfully" }, redis: { condition: "service_healthy" } },
  healthcheck: { test: ["CMD", "node", "-e", 'fetch("http://127.0.0.1:8787/ready").then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))'], interval: "2s", timeout: "5s", retries: 30 },
};
const config = {
  name: path.basename(root),
  services: {
    postgres: { image: "postgres:17-alpine", environment: { POSTGRES_USER: "rez", POSTGRES_PASSWORD: "rez-proof-only", POSTGRES_DB: "rez" }, ports: ["127.0.0.1:" + (port + 1) + ":5432"], volumes: ["pgdata:/var/lib/postgresql/data"], networks: ["data", "edge"], healthcheck: { test: ["CMD-SHELL", "pg_isready -U rez -d rez"], interval: "2s", timeout: "3s", retries: 30 } },
    redis: { image: "redis:7-alpine", ports: ["127.0.0.1:" + (port + 2) + ":6379"], networks: ["data", "edge"], healthcheck: { test: ["CMD", "redis-cli", "ping"], interval: "2s", timeout: "3s", retries: 30 } },
    migrate: { image, platform: "linux/amd64", command: ["node", "src/cli/index.js", "migrate"], environment: { REZ_PG_URL: node.environment.REZ_PG_URL }, networks: ["data"], depends_on: { postgres: { condition: "service_healthy" } } },
    node1: { ...node, volumes: [...node.volumes, "node1data:/data"] },
    node2: { ...node, environment: { ...node.environment, REZ_ADVERTISED_PORT: String(port + 5) }, volumes: [...node.volumes, "node2data:/data"] },
    lb: { image: "nginx:alpine", ports: ["127.0.0.1:" + (port + 3) + ":8443"], volumes: [deploy + "/nginx.conf:/etc/nginx/nginx.conf:ro", root + "/cert.pem:/etc/nginx/tls/fullchain.pem:ro", root + "/key.pem:/etc/nginx/tls/privkey.pem:ro"], networks: ["edge"], depends_on: { node1: { condition: "service_healthy" }, node2: { condition: "service_healthy" } } },
  },
  networks: { edge: {}, data: { internal: true } }, volumes: { pgdata: {}, node1data: {}, node2data: {} },
};
fs.writeFileSync(deploy + "/docker-compose.yml", JSON.stringify(config, null, 2) + "\n");
fs.writeFileSync(deploy + "/.env", "# Disposable release proof only\n");
fs.writeFileSync(root + "/compose.override.json", JSON.stringify({ services: {
  node1: { environment: { REZ_PG_URL: node.environment.REZ_PG_URL } },
  node2: { environment: { REZ_PG_URL: node.environment.REZ_PG_URL } },
  migrate: { environment: { REZ_PG_URL: node.environment.REZ_PG_URL } },
} }) + "\n");
const declarations = [
  "REZ_RELEASE_PROOF_ROOT=" + root,
  "REZ_PG_TEST_URL=postgres://rez:rez-proof-only@127.0.0.1:" + (port + 1) + "/postgres",
  "REZ_REDIS_TEST_URL=redis://127.0.0.1:" + (port + 2),
  "REZ_HOSTED_E2E_URL=wss://localhost:" + (port + 3) + "/ws",
  "NODE_EXTRA_CA_CERTS=" + root + "/cert.pem", "RUN_LOCAL_MESH_E2E=1",
];
fs.writeFileSync(root + "/proof.env", declarations.join(os.EOL) + os.EOL);
if (process.env.GITHUB_ENV) fs.appendFileSync(process.env.GITHUB_ENV, declarations.join("\n") + "\n");
console.log(root);
