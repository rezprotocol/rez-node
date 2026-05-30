import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { buildDeployArgs, loadRelayInfo, parseRelayEndpoint } from "../../relays/update-relays.js";
import { mapKnownRelays } from "../../relays/promote-friends-test.js";
import { transformRelayInfo } from "../../relays/prepare-tls-relay-info.js";

test("update-relays parses tls endpoint", () => {
  const parsed = parseRelayEndpoint("tls://r1.rezprotocol.io:8443");
  assert.deepEqual(parsed, {
    protocol: "tls",
    host: "r1.rezprotocol.io",
    port: 8443,
  });
  assert.equal(parseRelayEndpoint("http://r1.rezprotocol.io:443"), null);
});

test("update-relays validates new relay-info schema", async (t) => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "rez-relay-info-"));
  t.after(async () => {
    await fs.rm(tempRoot, { recursive: true, force: true }).catch(() => {});
  });
  const relayInfoFile = path.join(tempRoot, "relay-info.json");
  const good = {
    relays: [{
      name: "relay-1",
      ip: "203.0.113.1",
      relayKeyId: "ws:relay1",
      publicHost: "r1.rezprotocol.io",
      publicRelayPort: 8443,
      publicDirectoryPort: 443,
      backendRelayPort: 8081,
      backendDirectoryPort: 9081,
      relayEndpoint: "tls://r1.rezprotocol.io:8443",
      directoryUrl: "https://r1.rezprotocol.io",
    }],
  };

  await fs.writeFile(relayInfoFile, `${JSON.stringify(good, null, 2)}\n`, "utf8");
  const relays = loadRelayInfo(relayInfoFile);
  assert.equal(relays.length, 1);
  assert.equal(relays[0].publicRelayPort, 8443);

  const bad = {
    relays: [{ ...good.relays[0], relayEndpoint: "tls://wrong.example:8443" }],
  };
  await fs.writeFile(relayInfoFile, `${JSON.stringify(bad, null, 2)}\n`, "utf8");
  assert.throws(() => loadRelayInfo(relayInfoFile), /relayEndpoint must match publicHost/);
});

test("update-relays buildDeployArgs carries public/backend ports and peer JSON", () => {
  const config = {
    sshUser: "root",
    sshKeyPath: "/tmp/key.pem",
    letsencryptEmail: "support@rezprotocol.io",
    skipCertbot: true,
  };
  const relays = [
    {
      name: "relay-1",
      ip: "203.0.113.1",
      relayKeyId: "ws:relay1",
      publicHost: "r1.rezprotocol.io",
      publicRelayPort: 8443,
      publicDirectoryPort: 443,
      backendRelayPort: 8081,
      backendDirectoryPort: 9081,
      relayEndpoint: "tls://r1.rezprotocol.io:8443",
      directoryUrl: "https://r1.rezprotocol.io",
    },
    {
      name: "relay-2",
      ip: "203.0.113.2",
      relayKeyId: "ws:relay2",
      publicHost: "r2.rezprotocol.io",
      publicRelayPort: 8443,
      publicDirectoryPort: 443,
      backendRelayPort: 8082,
      backendDirectoryPort: 9082,
      relayEndpoint: "tls://r2.rezprotocol.io:8443",
      directoryUrl: "https://r2.rezprotocol.io",
    },
  ];

  const args = buildDeployArgs(config, relays[0], relays);
  assert.ok(args.includes("--public-relay-port"));
  assert.ok(args.includes("--backend-directory-port"));
  assert.ok(args.includes("--skip-certbot"));
  const jsonIndex = args.indexOf("--peer-relays-json");
  assert.ok(jsonIndex > -1);
  const peerJson = JSON.parse(args[jsonIndex + 1]);
  assert.equal(peerJson.length, 1);
  assert.equal(peerJson[0].relayKeyId, "ws:relay2");
});

test("promote-relays maps tls relayEndpoint into knownRelays tls flag", () => {
  const known = mapKnownRelays([
    {
      relayKeyId: "ws:relay1",
      relayEndpoint: "tls://r1.rezprotocol.io:8443",
      directoryUrl: "https://r1.rezprotocol.io",
    },
  ]);
  assert.equal(known.length, 1);
  assert.equal(known[0].transport, "tcp");
  assert.equal(known[0].tls, true);
  assert.equal(known[0].host, "r1.rezprotocol.io");
  assert.equal(known[0].port, 8443);
});

test("prepare-tls-relay-info transforms backend/public topology", () => {
  const transformed = transformRelayInfo({
    relays: [{
      name: "relay-1",
      ip: "203.0.113.1",
      relayKeyId: "ws:relay1",
      relayPort: 8081,
      directoryPort: 9081,
    }],
  }, {
    publicHosts: ["r1.rezprotocol.io"],
    publicRelayPort: 8443,
    publicDirectoryPort: 443,
  });

  assert.equal(transformed.relays.length, 1);
  assert.equal(transformed.relays[0].backendRelayPort, 8081);
  assert.equal(transformed.relays[0].backendDirectoryPort, 9081);
  assert.equal(transformed.relays[0].relayEndpoint, "tls://r1.rezprotocol.io:8443");
  assert.equal(transformed.relays[0].directoryUrl, "https://r1.rezprotocol.io");
});
