import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync, existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { request as httpsRequest } from "node:https";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { validateConfig } from "../src/app/NodeConfigValidator.js";

// Track 2 — TLS on the client-facing WS listener.
//
// A hosted node accepting stranger registrations carries claim signatures and session traffic that
// must not travel in the clear. TLS is CONFIGURED rather than forced, because terminating at a load
// balancer is a legitimate deployment — but the configuration must be unambiguous: half a TLS block
// is rejected, unreadable credentials refuse to start, and there is no path where a node believes
// it is serving TLS while actually serving plaintext.

// A throwaway self-signed cert for a real handshake. node:crypto cannot mint an X509 cert, so this
// shells out to openssl and returns null when it is unavailable — the live test then skips rather
// than pretending to have proven something.
function makeSelfSignedCert(dir) {
  const keyPath = join(dir, "key.pem");
  const certPath = join(dir, "cert.pem");
  try {
    execFileSync("openssl", [
      "req", "-x509", "-newkey", "rsa:2048", "-nodes",
      "-keyout", keyPath, "-out", certPath,
      "-days", "1", "-subj", "/CN=localhost",
      "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ], { stdio: "ignore" });
  } catch {
    return null;
  }
  if (!existsSync(keyPath) || !existsSync(certPath)) return null;
  return { keyPath, certPath };
}

test("config: a partial TLS block is REJECTED rather than silently falling back to plaintext", () => {
  const base = (tls) => ({
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws", ...(tls === undefined ? {} : { tls }) },
      network: { knownRelays: [] },
      storage: { dataDir: "/tmp/x" },
    },
  });

  // Half a TLS block is far more likely a deployment mistake than an intention, and the silent
  // plaintext fallback is exactly what this option exists to prevent.
  assert.throws(() => validateConfig(base({ keyPath: "/tmp/k.pem" })), /both config\.node\.ws\.tls\.keyPath and \.certPath/);
  assert.throws(() => validateConfig(base({ certPath: "/tmp/c.pem" })), /both config\.node\.ws\.tls\.keyPath and \.certPath/);
  assert.throws(() => validateConfig(base({ keyPath: "  ", certPath: "/tmp/c.pem" })), /both config\.node\.ws\.tls/);
  assert.throws(() => validateConfig(base("yes")), /object config\.node\.ws\.tls/);
});

test("config: omitting TLS resolves to an explicit null, not undefined", () => {
  // Downstream branches on `resolved.ws.tls` — an absent key would make "not configured" and
  // "malformed" indistinguishable at the call site.
  const resolved = validateConfig({
    node: {
      ws: { host: "127.0.0.1", port: 0, path: "/ws" },
      network: { knownRelays: [] },
      storage: { dataDir: "/tmp/x" },
    },
  });
  assert.equal(resolved.ws.tls, null);
});

test("config: a complete TLS block is carried through, including an optional CA", () => {
  const resolved = validateConfig({
    node: {
      ws: {
        host: "0.0.0.0",
        port: 8443,
        path: "/ws",
        tls: { keyPath: " /etc/rez/key.pem ", certPath: "/etc/rez/cert.pem", caPath: "/etc/rez/ca.pem" },
      },
      network: { knownRelays: [] },
      storage: { dataDir: "/tmp/x" },
    },
  });
  assert.deepEqual(resolved.ws.tls, {
    keyPath: "/etc/rez/key.pem",
    certPath: "/etc/rez/cert.pem",
    caPath: "/etc/rez/ca.pem",
  });
});

test("the server reports which transport it is on", () => {
  const runtime = {};
  const protocolFactory = () => ({ start() {} });
  const plain = new WsGatewayServer({ runtime, protocolFactory, port: 0 });
  assert.equal(plain.tlsEnabled, false, "no tls config ⇒ plaintext");

  const secure = new WsGatewayServer({
    runtime,
    protocolFactory,
    port: 0,
    tls: { keyPath: "/nonexistent/key.pem", certPath: "/nonexistent/cert.pem", caPath: null },
  });
  assert.equal(secure.tlsEnabled, true, "tls config ⇒ wss");
});

test("UNREADABLE credentials refuse to start — never a quiet plaintext fallback", async () => {
  // The failure this guards: a wrong cert path turning a node that its operator believes is
  // serving wss into one serving stranger registrations in the clear.
  const server = new WsGatewayServer({
    runtime: {},
    protocolFactory: () => ({ start() {} }),
    host: "127.0.0.1",
    port: 0,
    tls: { keyPath: "/nonexistent/key.pem", certPath: "/nonexistent/cert.pem", caPath: null },
  });
  await assert.rejects(
    () => server.start(),
    (err) => /cannot read TLS credentials/.test(err.message) && /Refusing to start/.test(err.message),
  );
  assert.equal(server.httpServer, null, "no listener was created");
});

test("a MALFORMED credential file also refuses to start", async () => {
  const dir = mkdtempSync(join(tmpdir(), "rez-tls-"));
  try {
    const keyPath = join(dir, "key.pem");
    const certPath = join(dir, "cert.pem");
    writeFileSync(keyPath, "not a key");
    writeFileSync(certPath, "not a cert");
    const server = new WsGatewayServer({
      runtime: {},
      protocolFactory: () => ({ start() {} }),
      host: "127.0.0.1",
      port: 0,
      tls: { keyPath, certPath, caPath: null },
    });
    // Readable but unusable: node's TLS server construction rejects it. Either way the node does
    // not come up serving plaintext.
    await assert.rejects(() => server.start());
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("plaintext start still works (local dev / termination upstream)", async () => {
  const server = new WsGatewayServer({
    runtime: {},
    protocolFactory: () => ({ start() {} }),
    host: "127.0.0.1",
    port: 0,
  });
  await server.start();
  try {
    assert.equal(server.tlsEnabled, false);
    assert.ok(server.httpServer, "a listener came up");
  } finally {
    await server.stop();
  }
});

test("LIVE: the listener actually completes a TLS handshake and serves /health over https", async (t) => {
  // The config and failure paths above prove the node refuses to lie about its transport. This
  // proves the transport works: a real client, a real handshake, a real response.
  const dir = mkdtempSync(join(tmpdir(), "rez-tls-live-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const pair = makeSelfSignedCert(dir);
  if (pair === null) {
    t.skip("openssl unavailable — cannot mint a throwaway cert");
    return;
  }

  const server = new WsGatewayServer({
    runtime: {},
    protocolFactory: () => ({ start() {} }),
    host: "127.0.0.1",
    port: 0,
    tls: { keyPath: pair.keyPath, certPath: pair.certPath, caPath: null },
  });
  await server.start();
  try {
    assert.equal(server.tlsEnabled, true);
    const port = server.httpServer.address().port;

    const body = await new Promise((resolve, reject) => {
      const req = httpsRequest(
        {
          host: "127.0.0.1",
          port,
          path: "/health",
          method: "GET",
          // Self-signed: trust THIS cert explicitly rather than disabling verification, so the
          // test still proves a real certificate chain was presented.
          ca: [readFileSync(pair.certPath)],
          servername: "localhost",
        },
        (res) => {
          let data = "";
          res.on("data", (chunk) => { data += chunk; });
          res.on("end", () => resolve({ status: res.statusCode, data }));
        },
      );
      req.on("error", reject);
      req.end();
    });

    assert.equal(body.status, 200, "health answered over TLS");
    assert.equal(JSON.parse(body.data).ok, true);
  } finally {
    await server.stop();
  }
});
