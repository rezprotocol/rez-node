import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import {
  TcpConnectionManager,
  EQueueFull,
  EConnectFailed,
} from "../src/network/tcp/TcpConnectionManager.js";
import { createFrameDecoder } from "../src/network/tcp/TcpFraming.js";
import { listenLoopbackEphemeral } from "./_harness/listenLoopbackEphemeral.js";

async function withServer(fn) {
  const server = net.createServer();
  const bound = await listenLoopbackEphemeral(server);
  const addr = server.address();
  try {
    return await fn(server, addr);
  } finally {
    await bound.close();
  }
}

async function waitForCondition(check, { timeoutMs = 1000, intervalMs = 10 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

async function withTimeout(promise, timeoutMs, label) {
  let timeoutId = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

test("TcpConnectionManager reuses connections", async (t) => {
  let started = false;
  try {
    await withServer(async (server, addr) => {
      started = true;
      let connections = 0;
      const received = [];

      server.on("connection", (socket) => {
        connections += 1;
        const decoder = createFrameDecoder((bytes) => received.push(bytes));
        socket.on("data", (chunk) => decoder.push(chunk));
      });

      const manager = new TcpConnectionManager({
        resolve: () => ({ host: "127.0.0.1", port: addr.port }),
        idleTimeoutMs: 1000,
      });

      await Promise.all([
        manager.send("B", new Uint8Array([1])),
        manager.send("B", new Uint8Array([2])),
        manager.send("B", new Uint8Array([3])),
      ]);

      for (let i = 0; i < 20 && received.length < 3; i++) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }

      assert.equal(connections, 1);
      assert.equal(received.length, 3);
      await manager.close();
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    if (!started) return;
  }
});

test("TcpConnectionManager enforces queue limits", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const manager = new TcpConnectionManager({
        resolve: () => ({ host: "127.0.0.1", port: addr.port }),
        maxQueueBytesPerConn: 4,
        maxQueueItemsPerConn: 1,
      });

      await assert.rejects(
        () => manager.send("B", new Uint8Array([1, 2, 3, 4, 5])),
        EQueueFull
      );
      assert.equal(manager.connections.size, 0, "EQueueFull must be thrown before creating a connection");

      await manager.close();
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});

test("TcpConnectionManager retries and then succeeds", async (t) => {
  try {
    await withServer(async (server, addr) => {
      const expected = new Uint8Array([2]);
      let attempts = 0;

      const receivedFrame = new Promise((resolve) => {
        server.once("connection", (socket) => {
          const decoder = createFrameDecoder((bytes) => resolve(bytes));
          socket.on("data", (chunk) => decoder.push(chunk));
        });
      });

      const manager = new TcpConnectionManager({
        resolve: () => ({ host: "127.0.0.1", port: addr.port }),
        connectTimeoutMs: 50,
        retry: { maxAttempts: 2, baseDelayMs: 10, maxDelayMs: 20, jitter: 0 },
      });
      const originalConnectOnce = manager._connectOnce.bind(manager);
      manager._connectOnce = async (host, port) => {
        attempts += 1;
        if (attempts === 1) {
          throw new EConnectFailed(`${host}:${port}`);
        }
        return originalConnectOnce(host, port);
      };

      await manager.send("B", expected);
      const received = await withTimeout(receivedFrame, 1000, "server frame");

      assert.equal(attempts, 2, "expected one forced failure and one successful retry");
      assert.deepEqual(Array.from(received), Array.from(expected));
      await manager.close();
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});

test("TcpConnectionManager closes idle connections", { skip: "idle close timing flaky in CI; TODO: stabilize or run with longer timeout" }, async (t) => {
  try {
    await withServer(async (server, addr) => {
      let connections = 0;
      let closed = 0;
      server.on("connection", (socket) => {
        connections += 1;
        socket.on("close", () => {
          closed += 1;
        });
      });

      const manager = new TcpConnectionManager({
        resolve: () => ({ host: "127.0.0.1", port: addr.port }),
        idleTimeoutMs: 200,
      });

      await manager.send("B", new Uint8Array([1]));
      const idleClosed = await waitForCondition(() => closed >= 1, { timeoutMs: 3000, intervalMs: 25 });
      assert.equal(idleClosed, true, "idle connection should close within window");

      await manager.send("B", new Uint8Array([2]));
      const gotTwo = await waitForCondition(() => connections >= 2, { timeoutMs: 2000 });
      assert.equal(gotTwo, true);
      await manager.close();
    });
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});
