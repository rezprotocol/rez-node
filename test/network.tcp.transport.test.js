import test from "node:test";
import assert from "node:assert/strict";
import { TcpTransport } from "../src/network/tcp/TcpTransport.js";
import { encodeFrame, createFrameDecoder, MAX_FRAME_BYTES } from "../src/network/tcp/TcpFraming.js";

function makeTransport(id, resolveRef = { current: () => ({ host: "127.0.0.1", port: 0 }) }) {
  return new TcpTransport({
    endpointId: id,
    listenHost: "127.0.0.1",
    listenPort: 0,
    resolve: (to) => resolveRef.current(to),
  });
}

async function waitForListenAddress(transport, { timeoutMs = 3000, intervalMs = 20 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const addr = transport.getListenAddress();
    if (addr && Number.isInteger(addr.port) && addr.port > 0) return addr;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error("TcpTransport did not bind to a port in time");
}

test("TcpFraming encodes and decodes frames", () => {
  const frames = [];
  const decoder = createFrameDecoder((bytes) => frames.push(bytes));

  const payload1 = new Uint8Array([1, 2, 3]);
  const payload2 = new Uint8Array([4, 5]);
  const data = Buffer.concat([encodeFrame(payload1), encodeFrame(payload2)]);

  decoder.push(data);

  assert.equal(frames.length, 2);
  assert.deepEqual(frames[0], payload1);
  assert.deepEqual(frames[1], payload2);
});

test("TcpFraming enforces max frame size", () => {
  const decoder = createFrameDecoder(() => {});
  const tooLarge = MAX_FRAME_BYTES + 1;
  const header = Buffer.alloc(4);
  header.writeUInt32BE(tooLarge, 0);
  assert.throws(() => decoder.push(header), /max size/);
});

test("TcpTransport sends and receives", async (t) => {
  const resolveA = { current: () => ({ host: "127.0.0.1", port: 0 }) };
  const a = makeTransport("A", resolveA);
  const b = makeTransport("B");
  const cleanup = async () => {
    await a.stop();
    await b.stop();
  };
  t.after(cleanup);

  let started = false;
  try {
    await a.start();
    await b.start();
    started = true;

    const bAddr = await waitForListenAddress(b);
    resolveA.current = () => bAddr;

    const received = [];
    b.onPacket((packet) => received.push(packet));

    await a.send({ to: "B", bytes: new Uint8Array([1, 2, 3]) });

    for (let i = 0; i < 50 && received.length < 1; i++) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    assert.equal(received.length, 1);
    assert.deepEqual(received[0].bytes, new Uint8Array([1, 2, 3]));
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});

test("TcpTransport multiple frames", async (t) => {
  const resolveA = { current: () => ({ host: "127.0.0.1", port: 0 }) };
  const a = makeTransport("A", resolveA);
  const b = makeTransport("B");
  const cleanup = async () => {
    await a.stop();
    await b.stop();
  };
  t.after(cleanup);

  let started = false;
  try {
    await a.start();
    await b.start();
    started = true;

    const bAddr = await waitForListenAddress(b);
    resolveA.current = () => bAddr;

    const received = [];
    b.onPacket((packet) => received.push(packet));

    for (let i = 0; i < 5; i++) {
      await a.send({ to: "B", bytes: new Uint8Array([i]) });
    }

    for (let i = 0; i < 50 && received.length < 5; i++) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    assert.equal(received.length, 5);
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("TCP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  }
});
