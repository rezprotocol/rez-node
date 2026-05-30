import test from "node:test";
import assert from "node:assert/strict";
import { HttpTransport } from "../src/network/http/HttpTransport.js";

async function createTransport(endpointId) {
  const transport = new HttpTransport({
    endpointId,
    listenUrl: "http://127.0.0.1:0",
    resolve: () => "",
  });
  await transport.start();
  return transport;
}

test("HttpTransport sends bytes", async (t) => {
  const a = new HttpTransport({
    endpointId: "A",
    listenUrl: "http://127.0.0.1:0",
    resolve: () => "",
  });
  const b = new HttpTransport({
    endpointId: "B",
    listenUrl: "http://127.0.0.1:0",
    resolve: () => "",
  });

  let started = false;
  try {
    await a.start();
    await b.start();
    started = true;

    a.resolve = (to) => (to === "B" ? b.url : "");
    b.resolve = (to) => (to === "A" ? a.url : "");

    const received = [];
    b.onPacket((packet) => received.push(packet));

    const bytes = new Uint8Array([1, 2, 3, 4]);
    await a.send({ bytes, to: "B", from: "A" });

    assert.equal(received.length, 1);
    assert.deepEqual(received[0].bytes, bytes);
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("HTTP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    if (started) {
      await a.stop();
      await b.stop();
    }
  }
});
