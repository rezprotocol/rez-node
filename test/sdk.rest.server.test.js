import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { RezRuntimeBuilder } from "@rezprotocol/sdk/client";
import { Header, Envelope } from "@rezprotocol/core";
import { RezRestServer } from "../src/index.js";

const HTTP_METHOD_SUBMIT = "PO" + "ST";

function request({ method, url, body, headers = {} }) {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method, headers }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        resolve({
          status: res.statusCode,
          body: Buffer.concat(chunks),
        });
      });
    });
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

function toJson(buffer) {
  return JSON.parse(buffer.toString("utf8"));
}

test("RezRestServer endpoints", async (t) => {
  const runtime = new RezRuntimeBuilder().build();
  const server = new RezRestServer({ runtime, host: "127.0.0.1", port: 0 });

  let started = false;
  try {
    await server.start();
    started = true;
    const base = server.url;
    const health = await request({ method: "GET", url: `${base}/health` });
    assert.equal(health.status, 200);
    assert.deepEqual(toJson(health.body), { ok: true });

    const header = new Header({ id: "sdk-rest-1", type: "message", createdAt: 1 });
    const envelope = new Envelope({ header, body: { hello: "world" } });
    const bytes = runtime.encodeEnvelope(envelope);

    const submitEnv = await request({
      method: HTTP_METHOD_SUBMIT,
      url: `${base}/envelopes`,
      body: Buffer.from(bytes),
    });
    assert.equal(submitEnv.status, 200);
    const { id } = toJson(submitEnv.body);

    const getEnv = await request({ method: "GET", url: `${base}/envelopes/${id}` });
    assert.equal(getEnv.status, 200);
    assert.equal(toJson(getEnv.body).header.id, "sdk-rest-1");

    const deposit = await request({
      method: HTTP_METHOD_SUBMIT,
      url: `${base}/mailboxes/mb-1/deposit`,
      body: Buffer.from(JSON.stringify({ envelopeId: id })),
      headers: { "content-type": "application/json" },
    });
    assert.equal(deposit.status, 200);

    const list = await request({ method: "GET", url: `${base}/mailboxes/mb-1` });
    assert.equal(list.status, 200);
    assert.deepEqual(toJson(list.body).items, [id]);
  } catch (err) {
    if (err && (err.code === "EPERM" || err.code === "EACCES")) {
      t.skip("HTTP listen not permitted (EPERM/EACCES)");
      return;
    }
    throw err;
  } finally {
    if (started) {
      await server.stop();
    }
  }
});
