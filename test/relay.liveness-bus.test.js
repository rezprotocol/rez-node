import test from "node:test";
import assert from "node:assert/strict";
import Redis from "ioredis";
import { LivenessBus } from "../src/relay/LivenessBus.js";

const REDIS_URL = process.env.REZ_REDIS_TEST_URL || "";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));
function withTimeout(p, ms, label) {
  return Promise.race([
    p,
    new Promise((_resolve, reject) => setTimeout(() => reject(new Error(`timeout: ${label}`)), ms)),
  ]);
}

test(
  "LivenessBus cross-node deposit pings + presence (real Redis)",
  { skip: REDIS_URL ? false : "set REZ_REDIS_TEST_URL to run" },
  async (t) => {
    const conns = [];
    const mk = () => {
      const c = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
      conns.push(c);
      return c;
    };
    t.after(() => {
      for (const c of conns) {
        c.disconnect();
      }
    });

    const busA = new LivenessBus({ publisher: mk(), subscriber: mk(), channelPrefix: "test" });
    const busB = new LivenessBus({ publisher: mk(), subscriber: mk(), channelPrefix: "test" });
    await busA.start();
    await busB.start();

    await t.test("a deposit ping reaches the node serving that inbox", async () => {
      const got = new Promise((resolve) => {
        busA.registerInbox("ibx-1", resolve);
      });
      await busB.publishDeposit("ibx-1", { seq: 7 });
      const payload = await withTimeout(got, 3000, "ibx-1 ping");
      assert.equal(payload.inboxId, "ibx-1");
      assert.equal(payload.seq, 7);
    });

    await t.test("a ping for an inbox this node does NOT serve is ignored", async () => {
      let fired = false;
      busA.registerInbox("served-by-a", () => {
        fired = true;
      });
      await busB.publishDeposit("not-served-by-a", { seq: 1 });
      await delay(300);
      assert.equal(fired, false);
    });

    await t.test("unregister stops delivery", async () => {
      let count = 0;
      const off = busA.registerInbox("toggle", () => {
        count += 1;
      });
      await busB.publishDeposit("toggle", { seq: 1 });
      await delay(200);
      off();
      await busB.publishDeposit("toggle", { seq: 2 });
      await delay(200);
      assert.equal(count, 1, "only the pre-unregister ping was delivered");
    });

    await t.test("presence set / exists (cross-node) / clear", async () => {
      assert.equal(await busB.isPresent("p1"), false);
      await busA.setPresence("p1", "nodeA");
      assert.equal(await busB.isPresent("p1"), true, "node B sees node A's presence");
      await busA.clearPresence("p1");
      assert.equal(await busB.isPresent("p1"), false);
    });
  },
);
