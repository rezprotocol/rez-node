import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { PgConnection } from "../src/storage/pg/PgConnection.js";

class FakePool extends EventEmitter {
  async query() { return { rows: [] }; }
  async connect() { throw new Error("not used"); }
}

test("PgConnection contains idle-client errors so a database outage cannot crash the node", async (t) => {
  const pool = new FakePool();
  const logged = [];
  const originalError = console.error;
  console.error = (message) => logged.push(message);
  t.after(() => { console.error = originalError; });

  const connection = new PgConnection({ pool });
  assert.equal(pool.listenerCount("error"), 1);
  assert.doesNotThrow(() => pool.emit("error", Object.assign(new Error("database stopped"), { code: "57P01" })));
  assert.deepEqual(logged, ["[PgConnection] idle client error code=57P01: database stopped"]);

  await connection.close();
  assert.equal(pool.listenerCount("error"), 0, "closing a borrowed pool wrapper removes its listener");
  await connection.close();
});
