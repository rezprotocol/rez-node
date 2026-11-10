import test from "node:test";
import assert from "node:assert/strict";
import { encodeOuterPacket } from "@rezprotocol/core";

import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { pgTestUrl } from "./support/integrationBackends.js";

const PG_URL = pgTestUrl();

function makeCtx({ durableInbox, isHostedHere = () => true, inboxStore = null, sessionDeviceId = "devA", authorize = async () => ({ ok: true }) } = {}) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { durableInbox, isHostedHere, inboxStore },
    ownerPublicKeyB64: "owner-pubkey-b64",
    sessionDeviceId,
    requireSession() { return true; },
    authorize,
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
  };
}

const wire = (...b) => encodeOuterPacket({ bodyBytes: new Uint8Array(b) });
const b64 = (...b) => Buffer.from(new Uint8Array(b)).toString("base64");

test("handleList durable branch requires a session device", async () => {
  const durableInbox = { readAfterCursor: async () => { throw new Error("must not read"); } };
  const ctx = makeCtx({ durableInbox, sessionDeviceId: "" });
  await new MailboxHandler(ctx).handleList("req1", { mailboxId: "ib" });
  assert.equal(ctx.captured.errors[0].code, "UNAUTHORIZED");
});

test("handleList bounds both durable count and raw bytes at the storage boundary", async () => {
  let args;
  const ctx = makeCtx({ durableInbox: { async readAfterCursor(...params) { args = params; return []; } } });
  await new MailboxHandler(ctx).handleList("page", { mailboxId: "ib", limit: 1_000_000 });
  assert.deepEqual(args, ["ib", "devA", 50, 700_000]);
});

test("handleList falls through to inboxStore for a non-hosted inbox", async () => {
  let durableRead = false;
  const durableInbox = { readAfterCursor: async () => { durableRead = true; return []; } };
  const listed = [];
  const inboxStore = { list: async (id, opts) => { listed.push({ id, opts }); return { items: [{ eventId: "e1" }], nextCursor: null }; } };
  const ctx = makeCtx({ durableInbox, isHostedHere: () => false, inboxStore });
  await new MailboxHandler(ctx).handleList("req1", { mailboxId: "wan", limit: 10 });
  assert.equal(durableRead, false, "durable read must not run for a non-hosted inbox");
  assert.equal(listed.length, 1);
  assert.equal(ctx.captured.responses[0].body.items[0].eventId, "e1");
});

test("handleList AWAITS an async (Pg-style) isHostedHere=false → falls through, no durable read (P1)", async () => {
  // The pg predicate is async; a bare Promise<false> is truthy. Without `await`
  // this would misroute a transient inbox into the durable branch and throw
  // DEVICE_NOT_REGISTERED. Asserts the await: durable read MUST NOT run.
  let durableRead = false;
  const durableInbox = { readAfterCursor: async () => { durableRead = true; throw new Error("durable read must not run"); } };
  const listed = [];
  const inboxStore = { list: async () => { listed.push(1); return { items: [{ eventId: "e1" }], nextCursor: null }; } };
  const ctx = makeCtx({ durableInbox, isHostedHere: async () => false, inboxStore });
  await new MailboxHandler(ctx).handleList("req1", { mailboxId: "wan", limit: 10 });
  assert.equal(durableRead, false, "async false must resolve to non-hosted → no durable read");
  assert.equal(ctx.captured.errors.length, 0);
  assert.equal(listed.length, 1, "fell through to the transient inboxStore");
});

test("handleList AWAITS an async isHostedHere=true → durable branch", async () => {
  const durableInbox = { readAfterCursor: async () => [] };
  const ctx = makeCtx({ durableInbox, isHostedHere: async () => true });
  await new MailboxHandler(ctx).handleList("req1", { mailboxId: "home:ib", limit: 10 });
  assert.equal(ctx.captured.errors.length, 0);
  assert.equal(ctx.captured.responses[0].type, "mailbox.list.res");
  assert.deepEqual(ctx.captured.responses[0].body.items, [], "durable branch ran (empty inbox)");
});

test(
  "handleList durable branch returns inline {seq, ciphertextB64} and respects the device cursor (real Pg)",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_handler_list_durable";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    const durableInbox = new PgDurableInbox({ connection: conn });
    const id = "home:inbox";
    const dev = "devA";
    await durableInbox.registerDevice(id, dev);
    await durableInbox.append(id, wire(1, 1));
    await durableInbox.append(id, wire(2, 2));
    await durableInbox.append(id, wire(3, 3));

    const ctx = makeCtx({ durableInbox, sessionDeviceId: dev });
    const handler = new MailboxHandler(ctx);

    await handler.handleList("req1", { mailboxId: id, limit: 50 });
    assert.equal(ctx.captured.errors.length, 0);
    const res = ctx.captured.responses[0];
    assert.equal(res.type, "mailbox.list.res");
    assert.equal(res.body.nextCursor, null);
    assert.deepEqual(res.body.items, [
      { seq: 1, ciphertextB64: b64(1, 1) },
      { seq: 2, ciphertextB64: b64(2, 2) },
      { seq: 3, ciphertextB64: b64(3, 3) },
    ]);

    // The read advanced last_delivered to 3, so a cursorAck through 2 is allowed;
    // re-listing then returns only seq 3 (consumed cursor moved past 1..2).
    await durableInbox.cursorAck(id, dev, 2);
    const ctx2 = makeCtx({ durableInbox, sessionDeviceId: dev });
    await new MailboxHandler(ctx2).handleList("req2", { mailboxId: id, limit: 50 });
    assert.deepEqual(ctx2.captured.responses[0].body.items, [
      { seq: 3, ciphertextB64: b64(3, 3) },
    ]);
  },
);

test("durable catch-up pages large image backlogs below the shipped client's frame cap without losing cursor coverage", async (t) => {
  const schema = "test_handler_list_byte_budget";
  const conn = await createIsolatedPgConnection(PG_URL, schema);
  t.after(async () => {
    await conn.close();
    await dropSchema(PG_URL, schema);
  });
  await new MigrationRunner({ connection: conn }).migrate();
  const inbox = new PgDurableInbox({ connection: conn });
  const id = "home:large-images";
  const device = "device-images";
  await inbox.registerDevice(id, device);
  for (let index = 0; index < 33; index += 1) {
    const size = index === 0 ? 558_169 : 176_000;
    await inbox.append(id, encodeOuterPacket({ bodyBytes: new Uint8Array(size).fill(index) }));
  }
  const received = [];
  while (received.length < 33) {
    const ctx = makeCtx({ durableInbox: inbox, sessionDeviceId: device });
    await new MailboxHandler(ctx).handleList("page", { mailboxId: id, limit: 50 });
    assert.deepEqual(ctx.captured.errors, []);
    const response = ctx.captured.responses[0];
    const items = response.body.items;
    assert.ok(items.length > 0, "every remaining page must make progress");
    assert.ok(Buffer.byteLength(JSON.stringify(response)) < 1_000_000, "a legal image backlog must not produce a silently discarded response");
    assert.deepEqual(items.map((item) => item.seq), Array.from({ length: items.length }, (_, offset) => received.length + offset + 1));
    for (const item of items) {
      const expectedIndex = item.seq - 1;
      const bytes = Buffer.from(item.ciphertextB64, "base64");
      assert.equal(bytes.length, expectedIndex === 0 ? 558_169 : 176_000);
      assert.ok(bytes.every((value) => value === expectedIndex), "exact image bytes survive paging");
    }
    const delivered = items[items.length - 1].seq;
    const watermark = await conn.query("SELECT last_delivered FROM device_cursors WHERE inbox_id=$1 AND device_id=$2", [id, device]);
    assert.equal(Number(watermark.rows[0].last_delivered), delivered, "never mark the withheld tail delivered");
    const replayCtx = makeCtx({ durableInbox: inbox, sessionDeviceId: device });
    await new MailboxHandler(replayCtx).handleList("retry", { mailboxId: id, limit: 50 });
    assert.deepEqual(replayCtx.captured.responses[0].body.items, items, "a dropped response must replay before consumption is acknowledged");
    received.push(...items.map((item) => item.seq));
    const ack = await inbox.cursorAck(id, device, 100_000);
    assert.equal(ack.lastSeq, delivered, "an overclaim cannot acknowledge the withheld tail");
  }
  assert.equal((await inbox.readAfterCursor(id, device)).length, 0);
  const retained = await conn.query("SELECT count(*)::int AS count FROM mailbox_events WHERE inbox_id=$1", [id]);
  assert.equal(retained.rows[0].count, 33, "paging and acknowledgement never delete mailbox events");

  const oversizedId = "home:single-oversized-event";
  await inbox.registerDevice(oversizedId, device);
  await inbox.append(oversizedId, encodeOuterPacket({ bodyBytes: new Uint8Array(750_000) }));
  const oversizedCtx = makeCtx({ durableInbox: inbox, sessionDeviceId: device });
  await new MailboxHandler(oversizedCtx).handleList("too-large", { mailboxId: oversizedId });
  assert.equal(oversizedCtx.captured.responses.length, 0);
  assert.equal(oversizedCtx.captured.errors[0].code, "INBOX_CAP_EXCEEDED", "an unsendable single event fails loudly instead of hanging");
  const oversizedCursor = await conn.query("SELECT last_seq, last_delivered FROM device_cursors WHERE inbox_id=$1 AND device_id=$2", [oversizedId, device]);
  assert.equal(Number(oversizedCursor.rows[0].last_seq), 0);
  assert.equal(Number(oversizedCursor.rows[0].last_delivered), 0);
  const oversizedRetained = await conn.query("SELECT count(*)::int AS count FROM mailbox_events WHERE inbox_id=$1", [oversizedId]);
  assert.equal(oversizedRetained.rows[0].count, 1, "even an unsendable event remains stored");
  await assert.rejects(() => inbox.readAfterCursor(id, device, 50, 0), /positive safe integer/);
  await assert.rejects(() => inbox.readAfterCursor(id, device, 50, NaN), /positive safe integer/);
});
