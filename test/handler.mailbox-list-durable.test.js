import test from "node:test";
import assert from "node:assert/strict";
import { encodeOuterPacket } from "@rezprotocol/core";

import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";

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
