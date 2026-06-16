import test from "node:test";
import assert from "node:assert/strict";

import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";
import { MailboxCursorAckRequest } from "../src/contracts/records/MailboxCursorAckRequest.js";
import { MailboxCursorAckResponse } from "../src/contracts/records/MailboxCursorAckResponse.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";

const PG_URL = process.env.REZ_PG_TEST_URL || "";
const bytes = (...n) => new Uint8Array(n);

// A mock ctx mirroring MailboxHandler's other handlers — authorize returns a
// truthy cap by default; sendResponse/sendError capture for assertions.
function makeCtx({ durableInbox, sessionDeviceId = "devA", authorize = async () => ({ ok: true }) } = {}) {
  const responses = [];
  const errors = [];
  return {
    captured: { responses, errors },
    runtime: { durableInbox },
    ownerPublicKeyB64: "owner-pubkey-b64",
    sessionDeviceId,
    requireSession() { return true; },
    authorize,
    sendResponse(id, type, body) { responses.push({ id, type, body }); },
    sendError(payload) { errors.push(payload); },
  };
}

// ---- Record validation (no DB) ----

test("MailboxCursorAckRequest validates required fields", () => {
  assert.doesNotThrow(() => new MailboxCursorAckRequest({ mailboxId: "ib", deviceId: "d", throughSeq: 3 }).validate());
  assert.throws(() => new MailboxCursorAckRequest({ mailboxId: "", deviceId: "d", throughSeq: 1 }).validate(), /mailboxId/);
  assert.throws(() => new MailboxCursorAckRequest({ mailboxId: "ib", deviceId: "", throughSeq: 1 }).validate(), /deviceId/);
  assert.throws(() => new MailboxCursorAckRequest({ mailboxId: "ib", deviceId: "d", throughSeq: -1 }).validate(), /throughSeq/);
});

test("MailboxCursorAckResponse carries the stored cursor", () => {
  const r = new MailboxCursorAckResponse({ mailboxId: "ib", deviceId: "d", lastSeq: 5 });
  assert.equal(r.lastSeq, 5);
  assert.doesNotThrow(() => r.validate());
});

// ---- Handler guards (no DB) ----

test("handleCursorAck returns SERVICE_UNAVAILABLE when no durable inbox is wired", async () => {
  const ctx = makeCtx({ durableInbox: null });
  await new MailboxHandler(ctx).handleCursorAck("req1", { mailboxId: "ib", deviceId: "devA", throughSeq: 1 });
  assert.equal(ctx.captured.errors.length, 1);
  assert.equal(ctx.captured.errors[0].code, "SERVICE_UNAVAILABLE");
});

test("handleCursorAck refuses when authorize denies", async () => {
  let called = false;
  const durableInbox = { cursorAck: async () => { called = true; return { lastSeq: 1 }; } };
  const ctx = makeCtx({ durableInbox, authorize: async () => null });
  await new MailboxHandler(ctx).handleCursorAck("req1", { mailboxId: "ib", deviceId: "devA", throughSeq: 1 });
  assert.equal(called, false, "cursorAck must not run when authorization fails");
  assert.equal(ctx.captured.responses.length, 0);
});

test("handleCursorAck binds the cursor to the SESSION device, not the body", async () => {
  const seen = [];
  const durableInbox = { cursorAck: async (inboxId, deviceId, throughSeq) => { seen.push({ inboxId, deviceId, throughSeq }); return { lastSeq: throughSeq }; } };
  const ctx = makeCtx({ durableInbox, sessionDeviceId: "session-device" });
  // Body claims a DIFFERENT device — the handler must ignore it and use the session's.
  await new MailboxHandler(ctx).handleCursorAck("req1", { mailboxId: "ib", deviceId: "attacker-device", throughSeq: 7 });
  assert.deepEqual(seen, [{ inboxId: "ib", deviceId: "session-device", throughSeq: 7 }]);
  assert.equal(ctx.captured.responses[0].body.deviceId, "session-device");
});

// ---- Round-trip against real Postgres ----

test(
  "handleCursorAck advances the durable cursor (clamped to delivered) against real Pg",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_handler_cursor_ack";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    const inbox = new PgDurableInbox({ connection: conn });
    const id = "ib-handler";
    const dev = "devA";
    await inbox.registerDevice(id, dev);
    await inbox.append(id, bytes(1));
    await inbox.append(id, bytes(2));
    await inbox.append(id, bytes(3));
    // Deliver only seqs 1..2 to the device (so delivered-bound = 2).
    const delivered = await inbox.readAfterCursor(id, dev, 2);
    assert.deepEqual(delivered.map((e) => e.seq), [1, 2]);

    const ctx = makeCtx({ durableInbox: inbox, sessionDeviceId: dev });
    const handler = new MailboxHandler(ctx);

    // Ask to advance through 3, but only 2 was delivered → clamped to 2.
    await handler.handleCursorAck("req1", { mailboxId: id, deviceId: dev, throughSeq: 3 });
    assert.equal(ctx.captured.errors.length, 0, "no error expected");
    assert.equal(ctx.captured.responses[0].type, "mailbox.cursorAck.res");
    assert.equal(ctx.captured.responses[0].body.lastSeq, 2, "clamped to delivered watermark");

    // After acking through 2, a re-read returns only seq 3.
    const rest = await inbox.readAfterCursor(id, dev, 50);
    assert.deepEqual(rest.map((e) => e.seq), [3]);
  },
);
