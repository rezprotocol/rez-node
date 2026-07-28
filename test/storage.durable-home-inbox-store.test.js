import test from "node:test";
import assert from "node:assert/strict";
import { encodeOuterPacket } from "@rezprotocol/core";

import { DurableHomeInboxStore } from "../src/storage/DurableHomeInboxStore.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { pgTestUrl } from "./support/integrationBackends.js";

const PG_URL = pgTestUrl();

// A minimal RMailbox stand-in that records calls. The decorator must delegate
// every NON-hosted inbox to it verbatim, and never touch it for a hosted inbox.
function makeRmailboxStub() {
  const calls = { deposits: [], fetches: [], lists: [], acks: [] };
  let onDeposit = null;
  return {
    calls,
    setOnDeposit(cb) { onDeposit = cb; },
    fireDeposit(inboxId, eventId) { if (onDeposit) onDeposit(inboxId, eventId); },
    async depositFromWire(mailboxId, wireBytes) {
      calls.deposits.push({ mailboxId, len: wireBytes.length });
      return "rmbox-evt-1";
    },
    async fetch(mailboxId, eventId) {
      calls.fetches.push({ mailboxId, eventId });
      return { objectId: "o", bytes: new Uint8Array([9]), metadata: {}, createdAt: 1 };
    },
    async list(mailboxId, opts) {
      calls.lists.push({ mailboxId, opts });
      return { items: [], nextCursor: null };
    },
    async ack(mailboxId, eventId) {
      calls.acks.push({ mailboxId, eventId });
      return true;
    },
  };
}

// A framed outer packet carrying the given body bytes (what depositFromWire
// receives on the wire and what MailboxHandler.handleDeposit builds).
function wire(...bodyBytes) {
  return encodeOuterPacket({ bodyBytes: new Uint8Array(bodyBytes) });
}

// ---- Construction guards (no DB) ----

test("DurableHomeInboxStore requires its three collaborators", () => {
  const rmailbox = makeRmailboxStub();
  const durableInbox = { setOnDeposit() {} };
  assert.throws(() => new DurableHomeInboxStore({ durableInbox, isHostedHere: () => true }), /rmailbox/);
  assert.throws(() => new DurableHomeInboxStore({ rmailbox, isHostedHere: () => true }), /durableInbox/);
  assert.throws(() => new DurableHomeInboxStore({ rmailbox, durableInbox }), /isHostedHere/);
});

test("non-hosted inboxes delegate verbatim to the wrapped RMailbox", async () => {
  const rmailbox = makeRmailboxStub();
  const durableInbox = {
    setOnDeposit() {},
    append() { throw new Error("durable append must not run for a non-hosted inbox"); },
    getEvent() { throw new Error("durable getEvent must not run for a non-hosted inbox"); },
  };
  const store = new DurableHomeInboxStore({ rmailbox, durableInbox, isHostedHere: () => false });

  const fired = [];
  store.setOnDeposit((inboxId, eventId) => fired.push({ inboxId, eventId }));

  const evtId = await store.depositFromWire("wan:inbox", wire(1, 2, 3));
  assert.equal(evtId, "rmbox-evt-1");
  assert.deepEqual(rmailbox.calls.deposits, [{ mailboxId: "wan:inbox", len: wire(1, 2, 3).length }]);

  const evt = await store.fetch("wan:inbox", "rmbox-evt-1");
  assert.equal(evt.objectId, "o");
  await store.list("wan:inbox", { limit: 5 });
  await store.ack("wan:inbox", "rmbox-evt-1");
  assert.equal(rmailbox.calls.fetches.length, 1);
  assert.equal(rmailbox.calls.lists.length, 1);
  assert.equal(rmailbox.calls.acks.length, 1);

  // The RMailbox deposit hook fans through to the single registered callback.
  rmailbox.fireDeposit("wan:inbox", "rmbox-evt-2");
  assert.deepEqual(fired, [{ inboxId: "wan:inbox", eventId: "rmbox-evt-2" }]);
});

// ---- Hosted-here routing against real Postgres ----

test(
  "hosted-here deposits go to the durable log: seq eventId, persist-then-notify, idempotent re-deposit",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_durable_home_store";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE mailbox_events, device_cursors, mailbox_seq");

    const rmailbox = makeRmailboxStub();
    const durableInbox = new PgDurableInbox({ connection: conn });
    const HOME = "home:inbox";
    const store = new DurableHomeInboxStore({
      rmailbox,
      durableInbox,
      isHostedHere: (id) => id === HOME,
    });

    const notified = [];
    store.setOnDeposit((inboxId, eventId) => notified.push({ inboxId, eventId }));

    // First deposit -> seq 1, returned as the string eventId, notified once.
    const id1 = await store.depositFromWire(HOME, wire(10, 11));
    assert.equal(id1, "1");
    // Second, distinct deposit -> seq 2.
    const id2 = await store.depositFromWire(HOME, wire(20, 21, 22));
    assert.equal(id2, "2");

    assert.deepEqual(notified, [
      { inboxId: HOME, eventId: "1" },
      { inboxId: HOME, eventId: "2" },
    ]);
    // The transient RMailbox was never touched for the home inbox.
    assert.equal(rmailbox.calls.deposits.length, 0);

    // fetch by the seq-string eventId returns the stored (framed) bytes + seq.
    const evt1 = await store.fetch(HOME, "1");
    assert.deepEqual(Array.from(evt1.bytes), Array.from(wire(10, 11)));
    assert.equal(evt1.seq, 1);
    assert.equal(evt1.objectId, null);
    assert.equal(await store.fetch(HOME, "999"), null, "missing seq -> null");
    assert.equal(await store.fetch(HOME, "not-a-number"), null, "non-numeric eventId -> null");

    // Re-delivering the SAME ciphertext (e.g. an outbound-queue retry) collapses
    // to the existing seq and does NOT re-notify (content-hash dedupe).
    const idDup = await store.depositFromWire(HOME, wire(10, 11));
    assert.equal(idDup, "1", "duplicate ciphertext dedupes to the first seq");
    assert.equal(notified.length, 2, "a dedupe hit must not re-notify");
  },
);
