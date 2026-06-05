import test from "node:test";
import assert from "node:assert/strict";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";

/**
 * Retention contract: the relay's on-disk inbox buffer is a TRANSIENT hand-off
 * for offline mail, not durable storage. Once a buffered deposit is delivered
 * to the just-attached owner socket, the relay MUST remove it (RMailbox.ack).
 *
 * Regression (2026-06-05): _replayPendingToSocket delivered deposits but never
 * ack'd them, so the buffer grew without bound — a real account accumulated
 * ~33K deposits (16 GB). Every reconnect then re-walked the whole tree
 * (FileSystemDataStore.list is O(files)) and pegged the CPU. This is also a DoS
 * surface (a sender can fill a victim's buffer + peg CPU on login).
 */

function makeSocket(label) {
  const sent = [];
  return { id: label, destroyed: false, sent, write(bytes) { sent.push(bytes); } };
}

/**
 * Stub inboxStore that actually mutates on ack(), so we can assert delivered
 * deposits are removed. list() respects the live (post-ack) item set.
 */
function makeMutableInboxStore(initial) {
  const items = { ...initial.items };
  const fetchByEventId = { ...initial.fetchByEventId };
  return {
    _items: items,
    async list(mailboxId, { cursor, limit } = {}) {
      const all = items[mailboxId] || [];
      const start = cursor ? all.findIndex((it) => it.eventId === cursor) + 1 : 0;
      const sliced = all.slice(start, start + (limit || 50));
      const nextCursor = start + sliced.length < all.length ? sliced[sliced.length - 1].eventId : null;
      return { items: sliced, nextCursor };
    },
    async fetch(mailboxId, eventId) {
      return fetchByEventId[mailboxId + "|" + eventId] || null;
    },
    async ack(mailboxId, eventId) {
      const all = items[mailboxId] || [];
      const idx = all.findIndex((it) => it.eventId === eventId);
      if (idx < 0) return false;
      all.splice(idx, 1);
      return true;
    },
    async depositFromWire() {},
  };
}

function setup(inboxStore) {
  const router = new InboxRouter({
    relayPeerDirectory: new RelayPeerDirectory(),
    selfRelayKeyId: "relay-self",
    inboxStore,
    logger: { error: () => {}, warn: () => {}, info: () => {}, log: () => {} },
  });
  return { router };
}

test("registerLocal removes each buffered deposit after it is delivered", async () => {
  const INBOX = "inbox:bob_offline";
  const inboxStore = makeMutableInboxStore({
    items: {
      [INBOX]: [
        { eventId: "evt_001", objectId: "obj1", createdAt: 1000 },
        { eventId: "evt_002", objectId: "obj2", createdAt: 2000 },
        { eventId: "evt_003", objectId: "obj3", createdAt: 3000 },
      ],
    },
    fetchByEventId: {
      [INBOX + "|evt_001"]: { bytes: new Uint8Array([1]), objectId: "obj1", metadata: {}, createdAt: 1000 },
      [INBOX + "|evt_002"]: { bytes: new Uint8Array([2]), objectId: "obj2", metadata: {}, createdAt: 2000 },
      [INBOX + "|evt_003"]: { bytes: new Uint8Array([3]), objectId: "obj3", metadata: {}, createdAt: 3000 },
    },
  });

  const { router } = setup(inboxStore);
  const socket = makeSocket("bob_reconnect");

  router.registerLocal([INBOX], socket, { announce: false });
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(socket.sent.length, 3, "all three pending deposits delivered");
  assert.equal(inboxStore._items[INBOX].length, 0, "delivered deposits removed from the buffer");

  // A second reconnect must have nothing left to drain (no unbounded re-walk).
  const socket2 = makeSocket("bob_reconnect_2");
  router.registerLocal([INBOX], socket2, { announce: false });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(socket2.sent.length, 0, "nothing re-delivered on a later reconnect");
});

test("a deposit whose send fails is NOT removed (redelivered next time)", async () => {
  const INBOX = "inbox:flaky";
  const inboxStore = makeMutableInboxStore({
    items: { [INBOX]: [{ eventId: "evt_001", objectId: "obj1", createdAt: 1000 }] },
    fetchByEventId: { [INBOX + "|evt_001"]: { bytes: new Uint8Array([1]), objectId: "obj1", metadata: {}, createdAt: 1000 } },
  });

  const { router } = setup(inboxStore);
  // A socket whose write throws → _sendToSocket reports failure.
  const deadSocket = { id: "dead", destroyed: false, write() { throw new Error("socket gone"); } };

  router.registerLocal([INBOX], deadSocket, { announce: false });
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(inboxStore._items[INBOX].length, 1, "undelivered deposit must survive a failed send");
});
