import test from "node:test";
import assert from "node:assert/strict";
import { InboxRouter } from "../src/relay/InboxRouter.js";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";

/**
 * Offline-receive contract.
 *
 * When a relay accepts a deposit for an inbox whose owner socket is
 * absent, the bytes land on the relay's local inboxStore via the
 * `entry.direct && !entry.socket` branch in routeDelivery. Until this
 * fix, those bytes sat on disk forever — the relay had no mechanism to
 * deliver them when the owner reconnected. Receivers reported sent
 * messages that never arrived.
 *
 * The fix: InboxRouter.registerLocal — fired when a reconnecting node
 * sends inbox.register — drains the relay's inboxStore for the
 * registered inboxIds and pushes each pending event over the just-
 * attached socket as an inbox.deposit ctl frame, the same shape used
 * for live forwarding.
 */

function makeSocket(label) {
  const sent = [];
  return {
    id: label,
    destroyed: false,
    sent,
    write(bytes) {
      sent.push(bytes);
    },
  };
}

/** Stub inboxStore: records calls and returns scripted pending items. */
function makeInboxStore({ items, fetchByEventId }) {
  return {
    async list(mailboxId, { cursor, limit } = {}) {
      const all = items[mailboxId] || [];
      const start = cursor ? all.findIndex((it) => it.eventId === cursor) + 1 : 0;
      const sliced = all.slice(start, start + (limit || 50));
      const nextCursor = start + sliced.length < all.length ? sliced[sliced.length - 1].eventId : null;
      return { items: sliced, nextCursor };
    },
    async fetch(mailboxId, eventId) {
      const evt = fetchByEventId[mailboxId + "|" + eventId];
      return evt || null;
    },
    async depositFromWire() {},
  };
}

function setup(inboxStore) {
  const directory = new RelayPeerDirectory();
  const router = new InboxRouter({
    relayPeerDirectory: directory,
    selfRelayKeyId: "relay-self",
    inboxStore,
    logger: { error: () => {}, warn: () => {}, info: () => {}, log: () => {} },
  });
  return { router };
}

test("registerLocal drains pending inboxStore items to the just-attached socket", async () => {
  const INBOX = "inbox:bob_offline";
  const bytes1 = new Uint8Array([1, 2, 3]);
  const bytes2 = new Uint8Array([4, 5, 6, 7]);

  const inboxStore = makeInboxStore({
    items: {
      [INBOX]: [
        { eventId: "evt_001", objectId: "obj1", createdAt: 1000 },
        { eventId: "evt_002", objectId: "obj2", createdAt: 2000 },
      ],
    },
    fetchByEventId: {
      [INBOX + "|evt_001"]: { bytes: bytes1, objectId: "obj1", metadata: {}, createdAt: 1000 },
      [INBOX + "|evt_002"]: { bytes: bytes2, objectId: "obj2", metadata: {}, createdAt: 2000 },
    },
  });

  const { router } = setup(inboxStore);
  const socket = makeSocket("bob_reconnect");

  router.registerLocal([INBOX], socket, { announce: false });

  // The drain runs async (fire-and-forget from registerLocal). Wait for it.
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(socket.sent.length, 2, "expected two inbox.deposit frames over the new socket");

  // Decode the ctl payloads and check inboxId + base64 inner.
  const decoded = socket.sent.map((frame) => {
    const text = frame instanceof Uint8Array ? Buffer.from(frame).toString("utf8") : String(frame);
    // SocketFrameRouter encoding wraps with length-prefixed JSON; tolerate
    // either bare JSON or len-prefixed (find first '{').
    const start = text.indexOf("{");
    return JSON.parse(text.slice(start));
  });

  assert.equal(decoded[0]._ctl, "inbox.deposit");
  assert.equal(decoded[0].inboxId, INBOX);
  assert.equal(decoded[0].inner, Buffer.from(bytes1).toString("base64"));

  assert.equal(decoded[1]._ctl, "inbox.deposit");
  assert.equal(decoded[1].inboxId, INBOX);
  assert.equal(decoded[1].inner, Buffer.from(bytes2).toString("base64"));
});

test("registerLocal with no pending items is a no-op", async () => {
  const inboxStore = makeInboxStore({ items: { "inbox:empty": [] }, fetchByEventId: {} });
  const { router } = setup(inboxStore);
  const socket = makeSocket("empty_inbox");

  router.registerLocal(["inbox:empty"], socket, { announce: false });
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(socket.sent.length, 0, "no pending items → no frames sent");
});

test("registerLocal without an inboxStore is a no-op (node-only mode)", async () => {
  const directory = new RelayPeerDirectory();
  const router = new InboxRouter({
    relayPeerDirectory: directory,
    selfRelayKeyId: "relay-self",
    inboxStore: null,
    logger: { error: () => {}, warn: () => {}, info: () => {}, log: () => {} },
  });
  const socket = makeSocket("no_store");

  router.registerLocal(["inbox:any"], socket, { announce: false });
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(socket.sent.length, 0, "no inboxStore → no drain path");
});

test("registerLocal pages through inboxStore.list using returned nextCursor", async () => {
  const INBOX = "inbox:many";
  const events = [];
  const fetchMap = {};
  for (let i = 0; i < 120; i++) {
    const eventId = "evt_" + String(i).padStart(4, "0");
    events.push({ eventId, objectId: "obj" + i, createdAt: 1000 + i });
    fetchMap[INBOX + "|" + eventId] = { bytes: new Uint8Array([i & 0xff]), objectId: "obj" + i, metadata: {}, createdAt: 1000 + i };
  }
  const inboxStore = makeInboxStore({ items: { [INBOX]: events }, fetchByEventId: fetchMap });

  const { router } = setup(inboxStore);
  const socket = makeSocket("paged");

  router.registerLocal([INBOX], socket, { announce: false });
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(socket.sent.length, 120, "drain must page through all 120 events (50-per-page default)");
});
