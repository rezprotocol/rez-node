import test from "node:test";
import assert from "node:assert/strict";
import { encodeOuterPacket, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

import { createRelayDepositRouter } from "../src/protocol/RelayDepositRouter.js";
import { buildMailboxDepositedFrame } from "../src/protocol/mailboxDepositedFrame.js";
import { MailboxDepositedEvent } from "../src/contracts/records/MailboxDepositedEvent.js";

const T = REZ_CONTRACT_TYPES;
const OWNER = "owner-pubkey-b64";

// localSessions: labels for sessions bound to the inbox HERE (drive the gate's
// single signal forEachSessionByInboxId). bus: a publishDeposit stub.
function makeHarness({ localSessions = [], bus = null } = {}) {
  const frames = [];          // transient broadcasts
  const localDeliveries = []; // one per notifyLocalDeposit (the durable drain trigger)
  const sessions = localSessions.map((label) => ({
    label,
    notifyLocalDeposit(inboxId, seq) { localDeliveries.push({ label, inboxId, seq }); },
  }));
  const sessionRegistry = {
    broadcastToOwner(ownerPublicKeyB64, frame) { frames.push({ ownerPublicKeyB64, frame }); },
    forEachSessionByInboxId(_inboxId, cb) { let c = 0; for (const s of sessions) { cb(s); c += 1; } return c; },
  };
  const runtime = { getOwnerPublicKeysForInbox: () => new Set([OWNER]), livenessBus: bus };
  return { frames, localDeliveries, sessionRegistry, runtime };
}

// ---- Durable gate: ONE signal (sessions bound to the inbox), drain-or-publish ----

test("durable + a local session → drains IN-PROCESS (notifyLocalDeposit), no publish, no broadcast", async () => {
  let publishCalled = false;
  const bus = { publishDeposit: async () => { publishCalled = true; } };
  const { frames, localDeliveries, sessionRegistry, runtime } = makeHarness({ localSessions: ["A"], bus });
  const route = createRelayDepositRouter();

  await route({ inboxId: "home:inbox", packetId: "5", packetBytes: encodeOuterPacket({ bodyBytes: new Uint8Array([1]) }), seq: 5, sessionRegistry, runtime });

  assert.deepEqual(localDeliveries, [{ label: "A", inboxId: "home:inbox", seq: 5 }],
    "the local socket drains in-process (advances last_delivered)");
  assert.equal(publishCalled, false, "no Redis round-trip when the socket is local (Option Y)");
  assert.equal(frames.length, 0, "no owner-bucket broadcast — direct per-session drain");
});

test("durable + NO local session → publishes to the bus, no local drain", async () => {
  const published = [];
  const bus = { publishDeposit: async (inboxId, body) => { published.push({ inboxId, body }); } };
  const { localDeliveries, sessionRegistry, runtime } = makeHarness({ localSessions: [], bus });
  const route = createRelayDepositRouter();

  await route({ inboxId: "home:inbox", packetId: "3", packetBytes: encodeOuterPacket({ bodyBytes: new Uint8Array([2]) }), seq: 3, sessionRegistry, runtime });

  assert.deepEqual(localDeliveries, [], "no local socket → no local drain");
  assert.deepEqual(published, [{ inboxId: "home:inbox", body: { seq: 3 } }], "ping the bus so a remote holder drains");
});

test("durable + no local session + no bus → neither (reconnect-drain handles it)", async () => {
  const { frames, localDeliveries, sessionRegistry, runtime } = makeHarness({ localSessions: [], bus: null });
  const route = createRelayDepositRouter();
  await route({ inboxId: "home:inbox", packetId: "9", packetBytes: encodeOuterPacket({ bodyBytes: new Uint8Array([1]) }), seq: 9, sessionRegistry, runtime });
  assert.equal(frames.length, 0);
  assert.equal(localDeliveries.length, 0);
});

// ---- Transient (no seq): the generic EVT broadcast via the shared builder ----

test("transient (no-seq) deposit broadcasts a frame that IS the MailboxDepositedEvent record (no drift)", async () => {
  const { frames, sessionRegistry, runtime } = makeHarness({ localSessions: ["A"] });
  const route = createRelayDepositRouter();

  const bodyBytes = new Uint8Array([9, 9, 9]);
  await route({ inboxId: "wan:inbox", packetId: "rmbox-evt-1", packetBytes: encodeOuterPacket({ bodyBytes }), sessionRegistry, runtime });

  assert.equal(frames.length, 1);
  assert.equal(frames[0].frame.t, T.EVT_MAILBOX_DEPOSITED);
  const expected = new MailboxDepositedEvent({
    mailboxId: "wan:inbox",
    eventId: "rmbox-evt-1",
    ciphertextB64: Buffer.from(bodyBytes).toString("base64"),
    seq: null,
  }).toJSON();
  assert.deepEqual(frames[0].frame.body, expected, "wire body is record.toJSON() (audit P2 guarantee)");
});

test("the shared frame builder carries seq for cursor-model clients (record==frame)", () => {
  const frame = buildMailboxDepositedFrame({ mailboxId: "ib", eventId: "7", ciphertextB64: "Y2lwaGVy", seq: 7 });
  assert.equal(frame.t, T.EVT_MAILBOX_DEPOSITED);
  assert.deepEqual(frame.body, new MailboxDepositedEvent({ mailboxId: "ib", eventId: "7", ciphertextB64: "Y2lwaGVy", seq: 7 }).toJSON());
  assert.equal(frame.body.seq, 7);
});
