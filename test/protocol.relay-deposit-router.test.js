import test from "node:test";
import assert from "node:assert/strict";
import { encodeOuterPacket, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

import { createRelayDepositRouter } from "../src/protocol/RelayDepositRouter.js";
import { MailboxDepositedEvent } from "../src/contracts/records/MailboxDepositedEvent.js";

const T = REZ_CONTRACT_TYPES;
const OWNER = "owner-pubkey-b64";

function makeHarness() {
  const frames = [];
  const sessionRegistry = {
    broadcastToOwner(ownerPublicKeyB64, frame) { frames.push({ ownerPublicKeyB64, frame }); },
    getOwnerPublicKeysByInboxId() { return new Set(); },
  };
  const runtime = { getOwnerPublicKeysForInbox: () => new Set([OWNER]) };
  return { frames, sessionRegistry, runtime };
}

test("emitted evt.mailbox.deposited frame body IS the MailboxDepositedEvent record (no drift)", async () => {
  const { frames, sessionRegistry, runtime } = makeHarness();
  const route = createRelayDepositRouter();

  const bodyBytes = new Uint8Array([1, 2, 3, 4]);
  const packetBytes = encodeOuterPacket({ bodyBytes });
  const expectedB64 = Buffer.from(bodyBytes).toString("base64");

  await route({
    inboxId: "home:inbox",
    packetId: "5",
    packetBytes,
    seq: 5,
    sessionRegistry,
    runtime,
  });

  assert.equal(frames.length, 1);
  const { ownerPublicKeyB64, frame } = frames[0];
  assert.equal(ownerPublicKeyB64, OWNER);
  assert.equal(frame.t, T.EVT_MAILBOX_DEPOSITED);

  // The wire body must be byte-for-byte the record's toJSON() — the audit-P2
  // guarantee that the registered contract and the emitted frame cannot drift.
  const expected = new MailboxDepositedEvent({
    mailboxId: "home:inbox",
    eventId: "5",
    ciphertextB64: expectedB64,
    seq: 5,
  }).toJSON();
  assert.deepEqual(frame.body, expected);
  assert.equal(frame.body.seq, 5, "durable seq is carried for cursor-model clients");
});

test("transient (no-seq) deposit emits a frame with seq=null", async () => {
  const { frames, sessionRegistry, runtime } = makeHarness();
  const route = createRelayDepositRouter();

  const packetBytes = encodeOuterPacket({ bodyBytes: new Uint8Array([9]) });
  // seq omitted entirely (RMailbox path).
  await route({ inboxId: "wan:inbox", packetId: "rmbox-evt-1", packetBytes, sessionRegistry, runtime });

  assert.equal(frames.length, 1);
  assert.equal(frames[0].frame.body.seq, null);
  assert.equal(frames[0].frame.body.eventId, "rmbox-evt-1");
});
