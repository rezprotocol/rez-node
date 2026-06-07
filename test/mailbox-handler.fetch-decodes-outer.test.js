import test from "node:test";
import assert from "node:assert/strict";

import { encodeOuterPacket, REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { MailboxHandler } from "../src/protocol/handlers/MailboxHandler.js";

const T = REZ_CONTRACT_TYPES;

// Regression (2026-06-06): mailbox.fetch must surface the DECODED outer-packet
// body — identical to the live push path (RelayDepositRouter sends
// decodeOuterPacket(packet).bodyBytesView). The relay stores the framed outer
// packet ([0x02 version][body]); when fetch returned the raw framed bytes, the
// catch-up drain (the only fetch consumer) JSON-parsed `\x02{...}` and failed, so
// OFFLINE deposits never applied while live-pushed (decoded) ones did.

function makeCtx({ inboxStore }) {
  const responses = [];
  const errors = [];
  return {
    ctx: {
      requireSession: () => true,
      ownerPublicKeyB64: "owner-pk",
      runtime: { inboxStore },
      async authorize() { return { ok: true }; },
      sendResponse(id, type, body) { responses.push({ id, type, body }); },
      sendError(e) { errors.push(e); },
    },
    responses,
    errors,
  };
}

function b64ToBytes(b64) { return new Uint8Array(Buffer.from(b64, "base64")); }

test("handleFetch returns the decoded outer-packet body (not the framed 0x02 packet)", async () => {
  const bodyBytes = new TextEncoder().encode(JSON.stringify({ e2ee: 1, type: "x3dh.handshake.v2" }));
  const framed = encodeOuterPacket({ bodyBytes }); // [0x02][body]
  assert.equal(framed[0], 0x02, "precondition: stored deposit is a framed outer packet");

  const inboxStore = {
    async fetch() {
      return { objectId: "obj_1", bytes: framed, metadata: { contentType: "rez.outer" }, createdAt: 123 };
    },
  };
  const { ctx, responses } = makeCtx({ inboxStore });
  const handler = new MailboxHandler(ctx);

  await handler.handleFetch("req-1", { mailboxId: "inbox:abc", eventId: "evt_1" });

  assert.equal(responses.length, 1, "one response sent");
  assert.equal(responses[0].type, T.MAILBOX_FETCH_RES);
  const out = b64ToBytes(responses[0].body.ciphertextB64);
  assert.deepEqual(out, bodyBytes, "ciphertextB64 decodes to the unframed body");
  assert.notEqual(out[0], 0x02, "the 0x02 framing byte is stripped");
  // And the body is valid JSON the chat-server's processDeposit can parse.
  assert.deepEqual(JSON.parse(new TextDecoder().decode(out)), { e2ee: 1, type: "x3dh.handshake.v2" });
});

test("handleFetch returns a non-outer-packet stored value unchanged (defensive fallback)", async () => {
  const raw = new Uint8Array([0x09, 0x10, 0x11]); // not a 0x02 outer packet
  const inboxStore = {
    async fetch() { return { objectId: "obj_2", bytes: raw, metadata: {}, createdAt: 1 }; },
  };
  const { ctx, responses } = makeCtx({ inboxStore });
  const handler = new MailboxHandler(ctx);

  await handler.handleFetch("req-2", { mailboxId: "inbox:abc", eventId: "evt_2" });

  assert.deepEqual(b64ToBytes(responses[0].body.ciphertextB64), raw, "non-outer value returned as-is");
});
