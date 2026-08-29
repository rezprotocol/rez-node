import test from "node:test";
import assert from "node:assert/strict";

import { GatewaySession } from "../src/protocol/GatewaySession.js";
import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";

// Minimal open fake socket — the constructor does no I/O; isOpen() reads
// ws.readyState === ws.OPEN, and we override session.send to capture frames.
function makeOpenWs() {
  return { OPEN: 1, readyState: 1, send() {}, on() {}, once() {}, off() {}, removeListener() {} };
}

// A durableInbox stub whose readUndelivered serves a backing log in batches,
// advancing the delivered watermark to the max seq read (like PgDurableInbox).
function makeDurableInbox(total) {
  let delivered = 0;
  return {
    calls: 0,
    async readUndelivered(_inboxId, _deviceId, limit) {
      this.calls += 1;
      const out = [];
      const end = Math.min(delivered + limit, total);
      for (let s = delivered + 1; s <= end; s += 1) out.push({ seq: s, body: new Uint8Array([s & 0xff]) });
      delivered = end;
      return out;
    },
  };
}

function makeSession(durableInbox) {
  const session = new GatewaySession({ runtime: { durableInbox }, ws: makeOpenWs() });
  session._commitPrincipal(new SessionPrincipal({
    kind: SessionPrincipal.KINDS.ACCOUNT,
    accountPublicKeyB64: "ownerA",
    sessionDeviceId: "devA",
    authority: { mode: "direct", signerPublicKeyB64: "ownerA", accountIdentityPublicKeyB64: "ownerA" },
  }));
  const sent = [];
  session.send = (frame) => sent.push(frame);
  return { session, sent };
}

test("drain loops past a single 100-batch to reach the triggering seq (P2 backlog)", async () => {
  const durableInbox = makeDurableInbox(250);
  const { session, sent } = makeSession(durableInbox);

  // Watermark is 250 behind; the ping is for seq 250. One fixed read of 100
  // would miss it — the loop must keep draining until 250 is pushed.
  await session._drainDurableToSocket("ib", { seq: 250 });

  assert.equal(sent.length, 250, "drained the whole backlog through the triggering seq");
  assert.equal(sent[249].body.seq, 250);
  assert.equal(durableInbox.calls, 3, "100 + 100 + 50 (last short batch ends the loop)");
});

test("drain stops once the triggering seq is delivered (does not over-drain a huge backlog)", async () => {
  const durableInbox = makeDurableInbox(100000);
  const { session, sent } = makeSession(durableInbox);

  // Trigger is seq 50 — inside the first batch. The loop pushes the first batch
  // and stops (reached target); the rest rides the next ping / reconnect.
  await session._drainDurableToSocket("ib", { seq: 50 });

  assert.equal(durableInbox.calls, 1, "one batch suffices to deliver the trigger");
  assert.equal(sent.length, 100, "the full first batch is pushed; backlog beyond it deferred");
  assert.ok(sent.some((f) => f.body.seq === 50), "the triggering seq was delivered");
});

test("drain honors the MAX_BATCHES backpressure cap with no target / unbounded backlog", async () => {
  const durableInbox = makeDurableInbox(100000); // 1000 batches available
  const { session, sent } = makeSession(durableInbox);

  // No payload seq → drain-all, but bounded by MAX_BATCHES=50 (5000 events).
  await session._drainDurableToSocket("ib", undefined);

  assert.equal(durableInbox.calls, 50, "capped at MAX_BATCHES");
  assert.equal(sent.length, 5000, "at most BATCH*MAX_BATCHES pushed per ping");
});

test("drain no-ops when the socket is not open", async () => {
  const durableInbox = makeDurableInbox(10);
  const session = new GatewaySession({ runtime: { durableInbox }, ws: { OPEN: 1, readyState: 3, on() {}, once() {} } });
  session._commitPrincipal(new SessionPrincipal({
    kind: SessionPrincipal.KINDS.ACCOUNT,
    accountPublicKeyB64: "ownerA",
    sessionDeviceId: "devA",
    authority: { mode: "direct", signerPublicKeyB64: "ownerA", accountIdentityPublicKeyB64: "ownerA" },
  }));
  const sent = [];
  session.send = (frame) => sent.push(frame);
  await session._drainDurableToSocket("ib", { seq: 5 });
  assert.equal(sent.length, 0, "closed socket → no drain");
  assert.equal(durableInbox.calls, 0);
});
