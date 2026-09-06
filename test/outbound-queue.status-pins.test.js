import test from "node:test";
import assert from "node:assert/strict";

import { PersistentOutboundQueue } from "../src/gateway/PersistentOutboundQueue.js";

// DT-002 characterization pins for evt.outbound.status semantics (frozen
// delivery-transports plan §2 findings; DT-006 evidence). Pinned AS-IS,
// defects included:
//
//   1. Cap eviction is SILENT: #enforcePerInboxLimit / #enforceGlobalLimit
//      delete the oldest entry with NO status callback — the only observable
//      event of an enqueue that evicted someone else's message is the NEW
//      entry's "queued". A caller's message can be dropped forever without
//      any "expired" notification. (TTL pruning, by contrast, does emit.)
//   2. "delivered" is emitted by markDelivered(), which RetryScheduler calls
//      when the entry-relay SEND resolves — it means socket-accept custody,
//      not inbox deposit (routeMode/custody distinction in the plan).
//
// Phase 1's OutboundQueueEntryV2/receipt work changes these; the pins must
// be updated deliberately when it does.

function makeKv() {
  const m = new Map();
  return {
    async get(k) { return m.has(k) ? m.get(k) : undefined; },
    async getStrict(k) { return this.get(k); },
    async set(k, v) { m.set(k, v); },
    async delete(k) { return m.delete(k); },
    async keys(prefix) {
      const out = [];
      for (const k of m.keys()) if (!prefix || k.startsWith(prefix)) out.push(k);
      return out;
    },
  };
}

function makeQueue({ maxPerInbox = 2, maxTotal = 100 } = {}) {
  let nowMs = 1_000_000;
  const queue = new PersistentOutboundQueue({
    keyValueStore: makeKv(),
    maxPerInbox,
    maxTotal,
    nowMs: () => (nowMs += 1),
  });
  const statuses = [];
  queue.setOnStatusChange((queueId, status, entry) => {
    statuses.push({ queueId, status, inbox: entry ? entry.deliverInboxId : null });
  });
  return { queue, statuses };
}

const BYTES = new Uint8Array([1, 2, 3]);

test("pin (defect): per-inbox cap eviction is SILENT — the evicted entry gets no status event", async () => {
  const { queue, statuses } = makeQueue({ maxPerInbox: 2 });

  const e1 = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: BYTES });
  const e2 = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: BYTES });
  assert.deepEqual(statuses.map((s) => s.status), ["queued", "queued"]);

  // Third enqueue on the same inbox evicts the oldest (e1) BEFORE its own
  // "queued" emit. The eviction produces no callback at all.
  const e3 = await queue.enqueue({ deliverInboxId: "inbox:a", innerBytes: BYTES });
  assert.deepEqual(
    statuses.map((s) => ({ id: s.queueId, status: s.status })),
    [
      { id: e1.queueId, status: "queued" },
      { id: e2.queueId, status: "queued" },
      { id: e3.queueId, status: "queued" },
    ],
    "no expired/evicted event for e1 — its disappearance is silent",
  );
  assert.equal(statuses.some((s) => s.queueId === e1.queueId && s.status !== "queued"), false);
});

// DT-005 hard retirement: receipts are gone and the return path is no longer
// built, so a receipt inbox is inert metadata that also links a second inbox
// to the sender. New entries must not carry it; entries queued BEFORE the
// retirement must still read back unchanged.
test("DT-005: newly queued entries never persist receiptInboxId, but old entries still read it back", async () => {
  const { queue } = makeQueue();

  const fresh = await queue.enqueue({
    deliverInboxId: "inbox:c",
    innerBytes: BYTES,
    // Even if a stale caller still passes it, it must not be persisted.
    receiptInboxId: "inbox:receipts:should-not-persist",
  });
  assert.equal(fresh.receiptInboxId, null, "no receipt inbox is stored at rest on a new entry");
  assert.equal(JSON.stringify(fresh.toJSON()).includes("should-not-persist"), false,
    "the value never reaches the persisted record");

  // Read compatibility: a pre-retirement entry decodes with its field intact.
  const legacy = fresh.constructor.fromJSON({
    ...fresh.toJSON(),
    receiptInboxId: "inbox:receipts:legacy",
  });
  assert.equal(legacy.receiptInboxId, "inbox:receipts:legacy", "old entries still decode their stored value");
});

test("pin: markDelivered emits 'delivered' (socket-accept custody, not inbox deposit) and removes the entry", async () => {
  const { queue, statuses } = makeQueue();

  const e1 = await queue.enqueue({ deliverInboxId: "inbox:b", innerBytes: BYTES });
  await queue.markDelivered(e1.queueId);

  assert.deepEqual(
    statuses.map((s) => s.status),
    ["queued", "delivered"],
    "'delivered' is the entry-relay accept signal — nothing here observed an inbox deposit",
  );
});
