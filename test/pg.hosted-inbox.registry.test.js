import test from "node:test";
import assert from "node:assert/strict";

import { PgHostedInboxRegistry } from "../src/storage/pg/PgHostedInboxRegistry.js";
import { createRelayRuntime } from "../src/app/createRelayRuntime.js";

function registration(relayKeyId, suffix = "one") {
  const nowMs = Date.now();
  return {
    inboxId: "inbox:" + suffix,
    nodeKeyId: "node:" + suffix,
    nodePublicKeyB64: "node-public:" + suffix,
    relayKeyId,
    issuedAtMs: nowMs,
    expiresAtMs: nowMs + 60_000,
    delegationSigB64: "signature:" + suffix,
  };
}

class FakeConnection {
  constructor(rows = []) {
    this.rows = new Map(rows.map((row) => [row.claimant_pubkey, row.delegation]));
    this.calls = [];
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    if (sql.startsWith("SELECT claimant_pubkey")) {
      const relayKeyId = params[0];
      const rows = [];
      for (const [claimant_pubkey, delegation] of this.rows.entries()) {
        if (delegation.relayKeyId === relayKeyId) rows.push({ claimant_pubkey, delegation });
      }
      return { rows, rowCount: rows.length };
    }
    if (sql.startsWith("INSERT INTO hosted_inboxes")) {
      this.rows.set(params[0], JSON.parse(params[1]));
      return { rows: [], rowCount: 1 };
    }
    if (sql.startsWith("DELETE FROM hosted_inboxes")) {
      const delegation = this.rows.get(params[0]);
      if (delegation && delegation.relayKeyId === params[1]) this.rows.delete(params[0]);
      return { rows: [], rowCount: 1 };
    }
    throw new Error("unexpected SQL: " + sql);
  }
}

test("PgHostedInboxRegistry hydrates only delegations for this relay", async () => {
  const connection = new FakeConnection([
    { claimant_pubkey: "alice", delegation: registration("relay:A", "alice") },
    { claimant_pubkey: "bob", delegation: registration("relay:B", "bob") },
  ]);
  const registry = new PgHostedInboxRegistry({ connection, relayKeyId: "relay:A" });
  await registry.hydrate();

  assert.deepEqual(registry.getInboxIds(), ["inbox:alice"]);
  assert.deepEqual(Array.from(registry.getOwnerPublicKeysForInbox("inbox:alice")), ["alice"]);
  assert.deepEqual(registry.getOwnerPublicKeysForInbox("inbox:bob").size, 0);
});

test("PgHostedInboxRegistry persists one row per claimant and refreshes its local route", async () => {
  const connection = new FakeConnection();
  const registry = new PgHostedInboxRegistry({ connection, relayKeyId: "relay:A" });
  let changes = 0;
  registry.setOnChange(() => { changes += 1; });
  await registry.hydrate();

  const record = registration("relay:A", "alice");
  await registry.add("alice", record);
  await registry.add("alice", record);

  assert.equal(connection.rows.get("alice").inboxId, "inbox:alice");
  assert.equal(changes, 1);
  assert.equal(registry.getRegistrations()[0].claimantPublicKeyB64, "alice");
});

test("durable relay runtime keeps hosted registration after socket disconnect", async () => {
  const removed = [];
  const hostedInboxRegistry = {
    remove(claimant) {
      removed.push(claimant);
    },
  };
  const durable = createRelayRuntime({
    hostedInboxRegistry,
    durableInbox: {},
    identity: {},
  });
  const transient = createRelayRuntime({
    hostedInboxRegistry,
    identity: {},
  });

  await durable.unregisterHostedSession("durable-claimant");
  await transient.unregisterHostedSession("transient-claimant");

  assert.deepEqual(removed, ["transient-claimant"]);
});
