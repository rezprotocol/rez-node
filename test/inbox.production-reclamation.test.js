import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { encodeOuterPacket } from "@rezprotocol/core";
import { startRezNode } from "../src/app/startRezNode.js";

test("production node startup and mesh maintenance reclaim expired leases and retry interrupted purges", async (t) => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "rez-production-reclaim-"));
  let app;
  t.after(async () => { if (app) await app.stop(); await fs.rm(dataDir, { recursive: true, force: true }); });
  const config = { node: { ws: { host: "127.0.0.1", port: 0, path: "/ws" }, storage: { dataDir }, network: { knownRelays: [] },
    mesh: { mode: "seed-only", seeds: [] }, relay: { listenHost: "127.0.0.1", listenPort: 0 } } };
  app = await startRezNode(config);
  const old = Date.now() - 8 * 24 * 60 * 60 * 1000;
  const id = "inbox:reclaim-production";
  await app.runtime.inboxClaimRegistry.claim({ inboxId: id, claimantPublicKeyB64: "test-claimant", claimedAtMs: old - 1,
    closePublicKeyB64: "test-close", generation: 1, retentionClass: "standard", leaseExpiresAtMs: old });
  await app.runtime.inboxStore.depositFromWire(id, encodeOuterPacket({ bodyBytes: new Uint8Array([1, 2, 3]) }));
  const ack = app.runtime.inboxStore.ack.bind(app.runtime.inboxStore);
  app.runtime.inboxStore.ack = async () => { throw new Error("temporary purge failure"); };
  await app.runtime.refreshMesh();
  assert.equal(app.runtime.inboxClaimRegistry.getClaim(id), null);
  assert.deepEqual(app.runtime.inboxClaimRegistry.pendingPurgeInboxIds(), [id]);
  app.runtime.inboxStore.ack = ack;
  await app.stop();
  app = null;
  app = await startRezNode(config);
  assert.deepEqual(app.runtime.inboxClaimRegistry.pendingPurgeInboxIds(), []);
  assert.equal((await app.runtime.inboxStore.list(id, { limit: 100 })).items.length, 0);
  await assert.rejects(() => app.runtime.inboxClaimRegistry.claim({ inboxId: id, claimantPublicKeyB64: "test-claimant",
    claimedAtMs: Date.now(), closePublicKeyB64: "test-close", generation: 1, retentionClass: "standard", leaseExpiresAtMs: Date.now() + 60_000 }),
  (err) => err.code === "INBOX_CLOSED");
});
