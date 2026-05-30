import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { promises as fs } from "node:fs";
import { FsStorageProvider } from "../src/storage/fs/FsStorageProvider.js";

async function makeTempDir() {
  return fs.mkdtemp(path.join(os.tmpdir(), "rez-peer-link-fs-"));
}

async function cleanup(dir) {
  await fs.rm(dir, { recursive: true, force: true });
}

test("FsStorageProvider peer-link storage persists across instances", async () => {
  const rootDir = await makeTempDir();
  try {
    const provider1 = new FsStorageProvider({ rootDir });
    const storage1 = provider1.getPeerLinkStorage();

    const createdPeerLink = await storage1.peerLinks.create({
      peerLinkId: "pl_alpha",
      localAccountId: "rez:acct:alice",
      peerAccountId: "rez:acct:bob",
      state: "invite_issued",
    });
    assert.equal(createdPeerLink.version, 1);

    await storage1.sessions.put({
      sessionId: "sess_alpha",
      peerLinkId: "pl_alpha",
      localAccountId: "rez:acct:alice",
      peerAccountId: "rez:acct:bob",
      status: "pending",
    });

    await storage1.handshakeAttempts.create({
      handshakeAttemptId: "hs_alpha",
      peerLinkId: "pl_alpha",
      ownerAccountId: "rez:acct:alice",
      status: "sent",
    });

    await storage1.events.append({
      ownerAccountId: "rez:acct:alice",
      eventId: "evt_alpha",
      peerLinkId: "pl_alpha",
      type: "invite_created",
      atMs: 1,
      summary: "invite created",
    });

    await storage1.keys.putInvitePreKey("rez:acct:alice", "inv_alpha", {
      keyId: "prekey_1",
      bytes: "abc123",
    });

    const provider2 = new FsStorageProvider({ rootDir });
    const storage2 = provider2.getPeerLinkStorage();

    const byPair = await storage2.peerLinks.getByPair("rez:acct:alice", "rez:acct:bob");
    assert.equal(byPair.peerLinkId, "pl_alpha");
    const missingReverse = await storage2.peerLinks.getByPair("rez:acct:bob", "rez:acct:alice");
    assert.equal(missingReverse, undefined);

    const recoverable = await storage2.sessions.listRecoverable("rez:acct:alice");
    assert.deepEqual(recoverable.map((item) => item.sessionId), ["sess_alpha"]);

    const pending = await storage2.handshakeAttempts.listPending("rez:acct:alice");
    assert.deepEqual(pending.map((item) => item.handshakeAttemptId), ["hs_alpha"]);

    const eventPage = await storage2.events.listByPeerLinkId("rez:acct:alice", "pl_alpha", { limit: 10 });
    assert.deepEqual(eventPage.items.map((item) => item.eventId), ["evt_alpha"]);
    assert.equal(eventPage.nextCursor, null);

    const preKey = await storage2.keys.getInvitePreKey("rez:acct:alice", "inv_alpha");
    assert.deepEqual(preKey, {
      keyId: "prekey_1",
      bytes: "abc123",
    });
    const missingPreKey = await storage2.keys.getInvitePreKey("rez:acct:bob", "inv_alpha");
    assert.equal(missingPreKey, undefined);
  } finally {
    await cleanup(rootDir);
  }
});
