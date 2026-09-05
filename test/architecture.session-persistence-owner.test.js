import test from "node:test";
import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import * as nodeApi from "../src/index.js";

test("DT-302: node no longer exports the dormant ratchet persistence API", () => {
  for (const name of [
    "PersistentSessionManager", "FsSessionStore",
    "ratchetStateToJson", "ratchetStateFromJson",
  ]) {
    assert.equal(Object.hasOwn(nodeApi, name), false, name + " must not create a second persistence authority");
  }
  assert.equal(typeof nodeApi.FsStorageProvider, "function");
  assert.equal(typeof nodeApi.PeerLinkService, "function");
});

test("DT-302: retired persistence implementations cannot remain as deep-import paths", async () => {
  for (const path of [
    "../src/services/sessions/PersistentSessionManager.js",
    "../src/services/sessions/serializeRatchetStateV1.js",
    "../src/storage/sessions/FsSessionStore.js",
  ]) {
    await assert.rejects(access(new URL(path, import.meta.url)), { code: "ENOENT" });
  }
});
