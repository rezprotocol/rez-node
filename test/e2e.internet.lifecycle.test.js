import test from "node:test";
import assert from "node:assert/strict";

import { runDigitalOceanRelayMeshSmoke } from "../../scripts/test-routing-do.mjs";

function envEnabled(name) {
  return String(process.env[name] || "").trim() === "1";
}

test("internet e2e: two local client nodes deliver payload through the live DigitalOcean relay mesh", {
  timeout: 120000,
}, async (t) => {
  if (!envEnabled("RUN_INTERNET_E2E")) {
    t.skip("set RUN_INTERNET_E2E=1 to exercise live DigitalOcean relay mesh");
    return;
  }

  const assertOnionProof = envEnabled("REZ_E2E_ASSERT_ONION_PROOF");
  const result = await runDigitalOceanRelayMeshSmoke({
    settleMs: 10000,
    receiveTimeoutMs: 30000,
    assertOnionProof,
  });

  assert.equal(result.relayCount, 3, "expected the three DigitalOcean relay nodes from relays/relay-info.json");
  assert.equal(result.received, true, "Bob should receive Alice's payload through the live relay mesh");
  assert.equal(typeof result.receivedText, "string");

  const payload = JSON.parse(result.receivedText);
  assert.equal(payload.kind, "rez.route-test");
  assert.equal(payload.nonce, result.nonce);
  assert.equal(payload.text, "hello from alice");
  assert.equal(typeof result.aliceInboxId, "string");
  assert.equal(typeof result.bobInboxId, "string");
  assert.equal(result.aliceInboxId.length > 0, true);
  assert.equal(result.bobInboxId.length > 0, true);

  if (assertOnionProof) {
    assert.notEqual(result.sendResult && result.sendResult.local, true, "onion proof mode must not use local delivery");
  }
});
