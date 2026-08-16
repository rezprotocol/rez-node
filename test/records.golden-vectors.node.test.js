/**
 * P7.1 — Node-runtime verification of the shared golden vectors, plus DHT
 * mapping and full ingress admission. NodeCryptoProvider must REPRODUCE the
 * golden signatures byte-for-byte (deterministic Ed25519).
 */
import test from "node:test";
import assert from "node:assert/strict";
import { base64ToBytes } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { verifyDurableRecordDual } from "../src/routing/dht/DurableRecord.js";
import { buildRelayDescriptorSigningPayload, verifyRelayDescriptorSignature, signedPayloadBytes } from "../src/relay/PeerAuthShared.js";
import { canonicalJSONStringify } from "../src/util/canonicalize.js";
import { RelayStore } from "../src/network/RelayStore.js";
import {
  GOLDEN_NODE_PRIVATE_KEY_B64,
  GOLDEN_RELAY_KEY_ID,
  GOLDEN_DHT_NODE_ID_HEX,
  GOLDEN_NOW_MS,
  GOLDEN_RELAY_DESCRIPTOR,
  GOLDEN_DESCRIPTOR_SIGNING_STRING,
  GOLDEN_DURABLE_RECORD_V1,
  GOLDEN_DURABLE_RECORD_V1_LOCAL_ID,
} from "../../rez-core/test/support/goldenVectors.js";

const CRYPTO = new NodeCryptoProvider();

test("descriptor canonical signing bytes and signature reproduce the goldens under Node crypto", () => {
  const signingString = canonicalJSONStringify(buildRelayDescriptorSigningPayload(GOLDEN_RELAY_DESCRIPTOR));
  assert.equal(signingString, GOLDEN_DESCRIPTOR_SIGNING_STRING);
  assert.equal(verifyRelayDescriptorSignature(GOLDEN_RELAY_DESCRIPTOR), true);
  const resigned = CRYPTO.sign({
    privateKey: base64ToBytes(GOLDEN_NODE_PRIVATE_KEY_B64),
    msg: signedPayloadBytes(buildRelayDescriptorSigningPayload(GOLDEN_RELAY_DESCRIPTOR)),
  });
  assert.equal(Buffer.from(resigned).toString("base64"), GOLDEN_RELAY_DESCRIPTOR.sig.sigB64);
});

test("golden relay ID maps to the pinned DHT position", () => {
  assert.equal(DhtNodeId.fromRelayKeyId(GOLDEN_RELAY_KEY_ID).hex, GOLDEN_DHT_NODE_ID_HEX);
});

test("the golden unknown-kind durable record passes full node ingress verification", async () => {
  const verdict = await verifyDurableRecordDual(structuredClone(GOLDEN_DURABLE_RECORD_V1), GOLDEN_NOW_MS + 1000);
  assert.equal(verdict.ok, true, verdict.reason || "");
  assert.equal(verdict.localId, GOLDEN_DURABLE_RECORD_V1_LOCAL_ID);
});

test("the golden descriptor is admitted by the canonical store choke point", () => {
  const store = new RelayStore({ nowMs: () => GOLDEN_NOW_MS + 1000 });
  const result = store.upsertDescriptor(structuredClone(GOLDEN_RELAY_DESCRIPTOR), {
    source: "discovery", receivedAtMs: GOLDEN_NOW_MS + 1000,
  });
  assert.equal(result.accepted, true, result.reason || "");
  assert.ok(store.getDescriptor(GOLDEN_RELAY_KEY_ID, { nowMs: GOLDEN_NOW_MS + 1000 }));
});

// ── DurableRecordV2 vectors through the LIVE node ingress (re-audit R7) ─────
import {
  GOLDEN_DURABLE_RECORD_V2_DIRECT,
  GOLDEN_DURABLE_RECORD_V2_DIRECT_SLOT,
  GOLDEN_DURABLE_RECORD_V2_DELEGATED,
  GOLDEN_DURABLE_RECORD_V2_DELEGATED_SLOT,
  GOLDEN_DEVICE_PRIVATE_KEY_B64,
} from "../../rez-core/test/support/goldenVectors.js";
import { durableRecordV2SignableBytes } from "@rezprotocol/core";

test("the node's dual verifier admits both golden V2 records at their frozen slots", async () => {
  const direct = await verifyDurableRecordDual(GOLDEN_DURABLE_RECORD_V2_DIRECT, GOLDEN_NOW_MS + 1, {});
  assert.equal(direct.ok, true, direct.reason || "");
  assert.equal(direct.localId, GOLDEN_DURABLE_RECORD_V2_DIRECT_SLOT);

  const delegated = await verifyDurableRecordDual(GOLDEN_DURABLE_RECORD_V2_DELEGATED, GOLDEN_NOW_MS + 1, {});
  assert.equal(delegated.ok, true, delegated.reason || "");
  assert.equal(delegated.localId, GOLDEN_DURABLE_RECORD_V2_DELEGATED_SLOT,
    "delegated record lands on the OWNER-keyed slot");
});

test("NodeCryptoProvider reproduces the delegated V2 device signature byte-for-byte", () => {
  const signable = durableRecordV2SignableBytes(GOLDEN_DURABLE_RECORD_V2_DELEGATED);
  const resigned = CRYPTO.sign({
    privateKey: base64ToBytes(GOLDEN_DEVICE_PRIVATE_KEY_B64),
    msg: signable,
  });
  assert.equal(Buffer.from(resigned).toString("base64"), GOLDEN_DURABLE_RECORD_V2_DELEGATED.sigB64);
});
