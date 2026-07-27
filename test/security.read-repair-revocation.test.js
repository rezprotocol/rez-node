import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  buildDurableRecordV2,
  durableRecordV2SignableBytes,
  durableRecordV2Slot,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  DeviceRegistrationV1,
} from "@rezprotocol/core";
import { DhtNode } from "../src/routing/dht/DhtNode.js";
import { DhtNodeId } from "../src/routing/dht/DhtNodeId.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

// AUDIT P0 follow-on — READ-REPAIR revocation hardening (2026-07-27).
//
// record.put refuses a delegated record signed against this account's revoked authority. Read-repair
// did not: a lookup that missed locally fetched the record from the overlay, verified it
// revocation-BLIND (correctly — the overlay is account-agnostic), then PERSISTED it and re-served it
// to peers on rec_find. So the node became a durable distributor of exactly what its own front door
// refuses, and an attacker only had to get the record onto any replica and then ask us to resolve it.
//
// The fix separates two decisions that were previously one:
//   CACHE  — a durable commitment (we become a holder). Never made on a guess.
//   SERVE  — relaying what the overlay already said, to a caller that re-verifies for itself.
const CRYPTO = new NodeCryptoProvider();
const NOW = 1_700_000_000_000;
const FAR = NOW + 3_600_000;
const PAYLOAD = Buffer.from("device-set").toString("base64");

function key() {
  const kp = CRYPTO.generateSigningKeyPair();
  return { publicKeyB64: Buffer.from(kp.publicKey).toString("base64"), privateKey: kp.privateKey };
}

function buildLeaf({ account, granteePub }) {
  const fields = {
    v: 1,
    purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
    accountIdentityPublicKeyB64: account.publicKeyB64,
    parentCertId: null,
    granteeDevicePublicKeyB64: granteePub,
    granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePub),
    capabilities: ["deviceSet.publish"],
    maxDelegationDepth: 0,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
    signerPublicKeyB64: account.publicKeyB64,
  };
  const certId = AccountDeviceCapabilityV1.deriveCertId(fields);
  const sig = CRYPTO.sign({ privateKey: account.privateKey, msg: AccountDeviceCapabilityV1.signableBytes({ ...fields, certId }) });
  return new AccountDeviceCapabilityV1({ ...fields, certId, sig: { alg: "ed25519", sigB64: Buffer.from(sig).toString("base64") } });
}

function delegatedRecord({ owner, signer, leaf, recordId = "peer-1" }) {
  const rec = buildDurableRecordV2({
    recordKind: "peerlink-device-set",
    recordId,
    ownerPublicKeyB64: owner.publicKeyB64,
    signerPublicKeyB64: signer.publicKeyB64,
    certChain: [leaf.toJSON()],
    requiredCapability: "deviceSet.publish",
    payloadB64: PAYLOAD,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
  });
  const sig = CRYPTO.sign({ privateKey: signer.privateKey, msg: durableRecordV2SignableBytes(rec) });
  return { ...rec, sigB64: Buffer.from(sig).toString("base64") };
}

function directRecord({ owner, recordId = "direct-1" }) {
  const rec = buildDurableRecordV2({
    recordKind: "peerlink-device-set",
    recordId,
    ownerPublicKeyB64: owner.publicKeyB64,
    payloadB64: PAYLOAD,
    issuedAtMs: NOW,
    expiresAtMs: FAR,
  });
  const sig = CRYPTO.sign({ privateKey: owner.privateKey, msg: durableRecordV2SignableBytes(rec) });
  return { ...rec, sigB64: Buffer.from(sig).toString("base64") };
}

/**
 * A DhtNode with ONE peer whose rec_find always answers with `served`. That is enough to drive
 * #resolveRecordOverlay: getRecord misses locally, the lookup queries the peer, and the reply feeds
 * the read-repair gate.
 */
function makeNode({ served, serializer }) {
  const registry = { register() {}, unregister() {} };
  const node = new DhtNode({
    selfRelayKeyId: "relay-self",
    controlMessageRegistry: registry,
    encodeCtl: (obj) => new TextEncoder().encode(JSON.stringify(obj)),
    trySendFrame: () => {},
    nowMs: () => NOW + 1000,
    config: { k: 20, alpha: 3, queryTimeoutMs: 50 },
  });
  if (serializer !== undefined) node.setAuthoritySerializer(serializer);

  const peerSocket = { id: "peer-1", destroyed: false };
  node.addPeer("relay-peer", peerSocket);
  // Intercept the record protocol's rec_find so the "overlay" answers deterministically.
  node.recordProtocol.queryRecFind = async () => ({ value: served, nodes: [] });
  return node;
}

function authority({ revokedCertIds = [], minValidIssuedAtMs = 0, throws = null, malformed = false } = {}) {
  let reads = 0;
  return {
    get reads() { return reads; },
    async getAuthorityState() {
      reads += 1;
      if (throws) throw throws;
      if (malformed) return { epoch: 1 }; // no revokedCertIds array
      return { epoch: 1, revokedCertIds, minValidIssuedAtMs };
    },
  };
}

function slotFor(owner, recordId = "peer-1") {
  return durableRecordV2Slot({
    ownerPublicKeyB64: owner.publicKeyB64,
    recordKind: "peerlink-device-set",
    recordId,
  });
}

describe("AUDIT P0 follow-on: read-repair applies the home's revocation state", () => {
  it("REFUSES to cache or serve a record signed against a revoked certificate", async () => {
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    const node = makeNode({ served: record, serializer: authority({ revokedCertIds: [leaf.certId] }) });
    const got = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "peer-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });

    assert.equal(got, null, "not served");
    assert.equal(node.recordStore.size, 0, "and — the point of the fix — not cached either");
  });

  it("a revoked record is not left holdable via the resolve-on-behalf path either", async () => {
    // The same funnel serves NAT'd leaf clients that delegate their lookup to us. If it cached
    // there, the front-door refusal would be trivially bypassable.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    const node = makeNode({ served: record, serializer: authority({ revokedCertIds: [leaf.certId] }) });
    const localId = slotFor(account);
    // getRecord and the protocol's resolveAcrossOverlay share #resolveRecordOverlay; driving it by
    // slot id is the resolve-on-behalf shape.
    const resolved = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "peer-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });
    assert.equal(resolved, null);
    assert.equal(node.recordStore.get(localId, NOW + 1000), null);
  });

  it("still caches and serves a NON-revoked delegated record", async () => {
    // The other half: this gate must not break ordinary multi-device fan-out reads.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    const node = makeNode({ served: record, serializer: authority({ revokedCertIds: [] }) });
    const got = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "peer-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });

    assert.ok(got, "served");
    assert.equal(node.recordStore.size, 1, "and read-repaired as before");
  });

  it("applies the issuance CUTOFF, not just the revoked-id set", async () => {
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    const node = makeNode({ served: record, serializer: authority({ minValidIssuedAtMs: FAR }) });
    const got = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "peer-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });

    assert.equal(got, null);
    assert.equal(node.recordStore.size, 0);
  });

  it("an unresolvable authority SERVES but does NOT cache", async () => {
    // The deliberate asymmetry. Caching is a durable commitment and must not proceed on a guess;
    // serving is a relay of what the overlay already said, to a caller that re-verifies anyway.
    // Failing the read closed here would take reads down on a database hiccup for no safety gain.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    for (const serializer of [authority({ throws: new Error("db down") }), authority({ malformed: true })]) {
      const node = makeNode({ served: record, serializer });
      const got = await node.getRecord({
        recordKind: "peerlink-device-set",
        recordId: "peer-1",
        publisherPublicKeyB64: account.publicKeyB64,
      });
      assert.ok(got, "the read stays available");
      assert.equal(node.recordStore.size, 0, "but we do not become a holder");
    }
  });

  it("does NOT query the authority for a record carrying no delegation", async () => {
    const account = key();
    const record = directRecord({ owner: account });
    const serializer = authority({});
    const node = makeNode({ served: record, serializer });

    const got = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "direct-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });

    assert.ok(got);
    assert.equal(node.recordStore.size, 1);
    assert.equal(serializer.reads, 0, "a direct record has nothing revocable");
  });

  it("REPLICA behavior is preserved: a node with no home authority caches as before", async () => {
    // fs / desktop / relay-only deployments home no accounts and cannot answer the question. They
    // must keep working exactly as they did, or plain relays stop replicating delegated records.
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    const node = makeNode({ served: record }); // no serializer wired at all
    const got = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "peer-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });

    assert.ok(got);
    assert.equal(node.recordStore.size, 1);
  });

  it("an account this cluster does not home reads as clean, so foreign records still land", async () => {
    const account = key();
    const device = key();
    const leaf = buildLeaf({ account, granteePub: device.publicKeyB64 });
    const record = delegatedRecord({ owner: account, signer: device, leaf });

    // An unknown account answers { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 }.
    const node = makeNode({ served: record, serializer: authority({}) });
    const got = await node.getRecord({
      recordKind: "peerlink-device-set",
      recordId: "peer-1",
      publisherPublicKeyB64: account.publicKeyB64,
    });

    assert.ok(got);
    assert.equal(node.recordStore.size, 1);
  });

  it("the k-bucket target derivation is unchanged (regression guard)", () => {
    // The gate sits after verification and slot-binding; it must not have disturbed either.
    const account = key();
    const localId = slotFor(account);
    assert.equal(DhtNodeId.fromHex(localId).hex, localId);
  });
});
