import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  AccountAuthorityStateV1,
  ACCOUNT_AUTHORITY_STATE_PURPOSE,
  ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
  DURABLE_RECORD_V2_VERSION,
  durableRecordV2SignableBytes,
  verifyDurableRecordV2,
  verifyAccountAuthority,
  AccountDeviceCapabilityV1,
  ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
  DeviceRegistrationV1,
  base64ToBytes,
} from "@rezprotocol/core";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { canonicalDeviceId } from "./helpers/deviceRegistryTestUtil.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgPropagationOutbox } from "../src/storage/pg/PgPropagationOutbox.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

// L7 precondition — the END-TO-END revoke-propagation proof.
//
// L6 proves a revoke lands in the account's authority STATE and that a verifier holding that state
// rejects the leaf. This proves the state actually REACHES such a verifier: the revoke's
// publication obligation is enqueued in the fold transaction, drained under the cluster lease,
// published as a signed record, stored by the node, and then FETCHED BACK and opened by a peer
// that has never spoken to the home — which then rejects the revoked leaf.
//
// The gap this closes is the difference between "the home knows" and "peers can find out". The
// 2026-07-15 No-Go was ultimately about that difference.
const PG_URL = process.env.REZ_PG_TEST_URL || "";
const CRYPTO = new NodeCryptoProvider();

test(
  "L7 precondition: a revoke propagates to a PUBLISHED record an off-home peer can act on",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run", timeout: 120_000 },
  async (t) => {
    const SCHEMA = "test_pg_revoke_propagation";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();

    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 4 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const outbox = new PgPropagationOutbox({ connection: conn });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox, registry, propagationOutbox: outbox });

    const b = await CRYPTO.generateSigningKeyPair();
    const ACCOUNT = bytesToBase64(b.publicKey);
    const DEVICE = canonicalDeviceId("l7-victim");
    const DRAINER = canonicalDeviceId("l7-drainer");

    // A real leaf cert for the device, signed by the account root — this is the credential an
    // off-home peer would otherwise accept.
    const deviceKp = await CRYPTO.generateSigningKeyPair();
    const granteePubB64 = bytesToBase64(deviceKp.publicKey);
    const leafNow = Date.now();
    const leafFields = {
      v: 1,
      purpose: ACCOUNT_DEVICE_CAPABILITY_PURPOSE,
      accountIdentityPublicKeyB64: ACCOUNT,
      parentCertId: null,
      granteeDevicePublicKeyB64: granteePubB64,
      granteeDeviceId: DeviceRegistrationV1.deviceIdFor(granteePubB64),
      capabilities: ["deviceSet.publish"],
      maxDelegationDepth: 0,
      issuedAtMs: leafNow - 1000,
      expiresAtMs: leafNow + 3_600_000,
      signerPublicKeyB64: ACCOUNT,
    };
    const leafCertId = AccountDeviceCapabilityV1.deriveCertId(leafFields);
    const leafSig = await CRYPTO.sign({
      privateKey: b.privateKey,
      msg: AccountDeviceCapabilityV1.signableBytes({ ...leafFields, certId: leafCertId }),
    });
    const leaf = new AccountDeviceCapabilityV1({
      ...leafFields,
      certId: leafCertId,
      sig: { alg: "ed25519", sigB64: bytesToBase64(leafSig) },
    });

    let opCounter = 0;
    async function submit(action, target) {
      const current = await serializer.getAuthorityState(ACCOUNT);
      opCounter += 1;
      return serializer.submitMutation({
        accountIdentityPublicKeyB64: ACCOUNT,
        opId: "opid:l7:" + opCounter,
        expectedRevision: current.epoch,
        action,
        target,
      });
    }

    await submit("device.add", { deviceId: DEVICE, inboxId: "inbox-l7-victim", certId: leaf.certId });

    let revokedEpoch = 0;
    await t.test("a revoke enqueues its publication obligation IN the fold transaction", async () => {
      const revoked = await submit("device.revoke", { revokedDeviceId: DEVICE });
      revokedEpoch = revoked.revision;
      assert.ok(revoked.authorityState.revokedCertIds.includes(leaf.certId), "the leaf's cert was auto-revoked");

      const pending = await conn.query(
        "SELECT epoch, status FROM account_propagation_outbox WHERE account_identity = $1 ORDER BY epoch",
        [ACCOUNT],
      );
      assert.ok(pending.rowCount >= 1, "the revoke left an obligation to publish");
      const forRevoke = pending.rows.find((r) => Number(r.epoch) === revokedEpoch);
      assert.ok(forRevoke, "including one for the revoke's own epoch");
      assert.equal(forRevoke.status, "pending");
    });

    // The DHT stand-in the node stores verified publications into, and that a peer reads from.
    const published = new Map();
    const dht = {
      async putRecord(record) {
        published.set(record.recordKind + "/" + record.recordId + "/" + record.ownerPublicKeyB64, { ...record });
        return { stored: true, localId: "L", replicas: 1 };
      },
    };

    let publishedRecord = null;
    await t.test("an authorized device drains the obligation and publishes a SIGNED authority state", async () => {
      // claim → prepare: the lease machinery decides which epoch this publication covers.
      const lease = await outbox.claim(ACCOUNT, DRAINER);
      assert.ok(lease, "the obligation was claimable");
      const prepared = await outbox.preparePublication(ACCOUNT, lease.token, DRAINER);
      assert.ok(prepared.headEpoch >= revokedEpoch, "the frozen epoch covers the revoke");

      // The client signs the account's authority state for exactly that epoch. In production this
      // is rez-chat (the node holds no account key); here the test plays that role with the same
      // account key and the same record shapes.
      const state = await serializer.getAuthorityState(ACCOUNT);
      const nowMs = Date.now();
      const stateBody = {
        v: 1,
        purpose: ACCOUNT_AUTHORITY_STATE_PURPOSE,
        accountIdentityPublicKeyB64: ACCOUNT,
        epoch: prepared.headEpoch,
        revokedCertIds: state.revokedCertIds,
        minValidIssuedAtMs: state.minValidIssuedAtMs,
        issuedAtMs: nowMs,
        signerPublicKeyB64: ACCOUNT,
      };
      const stateSig = bytesToBase64(await CRYPTO.sign({ privateKey: b.privateKey, msg: AccountAuthorityStateV1.signableBytes(stateBody) }));
      const authorityState = new AccountAuthorityStateV1({ ...stateBody, sig: { alg: "ed25519", sigB64: stateSig } });
      const payloadB64 = bytesToBase64(new TextEncoder().encode(JSON.stringify(authorityState.toJSON())));
      const envelope = {
        v: DURABLE_RECORD_V2_VERSION,
        recordKind: ACCOUNT_AUTHORITY_STATE_RECORD_KIND,
        recordId: "v1",
        ownerPublicKeyB64: ACCOUNT,
        signerPublicKeyB64: ACCOUNT,
        issuedAtMs: nowMs,
        expiresAtMs: nowMs + 3_600_000,
        payloadB64,
      };
      const record = {
        ...envelope,
        sigB64: bytesToBase64(await CRYPTO.sign({ privateKey: b.privateKey, msg: durableRecordV2SignableBytes(envelope) })),
      };

      // The node verifies before storing — the same order handleComplete uses: verify, STORE, then
      // mark done, so a 'done' watermark always implies the record is retrievable.
      const verdict = await verifyDurableRecordV2({ record, crypto: CRYPTO, nowMs, revocationState: { revokedCertIds: state.revokedCertIds, minValidIssuedAtMs: state.minValidIssuedAtMs } });
      assert.equal(verdict.ok, true, "the node accepts the publication");
      await dht.putRecord(record);
      const completed = await outbox.completePublication(ACCOUNT, lease.token, DRAINER, prepared.headEpoch);
      assert.equal(completed.completed, true);
      assert.equal(completed.doneThroughEpoch, prepared.headEpoch);
      publishedRecord = record;
    });

    await t.test("the obligation is DONE only after the record is retrievable", async () => {
      const rows = await conn.query(
        "SELECT epoch, status FROM account_propagation_outbox WHERE account_identity = $1 AND epoch <= $2",
        [ACCOUNT, revokedEpoch],
      );
      for (const row of rows.rows) {
        assert.equal(row.status, "done", "epoch " + row.epoch + " was drained");
      }
      assert.ok(published.size > 0, "and the record it drained is actually stored");
    });

    await t.test("a peer that never spoke to the home FETCHES the record and rejects the leaf", async () => {
      // This is the end of the chain: no home access, no shared state — just the published record.
      const fetched = published.get(ACCOUNT_AUTHORITY_STATE_RECORD_KIND + "/v1/" + ACCOUNT);
      assert.ok(fetched, "the peer can find the record by its owner-keyed coordinate");
      assert.deepEqual(fetched, publishedRecord);

      const nowMs = Date.now();
      const opened = await verifyDurableRecordV2({ record: fetched, crypto: CRYPTO, nowMs, revocationState: null });
      assert.equal(opened.ok, true, "the envelope verifies for a stranger");
      assert.equal(opened.ownerPublicKeyB64, ACCOUNT);

      const inner = new AccountAuthorityStateV1(JSON.parse(new TextDecoder().decode(base64ToBytes(fetched.payloadB64))));
      const innerOk = await CRYPTO.verify({
        publicKey: base64ToBytes(inner.signerPublicKeyB64),
        msg: AccountAuthorityStateV1.signableBytes(inner.toJSON()),
        sig: base64ToBytes(inner.sig.sigB64),
      });
      assert.equal(innerOk, true, "and so does the inner state");

      const revocationState = inner.toRevocationState();
      assert.ok(revocationState.revokedCertIds.includes(leaf.certId), "the published state names the revoked leaf");

      // With ONLY what it fetched, the peer refuses a chain it would otherwise accept.
      const signer = leaf.granteeDevicePublicKeyB64;
      const beforeState = await verifyAccountAuthority({
        expectedAccountIdentityPublicKeyB64: ACCOUNT,
        opSignerPublicKeyB64: signer,
        certChain: [leaf.toJSON()],
        crypto: CRYPTO,
        nowMs,
        revocationState: null,
      });
      assert.equal(beforeState.ok, true, "the leaf is valid on its own merits");

      const afterState = await verifyAccountAuthority({
        expectedAccountIdentityPublicKeyB64: ACCOUNT,
        opSignerPublicKeyB64: signer,
        certChain: [leaf.toJSON()],
        crypto: CRYPTO,
        nowMs,
        revocationState,
      });
      assert.equal(afterState.ok, false, "but the PUBLISHED revocation reaches the peer and it says no");
    });
  },
);
