import test from "node:test";
import assert from "node:assert/strict";
import {
  bytesToBase64,
  durableRecordLocalId,
  verifyAccountAuthority,
  AccountDeviceCapabilityV1,
} from "@rezprotocol/core";
import { DeviceLinkApprover, runDeviceLinkRequester } from "@rezprotocol/sdk/device-link";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { pgTestUrl } from "./support/integrationBackends.js";

// L6 — the registration-before-release proof the 2026-07-15 No-Go audit demanded.
//
// The finding was that the device-link ceremony minted and PUBLISHED a leaf cert to the new device
// before any home mutation bound its certId, leaving a window where the leaf was usable but
// off-home-unrevocable: peers verifying that leaf had no way to learn it should be rejected,
// because the home had never recorded which cert to revoke.
//
// This test closes the loop with the REAL ceremony crypto against REAL Postgres:
//   1. the home has the leaf's certId bound at the instant the response record is published;
//   2. a revoke in the PRE-ONLINE window (before the new device ever binds) auto-revokes THAT
//      certId into the account's authority state;
//   3. an OFF-HOME verifier, given only that published state, rejects the leaf.
//
// Step 3 is the point. Steps 1 and 2 are home-side bookkeeping; only step 3 shows the bookkeeping
// actually reaches a peer that has never talked to the home.
const PG_URL = pgTestUrl();
const CRYPTO = new NodeCryptoProvider();
const FAST = { pollIntervalMs: 5, pollMaxIntervalMs: 10, pollBackoff: 1 };

// An in-memory durable-record overlay. `onPut` fires BEFORE the record is retrievable, which is
// how the test observes home state at the exact moment of publication.
function makeOverlay({ onPut = null } = {}) {
  const map = new Map();
  return {
    async put({ record }) {
      if (onPut) await onPut(record);
      map.set(durableRecordLocalId(record), { ...record });
      return { localId: durableRecordLocalId(record), replicas: 1 };
    },
    async get({ recordKind, recordId, publisherPublicKeyB64 } = {}) {
      const found = map.get(durableRecordLocalId({ publisherPublicKeyB64, recordKind, recordId }));
      if (!found || found.expiresAtMs <= Date.now()) return null;
      return { ...found };
    },
  };
}

test(
  "L6: the home binds the leaf certId BEFORE release, and a pre-online revoke reaches off-home peers",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run", timeout: 120_000 },
  async (t) => {
    const SCHEMA = "test_pg_registration_before_release";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();

    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 4 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
    const serializer = new PgAccountMutationSerializer({ connection: conn, durableInbox, registry });

    // The account root B, and its identity-DH pair (the delegation bundle ships the pair).
    const b = await CRYPTO.generateSigningKeyPair();
    const bDh = await CRYPTO.dhGenerateKeyPair({ alg: "X25519", fmt: "spki" });
    const ACCOUNT = bytesToBase64(b.publicKey);

    // What the home knew at the moment the response record hit the overlay.
    let homeStateAtPublish = null;
    let publishedResponse = false;
    const overlay = makeOverlay({
      onPut: async (record) => {
        if (record.recordId !== "response") return;
        publishedResponse = true;
        homeStateAtPublish = await conn.query(
          "SELECT device_id, inbox_id, cert_id, status FROM account_device_registry WHERE account_identity = $1",
          [ACCOUNT],
        );
      },
    });

    // registerDevice: the production path is rez-chat submitting device.add; here it goes straight
    // at the real serializer, mirroring what AccountMutationHandler passes after verifying the
    // signed mutation envelope (that verification is proven in the handler suite).
    let opCounter = 0;
    const submitted = [];
    async function registerDevice({ newDeviceId, deviceInboxBinding, deviceCapability }) {
      submitted.push({ newDeviceId, deviceCapability });
      // The leaf must verify against the account before the home will bind it — the same check
      // AccountMutationHandler performs.
      const capability = new AccountDeviceCapabilityV1(deviceCapability);
      const authority = await verifyAccountAuthority({
        expectedAccountIdentityPublicKeyB64: ACCOUNT,
        opSignerPublicKeyB64: capability.granteeDevicePublicKeyB64,
        certChain: [capability.toJSON()],
        crypto: CRYPTO,
        nowMs: Date.now(),
        revocationState: null,
      });
      assert.equal(authority.ok, true, "the minted leaf verifies against the account root");

      const current = await serializer.getAuthorityState(ACCOUNT);
      opCounter += 1;
      const result = await serializer.submitMutation({
        accountIdentityPublicKeyB64: ACCOUNT,
        opId: "opid:l6:" + opCounter,
        expectedRevision: current.epoch,
        action: "device.add",
        target: {
          deviceId: deviceInboxBinding.deviceId,
          inboxId: deviceInboxBinding.inboxId,
          certId: capability.certId,
        },
      });
      const committed = result.devices.find((d) => d.deviceId === newDeviceId);
      assert.ok(committed, "the home committed this device");
      return { deviceId: committed.deviceId, inboxId: committed.inboxId, certId: committed.certId };
    }

    const journal = [];
    const approver = new DeviceLinkApprover({
      crypto: CRYPTO,
      records: overlay,
      accountSignPublicKeyB64: ACCOUNT,
      accountSign: async (bytes) => CRYPTO.sign({ privateKey: b.privateKey, msg: bytes }),
      accountDhKeyPair: { publicKeyB64: bytesToBase64(bDh.publicKey), privateKeyB64: bytesToBase64(bDh.privateKey) },
      registerDevice,
      registrationJournal: {
        async persistPending(record) { journal.push({ step: "persist", record }); },
        async markPublished() { journal.push({ step: "published" }); },
        async markConfirmed() { journal.push({ step: "confirmed" }); },
      },
      ...FAST,
    });

    const started = await approver.start();
    const requesterRun = runDeviceLinkRequester({ code: started.code, crypto: CRYPTO, records: overlay, persistDelegation: async () => null, ...FAST });
    await approver.waitForRequest();
    const approved = await approver.approve();
    const requester = await requesterRun;

    const LEAF_CERT_ID = approved.certId;
    const NEW_DEVICE = approved.newDeviceId;

    await t.test("the certId was bound at the home BEFORE the response record was published", async () => {
      assert.equal(publishedResponse, true, "the response was published");
      assert.ok(homeStateAtPublish, "home state was captured at publish time");
      assert.equal(homeStateAtPublish.rowCount, 1, "exactly one registered device at that instant");
      const row = homeStateAtPublish.rows[0];
      assert.equal(row.device_id, NEW_DEVICE);
      assert.equal(row.cert_id, LEAF_CERT_ID, "the leaf's certId was ALREADY bound — the No-Go window is closed");
      assert.equal(row.status, "active");
      assert.equal(row.inbox_id, requester.inboxId, "bound to the device's OWN ceremony inbox");

      // And the ordering the journal recorded agrees.
      assert.equal(journal[0].step, "persist", "the publication was durable before device.add");
      assert.equal(journal[0].record.certId, LEAF_CERT_ID);
    });

    await t.test("the new device received exactly the leaf the home bound", async () => {
      const chain = requester.delegation.certChain;
      assert.equal(chain.length, 1);
      assert.equal(chain[0].certId, LEAF_CERT_ID, "the released leaf IS the registered one");
      assert.equal(submitted.length, 1, "one device.add for one ceremony");
    });

    await t.test("a PRE-ONLINE revoke auto-revokes that certId into the account's authority state", async () => {
      // The device has never bound (no cursor yet) — this is the window the No-Go audit named.
      const cursors = await conn.query("SELECT 1 FROM device_cursors WHERE device_id = $1", [NEW_DEVICE]);
      assert.equal(cursors.rowCount, 0, "the device is still pre-online");

      const before = await serializer.getAuthorityState(ACCOUNT);
      const revoked = await serializer.submitMutation({
        accountIdentityPublicKeyB64: ACCOUNT,
        opId: "opid:l6:revoke",
        expectedRevision: before.epoch,
        action: "device.revoke",
        target: { revokedDeviceId: NEW_DEVICE },
      });
      // The revoke names no cert — the home auto-revokes the device's OWN bound cert, which is
      // only possible because device.add bound it in the first place.
      assert.ok(
        revoked.authorityState.revokedCertIds.includes(LEAF_CERT_ID),
        "the released leaf's certId is in the published revocation set",
      );
    });

    await t.test("an OFF-HOME verifier rejects the leaf using only the published state", async () => {
      // The whole point. A peer that has never spoken to the home gets the account's published
      // authority state and must refuse a chain it would otherwise have accepted.
      const state = await serializer.getAuthorityState(ACCOUNT);
      const chain = requester.delegation.certChain;
      const signer = chain[0].granteeDevicePublicKeyB64;

      const withoutRevocation = await verifyAccountAuthority({
        expectedAccountIdentityPublicKeyB64: ACCOUNT,
        opSignerPublicKeyB64: signer,
        certChain: chain,
        crypto: CRYPTO,
        nowMs: Date.now(),
        revocationState: null,
      });
      assert.equal(withoutRevocation.ok, true, "the leaf is cryptographically valid on its own");

      const withRevocation = await verifyAccountAuthority({
        expectedAccountIdentityPublicKeyB64: ACCOUNT,
        opSignerPublicKeyB64: signer,
        certChain: chain,
        crypto: CRYPTO,
        nowMs: Date.now(),
        revocationState: { revokedCertIds: state.revokedCertIds, minValidIssuedAtMs: state.minValidIssuedAtMs },
      });
      assert.equal(withRevocation.ok, false, "but an off-home peer with the published state REJECTS it");
    });

    await t.test("the revoked device cannot then come online and bind", async () => {
      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCOUNT,
          deviceId: NEW_DEVICE,
          inboxId: requester.inboxId,
          certId: LEAF_CERT_ID,
          authorityEpoch: 9,
          devicePublicKeyB64: "device-pub-l6",
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
    });
  },
);
