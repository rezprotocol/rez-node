import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { revokeDeviceForTest, canonicalDeviceId } from "./helpers/deviceRegistryTestUtil.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { pgTestUrl } from "./support/integrationBackends.js";

// S2.5 S7 leaf A (audit F3, OPEN-A): the account→device→inbox registry — the
// explicit opt-in linkage that lets the home resolve ALL of an account's device
// inboxes (the precondition for account-wide device revocation). Real Postgres:
// enroll idempotency + conflict guards, inbox uniqueness, account-wide resolve,
// monotonic status changes.
const PG_URL = pgTestUrl();

// Canonical rez:cap:<64-hex> cert ids (finding 3 — the registry enforces the exact shape
// on every non-null cert), deterministic per seed. Distinct seeds ⇒ distinct certs, so
// cert-conflict assertions (leaf-a vs leaf-b) still hold.
const cap = (h) => "rez:cap:" + createHash("sha256").update(String(h)).digest("hex");

test(
  "PgAccountDeviceRegistry against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_account_device_registry";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    await conn.query("TRUNCATE account_device_registry");
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });

    const ACCT_A = "B-SIGN-ACCOUNT-A";
    const ACCT_B = "B-SIGN-ACCOUNT-B";

    // The registry (L2c) requires canonical rez:dev:<64-hex> ids on every enroll.
    // Deterministic canonical ids keyed by a readable seed (see canonicalDeviceId).
    const D = {
      a1: canonicalDeviceId("a1"),
      aPrimary: canonicalDeviceId("a-primary"),
      a2: canonicalDeviceId("a2"),
      recon1: canonicalDeviceId("recon1"),
      recon2: canonicalDeviceId("recon2"),
      recon3: canonicalDeviceId("recon3"),
      bSteal: canonicalDeviceId("b-steal"),
      atomic1: canonicalDeviceId("atomic1"),
      atomic2: canonicalDeviceId("atomic2"),
      atomic3: canonicalDeviceId("atomic3"),
    };

    await t.test("enroll a device binding, then resolve it by account and by inbox", async () => {
      const row = await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: D.a1,
        inboxId: "inbox-a1",
        certId: cap("leaf-a1"),
        authorityEpoch: 1,
      });
      assert.equal(row.status, "active");
      assert.equal(row.inboxId, "inbox-a1");
      assert.equal(row.certId, cap("leaf-a1"));
      assert.equal(row.authorityEpoch, 1);

      const byDevice = await registry.getDevice(ACCT_A, D.a1);
      assert.equal(byDevice.inboxId, "inbox-a1");

      const byInbox = await registry.resolveInbox("inbox-a1");
      assert.equal(byInbox.accountIdentityPublicKeyB64, ACCT_A);
      assert.equal(byInbox.deviceId, D.a1);
    });

    await t.test("a primary/direct device enrolls with a null cert", async () => {
      const row = await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: D.aPrimary,
        inboxId: "inbox-a-primary",
        certId: null,
        authorityEpoch: 1,
      });
      assert.equal(row.certId, null);
    });

    await t.test("re-enrolling the SAME binding is an idempotent no-op", async () => {
      const again = await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: D.a1,
        inboxId: "inbox-a1",
        certId: cap("leaf-a1"),
        authorityEpoch: 1,
      });
      assert.equal(again.inboxId, "inbox-a1");
    });

    await t.test("a DIFFERENT binding for an enrolled device is refused", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: D.a1,
          inboxId: "inbox-different",
          certId: cap("leaf-a1"),
          authorityEpoch: 1,
        }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
    });

    // S2.5 S12 L4 — cert reconciliation (serializer device.add writes cert_id=NULL,
    // device.bind enroll writes the leaf certId; two writers on one column).
    const ACCT_R = "B-SIGN-ACCOUNT-RECON";
    await t.test("cert reconciliation: a NULL-cert row upgrades to a leaf cert (device.add then device.bind)", async () => {
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: D.recon1, inboxId: "inbox-recon1", certId: null, authorityEpoch: 1 });
      const upgraded = await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: D.recon1, inboxId: "inbox-recon1", certId: cap("leaf-recon1"), authorityEpoch: 1 });
      assert.equal(upgraded.certId, cap("leaf-recon1"), "a null cert upgrades to the device's leaf cert");
    });

    await t.test("cert reconciliation: a non-null cert is NOT clobbered to null (device.bind then device.add)", async () => {
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: D.recon2, inboxId: "inbox-recon2", certId: cap("leaf-recon2"), authorityEpoch: 1 });
      const kept = await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: D.recon2, inboxId: "inbox-recon2", certId: null, authorityEpoch: 1 });
      assert.equal(kept.certId, cap("leaf-recon2"), "an enroll with null cert keeps the existing leaf cert");
    });

    await t.test("cert reconciliation: two DIFFERENT non-null certs genuinely conflict", async () => {
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: D.recon3, inboxId: "inbox-recon3", certId: cap("leaf-a"), authorityEpoch: 1 });
      await assert.rejects(
        () => registry.enroll({ accountIdentityPublicKeyB64: ACCT_R, deviceId: D.recon3, inboxId: "inbox-recon3", certId: cap("leaf-b"), authorityEpoch: 1 }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
    });

    await t.test("an inbox already enrolled to another (account, device) is refused", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_B,
          deviceId: D.bSteal,
          inboxId: "inbox-a1",
          certId: cap("leaf-b"),
          authorityEpoch: 1,
        }),
        (err) => err.code === "INBOX_ALREADY_ENROLLED",
      );
    });

    await t.test("listDevices resolves all of an account's device bindings (sibling lookup)", async () => {
      await registry.enroll({
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: D.a2,
        inboxId: "inbox-a2",
        certId: cap("leaf-a2"),
        authorityEpoch: 1,
      });
      const devices = await registry.listDevices(ACCT_A);
      const ids = devices.map((d) => d.deviceId).sort();
      assert.deepEqual(ids, [D.aPrimary, D.a1, D.a2].sort());
    });

    await t.test("revoke flips a device to revoked and stamps the authority epoch", async () => {
      const revoked = await revokeDeviceForTest(conn, registry, {
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: D.a2,
        authorityEpoch: 2,
      });
      assert.equal(revoked.status, "revoked");
      assert.equal(revoked.authorityEpoch, 2);
      assert.equal((await registry.getDevice(ACCT_A, D.a2)).status, "revoked");
      // A revoke of an ENROLLED device also writes the terminal tombstone.
      assert.equal(await registry.isTombstoned(ACCT_A, D.a2), true);
    });

    // Audit 2026-07-09 P1 (revoke-before-bind): a device.add can enroll a device
    // before it binds; an account-wide revoke marks the row revoked; a later
    // device.bind must NOT be able to re-enroll (which would leave a live cursor
    // for a revoked device). enroll fails loud on a revoked row.
    await t.test("enroll refuses a device whose registry row is revoked (no resurrection)", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: D.a2,
          inboxId: "inbox-a2",
          certId: cap("leaf-a2"),
          authorityEpoch: 2,
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      // The row is untouched by the refused enroll.
      assert.equal((await registry.getDevice(ACCT_A, D.a2)).status, "revoked");
    });

    // Audit R4 F1: a revoke of a NEVER-ENROLLED device writes a durable tombstone
    // (there is no registry row to flip) so a later device.add / device.bind of
    // that same deviceId can never resurrect it. Before F1 the revoke left no
    // trace and the enroll succeeded ACTIVE.
    const NE_CANON = "rez:dev:" + "f".repeat(64); // canonical, never enrolled
    await t.test("revoke of a never-enrolled device tombstones it; later enroll is refused (F1)", async () => {
      const res = await revokeDeviceForTest(conn, registry, {
        accountIdentityPublicKeyB64: ACCT_A,
        deviceId: NE_CANON,
        authorityEpoch: 5,
      });
      assert.equal(res, null, "a tombstone-only revoke returns null (no enrolled binding)");
      assert.equal(await registry.isTombstoned(ACCT_A, NE_CANON), true);
      assert.equal(await registry.getDevice(ACCT_A, NE_CANON), null, "no active/revoked registry row exists");

      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: NE_CANON,
          inboxId: "inbox-ne-canon",
          certId: null,
          authorityEpoch: 6,
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
    });

    // Audit R4 F1 enforcement on the serializer's device.add fold path: foldAddInTx
    // (the method the serializer calls under its account lock) must ALSO honor the
    // tombstone, or a signed device.add resurrects the revoked deviceId.
    await t.test("foldAddInTx refuses a tombstoned deviceId (device.add cannot resurrect)", async () => {
      await conn.withClient(async (client) => {
        await client.query("BEGIN");
        try {
          await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [ACCT_A]);
          await assert.rejects(
            () => registry.foldAddInTx(client, {
              accountIdentityPublicKeyB64: ACCT_A,
              deviceId: NE_CANON,
              inboxId: "inbox-ne-canon2",
              certId: null,
              authorityEpoch: 7,
            }),
            (err) => err.code === "DEVICE_REVOKED",
          );
        } finally {
          await client.query("ROLLBACK");
        }
      });
    });

    // Audit R4 F1 review / L2c: the registry is the canonical device-ID invariant
    // OWNER. Every add/enroll rejects a non-canonical id, so a NEVER-ENROLLED
    // non-canonical revoke target can never enroll — it is REJECTED before any
    // tombstone (nothing to resurrect, nothing to store). A revoke that flips a REAL
    // row is unaffected (fail-close proceeds for any shape); that is the NE_CANON
    // and enrolled-device tombstone cases above.
    await t.test("enroll rejects a non-canonical deviceId (registry enforces shape, not just the record)", async () => {
      await assert.rejects(
        () => registry.enroll({
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:not-a-canonical-id",
          inboxId: "inbox-noncanon",
          certId: null,
          authorityEpoch: 1,
        }),
        (err) => err.code === "BAD_DEVICE_ID",
      );
    });

    await t.test("revoke of a NEVER-ENROLLED non-canonical deviceId is rejected before any tombstone", async () => {
      await assert.rejects(
        () => revokeDeviceForTest(conn, registry, {
          accountIdentityPublicKeyB64: ACCT_A,
          deviceId: "rez:dev:not-a-canonical-id",
          authorityEpoch: 8,
        }),
        (err) => err.code === "BAD_DEVICE_ID",
      );
      assert.equal(await registry.isTombstoned(ACCT_A, "rez:dev:not-a-canonical-id"), false, "no tombstone written for a rejected non-canonical never-enrolled revoke");
    });

    await t.test("setStatus and the public revoke() alternate-writer surfaces are removed", () => {
      assert.equal(typeof registry.setStatus, "undefined");
      assert.equal(typeof registry.revoke, "undefined", "no public split-brain revoke() — the serializer owns account-authority revoke");
    });

    await t.test("getDevice / resolveInbox return null for unknown keys", async () => {
      assert.equal(await registry.getDevice(ACCT_A, "rez:dev:ghost"), null);
      assert.equal(await registry.resolveInbox("inbox-ghost"), null);
      assert.equal(await registry.getDevice("", ""), null);
    });

    // ---- audit 2026-07-10 P2: atomic bind enroll+cursor ----

    await t.test("constructor fails loud without a durableInbox (atomic enroll+cursor dependency)", () => {
      assert.throws(() => new PgAccountDeviceRegistry({ connection: conn }), /durableInbox/);
    });

    await t.test("enrollWithCursor creates the registry row AND the delivery cursor in one commit", async () => {
      const ACCT = "B-SIGN-ACCOUNT-ATOMIC";
      const row = await registry.enrollWithCursor({
        accountIdentityPublicKeyB64: ACCT,
        deviceId: D.atomic1,
        inboxId: "inbox-atomic1",
        certId: cap("leaf-atomic1"),
        authorityEpoch: 1,
        devicePublicKeyB64: "DEVICE-PUB-ATOMIC1",
      });
      assert.equal(row.status, "active");
      assert.equal(row.inboxId, "inbox-atomic1");

      const cursor = await durableInbox.getDevice("inbox-atomic1", D.atomic1);
      assert.ok(cursor, "the delivery cursor exists");
      assert.equal(cursor.revoked, false);
      assert.equal(cursor.devicePublicKeyB64, "DEVICE-PUB-ATOMIC1", "the proven device key was persisted with the cursor");
    });

    // The exact race the audit flagged: a registry row already revoked when the
    // bind's persist runs. The refused enroll must roll the cursor create back
    // with it — no cursor row may exist afterward, live OR revoked.
    await t.test("enrollWithCursor against a revoked registry row throws DEVICE_REVOKED and leaves NO cursor row", async () => {
      const ACCT = "B-SIGN-ACCOUNT-ATOMIC-RVK";
      await registry.enroll({ accountIdentityPublicKeyB64: ACCT, deviceId: D.atomic2, inboxId: "inbox-atomic2", certId: null, authorityEpoch: 1 });
      await revokeDeviceForTest(conn, registry, { accountIdentityPublicKeyB64: ACCT, deviceId: D.atomic2, authorityEpoch: 2 });

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCT,
          deviceId: D.atomic2,
          inboxId: "inbox-atomic2",
          certId: cap("leaf-atomic2"),
          authorityEpoch: 2,
          devicePublicKeyB64: "DEVICE-PUB-ATOMIC2",
        }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      assert.equal(await durableInbox.getDevice("inbox-atomic2", D.atomic2), null, "no cursor row exists — the rollback covered the cursor create");
    });

    // Reverse direction: a cursor-create failure must roll the enroll back too.
    await t.test("a cursor-create failure (key mismatch) rolls back the registry enroll", async () => {
      const ACCT = "B-SIGN-ACCOUNT-ATOMIC-MISMATCH";
      // A cursor for this (inbox, device) already exists under a DIFFERENT key.
      await durableInbox.registerDevice("inbox-atomic3", D.atomic3, { devicePublicKeyB64: "KEY-A" });

      await assert.rejects(
        () => registry.enrollWithCursor({
          accountIdentityPublicKeyB64: ACCT,
          deviceId: D.atomic3,
          inboxId: "inbox-atomic3",
          certId: null,
          authorityEpoch: 1,
          devicePublicKeyB64: "KEY-B",
        }),
        (err) => err.code === "DEVICE_KEY_MISMATCH",
      );
      assert.equal(await registry.getDevice(ACCT, D.atomic3), null, "the enroll rolled back with the failed cursor create");
    });

    // ---- audit R4 F3: durable admission-control caps (constructor-configurable) ----

    await t.test("F3: the active-device cap refuses a device over the per-account limit", async () => {
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox, caps: { activeDevices: 2, lifetimeDevices: 100 } });
      const A = "B-SIGN-F3-ACTIVE-CAP";
      await reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("acv1"), inboxId: "inbox-acv1", authorityEpoch: 1 });
      await reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("acv2"), inboxId: "inbox-acv2", authorityEpoch: 1 });
      await assert.rejects(
        () => reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("acv3"), inboxId: "inbox-acv3", authorityEpoch: 1 }),
        (err) => err.code === "DEVICE_LIMIT",
      );
      // A re-enroll of an EXISTING active device is never gated (no new device).
      const same = await reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("acv1"), inboxId: "inbox-acv1", authorityEpoch: 1 });
      assert.equal(same.status, "active");
    });

    await t.test("F3: the lifetime-device cap counts a REVOKED/tombstoned device too", async () => {
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox, caps: { activeDevices: 100, lifetimeDevices: 2 } });
      const A = "B-SIGN-F3-LIFETIME-CAP";
      await reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("lif1"), inboxId: "inbox-lif1", authorityEpoch: 1 });
      // Revoke lif1 — it becomes revoked + tombstoned but STILL counts toward lifetime.
      await revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("lif1"), authorityEpoch: 2 });
      // lif2 is the 2nd distinct device (lifetime = {lif1 tombstoned, lif2 active} = 2).
      await reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("lif2"), inboxId: "inbox-lif2", authorityEpoch: 3 });
      // lif3 would be the 3rd distinct device — refused even though only 1 is active.
      await assert.rejects(
        () => reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("lif3"), inboxId: "inbox-lif3", authorityEpoch: 3 }),
        (err) => err.code === "DEVICE_LIMIT",
      );
    });

    await t.test("F3: the tombstone cap honors the configured value", async () => {
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox, caps: { revokedDevices: 2 } });
      const A = "B-SIGN-F3-TOMBSTONE-CAP";
      // Never-enrolled canonical revokes each write a quota-gated tombstone.
      await revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("tmb1"), authorityEpoch: 1 });
      await revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("tmb2"), authorityEpoch: 2 });
      await assert.rejects(
        () => revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("tmb3"), authorityEpoch: 3 }),
        (err) => err.code === "REVOKED_DEVICE_QUOTA_EXCEEDED",
      );
    });

    // F3-remediation finding 3: the lifetime union cap must ALSO bound never-enrolled
    // tombstones — before the fix they were gated only by the (independent) tombstone
    // cap, so with lifetime < revoked a run of never-enrolled revokes pushed
    // active∪revoked∪tombstoned past lifetimeDevices (the auditor's reproduced bypass).
    await t.test("F3-remediation finding 3: a never-enrolled tombstone is bounded by the LIFETIME cap, not only the tombstone cap", async () => {
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox, caps: { lifetimeDevices: 2, revokedDevices: 10 } });
      const A = "B-SIGN-F3R-TOMB-LIFETIME";
      await revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("tl1"), authorityEpoch: 1 });
      await revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("tl2"), authorityEpoch: 2 });
      // The 3rd never-enrolled tombstone would push the lifetime union to 3 > 2 — refused
      // by the lifetime cap (DEVICE_LIMIT) even though the tombstone cap (10) has room.
      await assert.rejects(
        () => revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("tl3"), authorityEpoch: 3 }),
        (err) => err.code === "DEVICE_LIMIT",
      );
    });

    // F3-remediation finding 3 (fail-close exemption preserved): a revoke that flips a
    // REAL enrolled row must NEVER be gated by the lifetime/tombstone caps — a fail-close
    // revoke must never fail — even with both caps at the boundary.
    await t.test("F3-remediation finding 3: a fail-close revoke of a REAL enrolled row is never gated by the caps", async () => {
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox, caps: { lifetimeDevices: 1, revokedDevices: 1 } });
      const A = "B-SIGN-F3R-FAILCLOSE";
      await reg.enroll({ accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("fc1"), inboxId: "inbox-fc1", authorityEpoch: 1 });
      // Both caps are at the boundary, but revoking the enrolled device (a real row →
      // registryRowExisted) must proceed and tombstone it regardless.
      const binding = await revokeDeviceForTest(conn, reg, { accountIdentityPublicKeyB64: A, deviceId: canonicalDeviceId("fc1"), authorityEpoch: 2 });
      assert.ok(binding, "the real-row revoke returned its binding (fail-close proceeded)");
      assert.equal(await reg.isTombstoned(A, canonicalDeviceId("fc1")), true, "the enrolled device was tombstoned despite caps at the boundary");
    });

    // Round-5 finding 3: session auth + the dispatch guard consume the canonical terminal
    // predicate (status='revoked' OR tombstoned), not tombstone alone — a HISTORICAL revoked
    // registry row can lack a tombstone.
    await t.test("round-5 finding 3: isTerminallyRevoked = status='revoked' OR tombstoned (not tombstone alone)", async () => {
      const A = "B-SIGN-R5-TERMINAL";
      const dActive = canonicalDeviceId("term-active");
      const dRevoked = canonicalDeviceId("term-revoked");
      const dHistorical = canonicalDeviceId("term-historical");

      await registry.enroll({ accountIdentityPublicKeyB64: A, deviceId: dActive, inboxId: "inbox-term-active", authorityEpoch: 1 });
      assert.equal(await registry.isTerminallyRevoked(A, dActive), false, "an active device is not terminal");

      // A canonical revoke → status='revoked' AND a tombstone.
      await registry.enroll({ accountIdentityPublicKeyB64: A, deviceId: dRevoked, inboxId: "inbox-term-revoked", authorityEpoch: 1 });
      await revokeDeviceForTest(conn, registry, { accountIdentityPublicKeyB64: A, deviceId: dRevoked, authorityEpoch: 2 });
      assert.equal(await registry.isTerminallyRevoked(A, dRevoked), true, "a revoked+tombstoned device is terminal");

      // A HISTORICAL revoked row written RAW with NO tombstone — terminal via status alone.
      await conn.query(
        "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
          + " VALUES ($1,$2,'inbox-term-hist',NULL,2,'revoked')",
        [A, dHistorical],
      );
      assert.equal(await registry.isTombstoned(A, dHistorical), false, "the historical row has no tombstone");
      assert.equal(await registry.isTerminallyRevoked(A, dHistorical), true, "but it IS terminal via status='revoked' (finding 3)");
    });
  },
);
