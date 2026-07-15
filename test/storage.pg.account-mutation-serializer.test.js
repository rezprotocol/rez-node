import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createIsolatedPgConnection, dropSchema } from "./helpers/pgTestSchema.js";
import { canonicalDeviceId } from "./helpers/deviceRegistryTestUtil.js";
import { MigrationRunner } from "../src/storage/pg/MigrationRunner.js";
import { PgAccountMutationSerializer } from "../src/storage/pg/PgAccountMutationSerializer.js";
import { PgAccountDeviceRegistry } from "../src/storage/pg/PgAccountDeviceRegistry.js";
import { PgDurableInbox } from "../src/storage/pg/PgDurableInbox.js";
import { PgConnection } from "../src/storage/pg/PgConnection.js";

// S2.5 S11 L4 (findings F4+F5, OPEN-B): the authority-home serializer. Real
// Postgres: opId idempotency, expectedRevision CAS (stale returns latest, no
// clobber), add/revoke fold (remove-wins), monotonic epoch, revoked-cert set +
// minValidIssuedAt cutoff, concurrent submits serialize.
const PG_URL = process.env.REZ_PG_TEST_URL || "";

test(
  "PgAccountMutationSerializer against real Postgres",
  { skip: PG_URL ? false : "set REZ_PG_TEST_URL to run" },
  async (t) => {
    const SCHEMA = "test_pg_account_mutation_serializer";
    const conn = await createIsolatedPgConnection(PG_URL, SCHEMA);
    t.after(async () => {
      await conn.close();
      await dropSchema(PG_URL, SCHEMA);
    });
    await new MigrationRunner({ connection: conn }).migrate();
    const durableInbox = new PgDurableInbox({ connection: conn, maxDevices: 1 });
    const s = new PgAccountMutationSerializer({ connection: conn, durableInbox });

    const ACCT = "B-SIGN-ACCT-1";
    // Canonical rez:cap:<64-hex> ids (finding 2) — deterministic per seed, matching what
    // deriveAccountCapabilityCertId emits, so a caller-supplied revokedCertId passes the
    // serializer's canonical guard and can equal a device's bound cert.
    const cap = (h) => "rez:cap:" + createHash("sha256").update(String(h)).digest("hex");
    // The registry (L2c) requires canonical device ids on every device.add fold, so
    // even the serializer's flat-target tests use real canonical ids (deterministic).
    const D = {
      d1: canonicalDeviceId("d1"), dX: canonicalDeviceId("dX"), d2: canonicalDeviceId("d2"),
      d3: canonicalDeviceId("d3"), s: canonicalDeviceId("s"), c1: canonicalDeviceId("c1"),
      c2: canonicalDeviceId("c2"), coal: canonicalDeviceId("coal"), ghost: canonicalDeviceId("ghost"),
      atomic: canonicalDeviceId("atomic"), rb: canonicalDeviceId("rb"), res: canonicalDeviceId("res"),
      rp: canonicalDeviceId("rp"), same: canonicalDeviceId("same"),
    };

    await t.test("first device.add: epoch 0 -> 1, device active, authorityState empty", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-1", expectedRevision: 0,
        action: "device.add", target: { deviceId: D.d1, inboxId: "inbox-d1", certId: cap("leaf1") },
      });
      assert.equal(r.revision, 1);
      assert.equal(r.idempotentReplay, false);
      assert.equal(r.devices.length, 1);
      assert.equal(r.devices[0].deviceId, D.d1);
      assert.deepEqual(r.authorityState, { epoch: 1, revokedCertIds: [], minValidIssuedAtMs: 0 });
    });

    await t.test("idempotent replay: the SAME opId returns the committed result, no re-apply", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-1", expectedRevision: 0,
        action: "device.add", target: { deviceId: D.d1, inboxId: "inbox-d1" },
      });
      assert.equal(r.revision, 1, "revision unchanged");
      assert.equal(r.idempotentReplay, true);
      // Epoch did not advance.
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 1);
    });

    await t.test("stale expectedRevision returns the latest state and does NOT clobber", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-stale", expectedRevision: 0, // current is 1
        action: "device.add", target: { deviceId: D.dX, inboxId: "inbox-dX" },
      });
      assert.equal(r.stale, true);
      assert.equal(r.currentRevision, 1);
      assert.equal(r.devices.length, 1, "dX was NOT added");
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 1, "epoch not bumped by a stale submit");
    });

    await t.test("second device.add at the right revision: epoch 1 -> 2, two active devices", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-add-2", expectedRevision: 1,
        action: "device.add", target: { deviceId: D.d2, inboxId: "inbox-d2", certId: cap("leaf2") },
      });
      assert.equal(r.revision, 2);
      assert.equal(r.devices.length, 2);
    });

    await t.test("device.add rejects an inbox already held by another device", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: ACCT, opId: "op-dup-inbox", expectedRevision: 2,
          action: "device.add", target: { deviceId: D.d3, inboxId: "inbox-d1" },
        }),
        (err) => err.code === "INBOX_ALREADY_ENROLLED",
      );
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 2, "a rejected add does not bump the epoch");
    });

    await t.test("device.revoke: remove-wins, bumps epoch, active set shrinks, and AUTO-revokes the target's OWN bound cert (Option A, finding 1)", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-d2", expectedRevision: 2,
        action: "device.revoke",
        // A supplied revokedCertId equal to D.d2's bound cert is accepted (redundant); the
        // fold auto-revokes that same bound cert to COMPLETE the device revocation.
        target: { revokedDeviceId: D.d2, revokedCertId: cap("leaf2") },
      });
      assert.equal(r.revision, 3);
      assert.equal(r.devices.length, 1, "only d1 remains active");
      assert.equal(r.devices[0].deviceId, D.d1);
      assert.deepEqual(r.authorityState.revokedCertIds, [cap("leaf2")], "the device's OWN bound cert is in the revoked set (completeness)");
      assert.equal(r.authorityState.minValidIssuedAtMs, 0);
      assert.equal(r.authorityState.epoch, 3);
    });

    await t.test("L5 review finding 3: getAuthorityState reads epoch + revoked set as ONE coherent snapshot", async () => {
      // After the revoke, a single transactional read must return the epoch AND the revoked-cert
      // set that belong to the SAME committed state — never a mixed (old-epoch / new-revoked) view.
      // This pins the happy-path coherence of the REPEATABLE READ snapshot (the two SELECTs cannot
      // straddle a concurrent commit).
      const st = await s.getAuthorityState(ACCT);
      assert.equal(st.epoch, 3, "epoch reflects the revoke");
      assert.deepEqual(st.revokedCertIds, [cap("leaf2")], "revoked set reflects the SAME revoke as the epoch");
      assert.equal(st.minValidIssuedAtMs, 0);
    });

    await t.test("L5 review finding 1 (TOCTOU): getDelegatedAuthoritySnapshot reads terminal + epoch coherently even when a cert_id=NULL revoke commits MID-SNAPSHOT", async () => {
      // The exact exploit: a delegated device with cert_id = NULL (revoke would auto-revoke no
      // cert) is revoked in the window between the guard's terminal read and its epoch read. The
      // old code read terminal via a separate pooled query (pre-revoke: false) but the epoch from a
      // later snapshot (post-revoke), then armed the fast-path watermark to the revoke epoch and
      // never re-checked terminal → the revoked device kept authority forever.
      //
      // The fix reads terminal WITHIN the same REPEATABLE READ snapshot as epoch/certs. We prove it
      // by committing the revoke, on another pooled client, DURING the snapshot's terminal read.
      const RACE = "B-SIGN-TOCTOU-NULLCERT";
      const nullDev = canonicalDeviceId("toctou-nullcert");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      // Enroll the device with NO cert (cert_id stays NULL) → revoke auto-revokes no cert.
      await s.submitMutation({
        accountIdentityPublicKeyB64: RACE, opId: "race-seed", expectedRevision: 0,
        action: "device.add", target: { deviceId: nullDev, inboxId: "inbox-toctou" },
      });
      assert.equal(await s.getCurrentEpoch(RACE), 1, "seeded at epoch 1");

      let injected = false;
      // review-3 finding P2: the snapshot resolves terminal through the serializer's OWN registry,
      // so the racing predicate is injected via the CONSTRUCTOR (not a per-call param). It delegates
      // the fold/context methods to the real registry and wraps only isTerminallyRevokedInTx: the
      // first time the snapshot reaches its terminal read it commits the revoke on another pooled
      // client, then runs the real in-snapshot query.
      const racingRegistry = {
        foldAddInTx: (...a) => reg.foldAddInTx(...a),
        foldRevokeInTx: (...a) => reg.foldRevokeInTx(...a),
        isActiveAddNoopInTx: (...a) => reg.isActiveAddNoopInTx(...a),
        getRevokeContextInTx: (...a) => reg.getRevokeContextInTx(...a),
        async isTerminallyRevokedInTx(client, account, deviceId) {
          if (!injected) {
            injected = true;
            await s.submitMutation({
              accountIdentityPublicKeyB64: RACE, opId: "race-revoke", expectedRevision: 1,
              action: "device.revoke", target: { revokedDeviceId: nullDev },
            });
          }
          return reg.isTerminallyRevokedInTx(client, account, deviceId);
        },
      };
      const sRace = new PgAccountMutationSerializer({ connection: conn, durableInbox, registry: racingRegistry });

      const snap = await sRace.getDelegatedAuthoritySnapshot({
        accountIdentityPublicKeyB64: RACE, deviceId: nullDev,
      });
      // The revoke committed mid-read, but the REPEATABLE READ snapshot (taken at the first read)
      // predates it: terminal AND epoch are BOTH the pre-revoke values — internally coherent.
      assert.equal(snap.epoch, 1, "snapshot epoch is the PRE-revoke epoch");
      assert.equal(snap.terminal, false, "terminal is coherent with that epoch (the mid-read commit is invisible)");
      assert.deepEqual(snap.revokedCertIds, [], "no cert was revoked (cert_id was NULL) — terminal is the ONLY signal");

      // The revoke really did commit out of band. A FRESH read now sees epoch 2 + terminal — so the
      // guard's next dispatch (epoch 2 !== armed watermark 1) re-checks and catches the revoke.
      assert.equal(await s.getCurrentEpoch(RACE), 2, "the racing revoke committed and bumped the epoch");
      const after = await s.getDelegatedAuthoritySnapshot({
        accountIdentityPublicKeyB64: RACE, deviceId: nullDev,
      });
      assert.equal(after.epoch, 2);
      assert.equal(after.terminal, true, "the next snapshot sees the revoke — no permanent admission");
    });

    await t.test("device.revoke: a caller revokedCertId that is NOT the target's bound cert is BAD_TARGET (no arbitrary cert-revoke escalation)", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-mismatch", expectedRevision: 3,
          action: "device.revoke",
          // D.d1's bound cert is cap("leaf1"); naming an unrelated ancestor cert must fail
          // (arbitrary cert revocation is the separate capability.revoke operation).
          target: { revokedDeviceId: D.d1, revokedCertId: cap("someone-elses-ancestor") },
        }),
        (err) => err.code === "BAD_TARGET",
      );
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(ACCT, D.d1)).status, "active", "the rejected revoke did not touch D.d1");
      assert.equal((await s.getAuthorityState(ACCT)).epoch, 3, "the rejected mismatch did not bump the epoch");
    });

    await t.test("revoke of a NEVER-ENROLLED device: tombstones + bumps, but revokes NO cert (nothing bound)", async () => {
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-ghost", expectedRevision: 3,
        action: "device.revoke", target: { revokedDeviceId: D.ghost },
      });
      assert.equal(r.revision, 4);
      assert.equal(r.devices.length, 1, "active set unchanged");
      assert.deepEqual(r.authorityState.revokedCertIds, [cap("leaf2")], "no new cert — a never-enrolled device has no bound cert to revoke");
    });

    await t.test("revoke of a never-enrolled device carrying a revokedCertId is BAD_TARGET (no verifiable device→cert binding)", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: ACCT, opId: "op-revoke-ghost2", expectedRevision: 4,
          action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("ghost2"), revokedCertId: cap("ghostcap") },
        }),
        (err) => err.code === "BAD_TARGET",
      );
    });

    await t.test("device.revoke: a minValidIssuedAtMs on the target is REJECTED (BAD_TARGET), not silently ignored (finding 3)", async () => {
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: ACCT, opId: "op-cutoff", expectedRevision: 4,
          action: "device.revoke", target: { revokedDeviceId: D.d1, minValidIssuedAtMs: 100 },
        }),
        (err) => err.code === "BAD_TARGET",
      );
      assert.equal((await s.getAuthorityState(ACCT)).epoch, 4, "the rejected mutation did not bump the epoch");
    });

    await t.test("concurrent submits at the same expectedRevision serialize: exactly one commits, the other goes stale", async () => {
      const A2 = "B-SIGN-ACCT-CONCURRENT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "seed", expectedRevision: 0, action: "device.add", target: { deviceId: D.s, inboxId: "inbox-s" } });
      // Both target revision 1 concurrently.
      const [r1, r2] = await Promise.all([
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c1", expectedRevision: 1, action: "device.add", target: { deviceId: D.c1, inboxId: "inbox-c1" } }),
        s.submitMutation({ accountIdentityPublicKeyB64: A2, opId: "c2", expectedRevision: 1, action: "device.add", target: { deviceId: D.c2, inboxId: "inbox-c2" } }),
      ]);
      const committed = [r1, r2].filter((r) => !r.stale);
      const stale = [r1, r2].filter((r) => r.stale);
      assert.equal(committed.length, 1, "exactly one commit");
      assert.equal(stale.length, 1, "the other is stale (CAS)");
      assert.equal(committed[0].revision, 2);
      const st = await s.getAuthorityState(A2);
      assert.equal(st.epoch, 2, "epoch advanced exactly once");
    });

    // S2.5 S12 L4 — cert reconciliation on the serializer side: a device.add fold
    // (certId=null) must NOT clobber a leaf cert already written by device.bind's
    // enroll (COALESCE keep).
    await t.test("device.add does not clobber a non-null cert_id to null", async () => {
      const A3 = "B-SIGN-ACCT-COALESCE";
      const registry = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      // device.bind enroll writes the leaf cert first.
      await registry.enroll({ accountIdentityPublicKeyB64: A3, deviceId: D.coal, inboxId: "inbox-coal", certId: cap("leaf-coal"), authorityEpoch: 0 });
      // A serializer device.add for the SAME device (certId=null) folds the row.
      await s.submitMutation({ accountIdentityPublicKeyB64: A3, opId: "coal-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.coal, inboxId: "inbox-coal", certId: null } });
      const dev = await registry.getDevice(A3, D.coal);
      assert.equal(dev.certId, cap("leaf-coal"), "the leaf cert survives the device.add fold");
      assert.equal(dev.status, "active");
    });

    // S2.5 S11 audit F4 (2026-07-09): the revoke fail-close is ATOMIC — the target
    // device's delivery cursor (device_cursors.revoked) is closed inside the SAME
    // transaction as the authority commit, so the two can never split on a crash.
    await t.test("device.revoke closes the target device's delivery cursor in the SAME transaction", async () => {
      const A4 = "B-SIGN-ACCT-ATOMIC";
      await s.submitMutation({ accountIdentityPublicKeyB64: A4, opId: "atomic-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.atomic, inboxId: "inbox-atomic" } });
      await durableInbox.registerDevice("inbox-atomic", D.atomic);
      const before = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", ["inbox-atomic", D.atomic]);
      assert.equal(before.rows[0].revoked, false, "cursor starts live");

      await s.submitMutation({ accountIdentityPublicKeyB64: A4, opId: "atomic-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D.atomic } });
      const after = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", ["inbox-atomic", D.atomic]);
      assert.equal(after.rows[0].revoked, true, "cursor was fail-closed atomically with the authority revoke");
    });

    await t.test("a durable cursor-close failure rolls back the ENTIRE mutation (authority not bumped, device stays active)", async () => {
      // A durableInbox whose cursor-CLOSE fails. It also carries registerDeviceInTx
      // (a real durableInbox has both) so the serializer can compose its registry;
      // foldRevokeInTx never calls it — only the serializer's own cursor close does,
      // which is the failure under test.
      const failingInbox = {
        revokeDeviceInTx: async () => { throw new Error("boom-durable"); },
        registerDeviceInTx: async () => {},
      };
      const sFail = new PgAccountMutationSerializer({ connection: conn, durableInbox: failingInbox });
      const A5 = "B-SIGN-ACCT-ROLLBACK";
      await s.submitMutation({ accountIdentityPublicKeyB64: A5, opId: "rb-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.rb, inboxId: "inbox-rb" } });

      await assert.rejects(
        () => sFail.submitMutation({ accountIdentityPublicKeyB64: A5, opId: "rb-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D.rb } }),
        /boom-durable/,
      );
      const st = await s.getAuthorityState(A5);
      assert.equal(st.epoch, 1, "the failed revoke rolled back — epoch not bumped");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const dev = await reg.getDevice(A5, D.rb);
      assert.equal(dev.status, "active", "the device is still active — the revoke did not partially apply");
    });

    await t.test("constructor fails loud without a durableInbox (atomic revoke fail-close dependency)", () => {
      assert.throws(() => new PgAccountMutationSerializer({ connection: conn }), /durableInbox/);
    });

    // L5 review-4 finding 3: the coherent delegated snapshot reads terminal status through the
    // serializer's OWN canonical registry, so the constructor must hard-require isTerminallyRevokedInTx
    // on any injected registry — otherwise a hand-built registry silently omits the terminal dimension.
    await t.test("constructor fails loud when the injected registry omits isTerminallyRevokedInTx (coherent delegated snapshot)", () => {
      // A registry with every OTHER required InTx method but missing the terminal predicate — proves
      // the new capability check is what rejects it (not one of the earlier fold/no-op/context checks).
      const registryMissingTerminal = {
        async foldAddInTx() {},
        async foldRevokeInTx() {},
        async isActiveAddNoopInTx() { return false; },
        async getRevokeContextInTx() { return null; },
      };
      assert.throws(
        () => new PgAccountMutationSerializer({ connection: conn, durableInbox, registry: registryMissingTerminal }),
        /isTerminallyRevokedInTx/,
      );
    });

    // Audit 2026-07-10 R3 F1 (fold resurrection): the device.add fold is the second
    // writer to account_device_registry; it must honor the registry's TERMINAL
    // revocation rule. A revoked device must not be flipped back to active by a
    // re-add — otherwise a device holding only device.add undoes a sibling's revoke,
    // and a subsequent device.bind opens a fresh LIVE cursor for the "revoked" device.
    await t.test("device.add cannot resurrect a REVOKED device (terminal revocation)", async () => {
      const A6 = "B-SIGN-ACCT-RESURRECT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.res, inboxId: "inbox-res" } });
      await s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: D.res } });
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A6, D.res)).status, "revoked");

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A6, opId: "res-readd", expectedRevision: 2, action: "device.add", target: { deviceId: D.res, inboxId: "inbox-res" } }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      // The row stays revoked and the epoch did not advance on the rejected re-add.
      assert.equal((await reg.getDevice(A6, D.res)).status, "revoked", "the revoked device was NOT resurrected");
      assert.equal((await s.getAuthorityState(A6)).epoch, 2, "a rejected re-add does not bump the epoch");
    });

    // Audit R4 F1 (NEVER-enrolled resurrection): a device.revoke can name a device
    // that was never enrolled (revoke racing ahead of its first device.add). Before
    // the durable tombstone this left NO trace, so a later device.add enrolled it
    // ACTIVE. The revoke now writes a tombstone (for a canonical id) and the fold
    // consults it — a subsequent device.add of that never-enrolled deviceId is
    // refused, exactly like an enrolled-then-revoked one.
    await t.test("device.add cannot resurrect a NEVER-ENROLLED then revoked device (F1 tombstone)", async () => {
      const A6b = "B-SIGN-ACCT-NEVER-ENROLLED";
      const NE = "rez:dev:" + "a".repeat(64); // canonical, never enrolled
      // Revoke it before it ever enrolls — succeeds (fail-close), writes a tombstone.
      const rv = await s.submitMutation({ accountIdentityPublicKeyB64: A6b, opId: "ne-revoke", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: NE } });
      assert.equal(rv.revision, 1, "the revoke committed and bumped the epoch");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal(await reg.getDevice(A6b, NE), null, "no active/revoked registry row — it was never enrolled");
      assert.equal(await reg.isTombstoned(A6b, NE), true, "but the terminal tombstone was written");

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A6b, opId: "ne-add", expectedRevision: 1, action: "device.add", target: { deviceId: NE, inboxId: "inbox-ne" } }),
        (err) => err.code === "DEVICE_REVOKED",
      );
      assert.equal(await reg.getDevice(A6b, NE), null, "the never-enrolled revoked device was NOT resurrected");
      assert.equal((await s.getAuthorityState(A6b)).epoch, 1, "the rejected add did not bump the epoch");
    });

    // Audit R4 L2c review P2: a HISTORICAL malformed (non-canonical) row — one that
    // predates the L2c enroll guard — must still be fail-closable. foldRevokeInTx
    // flips an existing row BEFORE applying the never-enrolled canonical-shape reject,
    // so a real row is always revoked + tombstoned + its cursor closed, whatever its
    // shape. This inserts the malformed row + live cursor DIRECTLY (bypassing the now
    // strict enroll) to simulate the legacy state, then proves the full fail-close.
    await t.test("a HISTORICAL malformed (non-canonical) enrolled row is still fail-closed + tombstoned on revoke", async () => {
      const A8 = "B-SIGN-ACCT-HIST-MALFORMED";
      const malformed = "rez:dev:legacy-malformed"; // non-canonical; could never enroll under L2c
      const inbox = "inbox-hist-malformed";
      // Legacy state: an active registry row + its live delivery cursor, written raw.
      await conn.query(
        "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
          + " VALUES ($1, $2, $3, NULL, 0, 'active')",
        [A8, malformed, inbox],
      );
      await durableInbox.registerDevice(inbox, malformed, { devicePublicKeyB64: "LEGACY-DEVICE-KEY" });
      const before = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", [inbox, malformed]);
      assert.equal(before.rows[0].revoked, false, "the legacy cursor starts live");

      // The account's first authority mutation revokes the malformed device.
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "hist-revoke", expectedRevision: 0, action: "device.revoke", target: { revokedDeviceId: malformed } });
      assert.equal(r.revision, 1, "the revoke committed");

      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A8, malformed)).status, "revoked", "the historical malformed row is fail-closed");
      assert.equal(await reg.isTombstoned(A8, malformed), true, "and terminally tombstoned");
      const after = await conn.query("SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2", [inbox, malformed]);
      assert.equal(after.rows[0].revoked, true, "its delivery cursor was fail-closed in the same tx");
    });

    // Audit 2026-07-10 R3 F2 (fold inbox re-point): a device's inbox is immutable
    // once enrolled (the registry throws ACCOUNT_DEVICE_CONFLICT). The fold must
    // not silently re-point inbox_id — that orphans the device's live cursor on the
    // old inbox, and a later revoke (resolving only the CURRENT inbox) closes the
    // wrong cursor, leaving the revoked device still draining the old inbox.
    await t.test("device.add cannot re-point an active device to a different inbox", async () => {
      const A7 = "B-SIGN-ACCT-REPOINT";
      await s.submitMutation({ accountIdentityPublicKeyB64: A7, opId: "rp-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.rp, inboxId: "inbox-rp-old" } });

      await assert.rejects(
        () => s.submitMutation({ accountIdentityPublicKeyB64: A7, opId: "rp-move", expectedRevision: 1, action: "device.add", target: { deviceId: D.rp, inboxId: "inbox-rp-new" } }),
        (err) => err.code === "ACCOUNT_DEVICE_CONFLICT",
      );
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      assert.equal((await reg.getDevice(A7, D.rp)).inboxId, "inbox-rp-old", "the inbox was NOT re-pointed");
      assert.equal((await s.getAuthorityState(A7)).epoch, 1, "a rejected re-point does not bump the epoch");
    });

    // Same-device, same-inbox re-add stays idempotent (the guards only fire on a
    // revoked row or a genuine inbox change) — the cert-coalesce path is unaffected.
    await t.test("device.add for the SAME device + SAME inbox still folds idempotently", async () => {
      const A8 = "B-SIGN-ACCT-SAME";
      await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "same-add", expectedRevision: 0, action: "device.add", target: { deviceId: D.same, inboxId: "inbox-same" } });
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A8, opId: "same-readd", expectedRevision: 1, action: "device.add", target: { deviceId: D.same, inboxId: "inbox-same", certId: cap("leaf-same") } });
      assert.equal(r.revision, 2, "a same-device same-inbox re-add is a normal fold (cert upgrade)");
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const dev = await reg.getDevice(A8, D.same);
      assert.equal(dev.status, "active");
      assert.equal(dev.certId, cap("leaf-same"), "the leaf cert was written by the fold");
    });

    // ---- audit R4 L3: TOCTOU-safe delegated recheck UNDER the account lock ----

    // A delegated session's authority is re-checked, but the check must run under the
    // SAME per-account lock as the fold and against the IN-TX revocation state — else a
    // device.revoke committing between a pre-lock read and the fold is a TOCTOU. The
    // serializer accepts an optional `revalidate(inTxRevocationState) -> Promise<bool>`.
    await t.test("L3: a revalidate returning false aborts the mutation (no fold, no epoch, no journal)", async () => {
      const A = "B-SIGN-ACCT-L3-FALSE";
      const dev = canonicalDeviceId("l3-false");
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: A, opId: "l3-false-op", expectedRevision: 0,
          action: "device.add", target: { deviceId: dev, inboxId: "inbox-l3-false" },
          revalidate: async () => false,
        }),
        (err) => err.code === "DELEGATED_AUTHORITY_INVALID",
      );
      assert.equal((await s.getAuthorityState(A)).epoch, 0, "epoch not bumped on a rejected recheck");
      const enrolled = await conn.query(
        "SELECT 1 FROM account_device_registry WHERE account_identity = $1 AND device_id = $2", [A, dev],
      );
      assert.equal(enrolled.rowCount, 0, "device not enrolled");
      const journal = await conn.query(
        "SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A],
      );
      assert.equal(journal.rows[0].c, 0, "no journal row written");
    });

    await t.test("L3: a revalidate returning true proceeds normally", async () => {
      const A = "B-SIGN-ACCT-L3-TRUE";
      const dev = canonicalDeviceId("l3-true");
      const r = await s.submitMutation({
        accountIdentityPublicKeyB64: A, opId: "l3-true-op", expectedRevision: 0,
        action: "device.add", target: { deviceId: dev, inboxId: "inbox-l3-true" },
        revalidate: async () => true,
      });
      assert.equal(r.revision, 1);
      assert.equal(r.devices.length, 1);
      assert.equal(r.devices[0].deviceId, dev);
    });

    await t.test("L3: the revalidate closure sees the IN-TX revocation state (a committed-before-lock revoke is visible)", async () => {
      const A = "B-SIGN-ACCT-L3-INTX";
      const dev = canonicalDeviceId("l3-intx");
      const revokedCert = cap("l3intx"); // canonical (DB CHECK now enforces the shape)
      // A revoke committed BEFORE this mutation takes the account lock.
      await conn.query(
        "INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch) VALUES ($1, $2, 0)",
        [A, revokedCert],
      );
      let seen = null;
      await assert.rejects(
        () => s.submitMutation({
          accountIdentityPublicKeyB64: A, opId: "l3-intx-op", expectedRevision: 0,
          action: "device.add", target: { deviceId: dev, inboxId: "inbox-l3-intx" },
          // Models the real verifier: reject iff the leaf cert is in the in-tx revoked
          // set. The closure receives the revocation state loaded UNDER the lock.
          revalidate: async (rev) => { seen = rev; return !rev.revokedCertIds.includes(revokedCert); },
        }),
        (err) => err.code === "DELEGATED_AUTHORITY_INVALID",
      );
      assert.ok(seen && seen.revokedCertIds.includes(revokedCert), "the closure saw the committed revoked cert in the in-tx state");
    });

    await t.test("L3: the recheck + fold are atomic under the account lock (a concurrent revoke cannot interleave)", async () => {
      const A = "B-SIGN-ACCT-L3-ATOMIC";
      const dA = canonicalDeviceId("l3-atomic-a");
      const dB = canonicalDeviceId("l3-atomic-b");
      // Seed dB active (epoch 1) so the concurrent revoke has a real target.
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "atomic-seed", expectedRevision: 0, action: "device.add", target: { deviceId: dB, inboxId: "inbox-l3-atomic-b" } });

      // A SECOND connection to the SAME schema — a genuine concurrent writer.
      const conn2 = new PgConnection({ connectionString: PG_URL, poolConfig: { options: `-c search_path=${SCHEMA}` } });
      const s2 = new PgAccountMutationSerializer({ connection: conn2, durableInbox: new PgDurableInbox({ connection: conn2, maxDevices: 1 }) });
      try {
        let concurrentResolved = false;
        let concurrentResult = null;
        let pendingConcurrent = null;
        const result = await s.submitMutation({
          accountIdentityPublicKeyB64: A, opId: "atomic-add", expectedRevision: 1,
          action: "device.add", target: { deviceId: dA, inboxId: "inbox-l3-atomic-a" },
          revalidate: async () => {
            // Fire a concurrent revoke on conn2 WITHOUT awaiting — it must BLOCK on the
            // per-account advisory lock this transaction already holds across the
            // recheck+fold.
            pendingConcurrent = s2.submitMutation({ accountIdentityPublicKeyB64: A, opId: "atomic-revoke", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: dB } })
              .then((r) => { concurrentResolved = true; concurrentResult = r; });
            await new Promise((resolve) => setTimeout(resolve, 200));
            assert.equal(concurrentResolved, false, "the concurrent revoke is BLOCKED on the account lock during our recheck+fold");
            return true;
          },
        });
        assert.equal(result.revision, 2, "our add committed at epoch 2");
        // The lock is released at our COMMIT; the concurrent revoke can now proceed.
        await pendingConcurrent;
        assert.equal(concurrentResolved, true, "the concurrent revoke resolved only AFTER our commit");
        assert.equal(concurrentResult.stale, true, "and it observed our committed epoch (serialized strictly after)");
      } finally {
        await conn2.close();
      }
    });

    // ---- audit R4 F3: durable admission control (input guards, caps, no-op, prune) ----

    await t.test("F3: an opId over the byte cap is rejected (BAD_REQUEST)", async () => {
      const sTiny = new PgAccountMutationSerializer({ connection: conn, durableInbox, caps: { opIdBytes: 8 } });
      await assert.rejects(
        () => sTiny.submitMutation({
          accountIdentityPublicKeyB64: "B-SIGN-F3-OPID", opId: "x".repeat(9), expectedRevision: 0,
          action: "device.add", target: { deviceId: canonicalDeviceId("f3op"), inboxId: "inbox-f3op" },
        }),
        (err) => err.code === "BAD_REQUEST",
      );
    });

    await t.test("F3-remediation finding 2: a non-canonical revokedCertId is rejected (BAD_TARGET) — exact rez:cap:<64-hex> only", async () => {
      const bad = [
        "not-a-cap-id",               // no prefix
        "rez:cap:revoked-leaf",       // prefix-only, arbitrary content
        "rez:cap:" + "a".repeat(63),  // too short (63 hex)
        "rez:cap:" + "a".repeat(65),  // too long (65 hex)
        "rez:cap:" + "A".repeat(64),  // uppercase (must be lowercase)
        "rez:cap:" + "g".repeat(64),  // non-hex chars
      ];
      let i = 0;
      for (const revokedCertId of bad) {
        i += 1;
        await assert.rejects(
          () => s.submitMutation({
            accountIdentityPublicKeyB64: "B-SIGN-F2-FORMAT", opId: "f2fmt-" + i, expectedRevision: 0,
            action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId("f2fmt" + i), revokedCertId },
          }),
          (err) => err.code === "BAD_TARGET",
          "must reject non-canonical cert id: " + revokedCertId,
        );
      }
    });

    await t.test("F3-remediation finding 1: revoking a REAL device ALWAYS fail-closes + auto-revokes its cert (NO revoked-cert quota)", async () => {
      const A = "B-SIGN-F3-FAILCLOSE-CERTS";
      // A tiny configured `revokedCerts` is IGNORED — the quota was removed: a fail-close
      // revoke of a real device must NEVER be blocked by a ceiling on the revoked-cert set.
      const sCap = new PgAccountMutationSerializer({ connection: conn, durableInbox, caps: { revokedCerts: 1 } });
      const reg = new PgAccountDeviceRegistry({ connection: conn, durableInbox });
      const ids = ["fc1", "fc2", "fc3"];
      let epoch = 0;
      for (const seed of ids) {
        await sCap.submitMutation({ accountIdentityPublicKeyB64: A, opId: "add-" + seed, expectedRevision: epoch, action: "device.add", target: { deviceId: canonicalDeviceId(seed), inboxId: "inbox-" + seed, certId: cap(seed) } });
        epoch += 1;
      }
      // Revoke ALL three — even the 3rd (well past any old cap) fully revokes.
      for (const seed of ids) {
        const r = await sCap.submitMutation({ accountIdentityPublicKeyB64: A, opId: "rev-" + seed, expectedRevision: epoch, action: "device.revoke", target: { revokedDeviceId: canonicalDeviceId(seed) } });
        epoch += 1;
        assert.equal(r.revision, epoch, seed + " revoke bumped the epoch (never blocked)");
        assert.equal((await reg.getDevice(A, canonicalDeviceId(seed))).status, "revoked", seed + " row is fail-closed");
        assert.equal(await reg.isTombstoned(A, canonicalDeviceId(seed)), true, seed + " is tombstoned");
      }
      const state = await s.getAuthorityState(A);
      assert.equal(state.epoch, 6, "all six mutations committed");
      for (const seed of ids) {
        assert.ok(state.revokedCertIds.includes(cap(seed)), seed + "'s bound cert was auto-revoked despite the tiny configured cap");
      }
    });

    await t.test("F3: a device.add that changes nothing is a no-op (no epoch bump, no journal row)", async () => {
      const A = "B-SIGN-F3-NOOPADD";
      const dev = canonicalDeviceId("f3noopadd");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "na-1", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3noopadd" } });
      const before = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "na-2", expectedRevision: 1, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3noopadd" } });
      assert.equal(r.noop, true, "the redundant add is a semantic no-op");
      assert.equal(r.revision, 1, "the epoch did NOT advance");
      assert.equal((await s.getAuthorityState(A)).epoch, 1);
      const after = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      assert.equal(after.rows[0].c, before.rows[0].c, "no journal row was appended for the no-op");
    });

    await t.test("F3: a re-revoke of an already-terminal device is a no-op (no epoch bump, no journal row)", async () => {
      const A = "B-SIGN-F3-NOOPREV";
      const dev = canonicalDeviceId("f3nooprev");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "nr-add", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3nooprev" } });
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "nr-rev", expectedRevision: 1, action: "device.revoke", target: { revokedDeviceId: dev } });
      const before = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      const r = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "nr-rev-again", expectedRevision: 2, action: "device.revoke", target: { revokedDeviceId: dev } });
      assert.equal(r.noop, true, "re-revoking an already-terminal device changes nothing");
      assert.equal(r.revision, 2, "the epoch did NOT advance");
      const after = await conn.query("SELECT count(*)::int AS c FROM account_device_mutation WHERE account_identity = $1", [A]);
      assert.equal(after.rows[0].c, before.rows[0].c, "no journal row for the no-op revoke");
    });

    await t.test("F3: pruneExpiredReplayPayloads NULLs an old payload; a later replay returns replayExpired", async () => {
      const A = "B-SIGN-F3-PRUNE";
      const dev = canonicalDeviceId("f3prune");
      await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "pr-op", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3prune" } });
      // A future nowMs with the default 30d TTL makes the cutoff land after this fresh
      // row's committed_at, so its replay payload is pruned (the audit row stays).
      const future = Date.now() + 40 * 24 * 60 * 60 * 1000;
      const pruned = await s.pruneExpiredReplayPayloads(future);
      assert.ok(pruned >= 1, "at least our row's payload was pruned");
      const check = await conn.query("SELECT result_json FROM account_device_mutation WHERE account_identity = $1 AND op_id = $2", [A, "pr-op"]);
      assert.equal(check.rows[0].result_json, null, "the replay payload was NULLed");
      // A replay of the pruned opId still proves it committed → replayExpired + current state.
      const replay = await s.submitMutation({ accountIdentityPublicKeyB64: A, opId: "pr-op", expectedRevision: 0, action: "device.add", target: { deviceId: dev, inboxId: "inbox-f3prune" } });
      assert.equal(replay.idempotentReplay, true);
      assert.equal(replay.replayExpired, true);
      assert.equal(replay.revision, 1, "the current authority epoch is returned");
    });
  },
);
