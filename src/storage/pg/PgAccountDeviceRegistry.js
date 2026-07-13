/**
 * Postgres-backed account→device→inbox registry — the EXPLICIT, opt-in linkage
 * for multi-device-hosted accounts (S2.5 S7 / audit F3, resolving OPEN-A).
 *
 * It maps an account identity (the B-sign public key) to each of its enrolled
 * (device, inbox) bindings so the home can resolve ALL of an account's device
 * inboxes — the precondition for account-wide device revocation (a device can
 * revoke a SIBLING device whose inbox the caller's session is not bound to).
 *
 * ACCOUNT-BLINDNESS BOUNDARY (deliberate, scoped exception — see migration
 * 0009). The node is otherwise account-blind (`inbox_claims` keys on the claimant
 * pubkey, never an account; CAPABILITY_MODEL §8-9). This registry is the one
 * documented carve-out: an account OPTS IN by enrolling here; the free single-
 * device path never enrolls, and this table is NEVER back-filled from / joined to
 * `inbox_claims`. SSOT: the proven device key lives in
 * `device_cursors.device_public_key`; this registry holds only the account
 * linkage (account ↔ device ↔ inbox + authorizing cert + authority epoch +
 * status), never duplicating key material.
 *
 * Mutations (enroll / revoke) serialize under a per-account advisory lock so
 * the read-then-write sequences are linearizable; the inbox-uniqueness invariant
 * is additionally backstopped by the DB unique index (cross-account races). Reads
 * are authoritative (hit Postgres) — callers making authz decisions must await.
 */

import { isCanonicalDeviceId } from "@rezprotocol/core";

// Per-account durable-tombstone count ceiling (audit R4 tombstone-DoS guard).
// A revoke of a NEVER-ENROLLED CANONICAL device writes a permanent tombstone (F1).
// This bounds that unbounded surface ONLY: a tombstone for a genuinely ENROLLED
// device is never quota-gated (a fail-close revoke must never fail) and is already
// bounded by the real device count. This registry is the canonical invariant owner
// for device-ID SHAPE too (L2c): every add/enroll rejects a non-canonical id, and a
// never-enrolled non-canonical revoke is rejected before any tombstone is written —
// so this quota bounds COUNT for the remaining (canonical, never-enrolled) surface.
// 4096 is far above any real account's lifetime device count (~tens) while capping
// worst-case durable growth to well under 1 MiB/account. Tunable; surfaced here as
// the single knob. Hitting it is logged, never silent.
export const MAX_REVOKED_DEVICES_PER_ACCOUNT = 4096;

function codedError(message, code) {
  const err = new Error(message);
  err.code = code;
  return err;
}

export class PgAccountDeviceRegistry {
  #conn;
  #durableInbox;

  constructor({ connection, durableInbox } = {}) {
    if (!connection) {
      throw new Error("PgAccountDeviceRegistry requires connection");
    }
    // Hard dependency (audit 2026-07-10 P2, mirroring PgAccountMutationSerializer's
    // revokeDeviceInTx requirement): enrollWithCursor folds the delivery-cursor
    // create into the enroll transaction, so a registry row and its cursor can
    // never split. Requiring the InTx hook at construction fails loud before any
    // split-write regression can ship.
    if (!durableInbox || typeof durableInbox.registerDeviceInTx !== "function") {
      throw new Error("PgAccountDeviceRegistry requires durableInbox with registerDeviceInTx");
    }
    this.#conn = connection;
    this.#durableInbox = durableInbox;
  }

  #normalize(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  #rowToBinding(row) {
    return {
      accountIdentityPublicKeyB64: row.account_identity,
      deviceId: row.device_id,
      inboxId: row.inbox_id,
      certId: row.cert_id == null ? null : String(row.cert_id),
      authorityEpoch: Number(row.authority_epoch),
      status: String(row.status),
    };
  }

  #validateEnrollArgs({ accountIdentityPublicKeyB64, deviceId, inboxId, certId = null, authorityEpoch = 0 } = {}) {
    const account = this.#normalize(accountIdentityPublicKeyB64);
    const dev = this.#normalize(deviceId);
    const inbox = this.#normalize(inboxId);
    const cert = certId == null ? null : this.#normalize(certId);
    const epoch = Number(authorityEpoch);
    if (!account) throw new Error("PgAccountDeviceRegistry.enroll requires accountIdentityPublicKeyB64");
    if (!dev) throw new Error("PgAccountDeviceRegistry.enroll requires deviceId");
    if (!inbox) throw new Error("PgAccountDeviceRegistry.enroll requires inboxId");
    if (certId != null && !cert) throw new Error("PgAccountDeviceRegistry.enroll certId must be a non-empty string when provided");
    if (!Number.isFinite(epoch) || epoch < 0) {
      throw new Error("PgAccountDeviceRegistry.enroll requires a finite non-negative authorityEpoch");
    }
    return { account, dev, inbox, cert, epoch };
  }

  /**
   * The terminal-revocation tombstone gate (audit R4 F1). A revoked deviceId can
   * NEVER (re)enroll — even when NO registry row exists, because an account-wide
   * device.revoke can name a device that was never enrolled (revoke racing ahead
   * of the sibling's first device.add / device.bind). The durable tombstone
   * (account_revoked_device, migration 0012) is the only trace in that case;
   * every device.add / device.bind path consults it. Assumes the caller holds the
   * per-account advisory lock. Throws DEVICE_REVOKED when tombstoned.
   */
  async #assertNotTombstoned(client, account, dev) {
    const tomb = await client.query(
      "SELECT 1 FROM account_revoked_device WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (tomb.rowCount > 0) {
      throw codedError(
        `device ${dev} is revoked for account and cannot re-enroll`,
        "DEVICE_REVOKED",
      );
    }
  }

  /**
   * The canonical device-add invariants — the SINGLE checker shared by device.bind
   * (#enrollInTx) AND the serializer's device.add fold (foldAddInTx). The R3
   * resurrection bug came from the serializer hand-mirroring these checks and
   * drifting (audit R4 F5a); centralizing them here is the fix. Assumes the caller
   * holds the per-account advisory lock. Returns the existing (account, device) row
   * or null so each caller can choose its own write shape (idempotent-return vs
   * epoch-bumping upsert). Rejects, in order:
   *   - a non-canonical deviceId → BAD_DEVICE_ID (L2c: this registry is the canonical
   *     invariant OWNER — a device can only ever enroll with a canonical
   *     rez:dev:<64-hex> id = deviceIdFor(pub); the record-layer guard is an upstream
   *     early-reject, not a substitute for enforcing it here);
   *   - a terminally-revoked deviceId (tombstone OR revoked row) → DEVICE_REVOKED;
   *   - re-pointing an enrolled device's inbox → ACCOUNT_DEVICE_CONFLICT;
   *   - two different non-null certs for one device → ACCOUNT_DEVICE_CONFLICT;
   *   - an inbox already held by a different device → INBOX_ALREADY_ENROLLED.
   * (NULL→leaf cert reconciliation is a WRITE, so each caller applies it.)
   */
  async #assertDeviceAddInvariants(client, { account, dev, inbox, cert }) {
    if (!isCanonicalDeviceId(dev)) {
      throw codedError(
        `device ${dev} is not a canonical rez:dev:<64-hex> id and cannot enroll`,
        "BAD_DEVICE_ID",
      );
    }
    await this.#assertNotTombstoned(client, account, dev);

    const existing = await client.query(
      "SELECT account_identity, device_id, inbox_id, cert_id, authority_epoch, status"
        + " FROM account_device_registry WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (existing.rowCount > 0) {
      const row = existing.rows[0];
      // Revoke-before-bind fail-close (audit 2026-07-09 P1). A revoked row is
      // terminal (a re-add uses a NEW deviceId); refusing here keeps the account
      // authority ("revoked") consistent with the delivery cursor. The tombstone
      // check above additionally covers a device revoked before it ever enrolled
      // (no row), which this row check alone would miss.
      if (String(row.status) === "revoked") {
        throw codedError(
          `device ${dev} is revoked for account and cannot re-enroll`,
          "DEVICE_REVOKED",
        );
      }
      if (String(row.inbox_id) !== inbox) {
        throw codedError(
          `device ${dev} is already enrolled to a different inbox for account`,
          "ACCOUNT_DEVICE_CONFLICT",
        );
      }
      // cert reconciliation (S2.5 S12): device.add writes cert_id=NULL, device.bind
      // writes the leaf certId — two writers on one column. Two DIFFERENT non-null
      // certs are a genuine conflict; the NULL→leaf upgrade is a write each caller
      // performs after this check.
      const storedCert = row.cert_id == null ? null : String(row.cert_id);
      if (storedCert != null && cert != null && storedCert !== cert) {
        throw codedError(
          `device ${dev} is already enrolled with a different cert for account`,
          "ACCOUNT_DEVICE_CONFLICT",
        );
      }
      return row;
    }

    // Not enrolled for this (account, device). Explicit inbox-uniqueness pre-check
    // for a clean error in the common case; the unique index is the backstop for a
    // cross-account race (caught as 23505 by each caller).
    const inboxHeld = await client.query(
      "SELECT account_identity, device_id FROM account_device_registry WHERE inbox_id = $1",
      [inbox],
    );
    if (inboxHeld.rowCount > 0) {
      throw codedError(
        `inbox ${inbox} is already enrolled to another device`,
        "INBOX_ALREADY_ENROLLED",
      );
    }
    return null;
  }

  /**
   * The device.bind enroll body, run WITHIN a caller-owned transaction (no BEGIN /
   * COMMIT / ROLLBACK here; coded throws propagate to the caller, which owns
   * ROLLBACK). Takes the per-ACCOUNT advisory xact lock as its first act — the
   * lock releases at the caller's COMMIT, so mutations stay serialized per account.
   * Idempotent-return semantics (device.bind uses the CURRENT epoch, so a re-bind
   * does not bump the row's epoch); shares #assertDeviceAddInvariants with the
   * serializer's device.add fold.
   */
  async #enrollInTx(client, { account, dev, inbox, cert, epoch }) {
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [account]);

    const row = await this.#assertDeviceAddInvariants(client, { account, dev, inbox, cert });
    if (row) {
      const storedCert = row.cert_id == null ? null : String(row.cert_id);
      if (storedCert == null && cert != null) {
        const upgraded = await client.query(
          "UPDATE account_device_registry SET cert_id = $3, updated_at = now()"
            + " WHERE account_identity = $1 AND device_id = $2"
            + " RETURNING account_identity, device_id, inbox_id, cert_id, authority_epoch, status",
          [account, dev, cert],
        );
        return this.#rowToBinding(upgraded.rows[0]);
      }
      return this.#rowToBinding(row);
    }

    let inserted;
    try {
      inserted = await client.query(
        "INSERT INTO account_device_registry"
          + " (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
          + " VALUES ($1, $2, $3, $4, $5, 'active')"
          + " RETURNING account_identity, device_id, inbox_id, cert_id, authority_epoch, status",
        [account, dev, inbox, cert, epoch],
      );
    } catch (err) {
      if (err && err.code === "23505") {
        throw codedError(
          `inbox ${inbox} is already enrolled to another device`,
          "INBOX_ALREADY_ENROLLED",
        );
      }
      throw err;
    }
    return this.#rowToBinding(inserted.rows[0]);
  }

  /**
   * The account-authority device.add fold, run WITHIN the serializer's account
   * transaction (audit R4 F5a). The serializer no longer hand-mirrors registry SQL
   * — it calls this, so the canonical add invariants live in ONE place
   * (#assertDeviceAddInvariants, shared with device.bind). The caller
   * (PgAccountMutationSerializer) already holds the per-account advisory lock and
   * owns the epoch; device.add is a NEW authority mutation, so this stamps the new
   * epoch on the row (an epoch-bumping upsert, vs #enrollInTx's idempotent return).
   * cert reconciliation via COALESCE (device.add carries certId=NULL; a non-null
   * cert already written by device.bind is preserved, never clobbered to NULL).
   * @returns {Promise<{accountIdentityPublicKeyB64,deviceId,inboxId,certId,authorityEpoch,status}>}
   */
  async foldAddInTx(client, { accountIdentityPublicKeyB64, deviceId, inboxId, certId = null, authorityEpoch } = {}) {
    const args = this.#validateEnrollArgs({ accountIdentityPublicKeyB64, deviceId, inboxId, certId, authorityEpoch });
    await this.#assertDeviceAddInvariants(client, args);

    let upserted;
    try {
      upserted = await client.query(
        "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
          + " VALUES ($1, $2, $3, $4, $5, 'active')"
          + " ON CONFLICT (account_identity, device_id)"
          + " DO UPDATE SET inbox_id = EXCLUDED.inbox_id,"
          + " cert_id = COALESCE(EXCLUDED.cert_id, account_device_registry.cert_id),"
          + " authority_epoch = EXCLUDED.authority_epoch, status = 'active', updated_at = now()"
          + " RETURNING account_identity, device_id, inbox_id, cert_id, authority_epoch, status",
        [args.account, args.dev, args.inbox, args.cert, args.epoch],
      );
    } catch (err) {
      if (err && err.code === "23505") {
        throw codedError(
          `inbox ${args.inbox} is already enrolled to another device`,
          "INBOX_ALREADY_ENROLLED",
        );
      }
      throw err;
    }
    return this.#rowToBinding(upserted.rows[0]);
  }

  /**
   * The account-authority device.revoke fold + durable tombstone (audit R4 F5a +
   * F1), run WITHIN the serializer's account transaction (caller holds the
   * per-account advisory lock and owns the epoch bump, the delivery-cursor close,
   * and the mutation journal). Remove-wins + idempotent: flips an enrolled row to
   * 'revoked' (returning its inbox so the caller can fail-close the cursor) AND
   * writes a terminal tombstone so a device revoked BEFORE it ever enrolled can
   * never re-enroll.
   *
   * Admission (L2c - this registry is the canonical device-ID invariant OWNER):
   *   - a revoke that flips a REAL enrolled row ALWAYS proceeds + tombstones, whatever
   *     the id's shape (a fail-close revoke must never fail; a historical non-canonical
   *     row must still be closable), bounded by the real device count;
   *   - a NEVER-ENROLLED CANONICAL id: tombstone, quota-gated (the only durable
   *     forgeable surface - a revoke racing ahead of a device's first enroll);
   *   - a NEVER-ENROLLED NON-CANONICAL id: REJECTED (BAD_DEVICE_ID) before any
   *     tombstone - it can never enroll (the add path rejects it), so there is nothing
   *     to resurrect and no tombstone to write.
   * The tombstone is thus written for every revoke that PROCEEDS, never suppressed on
   * a cleverness assumption once the id is known to be real or enrollable. Resurrection
   * is closed by #assertNotTombstoned on every add path (audit R4 F1 + its review).
   *
   * @returns {Promise<{revokedInboxId: string|null, registryRowExisted: boolean, binding: object|null}>}
   */
  async foldRevokeInTx(client, { accountIdentityPublicKeyB64, deviceId, authorityEpoch } = {}) {
    const account = this.#normalize(accountIdentityPublicKeyB64);
    const dev = this.#normalize(deviceId);
    const epoch = Number(authorityEpoch);
    if (!account) throw new Error("PgAccountDeviceRegistry.foldRevokeInTx requires accountIdentityPublicKeyB64");
    if (!dev) throw new Error("PgAccountDeviceRegistry.foldRevokeInTx requires deviceId");
    if (!Number.isFinite(epoch) || epoch < 0) {
      throw new Error("PgAccountDeviceRegistry.foldRevokeInTx requires a finite non-negative authorityEpoch");
    }

    const revUpd = await client.query(
      "UPDATE account_device_registry SET status = 'revoked', authority_epoch = $3, updated_at = now()"
        + " WHERE account_identity = $1 AND device_id = $2"
        + " RETURNING account_identity, device_id, inbox_id, cert_id, authority_epoch, status",
      [account, dev, epoch],
    );
    const registryRowExisted = revUpd.rowCount > 0;
    const binding = registryRowExisted ? this.#rowToBinding(revUpd.rows[0]) : null;
    const revokedInboxId = registryRowExisted ? this.#normalize(revUpd.rows[0].inbox_id) : null;

    // A never-enrolled NON-CANONICAL revoke target is rejected (L2c) — it can never
    // enroll (the add path rejects non-canonical ids), so there is nothing to
    // resurrect and no tombstone to write. A revoke that flipped a REAL row is past
    // this: a historical/enrolled device is always fail-closed regardless of shape.
    if (!registryRowExisted && !isCanonicalDeviceId(dev)) {
      throw codedError(
        `device ${dev} is not a canonical rez:dev:<64-hex> id and was never enrolled`,
        "BAD_DEVICE_ID",
      );
    }

    // Durable terminal tombstone (F1). Written for every revoke that PROCEEDS (real
    // row, or canonical never-enrolled). Insert only if absent (terminal + idempotent
    // via the PK; a re-revoke is a no-op and never re-checks the quota). The tombstone
    // is the ONLY trace of a revoke that races ahead of a device's first enroll, and
    // #assertNotTombstoned on every add path is what closes resurrection. The
    // per-account COUNT quota bounds only the durable forgeable surface: a canonical
    // NEVER-ENROLLED target (no row to flip). A revoke that flips a REAL enrolled row
    // is never quota-gated (fail-close must never fail) and is bounded by the real
    // device count.
    const tomb = await client.query(
      "SELECT 1 FROM account_revoked_device WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (tomb.rowCount === 0) {
      if (!registryRowExisted) {
        const cnt = await client.query(
          "SELECT count(*)::int AS c FROM account_revoked_device WHERE account_identity = $1",
          [account],
        );
        if (cnt.rows[0].c >= MAX_REVOKED_DEVICES_PER_ACCOUNT) {
          throw codedError(
            `account has reached the ${MAX_REVOKED_DEVICES_PER_ACCOUNT} revoked-device tombstone limit`,
            "REVOKED_DEVICE_QUOTA_EXCEEDED",
          );
        }
      }
      await client.query(
        "INSERT INTO account_revoked_device (account_identity, device_id, revoked_at_epoch)"
          + " VALUES ($1, $2, $3) ON CONFLICT (account_identity, device_id) DO NOTHING",
        [account, dev, epoch],
      );
    }
    return { revokedInboxId, registryRowExisted, binding };
  }

  /**
   * Enroll a device binding for an account. Idempotent: re-enrolling the SAME
   * (inbox, cert) for an (account, device) returns the existing row; a DIFFERENT
   * binding for an already-enrolled device, or an inbox already held by another
   * (account, device), fails loud. Serialized per account.
   *
   * @returns {Promise<{accountIdentityPublicKeyB64,deviceId,inboxId,certId,authorityEpoch,status}>}
   */
  async enroll(params) {
    const args = this.#validateEnrollArgs(params);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        const binding = await this.#enrollInTx(client, args);
        await client.query("COMMIT");
        return binding;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * device.bind's atomic persist (audit 2026-07-10 P2): the account-linkage
   * enroll AND the delivery-cursor create commit in ONE transaction. Order
   * inside the tx: per-ACCOUNT advisory lock + enroll checks first (so a
   * concurrent account-wide device.revoke serializes strictly before or after
   * the WHOLE bind — a revoked row rolls the cursor back too), then the cursor
   * create takes the per-INBOX lock (account → inbox, the documented lock
   * order; see PgDurableInbox.registerDeviceInTx). PgDurableInbox owns the
   * device_cursors SQL (SSOT); this method only supplies its transaction.
   *
   * Throws the union of enroll's coded errors (DEVICE_REVOKED,
   * ACCOUNT_DEVICE_CONFLICT, INBOX_ALREADY_ENROLLED) and registerDevice's
   * (DEVICE_KEY_MISMATCH, INBOX_CAP_EXCEEDED); any throw rolls back BOTH writes.
   *
   * @returns {Promise<{accountIdentityPublicKeyB64,deviceId,inboxId,certId,authorityEpoch,status}>}
   */
  async enrollWithCursor({ accountIdentityPublicKeyB64, deviceId, inboxId, certId = null, authorityEpoch = 0, devicePublicKeyB64 = null } = {}) {
    const args = this.#validateEnrollArgs({ accountIdentityPublicKeyB64, deviceId, inboxId, certId, authorityEpoch });
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        const binding = await this.#enrollInTx(client, args);
        await this.#durableInbox.registerDeviceInTx(client, args.inbox, args.dev, { devicePublicKeyB64 });
        await client.query("COMMIT");
        return binding;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * @returns {Promise<boolean>} whether (account, device) carries a terminal
   * revocation tombstone (account_revoked_device). A tombstoned device can never
   * re-enroll even with no active/revoked registry row.
   */
  async isTombstoned(accountIdentityPublicKeyB64, deviceId) {
    const account = this.#normalize(accountIdentityPublicKeyB64);
    const dev = this.#normalize(deviceId);
    if (!account || !dev) return false;
    const res = await this.#conn.query(
      "SELECT 1 FROM account_revoked_device WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    return res.rowCount > 0;
  }

  /** @returns {Promise<object|null>} the binding for (account, device), or null. */
  async getDevice(accountIdentityPublicKeyB64, deviceId) {
    const account = this.#normalize(accountIdentityPublicKeyB64);
    const dev = this.#normalize(deviceId);
    if (!account || !dev) return null;
    const res = await this.#conn.query(
      "SELECT account_identity, device_id, inbox_id, cert_id, authority_epoch, status"
        + " FROM account_device_registry WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (res.rowCount === 0) return null;
    return this.#rowToBinding(res.rows[0]);
  }

  /** @returns {Promise<object[]>} all device bindings for an account, by device_id. */
  async listDevices(accountIdentityPublicKeyB64) {
    const account = this.#normalize(accountIdentityPublicKeyB64);
    if (!account) return [];
    const res = await this.#conn.query(
      "SELECT account_identity, device_id, inbox_id, cert_id, authority_epoch, status"
        + " FROM account_device_registry WHERE account_identity = $1 ORDER BY device_id",
      [account],
    );
    return res.rows.map((r) => this.#rowToBinding(r));
  }

  /**
   * Reverse lookup: which (account, device) holds an inbox. Lets a device.revoke
   * resolve a sibling device's inbox without the caller's session being bound to
   * it. @returns {Promise<object|null>}
   */
  async resolveInbox(inboxId) {
    const inbox = this.#normalize(inboxId);
    if (!inbox) return null;
    const res = await this.#conn.query(
      "SELECT account_identity, device_id, inbox_id, cert_id, authority_epoch, status"
        + " FROM account_device_registry WHERE inbox_id = $1",
      [inbox],
    );
    if (res.rowCount === 0) return null;
    return this.#rowToBinding(res.rows[0]);
  }

  /** @returns {Promise<number>} */
  async size() {
    const res = await this.#conn.query("SELECT count(*)::int AS c FROM account_device_registry");
    return res.rows[0].c;
  }
}
