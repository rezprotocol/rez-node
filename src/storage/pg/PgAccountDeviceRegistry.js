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
 * Mutations (enroll / setStatus) serialize under a per-account advisory lock so
 * the read-then-write sequences are linearizable; the inbox-uniqueness invariant
 * is additionally backstopped by the DB unique index (cross-account races). Reads
 * are authoritative (hit Postgres) — callers making authz decisions must await.
 */

const ALLOWED_STATUSES = new Set(["active", "revoked"]);

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
   * The enroll body, run WITHIN a caller-owned transaction (no BEGIN / COMMIT /
   * ROLLBACK here; coded throws propagate to the caller, which owns ROLLBACK).
   * Takes the per-ACCOUNT advisory xact lock as its first act — the lock releases
   * at the caller's COMMIT, so mutations stay serialized per account.
   */
  async #enrollInTx(client, { account, dev, inbox, cert, epoch }) {
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [account]);

    const existing = await client.query(
      "SELECT account_identity, device_id, inbox_id, cert_id, authority_epoch, status"
        + " FROM account_device_registry WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (existing.rowCount > 0) {
      const row = existing.rows[0];
      // Revoke-before-bind fail-close (audit 2026-07-09 P1). A device.add can
      // enroll a device (active) BEFORE it has ever called device.bind; an
      // account-wide device.revoke then marks THIS row revoked (its cursor
      // close is a no-op because no device_cursors row exists yet). If the
      // revoked device now binds, enroll MUST refuse — otherwise the account's
      // authority says "revoked" while the freshly-created delivery cursor is
      // live, reopening the home-enforced revocation invariant. Revocation is
      // terminal for a deviceId (re-add uses a new deviceId), so this rejects
      // under the same per-account advisory lock that serializes the revoke.
      if (String(row.status) === "revoked") {
        throw codedError(
          `device ${dev} is revoked for account and cannot re-enroll`,
          "DEVICE_REVOKED",
        );
      }
      const sameInbox = String(row.inbox_id) === inbox;
      if (!sameInbox) {
        throw codedError(
          `device ${dev} is already enrolled to a different inbox for account`,
          "ACCOUNT_DEVICE_CONFLICT",
        );
      }
      // cert reconciliation (S2.5 S12): the serializer's device.add fold writes
      // cert_id=NULL, device.bind's enroll writes the leaf certId — two writers
      // on one column. A NULL stored cert is UPGRADEABLE to a non-null leaf; a
      // non-null cert is NEVER clobbered to NULL; two DIFFERENT non-null certs
      // are a genuine conflict.
      const storedCert = row.cert_id == null ? null : String(row.cert_id);
      if (storedCert != null && cert != null && storedCert !== cert) {
        throw codedError(
          `device ${dev} is already enrolled with a different cert for account`,
          "ACCOUNT_DEVICE_CONFLICT",
        );
      }
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

    // Not enrolled for this (account, device). Explicit inbox-uniqueness
    // pre-check for a clean error in the common case; the unique index is the
    // backstop for a cross-account race (caught as 23505 below).
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
   * Set a device's status (e.g. 'revoked'). Monotonic: the new authorityEpoch may
   * not regress below the stored one. Serialized per account.
   * @returns {Promise<{accountIdentityPublicKeyB64,deviceId,inboxId,certId,authorityEpoch,status}>}
   */
  async setStatus({ accountIdentityPublicKeyB64, deviceId, status, authorityEpoch } = {}) {
    const account = this.#normalize(accountIdentityPublicKeyB64);
    const dev = this.#normalize(deviceId);
    const next = this.#normalize(status);
    const epoch = Number(authorityEpoch);
    if (!account) throw new Error("PgAccountDeviceRegistry.setStatus requires accountIdentityPublicKeyB64");
    if (!dev) throw new Error("PgAccountDeviceRegistry.setStatus requires deviceId");
    if (!next || !ALLOWED_STATUSES.has(next)) {
      throw codedError(`invalid status ${String(status)}`, "BAD_STATUS");
    }
    if (!Number.isFinite(epoch) || epoch < 0) {
      throw new Error("PgAccountDeviceRegistry.setStatus requires a finite non-negative authorityEpoch");
    }

    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [account]);
        const existing = await client.query(
          "SELECT authority_epoch FROM account_device_registry WHERE account_identity = $1 AND device_id = $2",
          [account, dev],
        );
        if (existing.rowCount === 0) {
          await client.query("ROLLBACK");
          throw codedError(`device ${dev} is not enrolled for account`, "DEVICE_NOT_ENROLLED");
        }
        if (epoch < Number(existing.rows[0].authority_epoch)) {
          await client.query("ROLLBACK");
          throw codedError(
            `authority epoch ${epoch} regresses below stored ${existing.rows[0].authority_epoch}`,
            "AUTHORITY_EPOCH_REGRESSION",
          );
        }
        const updated = await client.query(
          "UPDATE account_device_registry SET status = $3, authority_epoch = $4, updated_at = now()"
            + " WHERE account_identity = $1 AND device_id = $2"
            + " RETURNING account_identity, device_id, inbox_id, cert_id, authority_epoch, status",
          [account, dev, next, epoch],
        );
        await client.query("COMMIT");
        return this.#rowToBinding(updated.rows[0]);
      } catch (err) {
        if (err && (err.code === "DEVICE_NOT_ENROLLED" || err.code === "AUTHORITY_EPOCH_REGRESSION")) {
          throw err;
        }
        await client.query("ROLLBACK").catch(() => {});
        throw err;
      }
    });
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
