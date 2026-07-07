/**
 * The account's AUTHORITY-HOME serializer (S2.5 S11, findings F4+F5, OPEN-B).
 *
 * A device submits a signed AccountDeviceMutationV1 (add/revoke a sibling); this
 * serializer applies it under a PER-ACCOUNT advisory lock so the read→fold→write
 * is linearizable (the same discipline PgDurableInbox.append uses for its
 * security-boundary mutations). It:
 *   - dedupes by opId (a replayed opId returns the committed result, no re-apply);
 *   - enforces expectedRevision (a stale value returns the latest state, NO clobber);
 *   - folds the canonical ACTIVE device set into account_device_registry (0009);
 *   - maintains the revoked-cert set + minValidIssuedAt cutoff (0010);
 *   - bumps ONE monotonic epoch that IS the DeviceSetRecordV1.revision AND the
 *     authority-state epoch.
 *
 * The result is a deterministic { revision, devices, authorityState } — persisted
 * as the journal row's result_json so a replay is byte-stable.
 *
 * The node is account-blind everywhere else (inbox_claims never joins an account);
 * this serializer, like the registry it folds into, is the deliberate opt-in
 * carve-out — the account identity always arrives from an AUTHENTICATED session,
 * never from joining claim rows.
 */

function codedError(message, code) {
  const err = new Error(message);
  err.code = code;
  return err;
}

export class PgAccountMutationSerializer {
  #conn;

  constructor({ connection } = {}) {
    if (!connection) {
      throw new Error("PgAccountMutationSerializer requires connection");
    }
    this.#conn = connection;
  }

  #norm(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  // The committed state at `epoch`: active device bindings + the revocationState
  // projection. Read inside the account lock (or a pooled read for getAuthorityState).
  async #loadState(client, account, epoch) {
    const devs = await client.query(
      "SELECT device_id, inbox_id, cert_id, status FROM account_device_registry"
        + " WHERE account_identity = $1 AND status = 'active' ORDER BY device_id",
      [account],
    );
    const revoked = await client.query(
      "SELECT cert_id FROM account_revoked_cert WHERE account_identity = $1 ORDER BY cert_id",
      [account],
    );
    const auth = await client.query(
      "SELECT min_valid_issued_at_ms FROM account_authority WHERE account_identity = $1",
      [account],
    );
    const minValid = auth.rowCount > 0 ? Number(auth.rows[0].min_valid_issued_at_ms) : 0;
    return {
      devices: devs.rows.map((r) => ({
        deviceId: r.device_id,
        inboxId: r.inbox_id,
        certId: r.cert_id == null ? null : String(r.cert_id),
        status: String(r.status),
      })),
      authorityState: {
        epoch,
        revokedCertIds: revoked.rows.map((r) => String(r.cert_id)),
        minValidIssuedAtMs: minValid,
      },
    };
  }

  /**
   * Serve the current authority state (pooled read — callers await for authz).
   * @returns {Promise<{epoch:number, revokedCertIds:string[], minValidIssuedAtMs:number}>}
   */
  async getAuthorityState(accountIdentityPublicKeyB64) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    if (!account) return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 };
    const auth = await this.#conn.query(
      "SELECT epoch, min_valid_issued_at_ms FROM account_authority WHERE account_identity = $1",
      [account],
    );
    const epoch = auth.rowCount > 0 ? Number(auth.rows[0].epoch) : 0;
    const minValid = auth.rowCount > 0 ? Number(auth.rows[0].min_valid_issued_at_ms) : 0;
    const revoked = await this.#conn.query(
      "SELECT cert_id FROM account_revoked_cert WHERE account_identity = $1 ORDER BY cert_id",
      [account],
    );
    return { epoch, revokedCertIds: revoked.rows.map((r) => String(r.cert_id)), minValidIssuedAtMs: minValid };
  }

  /**
   * Apply a serialized device mutation.
   *
   * @param {object} m
   * @param {string} m.accountIdentityPublicKeyB64
   * @param {string} m.opId                       idempotency key
   * @param {number} m.expectedRevision           optimistic concurrency (int ≥ 0)
   * @param {"device.add"|"device.revoke"} m.action
   * @param {object} m.target
   *   add    → { deviceId, inboxId, certId? }
   *   revoke → { revokedDeviceId, revokedCertId?, minValidIssuedAtMs? }
   * @returns {Promise<{revision, devices, authorityState, idempotentReplay?, stale?, currentRevision?}>}
   */
  async submitMutation({ accountIdentityPublicKeyB64, opId, expectedRevision, action, target } = {}) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    const op = this.#norm(opId);
    if (!account) throw new Error("submitMutation requires accountIdentityPublicKeyB64");
    if (!op) throw new Error("submitMutation requires opId");
    if (action !== "device.add" && action !== "device.revoke") {
      throw codedError("unknown mutation action " + String(action), "BAD_ACTION");
    }
    if (!Number.isInteger(expectedRevision) || expectedRevision < 0) {
      throw new Error("submitMutation requires a non-negative integer expectedRevision");
    }
    const tgt = target && typeof target === "object" ? target : {};

    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [account]);

        // (1) opId idempotency — a committed replay returns the same bytes.
        const prior = await client.query(
          "SELECT result_json FROM account_device_mutation WHERE account_identity = $1 AND op_id = $2",
          [account, op],
        );
        if (prior.rowCount > 0) {
          await client.query("COMMIT");
          return { ...prior.rows[0].result_json, idempotentReplay: true };
        }

        // (2) seed / load the epoch scalar.
        const cur = await client.query(
          "INSERT INTO account_authority (account_identity) VALUES ($1)"
            + " ON CONFLICT (account_identity) DO UPDATE SET updated_at = now()"
            + " RETURNING epoch, min_valid_issued_at_ms",
          [account],
        );
        const currentEpoch = Number(cur.rows[0].epoch);
        let minValid = Number(cur.rows[0].min_valid_issued_at_ms);

        // (3) expectedRevision CAS — stale ⇒ return latest, NO clobber.
        if (expectedRevision !== currentEpoch) {
          const state = await this.#loadState(client, account, currentEpoch);
          await client.query("COMMIT");
          return { stale: true, currentRevision: currentEpoch, ...state };
        }

        const nextEpoch = currentEpoch + 1;

        // (4) fold the canonical device set (remove-wins for revoke).
        if (action === "device.add") {
          const deviceId = this.#norm(tgt.deviceId);
          const inboxId = this.#norm(tgt.inboxId);
          const certId = tgt.certId == null ? null : this.#norm(tgt.certId);
          if (!deviceId || !inboxId) {
            await client.query("ROLLBACK");
            throw codedError("device.add target requires deviceId and inboxId", "BAD_TARGET");
          }
          // Inbox-uniqueness: reject an inbox already held by a DIFFERENT device
          // (explicit check + the registry's unique-index 23505 backstop).
          const held = await client.query(
            "SELECT device_id FROM account_device_registry WHERE inbox_id = $1",
            [inboxId],
          );
          if (held.rowCount > 0 && String(held.rows[0].device_id) !== deviceId) {
            await client.query("ROLLBACK");
            throw codedError("inbox " + inboxId + " is already enrolled to another device", "INBOX_ALREADY_ENROLLED");
          }
          try {
            await client.query(
              "INSERT INTO account_device_registry (account_identity, device_id, inbox_id, cert_id, authority_epoch, status)"
                + " VALUES ($1, $2, $3, $4, $5, 'active')"
                + " ON CONFLICT (account_identity, device_id)"
                + " DO UPDATE SET inbox_id = EXCLUDED.inbox_id, cert_id = EXCLUDED.cert_id,"
                + " authority_epoch = EXCLUDED.authority_epoch, status = 'active', updated_at = now()",
              [account, deviceId, inboxId, certId, nextEpoch],
            );
          } catch (err) {
            await client.query("ROLLBACK");
            if (err && err.code === "23505") {
              throw codedError("inbox " + inboxId + " is already enrolled to another device", "INBOX_ALREADY_ENROLLED");
            }
            throw err;
          }
        } else {
          const revokedDeviceId = this.#norm(tgt.revokedDeviceId);
          if (!revokedDeviceId) {
            await client.query("ROLLBACK");
            throw codedError("device.revoke target requires revokedDeviceId", "BAD_TARGET");
          }
          // Remove-wins: fail-close the device (idempotent if already revoked or
          // never enrolled — a revoke must never fail on "not found", the
          // security intent is "this device must not be active").
          await client.query(
            "UPDATE account_device_registry SET status = 'revoked', authority_epoch = $3, updated_at = now()"
              + " WHERE account_identity = $1 AND device_id = $2",
            [account, revokedDeviceId, nextEpoch],
          );
          const revokedCertId = tgt.revokedCertId == null ? null : this.#norm(tgt.revokedCertId);
          if (revokedCertId) {
            await client.query(
              "INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch)"
                + " VALUES ($1, $2, $3) ON CONFLICT (account_identity, cert_id) DO NOTHING",
              [account, revokedCertId, nextEpoch],
            );
          }
          if (Number.isFinite(Number(tgt.minValidIssuedAtMs)) && Number(tgt.minValidIssuedAtMs) > minValid) {
            minValid = Number(tgt.minValidIssuedAtMs);
          }
        }

        // (5) bump the monotonic epoch (+ any advanced cutoff).
        await client.query(
          "UPDATE account_authority SET epoch = $2, min_valid_issued_at_ms = $3, updated_at = now()"
            + " WHERE account_identity = $1",
          [account, nextEpoch, minValid],
        );

        // (6) assemble the deterministic committed result.
        const state = await this.#loadState(client, account, nextEpoch);
        const result = { revision: nextEpoch, ...state };

        // (7) append the immutable journal row (op_id PK = idempotency anchor).
        await client.query(
          "INSERT INTO account_device_mutation"
            + " (account_identity, op_id, epoch, action, target_device_id, target_cert_id, result_json)"
            + " VALUES ($1, $2, $3, $4, $5, $6, $7)",
          [
            account,
            op,
            nextEpoch,
            action,
            action === "device.add" ? this.#norm(tgt.deviceId) : this.#norm(tgt.revokedDeviceId),
            action === "device.revoke" && tgt.revokedCertId ? this.#norm(tgt.revokedCertId) : null,
            JSON.stringify(result),
          ],
        );

        await client.query("COMMIT");
        return { ...result, idempotentReplay: false };
      } catch (err) {
        if (err && (err.code === "BAD_TARGET" || err.code === "INBOX_ALREADY_ENROLLED")) {
          throw err;
        }
        await client.query("ROLLBACK").catch(() => {});
        throw err;
      }
    });
  }
}
