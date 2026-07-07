/**
 * Postgres-backed home store for per-device prekey bundles (S2.5 S12, multi-device
 * fan-out). Each of an account's devices self-publishes its DevicePrekeyBundleV1
 * here; any device of the account then fetches the WHOLE active device set (all
 * siblings' bundles) to assemble the multi-device DeviceSetRecordV1 it seals per
 * peer.
 *
 * The authoritative ACTIVE-device set is account_device_registry (migration 0009,
 * status column); this store only caches the self-published bundle keyed the same
 * way, and listActiveBundles JOINs the registry so a revoked device's stale bundle
 * is never served. `prekey_version` is monotonic per device — a stale republish
 * (an older version) cannot downgrade the live bundle.
 *
 * ACCOUNT-BLINDNESS BOUNDARY: same deliberate carve-out as the registry — the
 * account identity always arrives from an AUTHENTICATED session, never from
 * joining claim rows.
 */
export class PgAccountDeviceBundleStore {
  #conn;

  constructor({ connection } = {}) {
    if (!connection) {
      throw new Error("PgAccountDeviceBundleStore requires connection");
    }
    this.#conn = connection;
  }

  #norm(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  /**
   * Upsert a device's published bundle. Monotonic: an equal-or-newer prekeyVersion
   * replaces the row; a strictly-older one is a stale republish and leaves the live
   * bundle unchanged. Returns the CURRENT stored bundle either way.
   *
   * @param {object} b
   * @param {string} b.accountIdentityPublicKeyB64
   * @param {string} b.deviceId
   * @param {number} b.prekeyVersion  monotonic (int ≥ 0)
   * @param {object} b.bundleJson     the full DevicePrekeyBundleV1 (device-signed)
   * @returns {Promise<{deviceId, prekeyVersion, bundleJson, applied: boolean}>}
   */
  async putBundle({ accountIdentityPublicKeyB64, deviceId, prekeyVersion, bundleJson } = {}) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    const dev = this.#norm(deviceId);
    if (!account) throw new Error("putBundle requires accountIdentityPublicKeyB64");
    if (!dev) throw new Error("putBundle requires deviceId");
    if (!Number.isInteger(prekeyVersion) || prekeyVersion < 0) {
      throw new Error("putBundle requires a non-negative integer prekeyVersion");
    }
    if (!bundleJson || typeof bundleJson !== "object") {
      throw new Error("putBundle requires a bundleJson object");
    }
    const json = JSON.stringify(bundleJson);
    const upserted = await this.#conn.query(
      "INSERT INTO account_device_bundle (account_identity, device_id, prekey_version, bundle_json)"
        + " VALUES ($1, $2, $3, $4::jsonb)"
        + " ON CONFLICT (account_identity, device_id)"
        + " DO UPDATE SET bundle_json = EXCLUDED.bundle_json, prekey_version = EXCLUDED.prekey_version, updated_at = now()"
        + " WHERE EXCLUDED.prekey_version >= account_device_bundle.prekey_version"
        + " RETURNING device_id, prekey_version, bundle_json",
      [account, dev, prekeyVersion, json],
    );
    if (upserted.rowCount > 0) {
      const row = upserted.rows[0];
      return { deviceId: row.device_id, prekeyVersion: Number(row.prekey_version), bundleJson: row.bundle_json, applied: true };
    }
    // The monotonic guard rejected a stale version (or a no-op) — return the live one.
    const cur = await this.#conn.query(
      "SELECT device_id, prekey_version, bundle_json FROM account_device_bundle WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (cur.rowCount === 0) {
      // No row and the upsert didn't apply — should not happen for a fresh insert
      // (guard is >= against DEFAULT 0), but fail loud rather than return garbage.
      throw new Error("putBundle: no bundle stored and none present after upsert");
    }
    const row = cur.rows[0];
    return { deviceId: row.device_id, prekeyVersion: Number(row.prekey_version), bundleJson: row.bundle_json, applied: false };
  }

  /**
   * All ACTIVE devices' published bundles for an account (join the registry so a
   * revoked device's stale bundle is excluded), ordered by device_id.
   * @returns {Promise<Array<{deviceId, prekeyVersion, bundleJson}>>}
   */
  async listActiveBundles(accountIdentityPublicKeyB64) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    if (!account) return [];
    const res = await this.#conn.query(
      "SELECT b.device_id, b.prekey_version, b.bundle_json"
        + " FROM account_device_bundle b"
        + " JOIN account_device_registry r"
        + "   ON r.account_identity = b.account_identity AND r.device_id = b.device_id"
        + " WHERE b.account_identity = $1 AND r.status = 'active'"
        + " ORDER BY b.device_id",
      [account],
    );
    return res.rows.map((row) => ({
      deviceId: row.device_id,
      prekeyVersion: Number(row.prekey_version),
      bundleJson: row.bundle_json,
    }));
  }

  /** @returns {Promise<object|null>} the stored bundle for (account, device), or null. */
  async getBundle(accountIdentityPublicKeyB64, deviceId) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    const dev = this.#norm(deviceId);
    if (!account || !dev) return null;
    const res = await this.#conn.query(
      "SELECT device_id, prekey_version, bundle_json FROM account_device_bundle WHERE account_identity = $1 AND device_id = $2",
      [account, dev],
    );
    if (res.rowCount === 0) return null;
    const row = res.rows[0];
    return { deviceId: row.device_id, prekeyVersion: Number(row.prekey_version), bundleJson: row.bundle_json };
  }
}
