/**
 * Test-only helper: revoke a device at the STORAGE layer, the way tests need for
 * setup (get a device into a 'revoked' + tombstoned state without standing up a
 * whole PgAccountMutationSerializer).
 *
 * This deliberately lives in test/ and is NOT a production method on the registry:
 * a public registry.revoke() would be a split-brain writer (it mutates registry +
 * tombstone but not the account authority epoch / delivery cursor / mutation
 * journal, which only the serializer owns) — exactly the alternate-writer class the
 * R4 audit set out to remove. Tests drive the canonical foldRevokeInTx inside an
 * explicit transaction under the per-account advisory lock, mirroring what the
 * serializer does around it.
 *
 * @returns {Promise<object|null>} the revoked binding, or null when the device was
 *   never enrolled (a tombstone-only revoke — the F1 case).
 */
export async function revokeDeviceForTest(conn, registry, { accountIdentityPublicKeyB64, deviceId, authorityEpoch }) {
  return conn.withClient(async (client) => {
    await client.query("BEGIN");
    try {
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [accountIdentityPublicKeyB64]);
      const res = await registry.foldRevokeInTx(client, { accountIdentityPublicKeyB64, deviceId, authorityEpoch });
      await client.query("COMMIT");
      return res.binding;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    }
  });
}
