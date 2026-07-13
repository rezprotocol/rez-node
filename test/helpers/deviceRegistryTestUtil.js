import { createHash } from "node:crypto";

/**
 * Deterministic canonical device id for tests. The registry (L2c) is the canonical
 * device-ID invariant owner — every add/enroll and every never-enrolled revoke
 * requires a canonical `rez:dev:<64-lowercase-hex>` id — so tests must use real
 * canonical ids, not readable short stand-ins like "rez:dev:a1". This maps any seed
 * string to a stable canonical id (sha256 of the seed → 64 lowercase hex). No
 * Date/Math.random, so it is fully deterministic across runs.
 */
export function canonicalDeviceId(seed) {
  return "rez:dev:" + createHash("sha256").update(String(seed)).digest("hex");
}

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
