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

import { PgAccountDeviceRegistry } from "./PgAccountDeviceRegistry.js";

// Audit R4 F3 admission-control ceilings owned by the serializer (the registry owns
// the device caps). Constructor-overridable defaults.
//   REVOKED_CERTS: the per-account distinct revoked capability-cert set. The
//     revokedCertId is a forgeable string on the mutation, so bound its durable growth.
//   OPID_BYTES / CERT_ID_BYTES: input-shape guards so a giant opId / cert-id cannot
//     bloat the journal or the revoked-cert table.
export const MAX_REVOKED_CERTS_PER_ACCOUNT = 256;
export const MAX_OPID_BYTES = 256;
export const MAX_CERT_ID_BYTES = 256;
// The mutation journal's replay payload (result_json) is prunable after this window;
// the audit row stays forever (see migration 0013 + pruneExpiredReplayPayloads).
export const DEFAULT_REPLAY_RETENTION_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

function codedError(message, code) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function resolveCap(value, fallback) {
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

export class PgAccountMutationSerializer {
  #conn;
  #durableInbox;
  #registry;
  #caps;

  constructor({ connection, durableInbox, registry = null, caps = null } = {}) {
    if (!connection) {
      throw new Error("PgAccountMutationSerializer requires connection");
    }
    // S2.5 S11 audit F4: a serialized revoke must fail-close the target device's
    // delivery cursor ATOMICALLY with the authority commit — otherwise a crash
    // between the two phases leaves authority=revoked but the cursor live, and the
    // revoked device keeps draining its home inbox. The durable inbox is always
    // constructed alongside this serializer on the pg cluster block, so require it
    // and fail loud rather than silently regress to a split-write revoke.
    if (!durableInbox || typeof durableInbox.revokeDeviceInTx !== "function") {
      throw new Error("PgAccountMutationSerializer requires a durableInbox exposing revokeDeviceInTx (atomic revoke fail-close)");
    }
    this.#conn = connection;
    this.#durableInbox = durableInbox;
    // Audit R4 F5a: the serializer no longer hand-mirrors the registry's device
    // add/revoke SQL (that drift caused the R3 resurrection bug). It COMPOSES the
    // registry's canonical InTx fold methods under its own account lock. The
    // registry is a stateless SQL owner over this same (connection, durableInbox);
    // bootstrapRelay injects the shared instance, and a caller that omits it gets
    // an equivalent one built from the deps already required above.
    this.#registry = registry ? registry : new PgAccountDeviceRegistry({ connection, durableInbox });
    if (typeof this.#registry.foldAddInTx !== "function" || typeof this.#registry.foldRevokeInTx !== "function") {
      throw new Error("PgAccountMutationSerializer requires a registry exposing foldAddInTx/foldRevokeInTx");
    }
    if (typeof this.#registry.isActiveAddNoopInTx !== "function" || typeof this.#registry.isTerminallyRevokedInTx !== "function") {
      throw new Error("PgAccountMutationSerializer requires a registry exposing isActiveAddNoopInTx/isTerminallyRevokedInTx");
    }
    // Audit R4 F3 admission-control caps (constructor-overridable; safe defaults).
    const c = caps && typeof caps === "object" ? caps : {};
    this.#caps = {
      revokedCerts: resolveCap(c.revokedCerts, MAX_REVOKED_CERTS_PER_ACCOUNT),
      opIdBytes: resolveCap(c.opIdBytes, MAX_OPID_BYTES),
      certIdBytes: resolveCap(c.certIdBytes, MAX_CERT_ID_BYTES),
    };
  }

  #norm(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  // The revocationState projection ({revokedCertIds, minValidIssuedAtMs}) at the
  // caller's read point. Read INSIDE the account lock by submitMutation (the L3
  // under-lock recheck + #loadState both consume it), so it reflects everything a
  // concurrent revoke committed before this lock was taken.
  async #loadRevocationState(client, account) {
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
      revokedCertIds: revoked.rows.map((r) => String(r.cert_id)),
      minValidIssuedAtMs: minValid,
    };
  }

  // The committed state at `epoch`: active device bindings + the revocationState
  // projection. Read inside the account lock (or a pooled read for getAuthorityState).
  async #loadState(client, account, epoch) {
    const devs = await client.query(
      "SELECT device_id, inbox_id, cert_id, status FROM account_device_registry"
        + " WHERE account_identity = $1 AND status = 'active' ORDER BY device_id",
      [account],
    );
    const rev = await this.#loadRevocationState(client, account);
    return {
      devices: devs.rows.map((r) => ({
        deviceId: r.device_id,
        inboxId: r.inbox_id,
        certId: r.cert_id == null ? null : String(r.cert_id),
        status: String(r.status),
      })),
      authorityState: {
        epoch,
        revokedCertIds: rev.revokedCertIds,
        minValidIssuedAtMs: rev.minValidIssuedAtMs,
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
   * @param {(revocationState:{revokedCertIds:string[],minValidIssuedAtMs:number})=>Promise<boolean>} [m.revalidate]
   *   audit R4 L3: an OPTIONAL async recheck run UNDER the per-account lock, against
   *   the in-tx revocation state, before the fold. Returning anything but `true`
   *   aborts the mutation (DELEGATED_AUTHORITY_INVALID, no fold/epoch/journal). A
   *   DELEGATED session passes a closure over the account-authority verifier; a
   *   direct (primary) session omits it (the account root is unrevocable).
   * @returns {Promise<{revision, devices, authorityState, idempotentReplay?, stale?, currentRevision?}>}
   */
  async submitMutation({ accountIdentityPublicKeyB64, opId, expectedRevision, action, target, revalidate } = {}) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    const op = this.#norm(opId);
    if (!account) throw new Error("submitMutation requires accountIdentityPublicKeyB64");
    if (!op) throw new Error("submitMutation requires opId");
    // Audit R4 F3: bound the opId size (it is a client-chosen string persisted as the
    // journal PK) so an oversized key cannot bloat the audit log.
    if (Buffer.byteLength(op, "utf8") > this.#caps.opIdBytes) {
      throw codedError("opId exceeds the " + this.#caps.opIdBytes + "-byte limit", "BAD_REQUEST");
    }
    if (action !== "device.add" && action !== "device.revoke") {
      throw codedError("unknown mutation action " + String(action), "BAD_ACTION");
    }
    if (!Number.isInteger(expectedRevision) || expectedRevision < 0) {
      throw new Error("submitMutation requires a non-negative integer expectedRevision");
    }
    const tgt = target && typeof target === "object" ? target : {};

    // Audit R4 F3: bound the revoke target's cert-id SHAPE + size before it can reach
    // the durable revoked-cert set (it is a forgeable string on the mutation).
    if (action === "device.revoke" && tgt.revokedCertId != null) {
      const rc = this.#norm(tgt.revokedCertId);
      if (!rc || rc.slice(0, 8) !== "rez:cap:" || Buffer.byteLength(rc, "utf8") > this.#caps.certIdBytes) {
        throw codedError(
          "revokedCertId must be a rez:cap: id within the " + this.#caps.certIdBytes + "-byte limit",
          "BAD_TARGET",
        );
      }
    }

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
          const payload = prior.rows[0].result_json;
          if (payload != null) {
            await client.query("COMMIT");
            return { ...payload, idempotentReplay: true };
          }
          // Audit R4 F3: the replay payload was pruned by the retention sweep, but the
          // audit row proves the op committed. Return the CURRENT authority state
          // (replayExpired) rather than the exact historical snapshot — the caller only
          // needs to learn the op already applied and where authority now stands.
          const authRow = await client.query(
            "SELECT epoch FROM account_authority WHERE account_identity = $1",
            [account],
          );
          const epoch = authRow.rowCount > 0 ? Number(authRow.rows[0].epoch) : 0;
          const state = await this.#loadState(client, account, epoch);
          await client.query("COMMIT");
          return { revision: epoch, ...state, idempotentReplay: true, replayExpired: true };
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

        // (3.5) audit R4 L3 — re-validate delegated authority UNDER this account lock,
        // against the IN-TX revocation state. The caller's connect-time / pre-lock
        // authority read can go stale between that read and this lock; a concurrent
        // device.revoke of the delegated leaf must serialize on the SAME account lock,
        // so it either committed before this load (its cert appears in `rev` ⇒ the
        // recheck rejects) or waits until after our commit (it sees our epoch bump).
        // There is no interleaving where this mutation folds on already-revoked
        // authority. Direct (primary) sessions pass no revalidate.
        if (typeof revalidate === "function") {
          const rev = await this.#loadRevocationState(client, account);
          const ok = await revalidate(rev);
          if (ok !== true) {
            throw codedError(
              "delegated authority is no longer valid (revoked mid-flight)",
              "DELEGATED_AUTHORITY_INVALID",
            );
          }
        }

        // (3.6) audit R4 F3 — semantic no-op guard. A mutation that changes NOTHING
        // must not bump the epoch or append a journal row; otherwise an authorized (or
        // delegated) device could churn the authority epoch and grow the journal
        // indefinitely with repeats. CONSERVATIVE: only a CERTAIN no-change short-
        // circuits (the registry owns "what is an equivalent device"); anything
        // ambiguous folds normally. A distinct opId is still recorded only for real
        // changes — a no-op returns the current state WITHOUT a journal row, so a later
        // replay of that opId is itself a fresh no-op (idempotent + harmless).
        let isNoop = false;
        if (action === "device.add") {
          isNoop = await this.#registry.isActiveAddNoopInTx(client, {
            accountIdentityPublicKeyB64: account,
            deviceId: this.#norm(tgt.deviceId),
            inboxId: this.#norm(tgt.inboxId),
            certId: tgt.certId == null ? null : this.#norm(tgt.certId),
          });
        } else {
          const revokedDeviceId = this.#norm(tgt.revokedDeviceId);
          const revokedCertId = tgt.revokedCertId == null ? null : this.#norm(tgt.revokedCertId);
          const advancesCutoff = Number.isFinite(Number(tgt.minValidIssuedAtMs)) && Number(tgt.minValidIssuedAtMs) > minValid;
          if (revokedDeviceId && !advancesCutoff) {
            const alreadyTerminal = await this.#registry.isTerminallyRevokedInTx(client, {
              accountIdentityPublicKeyB64: account,
              deviceId: revokedDeviceId,
            });
            if (alreadyTerminal) {
              // A NEW revokedCertId still advances authority (it kills that cert chain),
              // so it is NOT a no-op even when the device is already revoked.
              let certIsNew = false;
              if (revokedCertId) {
                const existsCert = await client.query(
                  "SELECT 1 FROM account_revoked_cert WHERE account_identity = $1 AND cert_id = $2",
                  [account, revokedCertId],
                );
                certIsNew = existsCert.rowCount === 0;
              }
              isNoop = !certIsNew;
            }
          }
        }
        if (isNoop) {
          const state = await this.#loadState(client, account, currentEpoch);
          await client.query("COMMIT");
          return { noop: true, revision: currentEpoch, ...state, idempotentReplay: false };
        }

        const nextEpoch = currentEpoch + 1;

        // (4) fold the canonical device set (remove-wins for revoke) by COMPOSING
        // the registry's canonical InTx methods under the per-account lock already
        // held (audit R4 F5a — one writer, one place for the revoked-terminal /
        // inbox-immutable / inbox-unique / tombstone invariants). Coded throws from
        // the fold propagate to the outer catch, which owns the ROLLBACK.
        if (action === "device.add") {
          const deviceId = this.#norm(tgt.deviceId);
          const inboxId = this.#norm(tgt.inboxId);
          const certId = tgt.certId == null ? null : this.#norm(tgt.certId);
          if (!deviceId || !inboxId) {
            throw codedError("device.add target requires deviceId and inboxId", "BAD_TARGET");
          }
          // device.add is a NEW authority mutation, so the fold stamps nextEpoch on
          // the row (an epoch-bumping upsert). The registry rejects a
          // revoked/tombstoned deviceId (DEVICE_REVOKED — closes F1 on the add
          // path), an inbox re-point / cross-device inbox (ACCOUNT_DEVICE_CONFLICT /
          // INBOX_ALREADY_ENROLLED), preserving the R3 guards without duplicated SQL.
          await this.#registry.foldAddInTx(client, {
            accountIdentityPublicKeyB64: account,
            deviceId,
            inboxId,
            certId,
            authorityEpoch: nextEpoch,
          });
        } else {
          const revokedDeviceId = this.#norm(tgt.revokedDeviceId);
          if (!revokedDeviceId) {
            throw codedError("device.revoke target requires revokedDeviceId", "BAD_TARGET");
          }
          // Remove-wins + terminal tombstone (F5a + F1). The fold flips an enrolled
          // row to 'revoked' and writes the durable tombstone (so a device revoked
          // BEFORE it ever enrolled can never be resurrected by a later device.add);
          // it enforces the tombstone DoS guards on the never-enrolled forgeable
          // path (canonical syntax + per-account quota). A revoke never fails on
          // "not found" — a never-enrolled canonical target just gets a tombstone.
          const rev = await this.#registry.foldRevokeInTx(client, {
            accountIdentityPublicKeyB64: account,
            deviceId: revokedDeviceId,
            authorityEpoch: nextEpoch,
          });
          // Audit F4: close the target device's HOME delivery cursor in THIS same
          // transaction (see PgDurableInbox.revokeDeviceInTx). The authority commit
          // and the cursor close now succeed or roll back together — no split, and
          // no dependence on a caller retrying the exact op.
          if (rev.revokedInboxId) {
            await this.#durableInbox.revokeDeviceInTx(client, rev.revokedInboxId, revokedDeviceId);
          }
          const revokedCertId = tgt.revokedCertId == null ? null : this.#norm(tgt.revokedCertId);
          if (revokedCertId) {
            // Audit R4 F3: bound the durable revoked-cert set. Only a NEW cert counts
            // against the cap — an idempotent re-revoke of an already-listed cert is
            // free (and cannot be a no-op reaching here, since a new cert advances
            // authority and the (3.6) guard already excluded a same-cert repeat).
            const existsCert = await client.query(
              "SELECT 1 FROM account_revoked_cert WHERE account_identity = $1 AND cert_id = $2",
              [account, revokedCertId],
            );
            if (existsCert.rowCount === 0) {
              const cnt = await client.query(
                "SELECT count(*)::int AS c FROM account_revoked_cert WHERE account_identity = $1",
                [account],
              );
              if (cnt.rows[0].c >= this.#caps.revokedCerts) {
                throw codedError(
                  "account has reached the " + this.#caps.revokedCerts + " revoked-cert limit",
                  "REVOKED_CERT_QUOTA_EXCEEDED",
                );
              }
            }
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
        // The registry fold methods (foldAddInTx/foldRevokeInTx) and the
        // serializer's own validations throw coded errors WITHOUT rolling back
        // (they run inside this tx and do not own it), so the ROLLBACK is uniformly
        // owned here. All throws are pre-COMMIT (idempotent-replay and stale paths
        // COMMIT-then-return), so this can never roll back a committed tx.
        try {
          await client.query("ROLLBACK");
        } catch (rbErr) {
          console.error("[PgAccountMutationSerializer] rollback after mutation error failed: " + (rbErr && rbErr.message ? rbErr.message : rbErr));
        }
        throw err;
      }
    });
  }

  /**
   * Retention sweep for the mutation journal's REPLAY payload (audit R4 F3, migration
   * 0013). NULLs result_json for rows committed before nowMs - ttlMs; the audit row
   * (account, op_id, epoch, action, targets, committed_at) is untouched. A later
   * replay of a pruned opId still proves the op committed and returns the current
   * authority state with replayExpired:true. Idempotent — safe to run repeatedly.
   * Invoked by the DurableInboxPruner sweep on pg cluster nodes.
   *
   * @param {number} nowMs current wall-clock ms (the caller supplies it — this class
   *   takes no ambient clock).
   * @param {number} [ttlMs] retention window; defaults to DEFAULT_REPLAY_RETENTION_MS.
   * @returns {Promise<number>} rows whose payload was pruned this pass.
   */
  async pruneExpiredReplayPayloads(nowMs, ttlMs = DEFAULT_REPLAY_RETENTION_MS) {
    const now = Number(nowMs);
    if (!Number.isFinite(now)) {
      throw new Error("pruneExpiredReplayPayloads requires a finite nowMs");
    }
    const ttl = Number.isFinite(Number(ttlMs)) && Number(ttlMs) > 0 ? Number(ttlMs) : DEFAULT_REPLAY_RETENTION_MS;
    const cutoffMs = now - ttl;
    const res = await this.#conn.query(
      "UPDATE account_device_mutation SET result_json = NULL"
        + " WHERE committed_at < to_timestamp($1 / 1000.0) AND result_json IS NOT NULL",
      [cutoffMs],
    );
    return typeof res.rowCount === "number" ? res.rowCount : 0;
  }
}
