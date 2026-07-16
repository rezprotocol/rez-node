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

import { isCanonicalAccountCapabilityCertId } from "@rezprotocol/core";
import { PgAccountDeviceRegistry } from "./PgAccountDeviceRegistry.js";
import { PgPropagationOutbox } from "./PgPropagationOutbox.js";

// Audit R4 F3 admission-control ceiling owned by the serializer (the registry owns the
// device caps). Constructor-overridable default.
//   OPID_BYTES: input-shape guard so a giant opId cannot bloat the journal.
// There is deliberately NO revoked-cert quota (audit R4 F3-remediation finding 1):
// device.revoke only ever auto-revokes the target's OWN non-forgeable bound cert (at most
// one per device), so the revoked-cert set is already bounded by the lifetime-device cap,
// and a fail-close revoke of a real device must NEVER be blocked by a ceiling (a quota
// here would roll back the whole revoke once the set filled). A revoked-cert quota belongs
// to the FUTURE arbitrary capability.revoke path (unwired), which must bound its own
// forgeable input when built. Cert-id shape is the EXACT canonical rez:cap:<64-hex>
// (isCanonicalAccountCapabilityCertId), enforced at the registry (write) and here (input).
export const MAX_OPID_BYTES = 256;
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
  #propagationOutbox;
  #caps;

  constructor({ connection, durableInbox, registry = null, caps = null, propagationOutbox = null } = {}) {
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
    if (typeof this.#registry.isActiveAddNoopInTx !== "function" || typeof this.#registry.getRevokeContextInTx !== "function") {
      throw new Error("PgAccountMutationSerializer requires a registry exposing isActiveAddNoopInTx/getRevokeContextInTx");
    }
    // Audit R4 L5 review-3 finding P2: the coherent delegated snapshot reads terminal device
    // status through THIS canonical registry (never a per-call injected one), so a hand-built
    // runtime cannot split-brain terminal resolution away from the registry used for mutations.
    // Require it at construction (fail loud) — the invariant is intrinsic, not defended per call.
    if (typeof this.#registry.isTerminallyRevokedInTx !== "function") {
      throw new Error("PgAccountMutationSerializer requires a registry exposing isTerminallyRevokedInTx (coherent delegated snapshot)");
    }
    // P1#3 propagation outbox: enqueued IN this serializer's fold transaction on every real
    // epoch-changing mutation. Stateless SQL over the caller's client, so a caller that omits
    // it gets an equivalent one over the same connection (the enqueue always runs — a queue
    // failure rolls back the authority mutation). An INJECTED outbox is validated HERE (fail
    // loud at construction, not on the first mutation) — the invariant is intrinsic.
    this.#propagationOutbox = propagationOutbox ? propagationOutbox : new PgPropagationOutbox({ connection });
    if (typeof this.#propagationOutbox.enqueueInTx !== "function"
      || typeof this.#propagationOutbox.releaseOwnedInTx !== "function") {
      throw new Error("PgAccountMutationSerializer requires a propagationOutbox exposing enqueueInTx + releaseOwnedInTx (atomic enqueue + revoke-release)");
    }
    // Audit R4 F3 admission-control caps (constructor-overridable; safe defaults).
    const c = caps && typeof caps === "object" ? caps : {};
    this.#caps = {
      opIdBytes: resolveCap(c.opIdBytes, MAX_OPID_BYTES),
    };
  }

  /**
   * The propagation outbox this serializer enqueues into (audit leaf-3b F5). Exposed so the
   * runtime + wire handler read the SAME instance the fold writes to — the outbox is intrinsic
   * to the serializer (it shares the serializer's connection by construction), so there is no
   * separate wiring path that could point the wire lease surface at a different database.
   */
  get propagationOutbox() {
    return this.#propagationOutbox;
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
   * The account's current authority epoch, and nothing else — a single indexed row read (no
   * transaction, no revoked-set scan). The per-dispatch L5 fast path (review finding 1) calls
   * this on EVERY delegated frame: the account epoch is monotonic and bumps on every add/revoke,
   * so an unchanged epoch since a session was admitted proves its authority is unchanged, letting
   * the guard skip the heavier coherent read + chain re-verify until the epoch actually advances.
   * @returns {Promise<number>} the epoch (0 when the account has no authority row yet)
   */
  async getCurrentEpoch(accountIdentityPublicKeyB64) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    if (!account) return 0;
    const auth = await this.#conn.query(
      "SELECT epoch FROM account_authority WHERE account_identity = $1",
      [account],
    );
    return auth.rowCount > 0 ? Number(auth.rows[0].epoch) : 0;
  }

  /**
   * Serve the current authority state (callers await for authz).
   *
   * Audit R4 L5 review finding 3: the epoch/cutoff and the revoked-cert set are read inside ONE
   * REPEATABLE READ snapshot, so a mutation committing between the SELECTs cannot yield a mixed
   * view (the OLD epoch alongside the NEW revoked set, or vice versa). The epoch this returns
   * therefore always corresponds to exactly the revoked set / cutoff alongside it.
   * @returns {Promise<{epoch:number, revokedCertIds:string[], minValidIssuedAtMs:number}>}
   */
  async getAuthorityState(accountIdentityPublicKeyB64) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    if (!account) return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0 };
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      try {
        const auth = await client.query(
          "SELECT epoch FROM account_authority WHERE account_identity = $1",
          [account],
        );
        const epoch = auth.rowCount > 0 ? Number(auth.rows[0].epoch) : 0;
        const rev = await this.#loadRevocationState(client, account); // same snapshot
        await client.query("COMMIT");
        return { epoch, revokedCertIds: rev.revokedCertIds, minValidIssuedAtMs: rev.minValidIssuedAtMs };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * ONE COHERENT snapshot for a delegated (account, device): epoch + revocation state + the device's
   * TERMINAL status, read inside a single REPEATABLE READ transaction (audit R4 L5 review finding
   * 1). The three signals therefore reflect exactly one committed point in time. This is the fix for
   * the watermark-poisoning TOCTOU: previously the guard read terminal status via a separate pooled
   * query BEFORE the authority snapshot, so a `cert_id = NULL` device revoked in the gap left the
   * terminal read pre-revoke (false) while the epoch read post-revoke — the session then armed its
   * fast-path watermark to the revoke epoch and never consulted the terminal registry again. Reading
   * terminal WITHIN the snapshot closes that: if the snapshot predates the revoke, terminal is false
   * AND the epoch is the pre-revoke epoch (so the next dispatch's advanced epoch forces a re-check);
   * if it postdates, terminal is true and the session is refused.
   *
   * The terminal predicate SSOT stays in the registry (isTerminallyRevokedInTx) — the registry owns
   * the device table. This method uses THIS serializer's OWN canonical registry (audit R4 L5 review-3
   * finding P2), lending it the snapshot's transaction client, so terminal resolution can never come
   * from a different registry instance than the one that folds mutations.
   * @param {object} p
   * @param {string} p.accountIdentityPublicKeyB64
   * @param {string} p.deviceId
   * @returns {Promise<{epoch:number, revokedCertIds:string[], minValidIssuedAtMs:number, terminal:boolean}>}
   */
  async getDelegatedAuthoritySnapshot({ accountIdentityPublicKeyB64, deviceId } = {}) {
    const account = this.#norm(accountIdentityPublicKeyB64);
    if (!account) return { epoch: 0, revokedCertIds: [], minValidIssuedAtMs: 0, terminal: false };
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      try {
        const auth = await client.query(
          "SELECT epoch FROM account_authority WHERE account_identity = $1",
          [account],
        );
        const epoch = auth.rowCount > 0 ? Number(auth.rows[0].epoch) : 0;
        const rev = await this.#loadRevocationState(client, account);          // same snapshot
        const terminal = await this.#registry.isTerminallyRevokedInTx(client, account, deviceId); // same snapshot
        await client.query("COMMIT");
        return { epoch, revokedCertIds: rev.revokedCertIds, minValidIssuedAtMs: rev.minValidIssuedAtMs, terminal: terminal === true };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
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
   *   revoke → { revokedDeviceId, revokedCertId? }
   *     device.revoke AUTO-revokes the target registry row's own bound cert_id in the
   *     same tx (revocation completeness). A supplied revokedCertId is accepted ONLY when
   *     it EQUALS that bound cert (a redundant assertion); an arbitrary/mismatched cert is
   *     BAD_TARGET — revoking an unrelated cert is the standalone capability.revoke
   *     operation (AccountDeviceCapabilityRevokeV1), not this device-scoped mutation.
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

    // Audit R4 F3-remediation finding 3: minValidIssuedAtMs is NOT a supported target
    // field. No wire record carries it, so honoring it would be a dormant mass-revocation
    // lever; silently ignoring it could make a privileged caller believe a revocation
    // effect happened. Fail loud instead (no silent suppression).
    if (action === "device.revoke" && tgt.minValidIssuedAtMs !== undefined) {
      throw codedError(
        "device.revoke target does not accept minValidIssuedAtMs (arbitrary cutoff advancement is not supported)",
        "BAD_TARGET",
      );
    }

    // Audit R4 F3-remediation finding 2: a caller-supplied revokedCertId must be the EXACT
    // canonical rez:cap:<64-hex> shape (isCanonicalAccountCapabilityCertId), not a bare
    // rez:cap: prefix. Under Option A the cert to revoke is derived from the target's
    // registry binding; a supplied id is only ever a redundant assertion, but reject a
    // malformed one early (BAD_TARGET) rather than let it reach the equality check.
    if (action === "device.revoke" && tgt.revokedCertId != null) {
      if (!isCanonicalAccountCapabilityCertId(this.#norm(tgt.revokedCertId))) {
        throw codedError("revokedCertId must be a canonical rez:cap:<64-hex> id", "BAD_TARGET");
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
        // Audit R4 F3-remediation finding 3: the cutoff is never advanced by a mutation
        // (no wire record carries it), so it is immutable here — loaded and written back
        // unchanged so migration 0010's column is preserved byte-for-byte.
        const minValid = Number(cur.rows[0].min_valid_issued_at_ms);

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
        // The cert device.revoke will auto-revoke (Option A, finding 1): the target's OWN
        // registry-bound cert, resolved under the lock. null ⇒ nothing to revoke (a
        // never-enrolled / cert-NULL target). Carried into the fold below.
        let revokeBoundCert = null;
        if (action === "device.add") {
          isNoop = await this.#registry.isActiveAddNoopInTx(client, {
            accountIdentityPublicKeyB64: account,
            deviceId: this.#norm(tgt.deviceId),
            inboxId: this.#norm(tgt.inboxId),
            certId: tgt.certId == null ? null : this.#norm(tgt.certId),
          });
        } else {
          const revokedDeviceId = this.#norm(tgt.revokedDeviceId);
          const callerCert = tgt.revokedCertId == null ? null : this.#norm(tgt.revokedCertId);
          if (revokedDeviceId) {
            const rctx = await this.#registry.getRevokeContextInTx(client, {
              accountIdentityPublicKeyB64: account,
              deviceId: revokedDeviceId,
            });
            // Option A (finding 1): a supplied revokedCertId may ONLY equal the target's
            // OWN bound cert. Revoking an arbitrary/ancestor cert is the standalone
            // capability.revoke operation, not this device-scoped mutation — reject the
            // mismatch (this, not the removed require-capability.revoke gate, is what
            // closes the original escalation).
            if (callerCert !== null && callerCert !== rctx.boundCert) {
              throw codedError(
                "device.revoke may only revoke the target device's own bound cert; use capability.revoke for an arbitrary cert",
                "BAD_TARGET",
              );
            }
            revokeBoundCert = rctx.boundCert; // auto-revoke this device's own cert in the fold
            if (rctx.terminal) {
              // Already-terminal device: a no-op UNLESS its bound cert is not yet in the
              // revoked set — then the auto-revoke still advances authority by killing
              // that cert chain, so it must fold.
              let certAlreadyRevoked = true;
              if (rctx.boundCert) {
                const existsCert = await client.query(
                  "SELECT 1 FROM account_revoked_cert WHERE account_identity = $1 AND cert_id = $2",
                  [account, rctx.boundCert],
                );
                certAlreadyRevoked = existsCert.rowCount > 0;
              }
              isNoop = certAlreadyRevoked;
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
          // Leaf-3 req 5: if the revoked device holds a propagation lease, INVALIDATE it in THIS
          // same transaction — a revoked device loses its lease at once, not after the TTL. The
          // obligation returns to pending (immediately re-eligible for a surviving device).
          await this.#propagationOutbox.releaseOwnedInTx(client, account, revokedDeviceId);
          // Option A (F3-remediation finding 1): AUTO-revoke the target device's OWN bound
          // cert (resolved under the lock above) so device revocation is COMPLETE — the
          // leaf cert IS the device registration, so leaving it out of the revoked set
          // would let the future full-chain authority recheck still accept the "revoked"
          // device. A never-enrolled / cert-NULL target has no bound cert ⇒ nothing to
          // revoke here (its tombstone alone bars re-enrollment). Arbitrary cert revocation
          // is the separate capability.revoke path, not reachable through device.revoke.
          if (revokeBoundCert) {
            // Insert the target's OWN bound cert into the revoked set — NOT quota-gated
            // (finding 1): a bound cert is non-forgeable, at most one per device, so this
            // set is already bounded by the lifetime-device cap. Gating it would let a full
            // set roll back a real device's fail-close revoke. Idempotent on re-revoke.
            await client.query(
              "INSERT INTO account_revoked_cert (account_identity, cert_id, revoked_at_epoch)"
                + " VALUES ($1, $2, $3) ON CONFLICT (account_identity, cert_id) DO NOTHING",
              [account, revokeBoundCert, nextEpoch],
            );
          }
        }

        // (5) bump the monotonic epoch (the cutoff is written back unchanged — finding 3).
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
            // The journal records the cert actually revoked — the target's own bound cert
            // (Option A auto-revoke), not a caller-supplied string.
            action === "device.revoke" ? revokeBoundCert : null,
            JSON.stringify(result),
          ],
        );

        // (8) P1#3 — enqueue the authority-state propagation obligation ATOMICALLY, in THIS
        // transaction. Only reached on a real epoch-changing fold (no-op / stale / idempotent
        // replay all COMMIT-and-return above), so exactly one row per bumped epoch. A failure
        // here propagates to the outer ROLLBACK, so a committed fold can never lack its
        // publication obligation.
        await this.#propagationOutbox.enqueueInTx(client, {
          accountIdentityPublicKeyB64: account,
          epoch: nextEpoch,
          kind: "authority_state",
        });

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
