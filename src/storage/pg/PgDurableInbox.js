import {
  DurableInbox,
  RevokedDeviceError,
  InboxCapExceededError,
  DeviceNotRegisteredError,
  DeviceKeyMismatchError,
} from "../DurableInbox.js";

function toBuffer(body) {
  if (body instanceof Uint8Array) {
    return Buffer.from(body);
  }
  if (Buffer.isBuffer(body)) {
    return body;
  }
  throw new Error("PgDurableInbox.append requires body as Uint8Array");
}

/**
 * Postgres durable home inbox (see DurableInbox).
 *
 * - `seq` comes from a per-inbox DURABLE high-water counter (mailbox_seq), never
 *   from max(seq) of the prunable mailbox_events — so prune-to-empty never reuses
 *   a seq (which would lose mail for a device with an older cursor).
 * - seq assignment, cursor advance, register, and prune all serialize on the same
 *   per-inbox advisory xact lock.
 * - DoS caps: per-inbox event count, total bytes, single-body bytes, device count.
 * - Device rows are created ONLY by registerDevice (capped); read/ack require a
 *   registered, non-revoked device and never implicitly create rows.
 * - cursorAck is delivered-bounded: a device may only advance to what
 *   readAfterCursor actually delivered to it (last_delivered), not the global max.
 * - Revocation is home-enforced on read, ack, AND device-targeted append.
 */
export class PgDurableInbox extends DurableInbox {
  #conn;
  #maxEvents;
  #maxBytes;
  #maxBodyBytes;
  #maxDevices;
  #onDeposit;

  /**
   * @param {{ connection: object, maxEvents?: number|null, maxBytes?: number|null,
   *           maxBodyBytes?: number|null, maxDevices?: number|null }} opts
   */
  constructor({ connection, maxEvents = null, maxBytes = null, maxBodyBytes = null, maxDevices = null } = {}) {
    super();
    if (!connection) {
      throw new Error("PgDurableInbox requires connection");
    }
    this.#conn = connection;
    this.#maxEvents = this.#normCap(maxEvents);
    this.#maxBytes = this.#normCap(maxBytes);
    this.#maxBodyBytes = this.#normCap(maxBodyBytes);
    this.#maxDevices = this.#normCap(maxDevices);
    this.#onDeposit = null;
  }

  #normCap(v) {
    return Number.isFinite(v) && v > 0 ? Math.floor(v) : null;
  }

  /**
   * Register the persist-then-NOTIFY hook. Fired once per FRESH append (not on a
   * dedupe hit), AFTER the row commits — so the live owner's EVT_MAILBOX_DEPOSITED
   * never races ahead of the durable write (D4 persist-first). Mirrors the
   * RMailbox.setOnDeposit contract so WsGatewayServer can wire either store.
   * @param {(inboxId: string, seq: number) => void} cb
   */
  setOnDeposit(cb) {
    this.#onDeposit = typeof cb === "function" ? cb : null;
  }

  /**
   * Random-access read of a single event by seq (NOT cursor-affecting) — used by
   * the live-notify path to fetch the just-appended ciphertext for broadcast.
   * @returns {Promise<{ seq: number, body: Uint8Array } | null>}
   */
  async getEvent(inboxId, seq) {
    const res = await this.#conn.query(
      "SELECT seq, body FROM mailbox_events WHERE inbox_id = $1 AND seq = $2",
      [String(inboxId), Number(seq)],
    );
    if (res.rowCount === 0) return null;
    return { seq: Number(res.rows[0].seq), body: new Uint8Array(res.rows[0].body) };
  }

  /**
   * Append ciphertext. Persist-first; idempotent on (inboxId, dedupeKey). seq is
   * monotonic per inbox forever. Enforces event/byte/body caps. If `deviceId` is
   * given (a device-targeted deposit), rejects a revoked device binding.
   * @returns {Promise<{ seq: number, deduped: boolean }>}
   */
  async append(inboxId, body, { dedupeKey = null, deviceId = null } = {}) {
    const id = String(inboxId);
    const buf = toBuffer(body);
    if (this.#maxBodyBytes != null && buf.length > this.#maxBodyBytes) {
      throw new InboxCapExceededError(id, this.#maxBodyBytes, "bodyBytes");
    }
    const result = await this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);

        // Home-enforced revocation for device-targeted deposits.
        if (deviceId != null) {
          const dev = await client.query(
            "SELECT revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
            [id, String(deviceId)],
          );
          if (dev.rowCount > 0 && dev.rows[0].revoked === true) {
            // The single catch below owns ROLLBACK — a ROLLBACK here too would
            // double-rollback (and, if it threw, shadow this typed error).
            throw new RevokedDeviceError(id, String(deviceId));
          }
        }

        if (dedupeKey) {
          const dup = await client.query(
            "SELECT seq FROM mailbox_events WHERE inbox_id = $1 AND dedupe_key = $2",
            [id, String(dedupeKey)],
          );
          if (dup.rowCount > 0) {
            await client.query("COMMIT");
            return { seq: Number(dup.rows[0].seq), deduped: true };
          }
        }

        if (this.#maxEvents != null || this.#maxBytes != null) {
          const agg = await client.query(
            "SELECT count(*)::bigint AS c, coalesce(sum(length(body)), 0)::bigint AS b FROM mailbox_events WHERE inbox_id = $1",
            [id],
          );
          if (this.#maxEvents != null && Number(agg.rows[0].c) >= this.#maxEvents) {
            throw new InboxCapExceededError(id, this.#maxEvents, "events");
          }
          if (this.#maxBytes != null && Number(agg.rows[0].b) + buf.length > this.#maxBytes) {
            throw new InboxCapExceededError(id, this.#maxBytes, "bytes");
          }
        }

        const seqRow = await client.query(
          `INSERT INTO mailbox_seq (inbox_id, last_seq) VALUES ($1, 1)
           ON CONFLICT (inbox_id) DO UPDATE SET last_seq = mailbox_seq.last_seq + 1
           RETURNING last_seq`,
          [id],
        );
        const seq = Number(seqRow.rows[0].last_seq);
        await client.query(
          "INSERT INTO mailbox_events (inbox_id, seq, body, dedupe_key) VALUES ($1, $2, $3, $4)",
          [id, seq, buf, dedupeKey ? String(dedupeKey) : null],
        );
        await client.query("COMMIT");
        return { seq, deduped: false };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
    // Persist-then-notify (D4): fire the deposit hook ONLY after the row has
    // committed, and only for a FRESH append — a dedupe hit must not re-notify.
    // The notify is best-effort and MUST NOT reject append(): the row is already
    // durably committed, so a throwing hook would make a stored message look
    // failed and invite a duplicate (a no-dedupe retry) or a silent drop (a
    // dedupe retry that no longer re-notifies). Mirror RMailbox: log + swallow.
    if (!result.deduped && this.#onDeposit) {
      try {
        this.#onDeposit(id, result.seq);
      } catch (err) {
        console.error(
          "[PgDurableInbox] onDeposit hook threw after commit (inbox=" + id
            + " seq=" + result.seq + "); message is stored, notify skipped: "
            + (err && err.message ? err.message : err),
        );
      }
    }
    return result;
  }

  /**
   * Read events strictly after this device's cursor, in seq order. Requires a
   * registered, non-revoked device. Advances the device's delivered watermark.
   * @returns {Promise<Array<{ seq: number, body: Uint8Array }>>}
   */
  async readAfterCursor(inboxId, deviceId, limit = 50) {
    const id = String(inboxId);
    const dev = String(deviceId);
    // Serialized per inbox (advisory xact lock) exactly like readUndelivered /
    // cursorAck / prune. The read + the last_delivered/updated_at advance MUST be
    // one atomic unit: an unlocked read-then-update lets the `updated_at` refresh
    // straddle a concurrent prune, so the device could be judged stale and have
    // its just-read (still-unacked) events reclaimed in the gap.
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);
        const cur = await client.query(
          "SELECT last_seq, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [id, dev],
        );
        if (cur.rowCount === 0) {
          throw new DeviceNotRegisteredError(id, dev);
        }
        if (cur.rows[0].revoked === true) {
          throw new RevokedDeviceError(id, dev);
        }
        const cursor = Number(cur.rows[0].last_seq);
        const res = await client.query(
          "SELECT seq, body FROM mailbox_events WHERE inbox_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
          [id, cursor, Math.max(1, Number(limit) || 50)],
        );
        const rows = res.rows.map((r) => ({ seq: Number(r.seq), body: new Uint8Array(r.body) }));
        // A successful read by a registered, non-revoked device is proof of
        // liveness, so always refresh `updated_at` — the freshness clock that
        // stale-device pruning keys on. Otherwise a device that keeps reading but
        // cannot ack yet (unread mail it has not consumed) would age past
        // staleGraceMs and stop protecting its own unacked cursor.
        if (rows.length > 0) {
          const maxSeq = rows[rows.length - 1].seq;
          // Advance the delivered watermark (race-safe via GREATEST) + refresh seen.
          await client.query(
            "UPDATE device_cursors SET last_delivered = GREATEST(last_delivered, $3), updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
            [id, dev, maxSeq],
          );
        } else {
          await client.query(
            "UPDATE device_cursors SET updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
            [id, dev],
          );
        }
        await client.query("COMMIT");
        return rows;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Read events strictly after this device's DELIVERED watermark (last_delivered)
   * and advance it past them — the LIVE push path (a cross-node deposit ping).
   *
   * Distinct from readAfterCursor (which reads from the CONSUMED cursor for
   * reconnect catch-up / redeliver-unconsumed): this delivers each new event to
   * the device EXACTLY ONCE. A repeated liveness ping with no new deposits returns
   * nothing, so an un-acked / poison event can never pin the read point and
   * amplify duplicate pushes on every later deposit. cursorAck remains bounded by
   * last_delivered, so a live-pushed event is still ackable once consumed.
   * Serialized per inbox (advisory xact lock) so concurrent pings can't double-read.
   * Requires a registered, non-revoked device.
   * @returns {Promise<Array<{ seq: number, body: Uint8Array }>>}
   */
  async readUndelivered(inboxId, deviceId, limit = 100) {
    const id = String(inboxId);
    const dev = String(deviceId);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);
        const cur = await client.query(
          "SELECT last_delivered, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [id, dev],
        );
        if (cur.rowCount === 0) {
          throw new DeviceNotRegisteredError(id, dev);
        }
        if (cur.rows[0].revoked === true) {
          throw new RevokedDeviceError(id, dev);
        }
        const delivered = Number(cur.rows[0].last_delivered);
        const res = await client.query(
          "SELECT seq, body FROM mailbox_events WHERE inbox_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
          [id, delivered, Math.max(1, Number(limit) || 100)],
        );
        const rows = res.rows.map((r) => ({ seq: Number(r.seq), body: new Uint8Array(r.body) }));
        if (rows.length > 0) {
          const maxSeq = rows[rows.length - 1].seq;
          await client.query(
            "UPDATE device_cursors SET last_delivered = GREATEST(last_delivered, $3), updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
            [id, dev, maxSeq],
          );
        }
        await client.query("COMMIT");
        return rows;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Advance this device's cursor. Requires a registered, non-revoked device.
   * Monotonic, and bounded to what was actually DELIVERED to this device
   * (last_delivered) — never the inbox's global max. Returns the stored cursor.
   * @returns {Promise<{ lastSeq: number }>}
   */
  async cursorAck(inboxId, deviceId, throughSeq) {
    const id = String(inboxId);
    const dev = String(deviceId);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);
        const cur = await client.query(
          "SELECT last_seq, revoked, last_delivered FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [id, dev],
        );
        if (cur.rowCount === 0) {
          throw new DeviceNotRegisteredError(id, dev);
        }
        if (cur.rows[0].revoked === true) {
          throw new RevokedDeviceError(id, dev);
        }
        const current = Number(cur.rows[0].last_seq);
        const delivered = Number(cur.rows[0].last_delivered);
        // monotonic (>= current) AND bounded to delivered (not the global max).
        const target = Math.min(Math.max(Number(throughSeq), current), delivered);
        const upd = await client.query(
          `UPDATE device_cursors
             SET last_seq = GREATEST(last_seq, $3), updated_at = now()
           WHERE inbox_id = $1 AND device_id = $2
           RETURNING last_seq`,
          [id, dev, target],
        );
        await client.query("COMMIT");
        return { lastSeq: Number(upd.rows[0].last_seq) };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Register a device cursor (idempotent). The ONLY way to create a device row.
   * Throws over the device cap.
   *
   * `devicePublicKeyB64` (optional) is the PROVEN device key behind a
   * device.bind — the home's persisted copy of the verified DeviceInboxBindingV1.
   * On a fresh row it is stored alongside the cursor; on an existing row it
   * backfills a previously-null key and is otherwise a no-op, EXCEPT a non-null
   * stored key that differs throws DeviceKeyMismatchError (deviceId is
   * self-certifying, so a differing key for the same deviceId is a substitution
   * attempt). The legacy single-device claim path passes no key (null) and is
   * unchanged.
   */
  async registerDevice(inboxId, deviceId, { devicePublicKeyB64 = null } = {}) {
    const id = String(inboxId);
    const dev = String(deviceId);
    const pub = typeof devicePublicKeyB64 === "string" && devicePublicKeyB64.length > 0
      ? devicePublicKeyB64
      : null;
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);
        const exists = await client.query(
          "SELECT device_public_key FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [id, dev],
        );
        if (exists.rowCount > 0) {
          const stored = exists.rows[0].device_public_key;
          if (pub != null) {
            if (stored != null && stored !== pub) {
              // The catch below owns ROLLBACK (avoid double-rollback / shadowing).
              throw new DeviceKeyMismatchError(id, dev);
            }
            if (stored == null) {
              // Backfill: a row registered by the legacy claim path is now being
              // proven with its device key (the unification the plan describes).
              await client.query(
                "UPDATE device_cursors SET device_public_key = $3, updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
                [id, dev, pub],
              );
            }
          }
          await client.query("COMMIT");
          return; // idempotent
        }
        if (this.#maxDevices != null) {
          const cnt = await client.query(
            "SELECT count(*)::bigint AS c FROM device_cursors WHERE inbox_id = $1 AND revoked = false",
            [id],
          );
          if (Number(cnt.rows[0].c) >= this.#maxDevices) {
            // The catch below owns ROLLBACK (avoid double-rollback / error shadowing).
            throw new InboxCapExceededError(id, this.#maxDevices, "devices");
          }
        }
        await client.query(
          "INSERT INTO device_cursors (inbox_id, device_id, last_seq, last_delivered, revoked, device_public_key) VALUES ($1, $2, 0, 0, false, $3)",
          [id, dev, pub],
        );
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Read a device cursor row (the home's view of a registered device). Returns
   * null when the device is not registered. `devicePublicKeyB64` is the proven
   * bound key (null for a legacy claim-path device).
   * @returns {Promise<{ deviceId: string, devicePublicKeyB64: string|null, lastSeq: number, lastDelivered: number, revoked: boolean } | null>}
   */
  async getDevice(inboxId, deviceId) {
    const res = await this.#conn.query(
      "SELECT device_id, device_public_key, last_seq, last_delivered, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
      [String(inboxId), String(deviceId)],
    );
    if (res.rowCount === 0) return null;
    const row = res.rows[0];
    return {
      deviceId: row.device_id,
      devicePublicKeyB64: row.device_public_key == null ? null : String(row.device_public_key),
      lastSeq: Number(row.last_seq),
      lastDelivered: Number(row.last_delivered),
      revoked: row.revoked === true,
    };
  }

  /** Home-enforced revocation: the device can no longer read, ack, or be deposited to. */
  async revokeDevice(inboxId, deviceId) {
    const res = await this.#conn.query(
      "UPDATE device_cursors SET revoked = true, updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
      [String(inboxId), String(deviceId)],
    );
    return res.rowCount > 0;
  }

  /**
   * Prune consumed events under the per-inbox advisory lock. Deletes at/below the
   * slowest LIVE device's cursor. The TTL backstop only reclaims events when there
   * are NO live devices (abandoned inbox); it never deletes below a live cursor.
   * @returns {Promise<{ deleted: number, watermark: number|null }>}
   */
  async prune(inboxId, { ttlMs = null, staleGraceMs = null } = {}) {
    const id = String(inboxId);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);

        const liveFilter = staleGraceMs != null
          ? "revoked = false AND updated_at > now() - ($2::bigint * interval '1 millisecond')"
          : "revoked = false";
        const liveParams = staleGraceMs != null ? [id, Number(staleGraceMs)] : [id];
        const devs = await client.query(
          `SELECT min(last_seq) AS m, count(*)::bigint AS c FROM device_cursors WHERE inbox_id = $1 AND ${liveFilter}`,
          liveParams,
        );
        const liveCount = Number(devs.rows[0].c);

        let deleted = 0;
        let watermark = null;

        if (liveCount > 0) {
          watermark = Number(devs.rows[0].m);
          const r = await client.query(
            "DELETE FROM mailbox_events WHERE inbox_id = $1 AND seq <= $2",
            [id, watermark],
          );
          deleted += r.rowCount;
        } else if (ttlMs != null) {
          const r = await client.query(
            "DELETE FROM mailbox_events WHERE inbox_id = $1 AND created_at < now() - ($2::bigint * interval '1 millisecond')",
            [id, Number(ttlMs)],
          );
          deleted += r.rowCount;
        }

        await client.query("COMMIT");
        return { deleted, watermark };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /**
   * Prune EVERY inbox that currently holds events. Enumerates distinct inbox ids
   * from mailbox_events (only inboxes with stored rows can be over-cap or hold
   * consumed events) and prunes each under its own advisory lock via `prune`.
   * A failure on one inbox does not abort the sweep — the inbox id + error are
   * collected and rethrown after the rest complete, so one wedged inbox cannot
   * starve maintenance of the others.
   * @returns {Promise<{ inboxesSwept: number, deleted: number }>}
   */
  async pruneAll({ ttlMs = null, staleGraceMs = null } = {}) {
    const res = await this.#conn.query("SELECT DISTINCT inbox_id FROM mailbox_events");
    const ids = res.rows.map((r) => String(r.inbox_id));
    let deleted = 0;
    let inboxesSwept = 0;
    const failures = [];
    for (const id of ids) {
      try {
        const r = await this.prune(id, { ttlMs, staleGraceMs });
        deleted += r.deleted;
        inboxesSwept += 1;
      } catch (err) {
        failures.push(id + ": " + (err && err.message ? err.message : String(err)));
      }
    }
    if (failures.length > 0) {
      const err = new Error("PgDurableInbox.pruneAll: " + failures.length + " inbox(es) failed to prune: " + failures.join("; "));
      err.inboxesSwept = inboxesSwept;
      err.deleted = deleted;
      throw err;
    }
    return { inboxesSwept, deleted };
  }
}
