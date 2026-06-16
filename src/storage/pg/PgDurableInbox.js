import {
  DurableInbox,
  RevokedDeviceError,
  InboxCapExceededError,
  DeviceNotRegisteredError,
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
    if (!result.deduped && this.#onDeposit) {
      this.#onDeposit(id, result.seq);
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
    const cur = await this.#conn.query(
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
    const res = await this.#conn.query(
      "SELECT seq, body FROM mailbox_events WHERE inbox_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
      [id, cursor, Math.max(1, Number(limit) || 50)],
    );
    const rows = res.rows.map((r) => ({ seq: Number(r.seq), body: new Uint8Array(r.body) }));
    // A successful read by a registered, non-revoked device is proof of liveness,
    // so always refresh `updated_at` — the freshness clock that stale-device
    // pruning keys on. Otherwise a device that keeps reading but cannot ack yet
    // (it has unread mail it has not consumed) would age past staleGraceMs and
    // stop protecting its own unacked cursor, and prune could reclaim its unread.
    if (rows.length > 0) {
      const maxSeq = rows[rows.length - 1].seq;
      // Advance the delivered watermark (race-safe via GREATEST) and refresh seen.
      await this.#conn.query(
        "UPDATE device_cursors SET last_delivered = GREATEST(last_delivered, $3), updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
        [id, dev, maxSeq],
      );
    } else {
      await this.#conn.query(
        "UPDATE device_cursors SET updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
        [id, dev],
      );
    }
    return rows;
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

  /** Register a device cursor (idempotent). The ONLY way to create a device row. Throws over the device cap. */
  async registerDevice(inboxId, deviceId) {
    const id = String(inboxId);
    const dev = String(deviceId);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);
        const exists = await client.query(
          "SELECT 1 FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [id, dev],
        );
        if (exists.rowCount > 0) {
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
          "INSERT INTO device_cursors (inbox_id, device_id, last_seq, last_delivered, revoked) VALUES ($1, $2, 0, 0, false)",
          [id, dev],
        );
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
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
}
