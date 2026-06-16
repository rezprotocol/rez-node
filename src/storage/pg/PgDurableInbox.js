import { DurableInbox, RevokedDeviceError, InboxCapExceededError } from "../DurableInbox.js";

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
 * `seq` is assigned from a per-inbox DURABLE high-water counter (mailbox_seq),
 * NOT from max(seq) of the prunable mailbox_events — so pruning an inbox to empty
 * never reuses a seq (which would silently lose mail for a device with an older
 * cursor). All seq assignment, cursor advance, AND pruning serialize on the same
 * per-inbox advisory xact lock, so a reader never skips an out-of-order commit
 * and prune never races append.
 *
 * DoS caps (maxEvents / maxDevices per inbox) preserve the bounds the transient
 * RMailbox enforced, now that ack advances a cursor instead of deleting.
 */
export class PgDurableInbox extends DurableInbox {
  #conn;
  #maxEvents;
  #maxDevices;

  /**
   * @param {{ connection: object, maxEvents?: number|null, maxDevices?: number|null }} opts
   */
  constructor({ connection, maxEvents = null, maxDevices = null } = {}) {
    super();
    if (!connection) {
      throw new Error("PgDurableInbox requires connection");
    }
    this.#conn = connection;
    this.#maxEvents = Number.isFinite(maxEvents) && maxEvents > 0 ? Math.floor(maxEvents) : null;
    this.#maxDevices = Number.isFinite(maxDevices) && maxDevices > 0 ? Math.floor(maxDevices) : null;
  }

  /**
   * Append ciphertext. Persist-first; idempotent on (inboxId, dedupeKey). seq is
   * monotonic per inbox forever. Throws InboxCapExceededError over the event cap.
   * @returns {Promise<{ seq: number, deduped: boolean }>}
   */
  async append(inboxId, body, { dedupeKey = null } = {}) {
    const id = String(inboxId);
    const buf = toBuffer(body);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [id]);

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

        if (this.#maxEvents != null) {
          const cnt = await client.query(
            "SELECT count(*)::bigint AS c FROM mailbox_events WHERE inbox_id = $1",
            [id],
          );
          if (Number(cnt.rows[0].c) >= this.#maxEvents) {
            await client.query("ROLLBACK");
            throw new InboxCapExceededError(id, this.#maxEvents, "events");
          }
        }

        // Durable, monotonic high-water seq — never reset by prune.
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
  }

  /**
   * Read events strictly after this device's cursor, in seq order. Fails closed
   * for a revoked device.
   * @returns {Promise<Array<{ seq: number, body: Uint8Array }>>}
   */
  async readAfterCursor(inboxId, deviceId, limit = 50) {
    const id = String(inboxId);
    const cur = await this.#conn.query(
      "SELECT last_seq, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
      [id, String(deviceId)],
    );
    let cursor = 0;
    if (cur.rowCount > 0) {
      if (cur.rows[0].revoked === true) {
        throw new RevokedDeviceError(id, String(deviceId));
      }
      cursor = Number(cur.rows[0].last_seq);
    }
    const res = await this.#conn.query(
      "SELECT seq, body FROM mailbox_events WHERE inbox_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
      [id, cursor, Math.max(1, Number(limit) || 50)],
    );
    return res.rows.map((r) => ({ seq: Number(r.seq), body: new Uint8Array(r.body) }));
  }

  /**
   * Advance this device's cursor. Monotonic (GREATEST) and bounded to the durable
   * high-water (never acks past what was ever appended). Fails closed for a
   * revoked device. Returns the ACTUAL stored cursor.
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
          "SELECT last_seq, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [id, dev],
        );
        if (cur.rowCount > 0 && cur.rows[0].revoked === true) {
          throw new RevokedDeviceError(id, dev);
        }
        const current = cur.rowCount > 0 ? Number(cur.rows[0].last_seq) : 0;
        const hw = await client.query("SELECT last_seq FROM mailbox_seq WHERE inbox_id = $1", [id]);
        const highWater = hw.rowCount > 0 ? Number(hw.rows[0].last_seq) : 0;
        // monotonic (>= current) AND bounded to the durable high-water mark.
        const target = Math.min(Math.max(Number(throughSeq), current), highWater);
        const upd = await client.query(
          `INSERT INTO device_cursors (inbox_id, device_id, last_seq, updated_at)
           VALUES ($1, $2, $3, now())
           ON CONFLICT (inbox_id, device_id)
             DO UPDATE SET last_seq = GREATEST(device_cursors.last_seq, EXCLUDED.last_seq),
                           updated_at = now()
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

  /** Register a device cursor (idempotent). Throws over the device cap. */
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
            await client.query("ROLLBACK");
            throw new InboxCapExceededError(id, this.#maxDevices, "devices");
          }
        }
        await client.query(
          "INSERT INTO device_cursors (inbox_id, device_id, last_seq, revoked) VALUES ($1, $2, 0, false)",
          [id, dev],
        );
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /** Home-enforced revocation: the device can no longer read or ack. */
  async revokeDevice(inboxId, deviceId) {
    const res = await this.#conn.query(
      "UPDATE device_cursors SET revoked = true, updated_at = now() WHERE inbox_id = $1 AND device_id = $2",
      [String(inboxId), String(deviceId)],
    );
    return res.rowCount > 0;
  }

  /**
   * Prune consumed events. Serializes on the per-inbox advisory lock (so it never
   * races append/cursorAck). Deletes at/below the slowest LIVE device's cursor
   * (live = non-revoked, and within staleGraceMs if given). The TTL backstop only
   * reclaims events when there are NO live devices (an abandoned inbox) — it never
   * deletes below a live device's cursor, so a live-but-quiet device can't lose
   * unconsumed mail.
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
          // No live devices → abandoned inbox; TTL reclaims old events safely.
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
