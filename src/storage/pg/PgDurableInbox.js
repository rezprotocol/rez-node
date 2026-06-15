import { DurableInbox, RevokedDeviceError } from "../DurableInbox.js";

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
 * Per-inbox `seq` is assigned gap-free under a per-inbox transaction advisory
 * lock, so concurrent appends to one inbox commit in seq order and a reader
 * never skips an event (the sequence-gap hazard). Cross-inbox concurrency is
 * unaffected — the lock key is hashtext(inboxId).
 */
export class PgDurableInbox extends DurableInbox {
  #conn;

  /**
   * @param {{ connection: import("./PgConnection.js").PgConnection }} opts
   */
  constructor({ connection } = {}) {
    super();
    if (!connection) {
      throw new Error("PgDurableInbox requires connection");
    }
    this.#conn = connection;
  }

  /**
   * Append ciphertext to the inbox log. Persist-first: the row commits before
   * any notify. Idempotent on (inboxId, dedupeKey) — a re-delivery with the same
   * ciphertext hash returns the existing seq instead of double-appending.
   * @returns {Promise<{ seq: number, deduped: boolean }>}
   */
  async append(inboxId, body, { dedupeKey = null } = {}) {
    const buf = toBuffer(body);
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [String(inboxId)]);

        if (dedupeKey) {
          const dup = await client.query(
            "SELECT seq FROM mailbox_events WHERE inbox_id = $1 AND dedupe_key = $2",
            [String(inboxId), String(dedupeKey)],
          );
          if (dup.rowCount > 0) {
            await client.query("COMMIT");
            return { seq: Number(dup.rows[0].seq), deduped: true };
          }
        }

        const next = await client.query(
          "SELECT coalesce(max(seq), 0) + 1 AS s FROM mailbox_events WHERE inbox_id = $1",
          [String(inboxId)],
        );
        const seq = Number(next.rows[0].s);
        await client.query(
          "INSERT INTO mailbox_events (inbox_id, seq, body, dedupe_key) VALUES ($1, $2, $3, $4)",
          [String(inboxId), seq, buf, dedupeKey ? String(dedupeKey) : null],
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
    const cur = await this.#conn.query(
      "SELECT last_seq, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
      [String(inboxId), String(deviceId)],
    );
    let cursor = 0;
    if (cur.rowCount > 0) {
      if (cur.rows[0].revoked === true) {
        throw new RevokedDeviceError(String(inboxId), String(deviceId));
      }
      cursor = Number(cur.rows[0].last_seq);
    }
    const res = await this.#conn.query(
      "SELECT seq, body FROM mailbox_events WHERE inbox_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
      [String(inboxId), cursor, Math.max(1, Number(limit) || 50)],
    );
    return res.rows.map((r) => ({ seq: Number(r.seq), body: new Uint8Array(r.body) }));
  }

  /**
   * Advance this device's cursor. Monotonic (never regresses) and bounded to the
   * max existing seq (never acks ahead into rows that don't exist). Fails closed
   * for a revoked device. Returns the effective cursor.
   * @returns {Promise<{ lastSeq: number }>}
   */
  async cursorAck(inboxId, deviceId, throughSeq) {
    return this.#conn.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [String(inboxId)]);
        const cur = await client.query(
          "SELECT last_seq, revoked FROM device_cursors WHERE inbox_id = $1 AND device_id = $2",
          [String(inboxId), String(deviceId)],
        );
        if (cur.rowCount > 0 && cur.rows[0].revoked === true) {
          throw new RevokedDeviceError(String(inboxId), String(deviceId));
        }
        const current = cur.rowCount > 0 ? Number(cur.rows[0].last_seq) : 0;
        const maxRes = await client.query(
          "SELECT coalesce(max(seq), 0) AS m FROM mailbox_events WHERE inbox_id = $1",
          [String(inboxId)],
        );
        const maxSeq = Number(maxRes.rows[0].m);
        // monotonic (>= current) AND bounded (<= max delivered)
        const target = Math.min(Math.max(Number(throughSeq), current), maxSeq);
        await client.query(
          `INSERT INTO device_cursors (inbox_id, device_id, last_seq, updated_at)
           VALUES ($1, $2, $3, now())
           ON CONFLICT (inbox_id, device_id)
             DO UPDATE SET last_seq = GREATEST(device_cursors.last_seq, EXCLUDED.last_seq),
                           updated_at = now()`,
          [String(inboxId), String(deviceId), target],
        );
        await client.query("COMMIT");
        return { lastSeq: target };
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });
  }

  /** Register a device cursor (idempotent). */
  async registerDevice(inboxId, deviceId) {
    await this.#conn.query(
      `INSERT INTO device_cursors (inbox_id, device_id, last_seq, revoked)
       VALUES ($1, $2, 0, false)
       ON CONFLICT (inbox_id, device_id) DO NOTHING`,
      [String(inboxId), String(deviceId)],
    );
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
   * Prune events at/below the slowest live device's cursor, plus an optional TTL
   * backstop. Devices that are revoked, or stale past `staleGraceMs` (no cursor
   * activity), are EXCLUDED from the watermark so an abandoned zeroed cursor
   * can't pin the log forever.
   * @returns {Promise<{ deleted: number, watermark: number|null }>}
   */
  async prune(inboxId, { ttlMs = null, staleGraceMs = null } = {}) {
    let deleted = 0;
    let watermark = null;

    const liveFilter = staleGraceMs != null
      ? "revoked = false AND updated_at > now() - ($2::bigint * interval '1 millisecond')"
      : "revoked = false";
    const liveParams = staleGraceMs != null
      ? [String(inboxId), Number(staleGraceMs)]
      : [String(inboxId)];

    const devs = await this.#conn.query(
      `SELECT min(last_seq) AS m, count(*) AS c FROM device_cursors WHERE inbox_id = $1 AND ${liveFilter}`,
      liveParams,
    );
    if (Number(devs.rows[0].c) > 0) {
      watermark = Number(devs.rows[0].m);
      const r = await this.#conn.query(
        "DELETE FROM mailbox_events WHERE inbox_id = $1 AND seq <= $2",
        [String(inboxId), watermark],
      );
      deleted += r.rowCount;
    }

    if (ttlMs != null) {
      const r2 = await this.#conn.query(
        "DELETE FROM mailbox_events WHERE inbox_id = $1 AND created_at < now() - ($2::bigint * interval '1 millisecond')",
        [String(inboxId), Number(ttlMs)],
      );
      deleted += r2.rowCount;
    }

    return { deleted, watermark };
  }
}
