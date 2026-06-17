import { createHash } from "node:crypto";
import { decodeOuterPacket } from "@rezprotocol/core";

/**
 * Durable-home inbox decorator over the existing `inboxStore` interface.
 *
 * On a pg-cluster node, owner-hosted ("home") inboxes are the cluster's system
 * of record and MUST survive a client reconnecting to a DIFFERENT node, so their
 * deposits go to the durable append-log (PgDurableInbox: per-inbox monotonic seq +
 * per-device cursors). Every OTHER inbox is a WAN-transient buffer for
 * cross-operator forwarding and stays on the node-local RMailbox
 * (delete-after-delivery), unchanged (plan D1).
 *
 * Routing is per-inbox via `isHostedHere(inboxId)`:
 *   - hosted-here -> durable append-log (seq-addressed, persist-then-notify)
 *   - otherwise   -> delegate verbatim to the wrapped RMailbox
 *
 * Scope: this decorator owns ONLY the write + live-notify + single-event-fetch
 * surface that every ingress path and the WsGatewayServer notify hook already
 * funnel through. Device-aware catch-up reads (`readAfterCursor`) and cursor
 * advance (`cursorAck`) are resolved at the MailboxHandler layer, where the
 * authenticated session device is known — the plain inboxStore interface carries
 * no device, so it cannot (and must not) serve them. `list`/`ack` here therefore
 * only ever run for NON-hosted (transient) inboxes; a home client never deletes,
 * it advances a per-device cursor via mailbox.cursorAck.
 */
export class DurableHomeInboxStore {
  #rmailbox;
  #durable;
  #isHostedHere;
  #onDeposit;

  /**
   * @param {{ rmailbox: object, durableInbox: import("./DurableInbox.js").DurableInbox,
   *           isHostedHere: (inboxId: string) => (boolean | Promise<boolean>) }} opts
   *   isHostedHere may be async (the cluster predicate is a Pg claim-registry
   *   lookup); every routing method awaits it.
   */
  constructor({ rmailbox, durableInbox, isHostedHere } = {}) {
    if (!rmailbox) throw new Error("DurableHomeInboxStore requires rmailbox");
    if (!durableInbox) throw new Error("DurableHomeInboxStore requires durableInbox");
    if (typeof isHostedHere !== "function") {
      throw new Error("DurableHomeInboxStore requires isHostedHere(inboxId)");
    }
    this.#rmailbox = rmailbox;
    this.#durable = durableInbox;
    this.#isHostedHere = isHostedHere;
    this.#onDeposit = null;

    // Fan BOTH backends' deposit notifications into the single registered hook,
    // normalizing the durable seq to a string eventId so the generic
    // WsGatewayServer notify path (inboxStore.fetch(inboxId, eventId)) stays
    // backend-agnostic. The durable hook fires persist-first (after the row
    // commits, plan D4); a dedupe hit on the durable path does not re-notify.
    this.#durable.setOnDeposit((inboxId, seq) => this.#fire(inboxId, String(seq)));
    this.#rmailbox.setOnDeposit((inboxId, eventId) => this.#fire(inboxId, eventId));
  }

  #fire(inboxId, eventId) {
    if (!this.#onDeposit) return;
    this.#onDeposit(inboxId, eventId);
  }

  setOnDeposit(cb) {
    this.#onDeposit = typeof cb === "function" ? cb : null;
  }

  /**
   * Route a wire deposit. Hosted-here -> durable append-log (idempotent on a
   * content hash so a re-delivered identical ciphertext collapses to the same
   * seq); otherwise -> the transient RMailbox. Returns the eventId (the durable
   * seq as a string for the home path; the RMailbox eventId otherwise).
   */
  async depositFromWire(mailboxId, wireBytes) {
    if (!(await this.#isHostedHere(mailboxId))) {
      return this.#rmailbox.depositFromWire(mailboxId, wireBytes);
    }
    const dedupeKey = this.#contentDedupeKey(wireBytes);
    const result = await this.#durable.append(mailboxId, wireBytes, { dedupeKey });
    return String(result.seq);
  }

  /**
   * Typed-record deposit (OuterPacketRecord / AppDepositRecord). In practice the
   * ingress paths only PROBE for this method then call depositFromWire, but it is
   * implemented for completeness: hosted-here -> durable append of the record's
   * wire bytes (content-hash idempotent); otherwise -> RMailbox.deposit verbatim.
   */
  async deposit(mailboxId, record) {
    if (!(await this.#isHostedHere(mailboxId))) {
      return this.#rmailbox.deposit(mailboxId, record);
    }
    if (!record || typeof record.toBytes !== "function") {
      throw new Error("DurableHomeInboxStore.deposit requires a record with toBytes()");
    }
    const wireBytes = record.toBytes();
    const dedupeKey = this.#contentDedupeKey(wireBytes);
    const result = await this.#durable.append(mailboxId, wireBytes, { dedupeKey });
    return String(result.seq);
  }

  /**
   * Fetch a single stored event. Hosted-here -> random-access by seq (the
   * eventId IS the stringified seq); otherwise -> RMailbox. Shape is
   * RMailbox-compatible ({ objectId, bytes, metadata, createdAt }) plus an
   * explicit `seq` (null for the transient path) so the live-notify path can
   * carry it onto EVT_MAILBOX_DEPOSITED for client-side seq dedupe.
   */
  async fetch(mailboxId, eventId) {
    if (!(await this.#isHostedHere(mailboxId))) {
      return this.#rmailbox.fetch(mailboxId, eventId);
    }
    const seq = Number(eventId);
    if (!Number.isInteger(seq) || seq < 0) return null;
    const evt = await this.#durable.getEvent(mailboxId, seq);
    if (!evt) return null;
    return { objectId: null, bytes: evt.body, metadata: {}, createdAt: null, seq: evt.seq };
  }

  /**
   * Catch-up listing for NON-hosted (transient) inboxes only. Hosted-here
   * catch-up is device-aware (readAfterCursor) and resolved at the handler with
   * the session device — it never reaches this method.
   */
  async list(mailboxId, opts) {
    return this.#rmailbox.list(mailboxId, opts);
  }

  /**
   * Ack/delete for NON-hosted (transient) inboxes only. Hosted-here clients
   * advance a per-device cursor via mailbox.cursorAck (never delete), so this
   * never runs for a home inbox.
   */
  async ack(mailboxId, eventId) {
    return this.#rmailbox.ack(mailboxId, eventId);
  }

  #contentDedupeKey(wireBytes) {
    // Hash the DECODED outer-packet body (pre-decrypt, home-independent) so a
    // re-delivery of the same ciphertext — e.g. a sender's outbound-queue retry
    // landing again — collapses to the existing seq instead of minting a dup the
    // client cannot dedupe (it dedupes on seq, and two appends carry two seqs).
    // A deposit that is not a framed outer packet falls back to hashing the raw
    // bytes: still a stable key, no data lost (the raw wireBytes are stored
    // verbatim regardless). Home deposits are always framed outer packets.
    let body = wireBytes;
    try {
      body = decodeOuterPacket(wireBytes).bodyBytesView;
    } catch {
      // Expected, benign for a non-outer-packet deposit: select the raw bytes as
      // the hash input rather than failing the deposit (body stays = wireBytes).
      body = wireBytes;
    }
    return createHash("sha256").update(body).digest("hex");
  }
}
