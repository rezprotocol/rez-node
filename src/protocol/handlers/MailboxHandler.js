import { REZ_CONTRACT_TYPES, encodeOuterPacket, decodeOuterPacket, RCapability } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

/**
 * Decode `body.capChain` (array of plain RCapability JSON objects) into
 * RCapability instances. Returns null when no chain is presented, or the
 * decoded array. Throws on malformed entries — the handler catches and
 * returns BAD_REQUEST.
 */
function decodeCapChain(body) {
  if (!body || !Array.isArray(body.capChain) || body.capChain.length === 0) return null;
  return body.capChain.map((entry) => new RCapability(entry));
}

/**
 * Base64 of the DECODED outer-packet body — the exact bytes the live-push path
 * surfaces (RelayDepositRouter sends decodeOuterPacket(packet).bodyBytesView), so
 * durable catch-up and live delivery hand the client identical ciphertext. A
 * stored value that is not a framed outer packet is returned unchanged
 * (defensive; durable home deposits are always outer packets). Mirrors the decode
 * in handleFetch.
 */
function durableBodyB64(bytes) {
  if (!(bytes instanceof Uint8Array)) return null;
  let body = bytes;
  try {
    body = decodeOuterPacket(bytes).bodyBytesView;
  } catch {
    body = bytes;
  }
  return Buffer.from(body).toString("base64");
}

export class MailboxHandler {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
  }

  /**
   * Accept a deposit from an authenticated WS session and hand off to the
   * routing layer. ALL deposits — local-hosted-inbox OR remote — go through
   * `gatewayLoop.sendToInbox`. The gateway's internal "local route in
   * routeTable" branch turns into `inboxStore.depositFromWire`, which is the
   * same convergence point a cross-relay onion-final-hop hits.
   *
   * Deposit-policy enforcement (docs/SECURITY_AUDIT.md HIGH-1):
   *   - If the destination inbox has a claimant-signed DepositPolicyV1
   *     stored, reject deposits from blocked depositor pubkeys (and reject
   *     deposits from senders not on the allowlist when one is set).
   *   - Independent of policy, enforce a per-(depositor, inbox) sliding
   *     window rate limit to bound storage-exhaustion attacks.
   *   - Anonymous-by-default is preserved when no policy is published.
   *
   * The depositor is identified by the WS session's owner pubkey
   * (`ctx.ownerPublicKeyB64`) — already proven via session.authenticate.
   */
  async handleDeposit(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const { mailboxId, ciphertextB64 } = body;
    if (typeof mailboxId !== "string" || mailboxId.trim().length === 0) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "mailboxId required", retryable: false });
      return;
    }
    const targetInboxId = mailboxId.trim();
    const depositorPubkeyB64 = typeof this.#ctx.ownerPublicKeyB64 === "string"
      ? this.#ctx.ownerPublicKeyB64.trim()
      : "";

    // Policy enforcement BEFORE any work that touches storage/gateway. If
    // the policy says no, the deposit doesn't get to be expensive.
    const policyStore = this.#ctx.runtime && this.#ctx.runtime.depositPolicyStore;
    if (policyStore && typeof policyStore.get === "function") {
      const policy = policyStore.get(targetInboxId);
      if (policy && policy.isDepositorBlocked(depositorPubkeyB64)) {
        this.#ctx.sendError({
          id: requestId,
          code: "DEPOSIT_BLOCKED",
          message: "depositor blocked by inbox policy",
          retryable: false,
        });
        return;
      }
    }

    const rateLimitStore = this.#ctx.runtime && this.#ctx.runtime.depositRateLimitStore;
    if (rateLimitStore && typeof rateLimitStore.record === "function") {
      // Gate on BOTH (depositor pubkey, inbox) and (source IP, inbox).
      // The IP-keyed cap (docs/SECURITY_AUDIT.md LOW-4) survives
      // session-auth keypair rotation, so an attacker can't escape the
      // policy blocklist by reconnecting with a fresh `session.hello`
      // pubkey: their IP still hits the same per-inbox cap.
      const allowed = await rateLimitStore.record({
        depositorPubkeyB64,
        sourceIp: this.#ctx.peerIp,
        mailboxId: targetInboxId,
        nowMs: Date.now(),
      });
      if (!allowed) {
        this.#ctx.sendError({
          id: requestId,
          code: "RATE_LIMITED",
          message: "deposit rate limit exceeded",
          retryable: true,
        });
        return;
      }
    }

    const gatewayLoop = this.#ctx.runtime.gatewayLoop;
    if (!gatewayLoop || typeof gatewayLoop.sendToInbox !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "gateway routing unavailable", retryable: false });
      return;
    }

    const ciphertextBytes = typeof ciphertextB64 === "string"
      ? new Uint8Array(Buffer.from(ciphertextB64, "base64"))
      : new Uint8Array(0);
    const outerBytes = encodeOuterPacket({ bodyBytes: ciphertextBytes });

    try {
      const result = await gatewayLoop.sendToInbox({
        deliverInboxId: mailboxId,
        innerBytes: outerBytes,
        ownerPublicKeyB64: depositorPubkeyB64 || null,
      });
      const eventId = result && typeof result.packetId === "string" ? result.packetId : "";
      this.#ctx.sendResponse(requestId, T.MAILBOX_DEPOSIT_RES, { mailboxId, eventId });
    } catch (err) {
      // GatewayLoop annotates routing failures with err.queued=true when
      // the message was successfully persisted into PersistentOutboundQueue.
      // RetryScheduler will keep attempting delivery on its 15s timer (and
      // on routeTable.setOnRouteAdded — i.e. immediately when the
      // destination's route appears). Surface this as a successful queued
      // response, not an error.
      if (err && err.queued === true) {
        this.#ctx.sendResponse(requestId, T.MAILBOX_DEPOSIT_RES, { mailboxId, eventId: "", queued: true });
        return;
      }
      const code = err && err.code ? String(err.code) : "DELIVERY_FAILED";
      const message = err && err.message ? err.message : "deposit delivery failed";
      const retryable = !!(err && err.retryable);
      this.#ctx.sendError({ id: requestId, code, message, retryable });
    }
  }

  async handleList(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const { mailboxId, cursor, limit, sinceMs } = body;
    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "read",
      resource: `mailbox:${mailboxId}`,
      requestId,
    });
    if (!cap) return;

    // Durable home inbox (pg cluster): reconnect catch-up is a device-aware
    // cursor read that returns bodies INLINE by seq (Option 1) — no separate
    // fetch round-trip. Gated on this node being durable-capable AND this inbox
    // being hosted here; every other inbox falls through to the transient
    // RMailbox list below (dual-mode, so fs/desktop is untouched). The read
    // advances this device's delivered watermark, which bounds a later cursorAck.
    const durableInbox = this.#ctx.runtime && this.#ctx.runtime.durableInbox;
    const isHostedHere = this.#ctx.runtime && typeof this.#ctx.runtime.isHostedHere === "function"
      ? this.#ctx.runtime.isHostedHere(mailboxId)
      : false;
    if (durableInbox && isHostedHere && typeof durableInbox.readAfterCursor === "function") {
      const deviceId = typeof this.#ctx.sessionDeviceId === "string" ? this.#ctx.sessionDeviceId.trim() : "";
      if (deviceId.length === 0) {
        this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session deviceId required", retryable: false });
        return;
      }
      const readLimit = Number.isInteger(limit) && limit > 0 ? limit : 50;
      try {
        const events = await durableInbox.readAfterCursor(mailboxId, deviceId, readLimit);
        const items = events.map((e) => ({ seq: e.seq, ciphertextB64: durableBodyB64(e.body) }));
        // Cursor model: no opaque pagination cursor. The client drains by
        // consuming + cursorAck-ing the batch, then lists again until empty.
        this.#ctx.sendResponse(requestId, T.MAILBOX_LIST_RES, { mailboxId, items, nextCursor: null });
      } catch (err) {
        const code = err && err.code ? String(err.code) : "LIST_FAILED";
        const message = err && err.message ? err.message : "durable list failed";
        this.#ctx.sendError({ id: requestId, code, message, retryable: false });
      }
      return;
    }

    const inboxStore = this.#ctx.runtime.inboxStore;
    if (!inboxStore) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "Mailbox service unavailable", retryable: false });
      return;
    }

    const result = await inboxStore.list(mailboxId, { cursor, limit, sinceMs });
    this.#ctx.sendResponse(requestId, T.MAILBOX_LIST_RES, {
      mailboxId,
      items: result.items,
      nextCursor: result.nextCursor,
    });
  }

  async handleFetch(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const { mailboxId, eventId } = body;
    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "read",
      resource: `mailbox:${mailboxId}`,
      requestId,
    });
    if (!cap) return;

    const inboxStore = this.#ctx.runtime.inboxStore;
    if (!inboxStore) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "Mailbox service unavailable", retryable: false });
      return;
    }

    const evt = await inboxStore.fetch(mailboxId, eventId);
    if (!evt) {
      this.#ctx.sendResponse(requestId, T.MAILBOX_FETCH_RES, { mailboxId, eventId, objectId: null, ciphertextB64: null, metadata: {}, createdAtMs: null });
      return;
    }

    // Surface the DECODED outer-packet body, identical to the live push path
    // (RelayDepositRouter sends decodeOuterPacket(packet).bodyBytesView). The
    // stored deposit is the framed outer packet ([0x02 version][body]); without
    // this decode, catch-up — the only fetch consumer — received the framed bytes
    // and failed to JSON.parse them (leading 0x02), so OFFLINE deposits never
    // applied while live-pushed (already-decoded) ones did. A stored value that is
    // not an outer packet is returned unchanged (defensive; relay-inbox deposits
    // always are outer packets).
    let bodyBytes = evt.bytes instanceof Uint8Array ? evt.bytes : null;
    if (bodyBytes) {
      try {
        bodyBytes = decodeOuterPacket(evt.bytes).bodyBytesView;
      } catch {
        // Stored value is not an outer packet — return it unchanged.
        bodyBytes = evt.bytes;
      }
    }
    const ciphertextB64 = bodyBytes ? Buffer.from(bodyBytes).toString("base64") : null;

    this.#ctx.sendResponse(requestId, T.MAILBOX_FETCH_RES, {
      mailboxId,
      eventId,
      objectId: evt.objectId,
      ciphertextB64,
      metadata: evt.metadata || {},
      createdAtMs: evt.createdAt || null,
    });
  }

  async handleAck(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const { mailboxId, eventId } = body;
    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "write",
      resource: `mailbox:${mailboxId}`,
      requestId,
    });
    if (!cap) return;

    const inboxStore = this.#ctx.runtime.inboxStore;
    if (!inboxStore) {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "Mailbox service unavailable", retryable: false });
      return;
    }

    const removed = await inboxStore.ack(mailboxId, eventId);
    this.#ctx.sendResponse(requestId, T.MAILBOX_ACK_RES, { mailboxId, eventId, removed });
  }

  /**
   * Advance this session's device cursor on the DURABLE home log (S2). Unlike
   * `handleAck` (which deletes from the transient RMailbox), this advances a
   * per-(inbox, device) watermark on `runtime.durableInbox` and NEVER deletes —
   * pruning happens separately below the slowest live device's cursor.
   *
   * Authority is bound to the SESSION's device (`ctx.sessionDeviceId`), never
   * the client-supplied body: cursorAck is a data-loss primitive, so a session
   * may only advance its OWN device's cursor. The storage layer additionally
   * enforces monotonic + delivered-bounded advance.
   */
  async handleCursorAck(requestId, body) {
    if (!this.#ctx.requireSession(requestId)) return;

    const { mailboxId, throughSeq } = body;
    // Dispatch hands the handler the RAW body — the record class is not validated
    // automatically — so guard the inputs here before they reach authorize/
    // storage, where a bad value would surface as a storage error rather than a
    // clean BAD_REQUEST.
    if (typeof mailboxId !== "string" || mailboxId.trim().length === 0) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "mailboxId required", retryable: false });
      return;
    }
    const throughSeqNum = Number(throughSeq);
    if (!Number.isInteger(throughSeqNum) || throughSeqNum < 0) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: "throughSeq must be a non-negative integer", retryable: false });
      return;
    }
    let capabilityChain;
    try {
      capabilityChain = decodeCapChain(body);
    } catch (err) {
      this.#ctx.sendError({ id: requestId, code: "BAD_REQUEST", message: err.message || "invalid capChain", retryable: false });
      return;
    }
    const cap = await this.#ctx.authorize({
      capabilityChain,
      presenterPublicKeyB64: this.#ctx.ownerPublicKeyB64,
      action: "write",
      resource: `mailbox:${mailboxId}`,
      requestId,
    });
    if (!cap) return;

    const durableInbox = this.#ctx.runtime && this.#ctx.runtime.durableInbox;
    if (!durableInbox || typeof durableInbox.cursorAck !== "function") {
      this.#ctx.sendError({ id: requestId, code: "SERVICE_UNAVAILABLE", message: "durable inbox unavailable", retryable: false });
      return;
    }

    const deviceId = typeof this.#ctx.sessionDeviceId === "string" ? this.#ctx.sessionDeviceId.trim() : "";
    if (deviceId.length === 0) {
      this.#ctx.sendError({ id: requestId, code: "UNAUTHORIZED", message: "session deviceId required", retryable: false });
      return;
    }

    try {
      const result = await durableInbox.cursorAck(mailboxId, deviceId, throughSeqNum);
      const lastSeq = result && Number.isFinite(result.lastSeq) ? result.lastSeq : 0;
      this.#ctx.sendResponse(requestId, T.MAILBOX_CURSOR_ACK_RES, { mailboxId, deviceId, lastSeq });
    } catch (err) {
      const code = err && err.code ? String(err.code) : "CURSOR_ACK_FAILED";
      const message = err && err.message ? err.message : "cursorAck failed";
      this.#ctx.sendError({ id: requestId, code, message, retryable: false });
    }
  }
}
