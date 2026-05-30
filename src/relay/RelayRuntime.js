import {
  JsonCodec,
  Envelope,
  Header,
  OnionPacketV2,
  OnionLayerAeadV2,
  OnionReplayCacheV2,
  ReplayDetectedError,
  parseOnionLayerV2,
  RCryptoProvider,
  OnionKeyNotUsableError,
  isNonEmptyString,
  bytesToHex,
} from "@rezprotocol/core";
import { parseRelayOnionPlaintext } from "./parseRelayOnionPlaintext.js";
import { canonicalJSONStringify } from "../util/canonicalize.js";
import { encodeFrame, sendControlMessage } from "../network/tcp/TcpFraming.js";
import { buildOnionPacketV2 } from "../gateway/buildOnionPacketV2.js";

function toBytes(value, label) {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return new Uint8Array(value);
  if (typeof value === "string") return new Uint8Array(Buffer.from(value, "base64"));
  throw new Error(`RelayRuntime.${label} must be Uint8Array`);
}

function socketEndpoint(socket) {
  if (!socket || typeof socket !== "object") return null;
  const host = typeof socket.remoteAddress === "string" ? socket.remoteAddress : "";
  const port = Number(socket.remotePort);
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  return `${host}:${port}`;
}

function endpointString(endpoint) {
  if (!endpoint || typeof endpoint !== "object") return null;
  const host = typeof endpoint.host === "string" ? endpoint.host.trim() : "";
  const port = Number(endpoint.port);
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  return `${host}:${port}`;
}

export class RelayRuntime {
  constructor({
    transport,
    inboxStore,
    onion,
    inboxRouter = null,
    relayDirectory = null,
    frameRouter = null,
    bridge = null,
    logger = console,
    nowMs = () => Date.now(),
    traceOnion = String(process.env.REZ_TRACE_ONION || "").trim() === "1",
    routeDebug = String(process.env.REZ_ROUTE_DEBUG || "").trim() === "1",
    receiptSender = null,
  } = {}) {
    if (!transport) throw new Error("RelayRuntime requires transport");
    if (!inboxStore) throw new Error("RelayRuntime requires inboxStore");
    if (!onion || !(onion.crypto instanceof RCryptoProvider)) {
      throw new Error("RelayRuntime requires onion.crypto (RCryptoProvider)");
    }
    if (!onion.v2 || !onion.v2.keyring) {
      throw new Error("RelayRuntime requires onion.v2.keyring");
    }
    if (receiptSender) {
      if (!onion.relayIdentityKey || !(onion.relayIdentityKey.privateKeyBytes instanceof Uint8Array)) {
        throw new Error("RelayRuntime requires onion.relayIdentityKey.privateKeyBytes when receipts enabled");
      }
      if (!isNonEmptyString(onion.relayKeyId)) {
        throw new Error("RelayRuntime requires onion.relayKeyId when receipts enabled");
      }
    }

    this.transport = transport;
    this.inboxStore = inboxStore;
    this.onion = onion;
    this.inboxRouter = inboxRouter;
    this.relayDirectory = relayDirectory || null;
    this.frameRouter = frameRouter || null;
    this._bridge = bridge || null;
    this.logger = logger || console;
    this.nowMs = nowMs;
    this.traceOnion = traceOnion === true;
    this.routeDebug = routeDebug === true;
    this.receiptSender = receiptSender;

    /** Packet correlation for route.failed propagation: packetId -> { sourceSocket, atMs }. TTL 30s. */
    this._packetCorrelation = new Map();
    this._packetCorrelationTtlMs = 30_000;
    /** Called when this relay is the origin of a packet and receives route.failed (e.g. GatewayLoop). */
    this.onRouteFailedCallback = null;

    this.decoder = new JsonCodec();
    this.replayV2 = new OnionReplayCacheV2();
    this.layerV2 = new OnionLayerAeadV2({ crypto: onion.crypto });

    this.started = false;
  }

  /**
   * Set the receipt sender after construction (e.g. when gatewayLoop is available in node bootstrap).
   * When a bridge is present, delegates to the bridge. Otherwise sets directly (backward compat).
   */
  setReceiptSender(sender) {
    if (this._bridge) {
      this._bridge.setReceiptSender(sender);
    } else {
      this.receiptSender = sender;
    }
  }

  /**
   * Set callback when this relay receives route.failed for a packet it originated (e.g. GatewayLoop).
   * When a bridge is present, delegates to the bridge. Otherwise sets directly (backward compat).
   * @param {(obj: { packetId: string, relayKeyId: string, reason: string }) => void} cb
   */
  setRouteFailedCallback(cb) {
    if (this._bridge) {
      this._bridge.setRouteFailedCallback(cb);
    } else {
      this.onRouteFailedCallback = typeof cb === "function" ? cb : null;
    }
  }

  async start() {
    if (this.started) return;
    this.started = true;
    const onBytes = this.frameRouter
      ? (bytes, socket) => this.frameRouter.dispatch(bytes, socket)
      : (bytes, socket) => this._handleBytes(bytes, socket);
    await this.transport.start({
      onBytes,
      onSocketClose: (socket) => {
        this.relayDirectory?.remove(socket);
        this.inboxRouter?.removeConnection(socket);
      },
    });
  }

  async stop() {
    if (!this.started) return;
    this.started = false;
    await this.transport.stop();
  }

  /**
   * Process raw bytes as an onion Envelope. Called by SocketFrameRouter when the frame is classified as an envelope.
   * Decodes the envelope and dispatches to the v2 onion handler. Does not handle control messages.
   */
  async handleInboundEnvelope(bytes, sourceSocket) {
    const ctx = await this.decoder.decode({ bytes });
    const envelope = ctx.envelope;
    if (!(envelope instanceof Envelope)) return;

    this._routeLog("info", "envelope recv", { type: envelope.header?.type, packetId: envelope.header?.id });

    if (envelope.header.type === "rez.onion.v2") {
      await this._handleOnionV2(envelope, sourceSocket);
      return;
    }
  }

  async _handleBytes(bytes, sourceSocket) {
    let envelope;
    try {
      const ctx = await this.decoder.decode({ bytes });
      envelope = ctx.envelope;
    } catch (err) {
      // Not an envelope — try raw control message before giving up
      if (this.inboxRouter) {
        const handled = await this._tryHandleControlMessage(bytes, sourceSocket);
        if (handled) return;
      }
      this.logger?.warn?.("RelayRuntime failed to decode envelope", err);
      return;
    }

    if (!(envelope instanceof Envelope)) return;

    if (envelope.header.type === "rez.onion.v2") {
      await this._handleOnionV2(envelope, sourceSocket);
      return;
    }
  }

  async _handleOnionV2(envelope, sourceSocket) {
    const packet = envelope.body instanceof OnionPacketV2
      ? envelope.body
      : OnionPacketV2.fromJSON(envelope.body);
    const payloadBytes = packet.payload;
    const sizeClass = packet.sizeClass;
    const packetIdHex = bytesToHex(await this.onion.crypto.hashSha256(payloadBytes));

    const cipherObj = parseOnionLayerV2(payloadBytes);
    if (!cipherObj || cipherObj.v !== 2) return;

    const hopIndex = cipherObj.hopIndex;
    if (!Number.isSafeInteger(hopIndex) || hopIndex < 0) return;

    // Validate TTL bounds BEFORE expensive decrypt to prevent CPU DoS.
    // An attacker could send packets with inflated TTL to burn decrypt cycles.
    const cipherTtl = cipherObj.ttl;
    if (!Number.isInteger(cipherTtl) || cipherTtl <= 0 || cipherTtl > 20) {
      this._traceOnion("drop-ttl-bounds", {
        packetId: envelope && envelope.header ? envelope.header.id : null,
        hopIndex,
        ttl: cipherTtl,
      });
      return;
    }

    const onionKeyId = cipherObj.onionKeyId;
    if (!isNonEmptyString(onionKeyId)) return;
    this._traceOnion("recv", {
      packetId: envelope?.header?.id || null,
      hopIndex,
      onionKeyId,
      from: socketEndpoint(sourceSocket),
      sizeClass,
    });

    try {
      this.replayV2.checkAndMark(packetIdHex, hopIndex, onionKeyId);
    } catch (err) {
      if (err instanceof ReplayDetectedError) {
        this._traceOnion("drop-duplicate", {
          packetId: envelope?.header?.id || null,
          hopIndex,
          onionKeyId,
        });
        this.logger?.warn?.("RelayRuntime dropped replayed packet");
        return;
      }
      throw err;
    }

    let privKeyBytes;
    try {
      privKeyBytes = this.onion.v2.keyring.getKeyForDecrypt(onionKeyId, this.nowMs());
    } catch (err) {
      if (err instanceof OnionKeyNotUsableError) {
        this._traceOnion("drop-expired-key", {
          packetId: envelope?.header?.id || null,
          hopIndex,
          onionKeyId,
        });
        this.logger?.warn?.("RelayRuntime onion v2 key not usable (expired/revoked)", {
          onionKeyId,
          message: err?.message,
        });
      } else {
        this._traceOnion("drop-decrypt-failed", {
          packetId: envelope?.header?.id || null,
          hopIndex,
          onionKeyId,
        });
        this.logger?.warn?.("RelayRuntime onion v2 decrypt failed", err);
      }
      return;
    }

    let plaintextBytes;
    try {
      plaintextBytes = await this.layerV2.decryptLayerV2({
        relayPrivKeyBytes: privKeyBytes,
        layerObj: cipherObj,
        hopIndex,
      });
    } catch (err) {
      this._traceOnion("drop-decrypt-failed", {
        packetId: envelope?.header?.id || null,
        hopIndex,
        onionKeyId,
      });
      this.logger?.warn?.("RelayRuntime onion v2 decrypt failed", err);
      return;
    }

    const layerPlain = parseRelayOnionPlaintext(plaintextBytes);
    if (layerPlain.ttl !== cipherObj.ttl) {
      this._traceOnion("drop-ttl-mismatch", {
        packetId: envelope?.header?.id || null,
        hopIndex,
        expectedTtl: cipherObj.ttl,
        actualTtl: layerPlain.ttl,
      });
      this.logger?.warn?.("RelayRuntime onion v2 ttl mismatch");
      return;
    }

    await this._dispatchPlaintext({
      sizeClass,
      header: envelope.header,
      layerPlain,
      hopIndex,
      sourceSocket,
    });
  }

  async _dispatchPlaintext({ sizeClass, header, layerPlain, hopIndex = null, sourceSocket }) {
    this._routeLog("info", "dispatch-plaintext", { packetId: header?.id, hopIndex, hasDeliver: !!layerPlain.deliverInboxId, hasNext: !!layerPlain?.next?.relayKeyId });
    if (layerPlain.ttl <= 0) {
      this._traceOnion("drop-ttl-expired", {
        packetId: header?.id || null,
        hopIndex,
      });
      this._routeLog("warn", "drop-ttl-expired", { packetId: header?.id, hopIndex });
      return;
    }

    // Check if the inner payload is a control message for the router
    if (this.inboxRouter && layerPlain.deliverInboxId === "_ctl") {
      const handled = await this._tryHandleControlMessage(layerPlain.inner, sourceSocket);
      if (handled) return;
    }

    if (layerPlain.deliverInboxId) {
      // Try routing via InboxRouter first (may forward to another relay/node)
      if (this.inboxRouter) {
        const routed = await this.inboxRouter.routeDelivery(layerPlain.deliverInboxId, layerPlain.inner);
        if (routed) {
          this._traceOnion("deliver-routed", {
            packetId: header?.id || null,
            hopIndex,
            inboxId: layerPlain.deliverInboxId,
            bytes: layerPlain.inner.length,
          });
          this._routeLog("info", "deliver-routed", { packetId: header?.id, inboxId: layerPlain.deliverInboxId });
          // Relay-level receipts removed: rez.receipt.v1 Envelopes are not OuterPacket
          // and nothing in the stack processes them. E2EE delivery acks (E2eeDeliveryAckV1)
          // are now routed end-to-end through chat-server's ServerPeerLinkProtocolService.
          return;
        }
      }
      const localHosted = !this.inboxRouter
        || (
          typeof this.inboxRouter.isLocalHostedInbox === "function"
          && this.inboxRouter.isLocalHostedInbox(layerPlain.deliverInboxId)
        );
      if (!localHosted) {
        this._traceOnion("drop-no-route", {
          packetId: header?.id || null,
          hopIndex,
          inboxId: layerPlain.deliverInboxId,
        });
        this._routeLog("warn", "drop-no-route sending route.failed", {
          packetId: header?.id,
          inboxId: layerPlain.deliverInboxId,
        });
        this._sendRouteFailure(sourceSocket, header?.id, "", "no_route");
        return;
      }
      // Local hosted delivery
      const depositId = await this.inboxStore.depositFromWire(layerPlain.deliverInboxId, layerPlain.inner);
      this._traceOnion("deliver-local", {
        packetId: header?.id || null,
        hopIndex,
        inboxId: layerPlain.deliverInboxId,
        bytes: layerPlain.inner.length,
        depositId,
      });
      this._routeLog("info", "deliver-local", { packetId: header?.id, inboxId: layerPlain.deliverInboxId, depositId });
      // Relay-level receipts removed (see routed path comment above).
      return;
    }

    if (!layerPlain.next || !isNonEmptyString(layerPlain.next.relayKeyId)) {
      this._traceOnion("drop-no-next", {
        packetId: header?.id || null,
        hopIndex,
      });
      this._routeLog("warn", "drop-no-next", { packetId: header?.id, hopIndex });
      return;
    }
    const peerSocket = this.relayDirectory?.getSocket(layerPlain.next.relayKeyId);
    if (!peerSocket) {
      this._traceOnion("drop-no-peer", {
        packetId: header?.id || null,
        hopIndex,
        relayKeyId: layerPlain.next.relayKeyId,
      });
      this._routeLog("warn", "drop-no-peer sending route.failed", { packetId: header?.id, relayKeyId: layerPlain.next.relayKeyId });
      this._sendRouteFailure(sourceSocket, header?.id, layerPlain.next.relayKeyId, "no_peer");
      return;
    }
    const padded = this._pad(layerPlain.inner, sizeClass);
    if (!padded) {
      this._traceOnion("drop-inner-too-large", {
        packetId: header?.id || null,
        hopIndex,
      });
      this.logger?.warn?.("RelayRuntime dropped packet (inner exceeds size)");
      return;
    }
    const forwardEnvelope = new Envelope({
      header,
      body: new OnionPacketV2({ v: 2, sizeClass, payload: padded }).toJSON(),
    });
    const ctx = await this.decoder.encode({ envelope: forwardEnvelope });
    this._traceOnion("forward", {
      packetId: header?.id || null,
      hopIndex,
      next: layerPlain.next.relayKeyId,
      bytes: ctx.bytes.length,
    });
    this._routeLog("info", "forward", {
      packetId: header?.id,
      hopIndex,
      next: layerPlain.next.relayKeyId,
      bytes: ctx.bytes.length,
    });
    this._recordCorrelation(header?.id, sourceSocket);
    const frame = encodeFrame(ctx.bytes);
    peerSocket.write(frame);
  }

  _sendRouteFailure(sourceSocket, packetId, relayKeyId, reason) {
    sendControlMessage(sourceSocket, { _ctl: "route.failed", packetId: packetId || "", relayKeyId: relayKeyId || "", reason });
  }

  _traceOnion(event, fields = {}) {
    if (!this.traceOnion) return;
    const addr = typeof this.transport?.getListenAddress === "function"
      ? this.transport.getListenAddress()
      : null;
    const relay = endpointString(addr) || "unknown";
    this.logger?.info?.("[ONION-TRACE]", {
      relay,
      event,
      ...fields,
    });
  }

  _routeLog(level, message, fields = {}) {
    if (!this.routeDebug && !this.traceOnion) return;
    const addr = typeof this.transport?.getListenAddress === "function"
      ? this.transport.getListenAddress()
      : null;
    const relay = endpointString(addr) || "unknown";
    const line = `[ROUTE] ${relay} ${message} ${JSON.stringify(fields)}`;
    if (level === "warn") {
      this.logger?.warn?.(line);
      if (this.routeDebug) console.warn(line);
    } else {
      this.logger?.info?.(line);
      if (this.routeDebug) console.log(line);
    }
  }

  _evictCorrelation() {
    const now = this.nowMs();
    const ttl = this._packetCorrelationTtlMs;
    for (const [id, entry] of this._packetCorrelation.entries()) {
      if (now - entry.atMs > ttl) this._packetCorrelation.delete(id);
    }
  }

  _recordCorrelation(packetId, sourceSocket) {
    if (!packetId || !sourceSocket) return;
    this._evictCorrelation();
    this._packetCorrelation.set(packetId, { sourceSocket, atMs: this.nowMs() });
    this._routeLog("info", "correlation recorded", { packetId, size: this._packetCorrelation.size });
  }

  /**
   * Called by SocketFrameRouter when route.failed is received. Propagate back or notify origin.
   */
  handleRouteFailed(ctlObj, arrivalSocket) {
    const packetId = ctlObj?.packetId ?? "";
    const relayKeyId = ctlObj?.relayKeyId ?? "";
    const reason = ctlObj?.reason ?? "";
    this._routeLog("info", "route.failed received", { packetId, relayKeyId, reason });

    this._evictCorrelation();
    const entry = this._packetCorrelation.get(packetId);
    if (entry?.sourceSocket && !entry.sourceSocket.destroyed) {
      this._routeLog("info", "route.failed propagate", { packetId });
      this._sendRouteFailure(entry.sourceSocket, packetId, relayKeyId, reason);
      this._packetCorrelation.delete(packetId);
      return;
    }
    if (this._bridge) {
      this._routeLog("info", "route.failed origin callback (bridge)", { packetId });
      this._bridge.reportRouteFailed({ packetId, relayKeyId, reason });
    } else if (this.onRouteFailedCallback) {
      this._routeLog("info", "route.failed origin callback", { packetId });
      this.onRouteFailedCallback({ packetId, relayKeyId, reason });
    }
  }

  _tryHandleControlMessage(innerBytes, sourceSocket) {
    try {
      const json = new TextDecoder().decode(innerBytes);
      const obj = JSON.parse(json);
      if (obj && typeof obj === "object" && typeof obj._ctl === "string") {
        const result = this.inboxRouter.handleControlMessage(obj, sourceSocket);
        return Promise.resolve(result);
      }
    } catch {
      // Not a valid control message — ignore
    }
    return Promise.resolve(false);
  }

  _pad(bytes, size) {
    if (bytes.length > size) return null;
    const out = new Uint8Array(size);
    out.set(bytes, 0);
    return out;
  }

  _hasReceiptSender() {
    if (this._bridge) return this._bridge.hasReceiptSender;
    return this.receiptSender !== null && this.receiptSender !== undefined;
  }

  async _sendReceipt({ receiptInboxId, innerBytes, depositId, destInboxId, kind }) {
    try {
      const receiptBytes = await this._buildReceiptEnvelopeBytes(innerBytes, depositId, destInboxId, kind);
      if (this._bridge) {
        await this._bridge.sendReceipt({
          innerBytes: receiptBytes,
          deliverInboxId: receiptInboxId,
        });
      } else {
        await this.receiptSender.sendToInbox({
          innerBytes: receiptBytes,
          deliverInboxId: receiptInboxId,
        });
      }
    } catch (err) {
      this.logger?.warn?.("RelayRuntime receipt send failed", err);
    }
  }

  async _buildReceiptEnvelopeBytes(innerBytes, depositId, destInboxId, kind) {
    const hash = await this.onion.crypto.hashSha256(innerBytes);
    const receiptBody = {
      v: 1,
      kind,
      msg: {
        innerHash: Array.from(hash),
        depositId,
        inboxId: destInboxId,
        receivedAtMs: this.nowMs(),
        messageId: `client-${Date.now()}`,
      },
    };
    if (this.onion.relayIdentityKey && this.onion.relayIdentityKey.privateKeyBytes && this.onion.relayKeyId) {
      const bodyToSign = { ...receiptBody };
      const bytes = new TextEncoder().encode(canonicalJSONStringify(bodyToSign));
      const sig = await this.onion.crypto.sign({
        privateKey: this.onion.relayIdentityKey.privateKeyBytes,
        msg: bytes,
      });
      receiptBody.sig = {
        alg: "ed25519",
        relayKeyId: this.onion.relayKeyId,
        sig: Array.from(sig),
      };
    }
    const receiptEnvelope = new Envelope({
      header: new Header({ id: `receipt-${depositId}`, type: "rez.receipt.v1", createdAt: this.nowMs() }),
      body: receiptBody,
    });
    const ctx = await this.decoder.encode({ envelope: receiptEnvelope });
    return ctx.bytes;
  }

  async _sendReceiptViaReturnPath(returnPath, innerBytes, destInboxId, depositId) {
    if (!returnPath?.entryRelayKeyId || !Array.isArray(returnPath.pathEntries) || returnPath.pathEntries.length === 0) {
      this._routeLog("warn", "return path incomplete, skip receipt", { entryRelayKeyId: returnPath?.entryRelayKeyId });
      return;
    }
    if (!this.relayDirectory) {
      this._routeLog("warn", "no relay directory, cannot send receipt via return path");
      return;
    }
    try {
      const receiptBytes = await this._buildReceiptEnvelopeBytes(innerBytes, depositId, destInboxId, "delivered");
      const pathEntries = returnPath.pathEntries.map((e) => ({
        relayKeyId: e.relayKeyId,
        relayDescriptor: e.relayDescriptor || e,
        onionKeyId: e.onionKeyId,
        onionPubKeyBytes: e.onionPubKeyBytes,
      }));
      const built = await buildOnionPacketV2({
        crypto: this.onion.crypto,
        innerBytes: receiptBytes,
        deliverInboxId: returnPath.deliverInboxId,
        pathEntries,
        finalRelayKeyId: returnPath.finalRelayKeyId,
        nowMs: this.nowMs(),
      });
      const peerSocket = this.relayDirectory.getSocket(returnPath.entryRelayKeyId);
      if (!peerSocket || peerSocket.destroyed) {
        this._routeLog("warn", "return path entry relay not connected", { entryRelayKeyId: returnPath.entryRelayKeyId });
        return;
      }
      const frame = encodeFrame(built.packetBytes);
      peerSocket.write(frame);
      this._routeLog("info", "receipt sent via return path", {
        entryRelayKeyId: returnPath.entryRelayKeyId,
        deliverInboxId: returnPath.deliverInboxId,
      });
    } catch (err) {
      this.logger?.warn?.("RelayRuntime receipt via return path failed", err?.message);
    }
  }
}
