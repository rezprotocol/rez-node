/**
 * Thrown when a revoked device attempts to read or advance a cursor. Home
 * enforcement of device revocation: E2EE cannot erase keys already on a revoked
 * device, so the HOME must fail closed (see plan S2.5 P1a / S1).
 */
export class RevokedDeviceError extends Error {
  constructor(inboxId, deviceId) {
    super(`device ${deviceId} is revoked for inbox ${inboxId}`);
    this.name = "RevokedDeviceError";
    this.code = "DEVICE_REVOKED";
    this.inboxId = inboxId;
    this.deviceId = deviceId;
  }
}

/** Thrown when a per-inbox DoS cap (events, bytes, body size, or devices) is exceeded. */
export class InboxCapExceededError extends Error {
  constructor(inboxId, cap, limitType) {
    super(`inbox ${inboxId} ${limitType} cap (${cap}) exceeded`);
    this.name = "InboxCapExceededError";
    this.code = "INBOX_CAP_EXCEEDED";
    this.inboxId = inboxId;
    this.cap = cap;
    this.limitType = limitType; // "events" | "bytes" | "bodyBytes" | "devices"
  }
}

/**
 * Thrown when a device that is not registered tries to read or advance a cursor.
 * Registration (registerDevice) is the single capped entry point for device rows;
 * read/ack must not implicitly create them (Sybil / cursor-griefing guard).
 */
export class DeviceNotRegisteredError extends Error {
  constructor(inboxId, deviceId) {
    super(`device ${deviceId} is not registered for inbox ${inboxId}`);
    this.name = "DeviceNotRegisteredError";
    this.code = "DEVICE_NOT_REGISTERED";
    this.inboxId = inboxId;
    this.deviceId = deviceId;
  }
}

/**
 * The durable home inbox contract — a NEW contract, deliberately distinct from
 * the transient `RMailbox` (whose `ack` deletes). Verbs:
 *
 *   append(inboxId, body, { dedupeKey })  -> { seq, deduped }   (append-only)
 *   readAfterCursor(inboxId, deviceId, limit) -> [{ seq, body }]
 *   cursorAck(inboxId, deviceId, throughSeq)  -> { lastSeq }    (advance, never delete)
 *   prune(inboxId, { ttlMs, staleGraceMs })   -> { deleted }
 *
 * `ack` means advance THIS device's cursor, never destroy the log. The log is
 * the cluster's system of record; pruning is bounded by the slowest live device.
 */
export class DurableInbox {
  append(_inboxId, _body, _opts) {
    throw new Error("DurableInbox.append is abstract");
  }

  readAfterCursor(_inboxId, _deviceId, _limit) {
    throw new Error("DurableInbox.readAfterCursor is abstract");
  }

  readUndelivered(_inboxId, _deviceId, _limit) {
    throw new Error("DurableInbox.readUndelivered is abstract");
  }

  cursorAck(_inboxId, _deviceId, _throughSeq) {
    throw new Error("DurableInbox.cursorAck is abstract");
  }

  prune(_inboxId, _opts) {
    throw new Error("DurableInbox.prune is abstract");
  }
}
