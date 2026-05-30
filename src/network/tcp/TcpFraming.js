const MAX_FRAME_BYTES = 8 * 1024 * 1024;

export function encodeFrame(bytes) {
  if (!(bytes instanceof Uint8Array)) {
    throw new Error("encodeFrame(bytes) requires Uint8Array");
  }
  if (bytes.length > MAX_FRAME_BYTES) {
    throw new Error("encodeFrame(bytes) exceeds max frame size");
  }
  const buffer = Buffer.allocUnsafe(4 + bytes.length);
  buffer.writeUInt32BE(bytes.length, 0);
  Buffer.from(bytes).copy(buffer, 4);
  return buffer;
}

export function createFrameDecoder(onFrameBytes) {
  if (typeof onFrameBytes !== "function") {
    throw new Error("createFrameDecoder(onFrameBytes) requires a function");
  }

  let buffer = Buffer.alloc(0);

  function push(chunk) {
    if (!chunk || chunk.length === 0) return;
    if (buffer.length + chunk.length > MAX_FRAME_BYTES + 4) {
      throw new Error("frame buffer limit exceeded");
    }
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 4) {
      const length = buffer.readUInt32BE(0);
      if (length > MAX_FRAME_BYTES) {
        throw new Error("frame exceeds max size");
      }
      if (buffer.length < 4 + length) return;

      const payload = buffer.subarray(4, 4 + length);
      buffer = buffer.subarray(4 + length);
      onFrameBytes(new Uint8Array(payload));
    }
  }

  function reset() {
    buffer = Buffer.alloc(0);
  }

  return { push, reset };
}

/**
 * Encode a control message object as a framed byte payload.
 * @param {object} ctlObj — control message with _ctl field
 * @returns {Buffer} length-prefixed frame ready for socket.write()
 */
export function encodeControlMessage(ctlObj) {
  const bytes = new TextEncoder().encode(JSON.stringify(ctlObj));
  return encodeFrame(bytes);
}

/**
 * Send a control message to a socket with error handling.
 * @param {object} socket — TCP socket with write() method
 * @param {object} ctlObj — control message with _ctl field
 * @returns {boolean} true if sent, false if socket was unavailable or write failed
 */
export function sendControlMessage(socket, ctlObj) {
  if (!socket || socket.destroyed) return false;
  try {
    socket.write(encodeControlMessage(ctlObj));
    return true;
  } catch (_err) {
    return false;
  }
}

export { MAX_FRAME_BYTES };
