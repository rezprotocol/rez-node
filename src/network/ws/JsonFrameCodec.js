function parseJson(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function asBadFrameError() {
  const err = new Error("bad frame json");
  err.code = "BAD_FRAME";
  err.retryable = false;
  return err;
}

export function createJsonFrameCodec() {
  return {
    encodeFrame({ id, type, body = {}, version = 1 }) {
      return JSON.stringify({
        id: String(id || ""),
        t: String(type || ""),
        v: Number.isFinite(Number(version)) ? Number(version) : 1,
        body: body && typeof body === "object" ? body : {},
      });
    },
    decodeFrame(raw) {
      const parsed = parseJson(raw);
      if (!parsed || typeof parsed !== "object") {
        throw asBadFrameError();
      }
      const typeStr =
        typeof parsed.type === "string" && parsed.type.trim().length > 0
          ? parsed.type.trim()
          : typeof parsed.t === "string" && parsed.t.trim().length > 0
            ? parsed.t.trim()
            : "";
      return {
        id: typeof parsed.id === "string" && parsed.id.length > 0 ? parsed.id : null,
        type: typeStr,
        version: parsed.v,
        body: parsed.body && typeof parsed.body === "object" ? parsed.body : {},
      };
    },
  };
}
