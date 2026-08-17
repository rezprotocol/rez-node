import { parseUntrustedJson, UNSAFE_JSON_KEY } from "@rezprotocol/core";

/**
 * Node-side JSON frame codec.
 *
 * This is the trust boundary EVERY inbound frame crosses, and unlike the SDK's
 * codec it faces unauthenticated remotes: `decodeFrame` runs before session
 * authentication, so anyone who can open a socket reaches it. `parsed.body` used
 * to be handed straight to the handler layer, which meant a body carrying
 * `__proto__` was one `Object.assign` away from re-parenting whatever it was
 * copied into.
 *
 * Same rule, same implementation as the SDK codec and the profile/packet
 * boundaries — `parseUntrustedJson` lives in `@rezprotocol/core` precisely so
 * these cannot drift apart. They already had: this codec and
 * `rez-sdk/src/transport/FrameCodec.js` are near-identical files, and fixing one
 * without the other would have left the more exposed half open.
 *
 * Two refusals, distinguishable to the operator: `BAD_FRAME` is "not JSON" (a
 * broken or version-mismatched client) and `UNSAFE_FRAME` is "JSON built to
 * poison us". The peer is told the same thing either way — an attacker learns
 * nothing from a probe — but the server log names which one happened.
 */
function asBadFrameError() {
  const err = new Error("bad frame json");
  err.code = "BAD_FRAME";
  err.retryable = false;
  return err;
}

function asUnsafeFrameError(cause) {
  const err = new Error("unsafe frame json");
  err.code = "UNSAFE_FRAME";
  err.retryable = false;
  // One of exactly three constants — safe to log. The full path is NOT carried
  // forward: it is built from attacker-chosen key names, and an operator log is
  // not the place to interpolate those.
  err.unsafeKey = cause && typeof cause.key === "string" ? cause.key : "";
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
      let parsed;
      try {
        parsed = parseUntrustedJson(raw, "frame");
      } catch (err) {
        if (err && err.code === UNSAFE_JSON_KEY) throw asUnsafeFrameError(err);
        parsed = null;
      }
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
