const SAFE_MESSAGES_BY_CODE = Object.freeze({
  BAD_REQUEST: "Bad request",
  BAD_VERSION: "Unsupported contract version",
  UNAUTHORIZED: "Unauthorized",
  FORBIDDEN: "Forbidden",
  RATE_LIMITED: "Rate limited",
  RESOURCE_LIMIT: "Resource limit exceeded",
  REAUTH_REQUIRED: "Reauthentication required",
  INTERNAL: "Internal error",
});

export function toClientSafeError({ code, message, retryable = false, detail = null } = {}) {
  const normalizedCode = typeof code === "string" && code.trim() ? code.trim() : "INTERNAL";
  const safeCode = normalizedCode || "INTERNAL";
  const normalizedMessage = typeof message === "string" && message.trim() ? message.trim() : "";
  const safeMessage = safeCode !== "INTERNAL" && normalizedMessage
    ? normalizedMessage
    : (SAFE_MESSAGES_BY_CODE[safeCode] || SAFE_MESSAGES_BY_CODE.INTERNAL);
  return {
    code: safeCode,
    message: safeMessage,
    retryable: retryable === true,
    detail,
  };
}
