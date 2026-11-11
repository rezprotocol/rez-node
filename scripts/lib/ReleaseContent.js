import { createPrivateKey } from "node:crypto";

const CREDENTIAL_PATTERNS = [
  /\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,}|npm_[A-Za-z0-9]{30,}|dop_v1_[a-f0-9]{50,})\b/,
  /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/,
  /(?:_authToken|_password)\s*[=:]\s*["']?[A-Za-z0-9_+\/-]{20,}/,
  /"(?:nodePrivateKeyB64|privateKeyB64|encryptionKeyB64)"\s*:\s*"[A-Za-z0-9+/=]{32,}"/,
  /REZ_STORAGE_ENCRYPTION_KEY\s*[:=]\s*["'][A-Za-z0-9+/=]{40,}/,
];

export function isBinary(bytes) {
  return bytes.subarray(0, 4096).includes(0);
}

export function hasPrivateFilename(name) {
  return /(?:^|\/)(?:\.git|\.env(?:\.[^/]*)?|\.npmrc|\.netrc|id_rsa|id_ed25519)(?:\/|$)/.test(name)
    || /\.(?:p12|pfx|dump|sqlite|sqlite3)$/.test(name);
}

export function hasCredentialMaterial(bytes) {
  if (isBinary(bytes)) return false;
  const source = bytes.toString("utf8");
  if (CREDENTIAL_PATTERNS.some((pattern) => pattern.test(source))) return true;
  for (const key of source.matchAll(/-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----[\s\S]*?-----END (?:[A-Z]+ )*PRIVATE KEY-----/g)) {
    try { createPrivateKey(key[0]); return true; } catch { /* Documentation placeholders cannot authenticate. */ }
  }
  return false;
}
