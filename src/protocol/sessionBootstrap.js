/**
 * Session hello handling and bootstrap relay mapping.
 * Single place for building bootstrap relay hints and SessionReady payload.
 *
 * The relay-side session identity is the SDK's pubkey
 * (`accountIdentityPublicKeyB64`). Accounts are a chat-app concept — the
 * protocol does not see them. An inbox is associated with the session only
 * after a successful inbox.claim; until then `localInboxId` is empty and
 * inbox-using ops fail with `INBOX_NOT_CLAIMED`.
 */
import { base64ToBytes, CONTRACT_VERSION, REZ_CONTRACT_TYPES } from "@rezprotocol/core";
import { SessionHello } from "../contracts/records/SessionHello.js";
import { SessionReadyEvent } from "../contracts/records/SessionReadyEvent.js";
import { SessionCapabilities } from "../contracts/wireRecords/SessionCapabilities.js";
import { ContractError } from "../contracts/ContractError.js";

const SESSION_BOOTSTRAP_RELAY_MAX = 256;
const T = REZ_CONTRACT_TYPES;

/**
 * Maps relayStore row(s) to bootstrap hint shape expected by SessionCapabilities.
 * @param {Array<unknown>} relayRows
 * @param {number} [max=256]
 * @returns {Array<{ id: string, host?: string, port?: number, url?: string, transport?: string }>}
 */
export function buildBootstrapRelays(relayRows, max = SESSION_BOOTSTRAP_RELAY_MAX) {
  if (!Array.isArray(relayRows)) return [];
  return relayRows
    .map((relay) => {
      const id = String(relay?.relayKeyId || relay?.id || "").trim();
      if (!id) return null;
      return {
        id,
        host: relay?.endpoint?.host != null ? String(relay.endpoint.host) : relay?.host != null ? String(relay.host) : undefined,
        port: Number.isInteger(Number(relay?.endpoint?.port)) ? Number(relay.endpoint.port) : (Number.isInteger(Number(relay?.port)) ? Number(relay.port) : undefined),
        url: relay?.url != null ? String(relay.url) : undefined,
        transport: relay?.transport != null ? String(relay.transport) : undefined,
      };
    })
    .filter(Boolean)
    .slice(0, max);
}

/**
 * Handles session.hello body: validation, bootstrap data, and ready event.
 * Returns either an error or success data with pendingAuthentication.
 * @param {{ runtime: object, body: object }} opts
 */
export function handleSessionHello({ body } = {}) {
  let record;
  try {
    record = new SessionHello(body);
  } catch (err) {
    if (err instanceof ContractError) {
      return {
        error: {
          code: err.code,
          message: err.message,
          retryable: false,
          close: err.code === "BAD_VERSION",
        },
      };
    }
    return {
      error: {
        code: "BAD_REQUEST",
        message: err?.message || "Invalid session.hello payload",
        retryable: false,
      },
    };
  }

  if (!record.deviceId) {
    return { error: { code: "UNAUTHORIZED", message: "session deviceId required", retryable: false } };
  }

  const accountIdentityPublicKeyB64 = typeof record.accountIdentityPublicKeyB64 === "string"
    ? record.accountIdentityPublicKeyB64.trim()
    : "";
  if (!accountIdentityPublicKeyB64) {
    return { error: { code: "UNAUTHORIZED", message: "session identity key required", retryable: false } };
  }

  try {
    base64ToBytes(accountIdentityPublicKeyB64);
  } catch {
    return { error: { code: "UNAUTHORIZED", message: "session identity key invalid", retryable: false } };
  }

  return {
    sessionDeviceId: record.deviceId,
    accountIdentityPublicKeyB64,
    pendingAuthentication: {
      sessionDeviceId: record.deviceId,
      accountIdentityPublicKeyB64,
    },
  };
}

/**
 * Build the authenticated session payload. No inbox is bound here — that
 * happens on inbox.claim. `localInboxId` on the ready event is left empty;
 * the SDK can claim or re-attest an inbox after session.ready arrives.
 */
export async function buildAuthenticatedSession({ runtime, deviceId, accountIdentityPublicKeyB64 } = {}) {
  const identity = typeof runtime?.getIdentity === "function" ? runtime.getIdentity() : null;
  const meshStatus = typeof runtime?.getMeshStatus === "function" ? runtime.getMeshStatus() : null;
  const relayRows = typeof runtime?.relayStore?.getAll === "function" ? runtime.relayStore.getAll() : [];
  const bootstrapRelays = buildBootstrapRelays(relayRows, SESSION_BOOTSTRAP_RELAY_MAX);
  const bootstrapSeeds = [];

  let capabilities;
  try {
    capabilities = new SessionCapabilities({
      contractVersion: CONTRACT_VERSION,
      deviceId: identity?.deviceId ?? deviceId,
      localInboxId: "",
      capabilities: [],
      bootstrapRelays,
      bootstrapSeeds,
      meshMode: meshStatus?.mode || null,
    });
  } catch (err) {
    runtime?.logger?.warn?.("session bootstrap capabilities fallback", err?.message || err);
    capabilities = new SessionCapabilities({
      contractVersion: CONTRACT_VERSION,
      deviceId: identity?.deviceId ?? deviceId,
      localInboxId: "",
      capabilities: [],
    });
  }

  const readyEvent = new SessionReadyEvent({
    serverTime: Date.now(),
    capabilities,
  });

  return {
    accountIdentityPublicKeyB64: accountIdentityPublicKeyB64 || "",
    sessionDeviceId: deviceId,
    readyEvent,
    pendingAuthentication: null,
  };
}
