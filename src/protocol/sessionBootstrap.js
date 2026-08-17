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
import { assertMultiDeviceFanoutReady } from "../app/deviceFanoutReadiness.js";

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
      const id = String((relay && (relay.relayKeyId || relay.id)) || "").trim();
      const endpoint = relay && relay.endpoint ? relay.endpoint : null;
      if (!id) return null;
      return {
        id,
        host: endpoint && endpoint.host != null
          ? String(endpoint.host)
          : (relay && relay.host != null ? String(relay.host) : undefined),
        port: endpoint && Number.isInteger(Number(endpoint.port))
          ? Number(endpoint.port)
          : (relay && Number.isInteger(Number(relay.port)) ? Number(relay.port) : undefined),
        url: relay && relay.url != null ? String(relay.url) : undefined,
        transport: relay && relay.transport != null ? String(relay.transport) : undefined,
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
        message: err && err.message ? err.message : "Invalid session.hello payload",
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
  const identity = runtime && typeof runtime.getIdentity === "function" ? runtime.getIdentity() : null;
  const meshStatus = runtime && typeof runtime.getMeshStatus === "function" ? runtime.getMeshStatus() : null;
  const relayStore = runtime && runtime.relayStore ? runtime.relayStore : null;
  const relayRows = relayStore && typeof relayStore.getAll === "function" ? relayStore.getAll() : [];
  const bootstrapRelays = buildBootstrapRelays(relayRows, SESSION_BOOTSTRAP_RELAY_MAX);
  const bootstrapSeeds = [];
  // D2: advertise the durable-inbox capability when this node runs the durable
  // home log (pg-cluster). The client then uses the cursor model; against a node
  // without it, the client keeps the legacy delete-ack path. Computed without
  // optional chaining (repo policy) since this is a new line.
  const durableInbox = Boolean(runtime && runtime.durableInbox);
  // E6 gate state (Audit R2 #6) + audit-R4 L2c review P1: advertise whether this node
  // has lifted the single-device cap. This is the FINAL consumption boundary — the
  // runtime object is mutable and GatewaySession is a public export that accepts an
  // arbitrary runtime, so the construction-time interlock can be bypassed by mutating
  // runtime.multiDeviceFanout or handing GatewaySession a hand-built runtime. rez-chat
  // derives its sender behaviour from this advertised capability, so re-assert the
  // readiness interlock HERE (fail loud, never advertise fan-out while the F2/F3
  // release blockers are unbuilt), then AND it with the runtime's intent. Explicit
  // checks (no optional chaining) per repo policy.
  const requestedMultiDeviceFanout = Boolean(runtime && runtime.multiDeviceFanout === true);
  const multiDeviceFanoutReady = assertMultiDeviceFanoutReady(requestedMultiDeviceFanout);
  const multiDeviceFanout = requestedMultiDeviceFanout && multiDeviceFanoutReady;

  // Can this home carry a second device? Derived from the WIRING, never from a
  // config flag: a delegated device needs device.add committed under the
  // serializer's per-account lock AND an authority resolver for delegated
  // session admission, which fails closed without one. Both are constructed
  // only on a pg home, so their presence IS the capability — a flag could
  // advertise linking on a node that cannot perform it, which is precisely the
  // failure being fixed (rez-chat#3, rez-node#2).
  //
  // Both are required, not either: with the serializer alone the ceremony
  // commits a device that can then never authenticate.
  const canSerializeDeviceMutations = Boolean(
    runtime && runtime.accountMutationSerializer
    && typeof runtime.accountMutationSerializer.submitMutation === "function",
  );
  const canResolveDelegatedAuthority = Boolean(
    runtime && runtime.accountAuthorityRevocationCache
    && typeof runtime.accountAuthorityRevocationCache.resolveDelegatedSnapshot === "function",
  );
  const delegatedDevices = canSerializeDeviceMutations && canResolveDelegatedAuthority;

  let capabilities;
  try {
    capabilities = new SessionCapabilities({
      contractVersion: CONTRACT_VERSION,
      deviceId: identity && identity.deviceId != null ? identity.deviceId : deviceId,
      localInboxId: "",
      capabilities: [],
      bootstrapRelays,
      bootstrapSeeds,
      meshMode: meshStatus && meshStatus.mode ? meshStatus.mode : null,
      durableInbox,
      multiDeviceFanout,
      delegatedDevices,
    });
  } catch (err) {
    const logger = runtime && runtime.logger ? runtime.logger : null;
    if (logger && typeof logger.warn === "function") {
      logger.warn("session bootstrap capabilities fallback", err && err.message ? err.message : err);
    }
    capabilities = new SessionCapabilities({
      contractVersion: CONTRACT_VERSION,
      deviceId: identity && identity.deviceId != null ? identity.deviceId : deviceId,
      localInboxId: "",
      capabilities: [],
      durableInbox,
      multiDeviceFanout,
      delegatedDevices,
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
