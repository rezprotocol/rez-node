import { RRecord, RCapability } from "@rezprotocol/core";

const MAX_CAPS = 64;
const MAX_BOOTSTRAP_RELAYS = 512;
const MAX_BOOTSTRAP_SEEDS = 128;

class BootstrapRelayHint extends RRecord {
  constructor({ id, host = null, port = null, url = null, transport = null } = {}) {
    super();
    this.id = String(id || "").trim();
    this.host = host == null ? null : String(host);
    this.port = port == null ? null : Number(port);
    this.url = url == null ? null : String(url);
    this.transport = transport == null ? null : String(transport);
    if (this.constructor === BootstrapRelayHint) this._seal();
  }

  validate() {
    this.assert(this.id.length > 0, "bootstrapRelays.id must be non-empty string");
    if (this.host != null) this.assert(typeof this.host === "string", "bootstrapRelays.host must be string when provided");
    if (this.port != null) this.assert(Number.isInteger(this.port) && this.port > 0, "bootstrapRelays.port invalid");
    if (this.url != null) this.assert(typeof this.url === "string", "bootstrapRelays.url must be string when provided");
    if (this.transport != null) this.assert(typeof this.transport === "string", "bootstrapRelays.transport must be string when provided");
  }
}

export class SessionCapabilities extends RRecord {
  constructor({
    contractVersion,
    deviceId,
    localInboxId,
    capabilities = [],
    bootstrapRelays = [],
    bootstrapSeeds = [],
    meshMode = null,
    durableInbox = false,
    multiDeviceFanout = false,
    delegatedDevices = false,
  } = {}) {
    super();
    this.contractVersion = contractVersion == null ? null : Number(contractVersion);
    this.deviceId = deviceId == null ? "" : String(deviceId);
    this.localInboxId = localInboxId == null ? "" : String(localInboxId);
    // D2 negotiation: when true, this node is a durable-inbox (pg-cluster) node,
    // so the client uses the cursor model (mailbox.cursorAck, dedup on seq)
    // instead of the legacy delete-ack (mailbox.ack) model. Defaults false so
    // fs/desktop nodes and shipped clients keep today's behavior unchanged.
    this.durableInbox = durableInbox === true;
    // E6 multi-device gate negotiation: when true, this node has lifted the
    // single-device cap (maxDevices > 1), so a NEW device cursor is created ONLY
    // by a proven device.bind — the legacy claim no-ops the cursor. The client
    // must therefore treat device.bind as a readiness requirement (Audit R2 #6),
    // not a best-effort backfill. Defaults false so gate-closed / fs / desktop
    // nodes keep the legacy claim-creates-cursor behavior unchanged.
    this.multiDeviceFanout = multiDeviceFanout === true;
    // Whether this home can carry a SECOND device at all — distinct from
    // multiDeviceFanout, which negotiates cursor semantics once a home already
    // has several. Device linking needs two things an fs/desktop node does not
    // have: an account-mutation serializer (to commit device.add under the
    // per-account lock) and an authority revocation resolver (delegated session
    // admission FAILS CLOSED without one — see GatewaySession, "absent ⇒ fail
    // closed"). Both are constructed only on a pg home.
    //
    // Advertised so a client can decline to OFFER device linking rather than
    // letting a user start a ceremony that cannot finish. Before this existed,
    // rez-chat showed "Link a new device" unconditionally and an fs home
    // answered with a 68-second timeout (rez-chat#3, rez-node#2).
    //
    // Defaults false: an older or hand-built runtime advertises no capability
    // and the client treats linking as unsupported, which is the safe reading.
    this.delegatedDevices = delegatedDevices === true;
    this.capabilities = Array.isArray(capabilities)
      ? capabilities.map((cap) => cap instanceof RCapability ? cap : RCapability.fromJSON(cap))
      : [];
    this.bootstrapRelays = Array.isArray(bootstrapRelays)
      ? bootstrapRelays
        .map((relay) => normalizeBootstrapRelay(relay))
        .filter(Boolean)
        .map((relay) => new BootstrapRelayHint(relay))
      : [];
    this.bootstrapSeeds = Array.isArray(bootstrapSeeds)
      ? bootstrapSeeds.map((seed) => String(seed || "").trim()).filter(Boolean)
      : [];
    this.meshMode = meshMode == null ? null : String(meshMode);
    if (this.constructor === SessionCapabilities) this._seal();
  }

  validate() {
    if (this.contractVersion != null) {
      this.assert(Number.isFinite(this.contractVersion), "contractVersion must be finite when provided");
      this.assert(Number.isInteger(this.contractVersion), "contractVersion must be integer when provided");
      this.assert(this.contractVersion >= 0, "contractVersion must be >= 0");
    }
    this.assert(this.deviceId.trim().length > 0, "deviceId must be non-empty");
    // localInboxId is bound by inbox.claim after session.ready; on a fresh
    // session that has not claimed an inbox yet, this is an empty string.
    this.assert(Array.isArray(this.capabilities), "capabilities must be an array");
    this.assert(this.capabilities.length <= MAX_CAPS, `capabilities length must be <= ${MAX_CAPS}`);
    for (const cap of this.capabilities) {
      this.assert(cap instanceof RCapability, "capabilities entry must be RCapability");
    }
    this.assert(Array.isArray(this.bootstrapRelays), "bootstrapRelays must be an array");
    this.assert(
      this.bootstrapRelays.length <= MAX_BOOTSTRAP_RELAYS,
      `bootstrapRelays length must be <= ${MAX_BOOTSTRAP_RELAYS}`,
    );
    for (const relay of this.bootstrapRelays) {
      this.assert(relay instanceof BootstrapRelayHint, "bootstrapRelays entry must be BootstrapRelayHint");
    }
    this.assert(Array.isArray(this.bootstrapSeeds), "bootstrapSeeds must be an array");
    this.assert(this.bootstrapSeeds.length <= MAX_BOOTSTRAP_SEEDS, `bootstrapSeeds length must be <= ${MAX_BOOTSTRAP_SEEDS}`);
    for (const seed of this.bootstrapSeeds) {
      this.assert(typeof seed === "string" && seed.trim().length > 0, "bootstrapSeeds entry must be non-empty string");
    }
    if (this.meshMode != null) {
      this.assert(this.meshMode === "seeded-gossip" || this.meshMode === "seed-only", "meshMode invalid");
    }
  }
}

function normalizeBootstrapRelay(relay) {
  if (!relay || typeof relay !== "object") return null;
  // One canonical field: `id` carries the relay identity. The former
  // `relay.relayKeyId` alias is gone — dual-named identity fields are how
  // validation gets bypassed (ADR-RELAY-IDENTITY inventory).
  const id = String(relay.id || "").trim();
  if (!id) return null;
  const out = { id };
  const host = String(relay.host || "").trim();
  if (host) out.host = host;
  const port = Number(relay.port);
  if (Number.isInteger(port) && port > 0) out.port = port;
  const url = String(relay.url || "").trim();
  if (url) out.url = url;
  const transport = String(relay.transport || "").trim();
  if (transport) out.transport = transport;
  return out;
}
