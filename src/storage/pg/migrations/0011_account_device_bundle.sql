-- 0011_account_device_bundle — the home-aggregated per-device prekey bundle store
-- (S2.5 S12, multi-device fan-out). Each of an account's devices self-publishes
-- its DevicePrekeyBundleV1 (self-contained + device-signed: deviceId + device
-- pubkey + inbox + monotonic prekeyVersion + prekeys) here, so that ANY device of
-- the account can fetch the WHOLE active device set and assemble the multi-device
-- DeviceSetRecordV1 it seals to a peer. Without this, a publishing device only
-- knows its OWN key/inbox and can never enumerate its siblings (their pubkeys +
-- self-signed prekey bundles are needed for the peer to X3DH to each device).
--
-- ACCOUNT-BLINDNESS BOUNDARY: same deliberate carve-out as account_device_registry
-- (0009) — an account that opts into multi-device hosting stores here; free
-- single-device claims never do, and this table is NEVER joined to inbox_claims.
-- SSOT: the authoritative active-device SET is account_device_registry (status);
-- this table only caches each device's self-published bundle, keyed the same way.
-- listActiveBundles JOINs the registry so a revoked device's stale bundle is never
-- served.
--
-- `prekey_version` is monotonic per device (a refresh replaces the row only with a
-- version >= the stored one — a stale republish cannot downgrade the live bundle).

CREATE TABLE IF NOT EXISTS account_device_bundle (
  account_identity text        NOT NULL,        -- B-sign public key (base64)
  device_id        text        NOT NULL,        -- self-certifying rez:dev:sha256(devicePub)
  prekey_version   bigint      NOT NULL DEFAULT 0,
  bundle_json      jsonb       NOT NULL,         -- the full DevicePrekeyBundleV1 (device-signed)
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, device_id)
);
