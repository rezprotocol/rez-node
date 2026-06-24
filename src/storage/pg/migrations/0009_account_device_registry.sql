-- 0009_account_device_registry — the account→device→inbox registry (S2.5 S7 /
-- audit F3, resolving OPEN-A). The EXPLICIT, OPT-IN linkage for multi-device-
-- hosted accounts: it maps an account identity (the B-sign public key) to each of
-- its enrolled (device, inbox) bindings, so the home can resolve ALL of an
-- account's device inboxes — the precondition for account-wide device revocation
-- (DeviceHandler today can only revoke against the caller's CURRENT inbox).
--
-- ACCOUNT-BLINDNESS BOUNDARY (deliberate, scoped exception).
--   The node is otherwise account-blind: `inbox_claims` keys on the claimant
--   pubkey and is NEVER joined to an account (CAPABILITY_MODEL §8-9). This table
--   is the one documented carve-out — an account that opts into multi-device
--   hosting enrolls here. It is a privacy-MODE decision, not a paywall, and:
--     - free single-device claims stay blind (they never enroll here);
--     - this table is NEVER back-filled from `inbox_claims` (no join of existing
--       blind rows into an account);
--     - at launch all of an account's device inboxes share ONE home/authority
--       domain (cross-home account coordination is a later consensus problem).
--
-- SSOT: the proven device key lives in `device_cursors.device_public_key`
-- (per-inbox, migration 0008). This registry holds only the account LINKAGE
-- (account ↔ device ↔ inbox + the authorizing capability cert + authority epoch +
-- status); it does not duplicate the key material.
--
-- `cert_id` is the leaf AccountDeviceCapabilityV1 certId that authorized the
-- device (NULL for a primary/direct-authority device that authenticates with the
-- B-sign key itself — it holds the authority rather than a delegated cert).
-- `authority_epoch` is the account's monotonic authority revision at enrollment /
-- status change (the canonical epoch source + the serialized mutation journal are
-- S11; this column carries the value, monotonic, never regressing).
-- `status` is 'active' | 'revoked'; a 'revoked' row is the account-wide record
-- that lets the home fail-close that device's inbox (home-enforced revoke, P1a).

CREATE TABLE IF NOT EXISTS account_device_registry (
  account_identity text        NOT NULL,        -- B-sign public key (base64)
  device_id        text        NOT NULL,        -- self-certifying rez:dev:sha256(devicePub)
  inbox_id         text        NOT NULL,         -- the device's receiving inbox
  cert_id          text,                         -- authorizing leaf certId; NULL = primary/direct
  authority_epoch  bigint      NOT NULL DEFAULT 0,
  status           text        NOT NULL DEFAULT 'active',
  enrolled_at      timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, device_id)
);

-- One inbox belongs to exactly one (account, device): a device's inbox cannot be
-- enrolled under a second account/device. Enforced as a DB uniqueness constraint
-- (atomic across the cluster, mirrors the inbox_claims PRIMARY KEY discipline).
CREATE UNIQUE INDEX IF NOT EXISTS account_device_registry_inbox
  ON account_device_registry (inbox_id);

-- Resolve an account → all its enrolled device bindings (account-wide revoke,
-- sibling-inbox lookup).
CREATE INDEX IF NOT EXISTS account_device_registry_account
  ON account_device_registry (account_identity);
