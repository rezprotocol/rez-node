-- 0012_account_revoked_device — the durable device-revocation TOMBSTONE set
-- (S2.5 audit R4 F1). Revocation is TERMINAL per deviceId: once an account revokes
-- a device, that deviceId can never re-enroll (a real re-add uses a NEW deviceId,
-- because deviceId = rez:dev:sha256(devicePub), so "re-add" only ever means the
-- same key).
--
-- The registry (0009) records revocation as a `status='revoked'` row — but ONLY
-- for a device that was ENROLLED. An account-wide `device.revoke` can name a
-- device that was NEVER enrolled (revoke racing ahead of the sibling's first
-- device.add / device.bind). Before this table the revoke of a never-enrolled
-- device left NO durable trace, so a later device.add enrolled it ACTIVE —
-- resurrecting a revoked device (F1). This tombstone is that missing durable
-- trace: the enroll/add/bind paths consult it and refuse a tombstoned deviceId
-- even when no registry row exists.
--
-- DoS BOUNDARY (audit R4, Noah's tombstone warning). The AccountDeviceMutationV1
-- `device.revoke` target is a bare `rez:dev:` string with no proving pubkey (unlike
-- DeviceRevokeV1, whose revokedDeviceId is key-proven `= deviceIdFor(pub)`), so a
-- revoke-capable device could otherwise mint unlimited permanent tombstones. The
-- registry is the canonical invariant OWNER (L2c) and admits a NEVER-ENROLLED
-- revoke target only when it is both (a) syntactically canonical
-- `rez:dev:<64 hex>` (isCanonicalDeviceId — a non-canonical never-enrolled revoke
-- is REJECTED before any insert, since it could never enroll) and (b) under the
-- per-account count quota. Canonical syntax proves SHAPE only, not `= deviceIdFor(pub)`
-- (this record carries no pubkey to prove it). A tombstone for a genuinely ENROLLED
-- device is never quota-gated (a fail-close revoke must never fail) and is bounded by
-- the real device count. Tombstones are NEVER TTL'd/deleted — deletion would reopen
-- resurrection.

CREATE TABLE IF NOT EXISTS account_revoked_device (
  account_identity text        NOT NULL,        -- B-sign public key (base64)
  device_id        text        NOT NULL,        -- self-certifying rez:dev:sha256(devicePub)
  revoked_at_epoch bigint      NOT NULL,        -- the account authority epoch at revoke
  revoked_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, device_id)     -- terminal + idempotent (re-revoke is a no-op)
);

-- Count an account's tombstones (the per-account quota check) and enumerate them.
CREATE INDEX IF NOT EXISTS account_revoked_device_account
  ON account_revoked_device (account_identity);
