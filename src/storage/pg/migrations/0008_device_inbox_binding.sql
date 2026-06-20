-- 0008_device_inbox_binding — record the PROVEN device key behind a device
-- cursor (S2.5 Slice 4). The cursor's `device_id` is self-certifying
-- (rez:dev:sha256(devicePublicKeyB64)); persisting the device public key itself
-- is the home's copy of the verified DeviceInboxBindingV1 — the per-device
-- address that resolves "device D receives at this inbox". A DeviceRevokeV1
-- targets the deviceId; this column lets the home hold (and audit) the actual
-- key material the binding vouched for, not only its hash.
--
-- Nullable: the legacy single-device claim path (session-hello deviceId, no
-- device-key proof) registers a cursor WITHOUT a bound key, so existing rows and
-- the fs/desktop path stay valid. A row gains a non-null key only via the
-- proven device.bind path (PgDurableInbox.registerDevice with devicePublicKeyB64).

ALTER TABLE device_cursors
  ADD COLUMN IF NOT EXISTS device_public_key text;
