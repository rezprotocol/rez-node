-- 0015_backfill_revoked_tombstones — audit R4 F3-remediation round-5 finding 3. A device
-- with account_device_registry.status='revoked' is TERMINALLY revoked, but the durable
-- tombstone (account_revoked_device, migration 0012) has historically been written only by
-- the serializer's foldRevokeInTx. A revoked registry row that predates the tombstone table
-- (or was produced by a legacy cursor-only device.revoke that never tombstoned) therefore
-- has status='revoked' with NO tombstone, so an isTombstoned()-only reconnect check would
-- miss it. Session auth and the per-request delegated-dispatch guard now consume the
-- canonical `status='revoked' OR tombstoned` predicate, and this backfill makes the durable
-- terminal set complete + consistent so both predicates agree.
--
-- Idempotent: ON CONFLICT DO NOTHING (the tombstone PK is (account_identity, device_id)).
INSERT INTO account_revoked_device (account_identity, device_id, revoked_at_epoch)
  SELECT account_identity, device_id, authority_epoch
  FROM account_device_registry
  WHERE status = 'revoked'
  ON CONFLICT (account_identity, device_id) DO NOTHING;
