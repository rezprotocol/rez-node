-- 0010_account_authority_journal — the serialized device-mutation authority
-- (S2.5 S11, findings F4+F5, OPEN-B resolved). The account's AUTHORITY HOME is
-- the serializer: a device submits a signed AccountDeviceMutationV1; the home
-- runs a per-account advisory-lock CAS, appends an immutable journal row, folds
-- the canonical device set (in account_device_registry, migration 0009), and
-- bumps a monotonic epoch. These three tables hold the epoch scalar, the revoked
-- capability-cert set, and the idempotency journal.
--
-- account_device_registry (0009) stays the canonical ACTIVE device set (the
-- serializer folds add/revoke there under the account lock). 0009 is unchanged.

-- The per-account monotonic authority epoch — the single counter that serves BOTH
-- the published DeviceSetRecordV1.revision AND the authority-state epoch, plus the
-- min-valid-issuedAt cutoff (a revoked-before-cutoff cert fails every verifier).
CREATE TABLE IF NOT EXISTS account_authority (
  account_identity        text        NOT NULL,
  epoch                   bigint      NOT NULL DEFAULT 0,   -- bumped on every committed mutation
  min_valid_issued_at_ms  bigint      NOT NULL DEFAULT 0,   -- feeds revocationState.minValidIssuedAtMs
  updated_at              timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity)
);

-- The revoked capability-cert set feeding revocationState.revokedCertIds. A
-- revoked leaf/parent cert kills the whole chain at every verifier (recursive
-- revocation falls out of verifyAccountAuthority: every ancestor is in the chain).
CREATE TABLE IF NOT EXISTS account_revoked_cert (
  account_identity  text   NOT NULL,
  cert_id           text   NOT NULL,   -- rez:cap: leaf/parent certId
  revoked_at_epoch  bigint NOT NULL,
  PRIMARY KEY (account_identity, cert_id)
);

-- The immutable mutation journal. (account_identity, op_id) is the idempotency
-- anchor: replaying a committed opId returns the same result_json (the full
-- {revision, devices, authorityState} committed at apply time). It is also the
-- audit log of every add/revoke that moved the account authority forward.
CREATE TABLE IF NOT EXISTS account_device_mutation (
  account_identity  text        NOT NULL,
  op_id             text        NOT NULL,
  epoch             bigint      NOT NULL,   -- the epoch this mutation produced
  action            text        NOT NULL,   -- 'device.add' | 'device.revoke'
  target_device_id  text,
  target_cert_id    text,
  result_json       jsonb       NOT NULL,   -- the deterministic committed result (idempotency payload)
  committed_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, op_id)
);
