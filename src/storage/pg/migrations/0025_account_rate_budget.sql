-- 0025_account_rate_budget — a CLUSTER-WIDE per-account request budget (audit leaf-3c F3).
--
-- The per-node SlidingWindowRateLimiter in PropagationOutboxHandler bounds op frequency against
-- ONE node. The durable resource (the lease) is already cluster-serialized by the one-leased
-- partial unique index from 0019, so no amount of request volume can produce a second lease — but
-- REQUEST RATE was never bounded across the cluster. Behind a non-sticky load balancer an
-- authorized device can spread traffic over every node and multiply its effective ceiling by the
-- node count, spending node CPU and Pg round-trips on ops that are all going to lose the lease
-- race anyway.
--
-- This table is the shared counter. It lives in Pg rather than Redis deliberately: the ops it
-- guards already require Pg (it is the outbox's own database), so this adds no new infrastructure
-- dependency and no new failure mode — whereas Redis is optional in this deployment (liveness bus
-- only) and would have made the budget unavailable exactly where the outbox still worked.
--
-- FIXED window, not sliding. One upsert per request keeps the guard cheap on the hot path; the
-- cost is that a burst straddling a boundary can reach up to 2x the ceiling within one window
-- length. That is acceptable for an amplification bound (the point is that the ceiling does not
-- scale with node count) and is NOT acceptable for anything requiring exactness — do not reuse
-- this table for quota accounting that must be precise.

CREATE TABLE IF NOT EXISTS account_rate_budget (
  account_identity text        NOT NULL,
  bucket           text        NOT NULL,   -- which budget (e.g. 'outbox_lease'), so buckets cannot rob each other
  window_start_ms  bigint      NOT NULL,   -- floor(now / windowMs) * windowMs, computed by the caller's clock
  count            integer     NOT NULL DEFAULT 0,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_identity, bucket, window_start_ms)
);

-- Bound the counter so a sustained flood cannot overflow the column (the code also clamps).
ALTER TABLE account_rate_budget
  DROP CONSTRAINT IF EXISTS account_rate_budget_count_bounded;
ALTER TABLE account_rate_budget
  ADD CONSTRAINT account_rate_budget_count_bounded CHECK (count >= 0 AND count <= 1000000000);

ALTER TABLE account_rate_budget
  DROP CONSTRAINT IF EXISTS account_rate_budget_window_positive;
ALTER TABLE account_rate_budget
  ADD CONSTRAINT account_rate_budget_window_positive CHECK (window_start_ms >= 0);

-- Sweep support: expired windows are deleted by age, and the index keeps that a range scan
-- instead of a full table scan as accounts accumulate.
CREATE INDEX IF NOT EXISTS account_rate_budget_window_idx
  ON account_rate_budget (window_start_ms);
