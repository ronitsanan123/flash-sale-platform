-- PostgreSQL authoritative order/payment schema. Run on the home-region primary.
CREATE TABLE IF NOT EXISTS region_leases (
    home_region text PRIMARY KEY,
    owner_id text NOT NULL,
    epoch bigint NOT NULL CHECK (epoch > 0),
    lease_expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id text PRIMARY KEY,
    item_id text NOT NULL,
    unit_id text NOT NULL,
    user_id text NOT NULL,
    home_region text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING_PAYMENT','CONFIRMED','CANCELLED')),
    inventory_disposition text NOT NULL DEFAULT 'NONE'
      CHECK (inventory_disposition IN ('NONE','RELEASE','FORFEIT','SOLD')),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    fencing_epoch bigint NOT NULL CHECK (fencing_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- A cancelled loser may coexist with the durable owner, but a live unit has
-- exactly one order owner.
CREATE UNIQUE INDEX IF NOT EXISTS orders_one_owner_per_unit
    ON orders (item_id, unit_id) WHERE status <> 'CANCELLED';
CREATE INDEX IF NOT EXISTS orders_capture_sweep
    ON orders (updated_at, order_id) WHERE status = 'CONFIRMED';
CREATE INDEX IF NOT EXISTS orders_home_region_epoch
    ON orders (home_region, fencing_epoch, updated_at);

CREATE TABLE IF NOT EXISTS payments (
    order_id text PRIMARY KEY REFERENCES orders(order_id),
    status text NOT NULL CHECK (status IN ('NOT_ATTEMPTED','INTENT','AUTHORIZED','CAPTURED','FAILED','VOIDED','UNKNOWN')),
    psp_charge_id text,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_attempt_fencing_epoch bigint NOT NULL CHECK (last_attempt_fencing_epoch > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- A checkpoint is advanced only after every page through complete_through has
-- been reconciled and the resulting records are durable on the standby.
CREATE TABLE IF NOT EXISTS psp_ledger_scan_checkpoints (
    home_region text PRIMARY KEY,
    scan_from timestamptz NOT NULL,
    scan_to timestamptz NOT NULL,
    snapshot_id text,
    next_cursor text,
    complete_through timestamptz NOT NULL,
    fencing_epoch bigint NOT NULL CHECK (fencing_epoch > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (scan_to >= scan_from)
);
CREATE INDEX IF NOT EXISTS psp_ledger_checkpoints_epoch
    ON psp_ledger_scan_checkpoints (home_region, fencing_epoch);

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    run_id bigserial PRIMARY KEY,
    home_region text NOT NULL,
    fencing_epoch bigint NOT NULL,
    window_from timestamptz NOT NULL,
    window_to timestamptz NOT NULL,
    complete_through timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING','COMPLETE','INCOMPLETE','FAILED')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS regional_recovery_records (
    home_region text NOT NULL,
    idempotency_key text NOT NULL,
    psp_event_id text,
    action text NOT NULL CHECK (action IN ('ORPHAN_VOIDED','ORPHAN_FAILED')),
    source_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (home_region,idempotency_key)
);
