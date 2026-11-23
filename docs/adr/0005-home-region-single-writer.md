# ADR 0005: Home-region single writer and fenced failover

- **Status:** Accepted

## Context

Multi-region active-active inventory writes risk split brain and duplicate sale. Asynchronous replication alone can lose a customer-visible confirmed order during failover. Step-11 payment intent is locally WAL-durable only; if the home region is lost after PSP authorization but before the step-13 remote WAL-flush acknowledgement, the intent row and its `order_id` may be absent from the promoted standby.

## Decision

Each item has one home region. Other regions serve edge, waiting-room and read traffic; all writes are forwarded to the home gateway with a fencing epoch. APP services are stateless across AZs. Worker roles run in both AZs and acquire per-item, per-stream, per-shard, per-partition or periodic leases; a higher fencing epoch is required for takeover. `region_leases.owner_id` identifies the logical active region/cluster writer (for example `us-east-1-primary`) and is shared by worker replicas; it must never be a pod hostname. A leader-elected `lease-manager` controller, with replacement instances across AZs, renews the regional writer lease independently of order-worker loops. It renews at `lease_expires_at - renewal_lead_time` (configurable; target approximately one-third of lease duration) using a conditional update matching `home_region`, logical `owner_id`, current `epoch` and an unexpired lease. Exactly one returned row is required. Zero rows, DB uncertainty beyond the safety margin or a missed deadline triggers self-fencing and pauses writes/admissions; an expired lease is never revived by renewal. No database transaction is held during PSP network calls. The renewer itself is leader-elected: exactly one controller actively renews a given home-region writer lease, while standby controller instances may run in the other AZ. If the renewer process is replaced but the active region remains authoritative, the replacement renews with the same logical `owner_id` and epoch; worker pod churn does not alter the regional writer identity. Only regional takeover changes `owner_id` and increments the epoch.

Customer-visible `CONFIRMED` state is exposed only after the PostgreSQL synchronous primary/standby commit acknowledgement from one remote standby (`synchronous_commit=on`, named synchronous standby, remote WAL flush). This is a two-location 2-of-2 primary/standby requirement, not a majority consensus quorum. The AWS reference implementation is self-managed PostgreSQL 16 on EC2 (or another PostgreSQL deployment that explicitly supports cross-region synchronous remote WAL-flush acknowledgement), with `synchronous_standby_names` configured for the remote standby. Aurora Global Database is not selected for this path because its cross-region replication is asynchronous and does not provide this remote-WAL-flush commit gate.

The PSP is required to support two distinct recovery operations:

1. **Known-key lookup** by idempotency key for ordinary timeout, webhook and worker-retry resolution.
2. **Unknown-key discovery** through a complete, paginated authorization-ledger scan over a bounded time window. Each entry must expose the idempotency key/order ID, PSP event and charge IDs, status, event time, and retained `item_id`, `unit_id`, `user_id`, `home_region` and fencing-epoch metadata. Pages use a stable snapshot/cursor and expose a provider `complete_through` watermark. The PSP retains searchable records for at least the maximum regional recovery window.

REC continuously maintains a last-complete scan checkpoint during normal operation: it scans the PSP ledger on a schedule, reconciles entries, then persists the checkpoint with remote-standby durability. The checkpoint records home region, snapshot ID, scan bounds, provider `complete_through` and fencing epoch. Failover scans start from that standby-durable checkpoint minus a configured overlap and end after the failover cutoff plus PSP authorization-settlement grace. That grace must exceed the PSP's maximum in-flight authorization-processing delay plus clock-skew allowance; unresolved old-epoch attempts keep admissions paused. Pages are deduplicated by PSP event ID and idempotency key.

Regional recovery proceeds in this order:

1. Fence the old writer and acquire a higher fencing epoch.
2. Load the last standby-durable scan checkpoint. If none exists, scan from the earliest PSP-retained time that could contain an in-flight authorization; if retention cannot cover the window, fail closed.
3. Scan the bounded authorization window using one stable paginated snapshot. Filter returned metadata by home region and fencing epoch; do not rely on an optional provider-side metadata filter.
4. Require every page to be read and the provider `complete_through` watermark to cover the scan end. An empty page or end-of-pagination without that watermark is not proof of absence.
5. Perform individual key lookups for discovered IDs and IDs known from the promoted standby. Verify whether a durable step-13 `CONFIRMED` commit exists on the standby.
6. Reconstruct idempotent recovery records. Preserve only records with proof of durable remote confirmation; void authorizations without that proof.
7. Reconcile DB, PSP ledger and inventory. Persist the next checkpoint with synchronous standby durability only after the scan and reconciliation complete. Reopen admissions only after this gate succeeds.

If the PSP cannot provide complete ledger discovery, metadata retention and a completeness watermark, autonomous regional write failover is not safe. Keep admissions paused until an independently cross-region-durable pre-PSP attempt journal or equivalent recovery mechanism is deployed. The PSP ledger API contract and mock/test requirements are specified in `docs/contracts/psp-ledger-discovery.md`.

## Lease lifecycle and renewal

The regional writer lease has a bounded expiry and must be renewed continuously while the region is active. One leader-elected renewal controller is active per home-region writer lease; HA replacement controllers may run in both AZs, but all renew using the same logical writer `owner_id` and current epoch. The renewal update is a compare-and-set on home region, owner, epoch and unexpired lease. If it affects zero rows or cannot be confirmed before the safety deadline, the controller self-fences: no new order/payment writes are accepted, the control plane marks the writer/standby health unavailable, and admissions/confirmation pause until ownership is re-established. Renewal is independent of worker goroutines and is never held open across PSP I/O.

The renewal is a short compare-and-set, not a blind heartbeat:

```sql
UPDATE region_leases
SET lease_expires_at = clock_timestamp() + $duration,
    updated_at = clock_timestamp()
WHERE home_region = $home_region
  AND owner_id = $logical_owner_id
  AND epoch = $current_epoch
  AND lease_expires_at > clock_timestamp()
RETURNING lease_expires_at;
```

Exactly one row must be returned. A zero-row result, a DB error beyond the renewal safety margin, or a missed deadline self-fences the controller: stop accepting writes, publish writer-unhealthy to CTRL, pause new admissions/confirmations, and re-establish ownership. An expired lease is never revived by this renewal path. Renewal is independent of order-worker goroutines and cannot hold any database lock during PSP I/O.

## Split-brain boundary

The per-region `region_leases` row is not a global fencing authority when a network partition leaves two independently writable regional primaries. The accepted failover procedure covers a confirmed region failure or a promotion where the old primary is independently proven unreachable/fenced before the standby becomes writable. The 2-of-2 synchronous standby ACK blocks confirmation commits while the remote ACK is absent, but it does not by itself prove that an old primary cannot continue local-only intent writes or issue an already-in-flight PSP call. Therefore, do not promote solely because a timeout elapsed or because the standby's local lease row has a higher epoch. Confirm old-primary fencing through infrastructure control (or an external/consensus fencing authority) first. Fully automatic split-brain recovery with both regions independently reachable is outside this design/prototype; tests cover region loss and controlled promotion, not a dual-primary partition.

## Alternatives

- Active-active writes.
- Asynchronous failover with accepted non-zero RPO.
- Fail open during partition.
- Synchronously replicate a pre-PSP attempt journal before authorizing, as a fallback when the PSP cannot provide complete ledger discovery.

## Consequences

Writes can pause during partitions. Synchronous cross-region durability adds WAN latency and pauses confirmation commits when the remote standby/WAN is unavailable; this trade-off must be measured against the SLO. PSP ledger discovery is a production dependency, not an optional convenience. Normal operation periodically scans and reconciles the PSP ledger, then advances a remote-standby-durable checkpoint. Recovery scans overlap that checkpoint, deduplicate by PSP event ID and idempotency key, and advance it only after the provider watermark covers the scan end and reconciliation is complete. A single empty page or an incomplete scan is never evidence that no orphan authorization exists.

Do not reopen regional writes based on key lookups alone if PSP discovery guarantees are unavailable. Do not substitute Aurora Global Database while retaining the RPO 0/remote-WAL-ACK claim. Event-log replication may remain asynchronous only if missing events can be rebuilt from durable DB/outbox state before reopening.


## PSP void capability during regional recovery

PSP void semantics are capability-dependent. A provider may be treated as supporting definitive void-by-key tombstoning only when its contract guarantees that, after a confirmed void, a late/retried authorization using the same key cannot create an authorization effect. The mock PSP is required to implement and test this stronger behaviour.

If a production PSP lacks definitive tombstoning, an unresolved authorization remains `UNKNOWN` and its hold remains `PAYING`; do not cancel/release it merely because the idempotency window expired. Wait for a definitive webhook/status/ledger outcome or provider-guaranteed authorization-finality boundary. If no such finality guarantee exists, keep the affected item paused for operator/provider resolution. The unit is never returned to sale while a late authorization can still land.


## Fenced order/payment projection versions

Every order/payment aggregate transition increments `orders.version` once in the same transaction, even when both order and payment rows change atomically. Use one lock order everywhere: worker write transactions first acquire the matching `region_leases` row with `SELECT ... FOR SHARE`, verify the owner, unexpired lease and supplied epoch, then lock/update order and payment rows; hold the lease lock until commit. Takeover executes `BEGIN; SELECT ... FROM region_leases WHERE home_region=$1 FOR UPDATE; UPDATE region_leases SET owner_id=$2, epoch=epoch+1, lease_expires_at=$3, updated_at=now() WHERE home_region=$1; COMMIT;`. `FOR UPDATE` waits for in-flight worker transactions holding `FOR SHARE`; after takeover commits, all new old-epoch writes fail validation. Never lock order/payment rows before the lease row and never hold a database transaction open during a PSP call. Revalidate the epoch in a new short transaction before persisting a PSP result. The KV projection carries both order and payment status plus inventory disposition and accepts only a strictly newer aggregate version.
