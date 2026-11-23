# Live Drop Incident Runbook

## Roles

- **Incident commander (IC):** owns decisions, timeline and escalation.
- **Drop operator:** owns item admission/queue/rate flags.
- **Inventory lead:** checks holds, unit conservation, Redis and database uniqueness.
- **Payments lead:** owns PSP breaker, UNKNOWN outcomes, void/capture and ledger reconciliation.
- **Platform lead:** owns WAF/CDN, gateway, pools, workers, Kafka and capacity.
- **Communications lead:** customer status page, in-queue messages and support guidance.

## Pre-drop checklist (T-30 minutes)

1. Confirm item home region, writer identity/epoch and lease manager healthy.
2. Confirm inventory seeded; compare `available + held + paying + confirmed + forfeited` to total.
3. Confirm synchronous standby is streaming and remote WAL ACK health is fresh.
4. Pre-warm app replicas, worker capacity, DB pools, Redis, Kafka partitions and caches.
5. Confirm PSP quota/budget includes authorize, capture, void and status queries.
6. Verify WAF/bot rules, token signing key set and key-rotation overlap.
7. Run health checks, a small synthetic checkout, reconciliation and pause/resume drill.
8. Confirm queue messaging, status page, escalation contacts and rollback operator.

## What to reject first / graceful degradation order

1. **503 unauthenticated/invalid-token traffic** and obvious bot traffic at the edge.
2. **429 non-essential traffic** (recommendations, search expansion, beacons, non-critical writes) with `Retry-After`.
3. **Stale freshness:** serve stale catalog/availability buckets and cached counts; never use cache to allocate inventory.
4. **Slow/pause the queue:** lower admissions according to PSP, stock, DB pool, standby ACK and worker lag; preserve existing queue position.

Never shed payment webhooks, recovery/confirm/release for existing holds, or status reads for customers already in checkout. Never release a `PAYING` hold because a timer elapsed.

## Kill switches and flags

- `pause_admissions(item)` — stops new tokens, preserves positions.
- `set_admission_rate(item, rps)` — adjusts the per-item head rate.
- `pause_reserve(item)` — stops new holds; keep checkout/status for existing holds.
- `pause_checkout_new(item)` — use only under IC + payments lead approval; do not disturb already-`PAYING` holds.
- `degrade_nonessential(true)` — shed optional features.
- `hold_timer_freeze` — documented production control for planned PSP incident handling; not part of the local prototype unless explicitly enabled.
- `stop_item(item)` — terminal operator kill switch; requires inventory/payment reconciliation before reopening.

## Live incident playbooks

### PSP 429, timeout or lost response

1. PSP lead confirms breaker state and per-call token bucket (authorize/capture/void/query).
2. Reduce admissions to effective PSP capacity; open breaker pauses new admissions.
3. Treat timeout/5xx as `UNKNOWN`, query by the same idempotency key, and never mint a new key.
4. Keep affected holds `PAYING`; reaper emits `HoldPayOverdue`, never releases them.
5. If PSP supports definitive void-by-key tombstoning, void first and persist proof before release. Otherwise keep UNKNOWN/PAYING until definitive provider outcome/finality.

### Standby ACK lost

1. Admission controller sets gate PAUSED (not SOLD_OUT); waiting-room users retain position and receive no token.
2. Stop customer-visible confirmation. Leave authorized orders pending with `PAYING` holds.
3. Verify primary/standby replication and network health. Do not weaken `synchronous_commit` to get throughput.
4. Restore standby ACK, verify lease/fencing epoch, reconcile DB/PSP/Redis, then resume gradually.

### Redis shard / control-plane failure

1. If inventory shard is unknown/unavailable, pause admissions for that item; never allocate from a cached count.
2. If CTRL is unavailable or stale, hold last watermark and fail closed for admission; CDN browse remains open.
3. Do not remap a bound `hold_id` to another shard. Rebuild inventory from durable DB and PSP truth before reopening.

### Kafka/worker outage

1. Pause admissions if outbox age, consumer lag or oldest event age exceeds the safe bound.
2. Restart worker/relay; pending Redis Stream entries are replayed, and consumer operations remain idempotent.
3. Commit offsets only after durable handler success, retry/requeue or DLQ publication.
4. DLQ replay is audited and only after the cause is fixed.

### Region loss

1. Confirm the old primary is unreachable or infrastructure-fenced. Per-region lease rows are not a global split-brain fence.
2. Promote standby with a higher epoch; old-epoch writes must fail.
3. Keep admissions paused. Read the standby-durable PSP ledger scan checkpoint.
4. Scan PSP ledger by home-region and bounded window with stable pagination/complete-through; discover missing order IDs from metadata.
5. Reconcile every discovered key with PSP and standby DB; preserve only proven durable CONFIRMED records; void orphan authorizations; repair inventory.
6. Reopen only after full scan, settlement grace, reconciliation and lease health.

## Manual scale-up

Scale reservation/API replicas and order workers independently. Watch admitted RPS per pod, in-flight concurrency, database pool waiters, Redis script latency, outbox age, Kafka oldest-event age and PSP token-bucket utilization. Do not increase admission merely because more pods are ready; the PSP and durable confirmation path remain the cap.

## Communications templates

**In queue:** “Demand is higher than the safe checkout rate. Your place is being preserved. We will admit customers as capacity becomes available.”

**Payment processing:** “Your reservation is held while payment confirmation is processing. Please do not submit another payment. You can check the same order-status link for updates.”

**Temporary pause:** “We have temporarily paused new admissions while we restore a payment/inventory dependency. Existing reservations remain protected.”

**Sold out:** “This item is sold out. No further admissions will be issued.” Use only after terminal inventory state is proven; never use this message for PAUSED/ALL_HELD.

## Recovery closeout

Record timeline, impact, admitted-user SLO, max queue age, PSP unknown/orphan count, inventory conservation, writer epoch, recovery duration and actions. Reconcile all orders and payments; preserve logs/traces; create follow-up actions with owners. Do not close while unresolved PSP UNKNOWN or inventory ownership remains ambiguous.

## Regional recovery CLI

After independent old-primary fencing and standby promotion, update the environment to the promoted logical writer ID and higher epoch. The `cmd/reconcile` executable first writes a persistent regional recovery pause to the dedicated CTRL Redis service, then runs the stable paginated PSP ledger scan, verifies the standby-durable checkpoint, reconciles PostgreSQL/PSP and repairs Redis inventory. It clears the pause only after every gate succeeds. Any failure leaves admissions and confirmations paused. Example:

```bash
docker compose --profile app run --rm --entrypoint /usr/local/bin/reconcile worker \
  --old-primary-fenced \
  --from '2026-11-22T00:00:00Z' \
  --failover-detected-at '2026-11-23T00:00:00Z' \
  --settlement-grace 2m --overlap 2m --page-size 100
```

`--old-primary-fenced` is an explicit operator attestation. The command does not itself prove the old region is dead or perform database promotion. `region_leases` must already have the promoted owner and higher epoch. The recovery pause has no automatic TTL; only a successful recovery clears it.
