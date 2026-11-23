# Scalability, Performance and Fault Tolerance

## Payment idempotency, void and late authorization

The payment worker creates `PENDING_PAYMENT + INTENT` before calling the PSP and uses `order_id` as the stable idempotency key. Timeouts are `UNKNOWN`, never declines. A PSP's idempotency-window expiry is not evidence that an authorization cannot still land.

The implemented mock PSP provides definitive void-by-key tombstoning: after a successful void tombstone, any late/retried authorization for that same key is refused. Unit tests cover that contract. Production PSP selection must independently verify and contract-test equivalent semantics before using void as a release boundary; the mock is not evidence that every real PSP supports tombstoning.

For PSPs that cannot guarantee tombstoning, an unresolved `UNKNOWN` attempt remains `PAYING`; its inventory is not released. Recovery continues through signed webhook events, per-key status lookup and complete PSP-ledger discovery. Release is allowed only after a definitive decline/void result or a provider-guaranteed authorization-finality boundary proves that no late authorization can land. If no such boundary exists, keep admissions paused for the affected item and require provider/operator resolution. This intentionally prefers temporary underselling to duplicate charge or sale.

Network/timeouts and ambiguous outcomes prefer temporary under-selling. A unique-unit database guard loser is a separate correctness case: it is terminally `FORFEITED` and the unit is not re-offered by that losing hold. Never describe that deliberate quarantine as a temporary stranded hold.

## Fencing and replay-safe order transitions

`CONFIRMED` and `CANCELLED` are absorbing order states. Worker redelivery checks terminal order/payment state before any PSP call. Every order/payment transition increments the aggregate `orders.version` in the same transaction; KV projections apply only strictly newer versions. Every worker write transaction locks `region_leases` first with `SELECT ... FOR SHARE`, validates owner/expiry/epoch, then locks order/payment rows and commits while holding the share lock. Region takeover uses `SELECT ... FOR UPDATE`, increments the epoch and commits after prior share holders finish; stale-epoch writes then fail validation. Never hold a database transaction open across a PSP call; revalidate the epoch in a new short transaction before persisting its result. PSP request metadata carries the fencing epoch so regional ledger recovery can identify attempts issued by an old writer.

## Local standby-ACK test limitation

The default laptop Compose profile has one PostgreSQL instance and cannot prove remote synchronous replication. The prototype uses a `StandbyAckGate` backed by a separate control-plane health key to test that confirmations, KV `CONFIRMED` projections and new admissions pause when standby ACK is unavailable. A separate PostgreSQL primary/standby integration profile is required to test actual synchronous replication configuration and WAL-flush acknowledgement.

## Regional writer lease renewal

`region_leases.owner_id` is the logical active region/cluster writer identity shared by all worker replicas (for example `us-east-1-primary`), never an individual worker pod hostname. One leader-elected `lease-manager` controller is active per home-region writer lease; replacement instances may run in both AZs. The controller renews at `lease_expires_at - renewal_lead_time` (configurable; target approximately one-third of lease duration) with a conditional update matching `home_region`, logical `owner_id`, current `epoch` and an unexpired lease:

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

Exactly one row must be returned. Zero rows, database uncertainty beyond the configured safety margin, or a missed renewal deadline means self-fence: stop new writes, mark writer/standby health unavailable in CTRL, pause admissions and confirmation, and re-establish ownership before resuming. Never revive an expired lease through the renewal path. The renewal loop is independent of order-worker execution; no database transaction is held during a PSP call.

## Regional failover and split-brain scope

The per-region `region_leases` row and epoch protect writes in the authoritative promoted PostgreSQL history; they are not a global fence when a network partition leaves two independently writable regional primaries. The accepted recovery procedure covers confirmed region loss or controlled promotion only after the old primary is independently verified unreachable or infrastructure-fenced. Synchronous remote standby ACK prevents customer-visible confirmation while the remote ACK is missing, but does not alone prevent local-only intent writes or an already-in-flight PSP request in an isolated old region. Do not promote merely because a timeout elapsed or a local standby lease row has a higher epoch. A dual-primary partition requires an external/consensus fencing authority or infrastructure fencing that makes the old primary unable to write or call the PSP. That split-brain scenario is explicitly outside the current prototype; tests cover region loss and controlled promotion, not both regions remaining independently writable.

## Availability during deployments and dependency failures

The service-level availability objective is 99.95% over the agreed measurement window. Sustaining it during deployments requires pre-warmed N+1 capacity, stateless replicas across both home-region AZs, readiness-gated rolling/canary rollout, connection-pool and worker bulkheads, disruption budgets, backward-compatible schema/event changes, and rollback on error-budget burn or admitted-user SLO regression. Do not deploy all replicas or a single owner role at once; lease ownership must transfer with fencing and the previous owner must stop before the replacement acts. PSP webhook ingestion, confirmation/release for existing holds and order-status reads retain priority during a drop. If a deployment cannot preserve the capacity or correctness gates, pause the rollout rather than spend the error budget blindly.

The concrete reject/degrade order is: **503 unauthenticated traffic**; **429 non-essential traffic** (recommendations, beacons, search and other optional endpoints); **serve stale freshness** for cacheable catalog/availability data; then **slow or pause new queue admissions**. Preserve checkout for customers with valid holds, PSP webhooks, payment resolution, confirmation and safe release. `PAUSED` is not `SOLD_OUT` and preserves queue position.

Scale-time goals are explicit: the edge waiting room engages in under 10 seconds after an unscheduled spike trigger; pre-warmed origin capacity reaches the admitted-origin safe cap in under 60 seconds. Measure the latter against admitted-origin work, not the 500K/s edge-poll stream.


## Prototype verification boundary

The Go unit/race suite and in-memory end-to-end demonstration are executable locally. Docker-backed Redis Cluster, PostgreSQL and Redpanda integration tests are defined under `prototype/tests/integration/` but were not run in this environment because Docker is unavailable. The Compose profile simulates standby ACK health with `StandbyAckGate`; it does not prove remote synchronous PostgreSQL replication.


## Regional recovery operation

The repository includes `cmd/reconcile`, which requires an operator attestation that the old primary is fenced, uses the promoted logical writer/epoch, sets a persistent recovery pause in dedicated CTRL, scans the PSP ledger, repairs DB/Redis state and clears the pause only after complete success. It does not perform cloud fencing or database promotion. The local Compose profile still simulates standby ACK and cannot prove real cross-region PostgreSQL replication.
