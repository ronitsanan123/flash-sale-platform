# 4. Load Testing & Observability

## Test strategy

Testing is layered: pure unit tests, Redis/PostgreSQL/Redpanda integration tests, end-to-end flow tests, load profiles and scheduled game days. Shared-container integration tests run sequentially inside the Compose network; Redis Cluster tests must execute there so MOVED/ASK node addresses are reachable.

| Profile | Shape | Purpose | Pass criteria |
|---|---|---|---|
| Spike | 20 → configured admitted-origin rate over 30 s, hold 5–15 min, sharp drop | Validate waiting-room engagement, warm capacity and queue drain | Reservation p99 <500 ms for successful requests; no invariant violations; admission respects PSP/service caps |
| Soak | Stable representative admitted-origin rate for 30 min or longer | Detect leaks, unbounded streams, pool/session leaks and lag growth | Stable memory/pool/consumer state; no growing oldest-event age |
| Stress | Increase admitted-origin rate in steps until the limiter sheds | Find safe origin ceiling and verify graceful degradation | Non-essential work shed before checkout; 429/Retry-After on rate-limited work; no oversell/double charge |
| Chaos | Inject PSP timeout/lost response/429, kill workers, pause Redpanda, kill CTRL/KV, pause standby ACK | Prove recovery and fail-closed controls | Unknown payments remain locked; replay is idempotent; admissions pause when dependencies are unsafe |
| Regional recovery | Lose local-only intent and delayed async log record after PSP auth but before remote ACK | Test PSP-ledger discovery when order ID is unknown | Stable scan complete-through gate; orphan authorization voided or durable confirmation proven before admissions reopen |

The local deterministic `cmd/demo` uses in-memory adapters. It is a correctness demonstration, not a benchmark. k6 thresholds and production-capacity claims must only be published after running against a measured environment.

## SLIs and SLOs

| SLI | Definition | Target / alert |
|---|---|---|
| Reservation latency | p50/p95/p99 successful reserve and checkout-accept | p99 <500 ms |
| Admitted-user success | Successful customer sale-path requests / admitted customer requests | SLO aligned to 99.95% availability |
| Queue age | Oldest waiting entry age and queue depth | Alert on age and expected-traffic envelope, not raw waiter count alone |
| Admission health | Active/paused state, effective cap, stock state, standby ACK age | Any stale/unknown control state pauses admission |
| PSP health | Calls/s by operation, 429, timeout, unknown, breaker state, token-bucket utilization | Pause/reduce admission before quota exhaustion |
| Worker health | In-flight, active partitions, rebalance, retries, DLQ rate, oldest event age | Alert on age/lag and sustained retry rate |
| Inventory correctness | Duplicate live owners, unit conservation, guard trips, HELD/PAYING age | Any duplicate live owner is page-severity |
| Payment correctness | Authorization/capture effects per key, orphan authorizations, UNKNOWN age | Any duplicate effect or unresolved orphan beyond grace is page-severity |
| Database saturation | Pool in-use/waiters, transaction latency, lock waits, replica sync state | Alert before pool saturation; standby loss pauses confirms/admissions |
| Error budget | Multi-window burn rate on admitted-user SLI | Page on fast burn; ticket on slow burn |

## Observability implementation

Use structured logs with `request_id`, `order_id`, `hold_id`, `item_id`, `unit_id`, `home_region`, `fencing_epoch`, `partition`, `offset` and PSP idempotency key. Avoid card data, tokens and secrets. Trace the request across gateway, reservation, Redis script, outbox relay, Kafka, worker, PostgreSQL and PSP; never use high-cardinality IDs as metric labels.

Required metrics include `admissions_total`, `admission_paused`, `queue_wait_seconds`, `reservation_attempts_total`, `reservation_conflicts_total`, `oversell_guard_trips_total`, `holds{state}`, `hold_overdue_total`, `outbox_pending`, `outbox_oldest_age_seconds`, `consumer_lag`, `consumer_oldest_age_seconds`, `psp_calls_total{operation,result}`, `psp_unknown_total`, `psp_breaker_state`, `db_pool_in_use`, `db_pool_wait_seconds`, `standby_ack_healthy`, `reconciliation_orphans_total`, `duplicate_unit_owners` and error-budget burn.

OTel traces and Prometheus metrics are non-critical: telemetry failure must not block reservation, payment or confirmation. Sample high-volume successful reads; retain errors, payment ambiguity and correctness-guard events at full fidelity.

## Alerting during an intentional drop

Do not silence alerts because traffic is high. Evaluate the sale-path SLO against admitted users and expected-traffic envelopes. Raw edge polls and queue depth are capacity signals, not customer error rates. Use multi-window burn-rate alerts (fast page and slower ticket), and suppress only alerts whose conditions are explicitly expected and bounded by a drop-mode runbook—not the correctness or payment alerts.

## Game-day approach

Before a scheduled drop, run a game day with service owners, incident commander, communications lead and database/payment owners. Exercise: waiting-room vendor outage, WAF false positive, Redis shard unavailable, standby ACK lost, PSP 429/timeout/lost response, Kafka pause, worker kill during each state transition, stale lease manager, region loss and incomplete PSP ledger scan. Capture decisions, recovery times, user messaging, post-incident actions and evidence for each invariant.

## Run results honestly

For every run publish commit/version, host/container resource limits, profile, configured/admitted rate, actual throughput, p50/p95/p99, error/shed rate, PSP fault settings, worker lag, invariant counts and recovery duration. Distinguish in-memory demo measurements from Redis/PostgreSQL/Redpanda integration measurements and from production-scale capacity tests.
