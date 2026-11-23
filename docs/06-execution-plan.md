# 6. Execution Plan

## Rollout principles

No phase is promoted based on a green unit test alone. Each phase requires threat-model review, shadow traffic or replay, an item-scoped canary, load testing at 1.5× the last peak **admitted-origin rate** (not CDN/waiting-room edge polls), reconciliation evidence and an explicit rollback plan. Do not run a production drop until the critical inventory/payment invariants have passed under injected failures.

## v0 — Before the next drop

- Buy/enable CDN, WAF, bot controls and a waiting-room product with a static conservative admission cap.
- Put order creation before PSP authorization; use one stable idempotency key per order and a definitive timeout/UNKNOWN process.
- Add database connection pooling and explicit pool arithmetic; remove per-request unbounded connections.
- Add inventory uniqueness constraint, payment/order reconciliation, pause-admissions and per-item kill switches.
- Add checkout status URL and customer messaging for delayed confirmation.
- Run shadow traffic replay, synthetic checkout and a small item-scoped canary; use the existing DB-backed path as the safety authority.

**Gate to v1:** no orphan authorization after reconciliation; no duplicate live unit owner; database pool saturation stays within budget; waiting room engages within the time target; rollback has been rehearsed.

## v1 — Reservation core and asynchronous pipeline

- Deploy Redis unit-token holds, immutable per-hold claims, TTL/reaper and Redis Stream outbox.
- Deploy Redpanda/Kafka API relay and partitioned order workers; use at-least-once delivery, bounded retries, requeue and DLQ.
- Add PSP bulkhead, per-call token bucket, circuit breaker, timeout and same-key UNKNOWN lookup.
- Add versioned KV status projections, PostgreSQL writer leases/fencing, standby-ACK gate and reconciliation.
- Pre-warm pools, replicas, consumer partitions and cache. Use leading signals: admitted RPS, in-flight concurrency, queue age, outbox age, DB pool waiters and PSP capacity—not CPU alone.
- Run AZ failover, PSP timeout/lost-response, Kafka pause and worker crash game days.

**Gate to v2:** shadow replay and item canary show zero duplicate unit owners/effects; all PAYMENT UNKNOWN scenarios remain locked until resolved; worker replay and failover tests pass; load test at 1.5× prior peak admitted-origin rate passes; measured p99 <500 ms for successful reservation/checkout-accept.

## v2 — Adaptive admission and multi-region operation

- Add remote-region edge/waiting-room/read serving and home-region write forwarding with item route and fencing epoch.
- Promote standby only after old-primary unreachability/infrastructure fencing; scan PSP ledger to a provider complete-through watermark, reconstruct/void orphans and reconcile before reopening.
- Add adaptive admission from available inventory, effective PSP capacity, standby ACK and downstream saturation.
- Automate scheduled pre-warm, failover drills, regional PSP ledger scans, reconciliation and auditable DLQ replay.
- Evaluate lottery/batch allocation for drops where product experience allows it.

**Gate to general availability:** repeated regional-loss game days meet RPO 0 for acknowledged confirmations and documented RTO; provider ledger recovery meets stable pagination/finality contract; security, runbook, SLO and deployment rollback review are signed off. True dual-primary network-partition protection requires an external/consensus fence and is not claimed by the current design.

## Time-to-scale goals

- Edge/waiting-room engagement <10 seconds after origin overload signal.
- Pre-warmed origin replicas and pools reach the configured safe admitted-origin cap <60 seconds after ramp starts.
- Admission rate decreases within one control-plane tick (target 1 second) when PSP/standby/stock health degrades.
- CPU is a secondary saturation signal; queue age, in-flight concurrency, per-pod RPS, pool waiters, outbox/consumer age and PSP quota are leading signals.

## Rollback and risk controls

Each rollout has item-scoped flags: pause new admissions, pause reserve, pause checkout for new holds, disable optional features, slow queue, and retain checkout/status for already-paying users. Never roll back by blindly releasing PAYING holds or deleting idempotency/tombstone records. Deploy stateless replicas with surge capacity and disruption budgets; deploy DB changes expand/contract; preserve backward-compatible event schemas during worker rollouts.
