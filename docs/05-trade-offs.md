# 5. Trade-off Analysis

## Waiting-room fairness versus throughput

Strict global FIFO across 10M users is costly and gives refresh-spamming clients an advantage if arrival order is determined by repeated origin calls. The design uses an edge queue with signed tokens, a one-second cached watermark, server-controlled jitter and randomized order for the pre-drop cohort; arrivals after T0 can be FIFO within the admission service. Position is approximate and may degrade during cache/vendor failure. The system prioritizes bounded origin load and bot resistance over exact global fairness. Lottery/batch allocation remains a possible future mode for drops where seat choice is not required.

## Strong inventory consistency versus availability and latency

A hot item has one logical inventory owner per unit, represented by sharded unit tokens and atomic same-slot Lua scripts. PostgreSQL's partial unique index `(item_id, unit_id) WHERE status <> 'CANCELLED'` is an independent final guard. Under partition, sales pause rather than allocate from stale inventory. This can temporarily undersell or report a bound-shard attempt as sold out, but it prevents double sale. Availability counts and queue positions are cached/approximate; allocation is never based on cached counts.

## Synchronous versus asynchronous confirmation

Reservation and checkout acceptance are synchronous and fast; order intent, authorization, confirmation, capture and projection proceed asynchronously. The customer receives `202 Accepted` and polls a status URL. The order/payment intent is committed locally before PSP authorization. Customer-visible `CONFIRMED` waits for synchronous remote PostgreSQL standby WAL-flush acknowledgement (primary + one remote standby, 2-of-2). This increases confirmation latency and pauses confirmations when the WAN/standby is unhealthy, but gives RPO 0 for acknowledged confirmations. It is not a majority quorum. Intent rows that exist only in the failed home region are recovered from the PSP ledger; failover stays closed until discovery/reconciliation completes.

## Over-provisioning versus autoscaling

CPU is a lagging signal during a 30-second ramp and cannot create capacity quickly enough. Scheduled drops pre-warm application replicas, connection pools, Redis memory, consumers and partitions at T-30 minutes. Unscheduled spikes engage the edge queue when origin RPS, in-flight concurrency, queue age or error rate crosses thresholds; warm capacity is maintained for the agreed safe cap. Autoscaling handles sustained demand beyond warm capacity, but the admission controller remains the hard safety boundary. This costs more idle capacity than autoscale-only and reduces collapse risk.

## Build versus buy

| Capability | Decision | Rationale |
|---|---|---|
| CDN/WAF/bot controls | Buy managed edge service | Global absorption, bot intelligence and operational maturity |
| Virtual waiting room | Buy initially; retain portable token/admission contract | Avoid building a 10M-user queue while keeping admission policy portable |
| Reservation core | Build | Unit allocation, holds, forfeiture and no-oversell semantics are differentiating correctness logic |
| Payment orchestration/reconciliation | Build on PSP primitives | Idempotency, UNKNOWN resolution, void policy, remote-confirmation gate and ledger recovery are business-critical |
| Database/log/cache | Managed services where semantics fit | Reduce operations burden, but verify replication, consistency, connection and failover semantics rather than relying on product labels |
| Analytics | Buy/managed columnar store | Keep analytics off the checkout critical path |

## Additional trade-offs

- Per-hold immutable shard claims simplify replay and avoid cross-shard atomicity; a drained bound shard may return a false SOLD_OUT for that hold attempt rather than remap.
- At-least-once log delivery plus idempotent consumers gives exactly-once effects, not exactly-once message delivery.
- `FORFEITED` inventory is intentionally retired when the DB uniqueness guard detects a conflicting owner; this permanent undersell is safer than re-offering an ambiguous unit.
- PostgreSQL sync primary/standby does not prevent dual-writable split brain by itself. Automatic promotion assumes old primary unreachable or externally fenced; consensus/global fencing is outside the prototype.
- A PSP that cannot provide stable ledger discovery, retained metadata, complete-through watermark or definitive void may not support autonomous regional recovery. In that case fail closed until a cross-region durable pre-PSP journal exists.
