# 1. Problem Understanding & Goals

## Situation and impact

The checkout platform normally handles approximately 2,000 requests/s. A high-demand drop can rise to 500,000+ requests/s within 30 seconds and remain elevated for 5–15 minutes. The last drop exposed four independent failure modes: p99 latency rose from 300 ms to 25 s, the database exhausted its connection limit, some customers were charged without a durable order, and a unit/seat was sold twice.

The peak is roughly 250× normal traffic. The dangerous part is not only aggregate throughput: clients retry together, all requests target one hot item, and reservation/payment writes are much more expensive than reads. Without traffic shaping, retries create a **thundering herd** that amplifies the original overload.

## Failure modes of the naive design

| Failure mode | Why the naive design fails | Result |
|---|---|---|
| Hot item/row | Every reservation decrements the same stock row or cache key | Lock queues, serialization and p99 collapse |
| Connection exhaustion | Each browse, poll and checkout consumes an origin/DB connection | Pool saturation and cascading timeouts |
| Thundering herd | Clients retry or poll on fixed intervals | Synchronized bursts and retry amplification |
| Retry storm | Multiple layers retry the same ambiguous operation | Duplicate work and PSP quota exhaustion |
| Charge without order | PSP call happens before durable order intent | Payment exists without a recoverable order |
| Double sale | Read stock → decide → write is not atomic across requests | Two customers receive the same unit |
| PSP ambiguity | Timeout is treated as failure even when authorization succeeded | Blind retry can create duplicate payment effects |
| Consumer replay | Worker crashes after side effect but before offset commit | At-least-once delivery repeats work |

## Goals and measurable targets

| Goal | Target / acceptance criterion | Scope |
|---|---|---|
| Edge absorption | Up to 10M concurrent waiters; ~500K waiting-room polls/s at the stated 20 s poll assumption | Edge/CDN, not origin |
| Admission | Initial modeled safe cap ~1,750 admitted users/s, dynamically reduced by PSP and service health | Origin traffic |
| Reservation latency | p99 < 500 ms | Successful reserve and checkout-accept requests |
| Availability | 99.95% monthly target (about 21.9 minutes/month error budget) | Customer-facing sale path; explicitly degraded during a safety-critical partition |
| Oversell | Zero duplicate live `(item_id, unit_id)` owners | Strict invariant |
| Duplicate charge | At most one PSP authorization/capture effect per order idempotency key | Strict invariant, PSP contract dependent |
| Order-before-payment | Durable `PENDING_PAYMENT + INTENT` exists before PSP authorization | Strict ordering |
| Confirm durability | Customer-visible `CONFIRMED` only after primary + remote synchronous standby WAL-flush ACK | RPO 0 for acknowledged confirmations |
| Scale-out time | Edge waiting room engages <10 s; warm origin capacity reaches safe admitted-origin cap <60 s | Design target, measured in game day |
| Recovery | AZ RTO <60 s; regional sale-path RTO <15 min (target; requires fencing + reconciliation) | Production target, not local-demo proof |

The system does **not** claim that a laptop prototype can generate 500K origin RPS. It demonstrates correctness under contention and injected failures; a capacity model explains why the origin sees only the admitted rate.

## Strict versus degradable guarantees

**Strict:** no unit has two live owners; no order is confirmed before successful authorization and remote durability; PSP calls are idempotent; ambiguous payment outcomes never release inventory; stale epochs cannot mutate authoritative PostgreSQL state; `PAYING` holds are never timer-released.

**Degradable:** queue position may be approximate; cached availability counts may be stale by one second; catalog freshness may be reduced; analytics may lag; non-essential recommendations/search/beacons may be shed; admission rate may fall to zero while the system preserves payment/inventory correctness.

## Capacity assumptions

Assignment-provided assumptions are 10M concurrent waiting users, 50K inventory units for one hot item, 50:1 read:write ratio and third-party PSP limits/timeouts. Additional assumptions and derivations are enumerated in [`capacity-model.md`](capacity-model.md). The PSP limit of 2,000 calls/s and initial authorization budget of 1,400 calls/s are modeling assumptions, not externally verified provider facts.
