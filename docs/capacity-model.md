# Capacity Model and Explicit Assumptions

## Assignment-provided inputs

| Input | Value |
|---|---:|
| Normal traffic | ~2,000 requests/s |
| Spike | 500,000+ requests/s within 30 seconds |
| Drop duration | 5–15 minutes |
| Waiting room | up to 10 million concurrent users |
| Hot item inventory | 50,000 units |
| Read:write ratio | ~50:1 |
| Reservation p99 target | <500 ms |
| Availability target | 99.95% |

## Topology assumptions

| Topology | Design assumption |
|---|---|
| Home region | Two availability zones (AZ a and AZ b), each with app and worker replicas |
| Disaster recovery | One separate standby region with PostgreSQL synchronous standby for confirmed-order durability; the Kafka event-log replica is asynchronous |
| AWS database reference | Self-managed PostgreSQL 16 on EC2 with a named remote synchronous standby (`synchronous_standby_names`, `synchronous_commit=on`); Aurora Global Database is excluded from the RPO 0 confirmation path because cross-region replication is asynchronous |
| Cross-region confirmation | PostgreSQL primary + one remote standby are both required (2-of-2 synchronous primary/standby replication, not a majority quorum); `synchronous_commit=on` waits for remote WAL flush |
| Partition behaviour | If the remote standby/WAN is unavailable, new confirmation commits pause; acknowledged confirmations retain the stated RPO 0 assumption |
| Inventory claim key | One Redis Cluster key per hold: `hold-claim:{base64url(hold_id)}`; the tag is hold-specific, not per-item, to distribute claims across slots |
| Bound-shard drain | If an any-unit shard drains between stock probe and reserve, keep the immutable claim and return `SOLD_OUT` for that hold ID; do not remap it |
| Admission control state | Dedicated replicated HA control-plane keyspace/partition, isolated from per-item inventory shard keys |

## Added assumptions (must be validated)

| Assumption | Initial value | Why |
|---|---:|---|
| Waiting-room poll period | 20 s average, jittered | converts 10M waiting users to about 500K edge polls/s |
| PSP global quota | 2,000 calls/s | illustrative provider constraint, not given by assignment |
| PSP quota reserved for authorizations | 70% = 1,400 calls/s | leaves budget for captures, voids, status queries and webhooks |
| Hold-to-payment conversion | 80% | illustrative; measure by event type |
| Safe admission rate | min(1,400 / 0.80, inventory and reservation caps) = 1,750/s | keeps expected auth rate below reserved quota |
| Availability cache TTL | 1 s | balances freshness and read absorption |
| HELD TTL | 5 min in production proposal | allows customer checkout time; demo can shorten |
| PAYING resolution budget | 2 min initial target | overdue means reconcile, never timer-release |
| AZ failover | <60 s target | requires game-day validation |
| Region recovery | <15 min target | requires fencing and rebuild drill |

## Derivations

- Poll load at the edge: `10,000,000 / 20 seconds = 500,000 polls/s`. These must be answered at the edge from cached/token state; forwarding each poll to origin defeats the architecture.
- PSP-safe admission: `0.70 × 2,000 / 0.80 = 1,750 admitted users/s`, assuming 80% eventually attempt payment. The actual formula must use measured conversion and a shared per-call token bucket for every PSP operation.
- Admission watermark states: `OPEN` when available units exist; `TEMPORARILY_UNAVAILABLE` when no units are free but HELD/PAYING units may be released after expiry or definitive payment resolution; terminal `SOLD_OUT` only when no available or recoverable units remain.
- Time to admit 50K units at 1,750/s is approximately `28.6 seconds` if every admission consumes one unit. Real admissions may include abandoned/failed checkouts; stock-aware admission should stop when no reservable units remain.
- 99.95% availability permits about `30 days × 24 × 60 × 0.0005 = 21.6 minutes` unavailable per 30-day month.

## Limits

These are planning calculations, not benchmark results. The prototype must report measured p50/p95/p99 and throughput on its actual test hardware. Do not infer 500K RPS origin capability from 500K edge polls/s.
