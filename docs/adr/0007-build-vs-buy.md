# ADR 0007: Build versus buy for the flash-sale platform

- **Status:** Accepted
- **Context:** The assignment includes global waiting-room, bot protection, inventory correctness, payment recovery and operational tooling. Building every edge component would distract from the correctness-critical checkout domain.
- **Decision:** Buy CDN, WAF/bot management and initially the virtual waiting-room service. Build the reservation core, hold state machine, payment orchestration, idempotency policy and reconciliation. Use managed Kafka-compatible log, Redis-compatible cache and PostgreSQL only where their documented semantics meet the required durability/failover contract. Analytics is managed and isolated from checkout.
- **Alternatives:** Build every component; buy a full ticketing/checkout platform; use a single database-centric application.
- **Consequences:** Faster time to market and stronger edge operations while retaining ownership of no-oversell/no-double-charge logic. Vendor contracts must be checked for token signing, queue outage behaviour, PSP idempotency/void semantics, stable ledger discovery and cross-region replication guarantees. Aurora Global Database is not selected for the synchronous RPO 0 confirmed-order path.
