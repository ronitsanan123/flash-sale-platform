# ADR 0004: Kafka API event log and per-order partitioning

- **Status:** Accepted
- **Context:** Inventory mutations and checkout requests must be processed asynchronously with ordering for one order, horizontal worker scale and crash recovery.
- **Decision:** Use Redpanda (Kafka API) with 12 initial partitions and `order_id` as the partition key. Delivery is at-least-once. Consumers use idempotent DB/PSP/inventory transitions, bounded inline retry, durable retry/requeue, then DLQ. Commit source offset only after durable success, durable retry publication or durable DLQ plus `UNKNOWN` state. The Redis Stream outbox relay publishes to Kafka and ACKs the Redis entry only after broker ACK; pending entries replay after relay restart.
- **Alternatives:** Direct Redis Streams end-to-end; a single FIFO queue; partition by hot `item_id`.
- **Consequences:** Per-order ordering and horizontal parallelism. A hot item does not collapse to one Kafka partition. Delivery is not exactly once; exactly-once effects rely on idempotency and durable state transitions. DLQ replay is audited and follows remediation, never blind auto-replay.
