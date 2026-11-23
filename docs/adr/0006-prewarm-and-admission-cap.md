# ADR 0006: Pre-warm capacity and enforce an admission cap

- **Status:** Accepted
- **Context:** CPU and conventional autoscaling are lagging signals during a 30-second ramp. A 500K edge poll spike must not become a 500K origin spike.
- **Decision:** Pre-warm stateless replicas, DB pools, Redis, Kafka partitions and caches at T-30 minutes for scheduled drops. For unscheduled drops, engage the edge queue in <10 s based on origin RPS, in-flight concurrency, queue age and error/saturation thresholds. The safe admitted-origin cap is calculated from reservation capacity, inventory and full PSP call budget (authorize, capture, void and query). Scale warm origin capacity to that cap <60 s; admission remains the hard limit.
- **Alternatives:** Autoscaling-only; overprovision for the raw edge peak; CPU-only autoscaling.
- **Consequences:** Higher idle cost but bounded origin load and predictable p99. Admission may slow/pause when dependencies are unsafe. CPU remains a secondary signal, not the primary trigger.
