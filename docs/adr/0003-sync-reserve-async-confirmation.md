# ADR 0003: Synchronous reserve and asynchronous confirmation

- **Status:** Accepted
- **Context:** Reservation-path p99 must stay below 500 ms during a 250× traffic spike, while payment and remote durability can be slower. Synchronous PSP calls on the customer request path previously contributed to 25 s p99 and connection exhaustion.
- **Decision:** Reserve a unit and lock checkout synchronously; checkout returns `202 Accepted` with a status URL. The order worker persists `PENDING_PAYMENT + INTENT` before authorization. PSP authorize and confirmation are asynchronous; capture is a separate sweeper. `CONFIRMED` is visible only after successful authorization and remote synchronous PostgreSQL standby WAL-flush acknowledgement.
- **Alternatives:** Fully synchronous checkout through PSP and DB; asynchronous reservation without an atomic hold; confirm before capture.
- **Consequences:** Fast bounded accept path and replayable order processing. Customers may wait for final status. `PAYING` holds are not released solely by a timer. Remote standby loss pauses confirmations and new admissions; RPO 0 applies only to acknowledged confirmations.
