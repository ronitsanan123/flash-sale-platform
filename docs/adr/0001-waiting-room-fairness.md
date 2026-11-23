# ADR 0001: Edge waiting-room fairness and admission

- **Status:** Accepted
- **Context:** A drop can produce 500K+ edge polls/s from up to 10M waiting users while the origin can safely process only a PSP- and inventory-bounded admitted rate. Strict global FIFO at the origin is expensive and refresh-spamming must not improve position.
- **Decision:** Use a virtual waiting room at the edge. Polls read a one-second cached watermark; no origin call is made per poll. Pre-drop arrivals are randomized into a cohort; post-start arrivals use FIFO within the queue. Signed short-lived admission tokens are verified offline at the gateway. `PAUSED` preserves place and issues no token; terminal `SOLD_OUT` is a separate inventory state.
- **Alternatives:** Per-request origin queue polling; strict global distributed FIFO; lottery-only allocation.
- **Consequences:** Origin load is bounded and refresh-spam is ineffective. Queue position is approximate and cached freshness may be one second stale. If queue/vendor/control state is unknown, new admissions fail closed while CDN browse remains open.
