# Assignment Submission Checklist

This checklist maps the supplied **Assignment 1 — SDE 3 — Golang** PDF to this repository. The assignment's scale numbers are design assumptions; local demos are correctness examples, not a 500K-RPS benchmark.

| Assignment requirement | Submission location | Evidence / boundary |
|---|---|---|
| 1. Problem understanding and goals | `docs/01-problem-and-goals.md` | 250x failure modes, strict/degradable guarantees, SLO and recovery targets |
| 2. High-level and component architecture | `docs/02-architecture.md`, `docs/diagrams/hld.mmd` | Request flow, waiting room, inventory, event pipeline, data placement, cache and pre-warm |
| HLD source and exported files | `docs/diagrams/hld.mmd`, `hld.svg`, `hld.png`, `hld.pdf` | Regenerate with `make diagrams`; walkthrough in `docs/architecture/hld-walkthrough.md` |
| 3. Scalability and fault tolerance | `docs/03-scalability-fault-tolerance.md`, `docs/capacity-model.md`, `docs/adr/` | Sharding, backpressure, retries, idempotency, PSP ambiguity, failover limits |
| 4. Load testing and observability | `docs/04-load-testing-observability.md`, `loadtest/flash.js`, `scripts/chaos.sh` | Spike/soak/stress profiles and game-day guidance; actual k6 run requires Docker/k6 |
| 5. Trade-offs | `docs/05-trade-offs.md`, `docs/adr/` | Fairness, consistency, confirmation, capacity and build-vs-buy decisions |
| 6. Execution plan | `docs/06-execution-plan.md` | v0/v1/v2 rollout, canary gates and rollback |
| README, ADRs and supporting code | `README.md`, `docs/adr/`, `prototype/` | Local run commands and architecture decisions |
| Secrets hygiene | `.env.example`, `.gitignore`, `.github/workflows/ci.yml` | No `.env` is included; generate local secrets; CI runs Gitleaks |
| Runnable reservation/payment demo | `make demo`, `make demo-e2e` | In-memory correctness demonstration; not distributed infrastructure proof |
| Automated checks | `make audit-local`, `.github/workflows/ci.yml` | Go tests/race/build/vet, deterministic demos and HTTP E2E; Docker integration is a separate CI job |

## Reproduce locally

Requirements for the in-memory demo: Go 1.23+ and Python 3. For `make audit-local`, install PyYAML; for `make diagrams`, install CairoSVG.

```bash
make audit-local
make demo-e2e
make diagrams
```

For Redis Cluster, PostgreSQL and Redpanda integration, install Docker Engine + Compose v2, create `.env` from `.env.example` with fresh random values, then run:

```bash
cp .env.example .env
# Replace every placeholder in .env with unique random local secrets.
make integration
make race-integration
make app-up
make smoke-compose
make app-down
```

Do not commit `.env`.

## Honest verification boundary

The in-memory HTTP demo is fully exercised locally from queue admission through capture. The Docker-backed integration suite, k6 benchmark, Gitleaks scan and real regional replication/fencing must be executed in an environment that provides those dependencies. A single-node Compose standby-ACK simulation is not evidence of cross-region synchronous replication. See `docs/verification-status.md`.
