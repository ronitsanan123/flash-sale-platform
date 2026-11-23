# Spike-Resilient Flash Sale & Ticketing Platform

A Go prototype and architecture proposal for a limited-inventory checkout system that must remain correct during a 250x traffic spike. The design absorbs waiting-room traffic at the edge, admits only a PSP- and inventory-safe origin rate, reserves concrete unit tokens atomically, and processes orders/payments asynchronously with idempotent effects.

## Assignment assumptions

- Up to 10M concurrent waiting-room users.
- 50K inventory units for one hot item.
- Approximately 50:1 browse/availability reads to reservation/payment writes.
- Normal traffic around 2K RPS; demand can reach 500K+ RPS within 30 seconds.
- PSP quotas and timeouts are independent downstream limits.
- Availability target 99.95%; reservation-path p99 below 500 ms.

The 500K/s number is edge waiting-room polling, not origin throughput. A 20-second jittered polling interval is an explicit planning assumption. The local demo does not claim a 500K-RPS benchmark.

## Architecture at a glance

1. CDN serves static/catalog/availability data; the edge waiting room answers queue polls from a cached watermark.
2. Admission combines inventory state (`OPEN`, `TEMPORARILY_UNAVAILABLE`, `SOLD_OUT`) with a separate `ACTIVE`/`PAUSED` gate. Pausing preserves a user's queue position and never means sold out.
3. API verifies short-lived signed admission tokens offline. A Redis Cluster Lua transaction claims a concrete unit and writes hold/deadline/outbox state atomically.
4. Redis Streams relay events to Redpanda/Kafka, partitioned by `order_id`. Consumers are at-least-once; state transitions and PSP calls are idempotent.
5. PostgreSQL owns orders/payments, unit uniqueness, aggregate versions and regional writer leases. The worker creates an intent before calling the PSP.
6. PSP authorization is separate from capture. `CONFIRMED` requires a synchronous remote PostgreSQL standby WAL-flush acknowledgement (primary + one remote standby, 2-of-2; not a majority quorum). Capture runs off the hot path.
7. Regional recovery discovers unknown order IDs through a stable, paginated PSP authorization ledger with metadata and a `complete_through` watermark. Incomplete scans fail closed.

See the [submission checklist](SUBMISSION-CHECKLIST.md), [HLD](docs/diagrams/hld.pdf), [walkthrough](docs/architecture/hld-walkthrough.md), [capacity model](docs/capacity-model.md), [ADRs](docs/adr/) and [PSP recovery contract](docs/contracts/psp-ledger-discovery.md).

## Repository layout

```text
README.md
plan.md                         canonical implementation plan
Makefile Dockerfile docker-compose.yml .env.example
prototype/
  cmd/{flashsale,worker,mockpsp,flashctl,demo,demo-naive}/
  internal/{admission,app,inventory,orders,pgwire,pipeline,psp,rediswire,service,shed,token}/
  migrations/001_init.sql
  tests/integration/
docs/                           six proposal sections, HLD, ADRs, runbooks, contracts
loadtest/flash.js               k6 spike/soak/stress profiles
scripts/{seed.sh,chaos.sh,render_hld.py,demo-e2e.py,smoke-compose.sh}
infra/{k8s,terraform}/           illustrative deployment snippets
```

## Requirements

- Go 1.23+ for the checked-in module. The current local code and tests use the Go version in `prototype/go.mod`.
- Python 3, PyYAML (for `make audit-local` Compose validation), and CairoSVG (to regenerate HLD exports).
- Docker Engine and Docker Compose v2 for Redis Cluster/PostgreSQL/Redpanda integration and the distributed local profile.
- k6 for load testing.

## Quick start: local unit tests and deterministic demos

```bash
cd prototype
go test -count=1 ./...
go test -race -count=1 ./...
go build ./...
go vet ./...
cd ..
make test-unit
make race-unit
make demo
make demo-e2e
make demo-naive
```

The HTTP end-to-end demo starts the real Go API process and drives health, availability, queue join/poll, signed admission, reservation, checkout, asynchronous worker processing, PSP authorization, confirmation, capture and final order/inventory verification. It requires only Go and Python 3; it uses in-memory adapters and is not a distributed benchmark.

Expected deterministic demo invariants:

- Safe in-memory full-flow demo: 20,000 waiters, 500 admitted/reserved/confirmed/captured, 500 PSP effects, zero duplicate units and zero orphan authorizations.
- Intentionally broken naive baseline: 10 units, 100 contenders, 90 oversells detected.

These are laptop-scale correctness demonstrations using in-memory adapters, not distributed integration benchmarks or 500K-RPS measurements.

## Quick start: Docker Compose integration profile

Copy `.env.example` to `.env` and replace every placeholder with local random values. Never commit `.env`.

```bash
cp .env.example .env
make setup
make compose-ready
make test
make race
```

`make test` runs host unit tests and then runs integration tests inside the Compose network. `make race` runs host unit tests with `-race`, then runs the integration slice inside the `integration` container with `CGO_ENABLED=1`, GCC and libc development headers. Redis Cluster integration tests must run inside Compose because Redis advertises Docker service hostnames in `MOVED`/`ASK` responses.

To start the distributed local profile:

```bash
docker compose --profile app up -d --build
curl -fsS http://localhost:8080/healthz
```

The default Compose profile uses a single local PostgreSQL instance and `SIMULATE_STANDBY_ACK=true`. This exercises the application-level fail-closed gate but does **not** prove remote PostgreSQL synchronous replication or cross-region RPO 0. The HLD/ADR production design requires a real named remote synchronous standby.

## Service endpoints

- API: `http://localhost:8080`
- Mock PSP: `http://localhost:8090`
- Health: `GET /healthz`
- PSP webhook: `POST /webhooks/psp` (HMAC signature and timestamp verified)
- Mock PSP ledger: `GET /ledger` with bounded window, event type and cursor.

Admin operations use `X-Admin-Token`; do not expose the local admin endpoint publicly.


## Regional recovery command

After the old home-region primary has been independently confirmed unreachable or infrastructure-fenced, promote the standby and install its higher `REGION_WRITER_ID`/`LEASE_EPOCH` in the environment. Run the recovery CLI from the worker image (the command sets a persistent CTRL pause before scanning and clears it only on success):

```bash
docker compose --profile app run --rm --entrypoint /usr/local/bin/reconcile worker \
  --old-primary-fenced \
  --from '2026-11-22T00:00:00Z' \
  --failover-detected-at '2026-11-23T00:00:00Z' \
  --settlement-grace 2m --overlap 2m --page-size 100
```

The `--old-primary-fenced` flag is an operator attestation, not an automated split-brain detector. The promoted PostgreSQL `region_leases` row must already contain the new logical writer identity and incremented epoch; `BOOTSTRAP_LEASE` must be false for the recovery invocation. Any scan, metadata, PSP, DB or Redis repair failure leaves the persistent recovery pause set. Clear it only by rerunning recovery successfully or through an audited operator action after independent reconciliation.

## Load and chaos testing

`loadtest/flash.js` defines `spike`, `soak` and `stress` profiles. Example (after starting the Compose app):

```bash
k6 run -e BASE_URL=http://localhost:8080 -e PROFILE=spike loadtest/flash.js
k6 run -e BASE_URL=http://localhost:8080 -e PROFILE=soak loadtest/flash.js
k6 run -e BASE_URL=http://localhost:8080 -e PROFILE=stress loadtest/flash.js

# Or run k6 from the optional Compose profile (after the app is up):
docker compose --profile loadtest run --rm k6
```

`scripts/chaos.sh` includes dependency-fault and worker-restart hooks. Run game days against a disposable environment; do not use the single-node local Compose setup to claim production multi-region behaviour. Record admitted-origin rate, successful-response p99, shed/429/503 rates, queue age, outbox/Kafka age, PSP utilization, duplicate-unit count and orphan authorization count.

## Implementation status and evidence

Implemented in the current tree:

- Go module, build/test tooling, Dockerfile and Compose definitions.
- Redis Cluster RESP client, inventory Lua scripts (reserve/checkout/confirm/release/forfeit/reap/terminal cleanup), immutable per-hold claims, Redis Streams relay.
- Admission engine, signed tokens, HTTP API, shed limiter, status projection and health gates.
- PostgreSQL schema/repository, aggregate CAS/versioning, region lease manager, PSP ledger checkpoints and recovery scanner.
- Mock PSP authorization/capture/void, idempotency, fault injection, signed webhook delivery and paginated ledger discovery.
- Redpanda HTTP Proxy adapter, relay/consumer retry/DLQ code, worker process and capture processing.
- Six proposal documents, eight ADRs, HLD source/PNG/SVG/PDF, walkthrough, capacity model and runbooks.

Verified locally in this environment:

- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `go build ./...`
- `go vet ./...`
- `make test-unit`, `make race-unit`
- `make demo`, `make demo-naive`
- Docker Compose YAML parses successfully.

Not verified in this environment:

- Docker Compose startup and actual Redis Cluster/PostgreSQL/Redpanda integration, because Docker is unavailable here.
- Real remote PostgreSQL synchronous standby and cross-region promotion. Local Compose simulates standby health using `StandbyAckGate`.
- A true split-brain partition with two writable regions; per-region lease rows are not a global fence. Promotion requires old-primary fencing; consensus/external fencing remains out of scope.
- CDN/WAF/vendor waiting-room behaviour at 10M waiters, production autoscaling, 500K edge-poll performance, and the k6 thresholds under production-like resources.
- Production OTel backend/analytics store and managed AWS deployment. K8s/Terraform files are illustrative.

The integration tests are present under `prototype/tests/integration/`; when their required environment variables are absent they skip rather than pretending external dependencies passed. See `docs/runbooks/local-redis-cluster.md`.

## Local audit without Docker

Run `make audit-local` for the host-only unit/race suites, build, vet, deterministic safe/naive demos, canonical-plan mirror check, Compose YAML parsing and shell syntax checks. This deliberately does not claim that Docker-backed integration, k6 load, gitleaks or production topology gates passed.

## Security and repository hygiene

- Only `.env.example` is included; `.env` is ignored.
- Use random local secrets for `ADMIN_TOKEN`, `ADMISSION_SIGNING_KEY`, `PSP_WEBHOOK_SECRET` and database credentials.
- Run a secret scan before publication:

```bash
docker run --rm -v "$PWD:/repo" zricethezav/gitleaks detect --no-git -s /repo
```

No Git repository is initialized in this working copy. The assignment's final submission requires creating/publishing a GitHub repository after the project is reviewed and the user explicitly authorizes it.

## Important correctness boundaries

- PostgreSQL is authoritative; Redis is the fast allocation layer.
- A `PAYING` hold is never released by a timer.
- `UNKNOWN` PSP outcomes remain locked until resolved; idempotency-window expiry alone is not proof of finality.
- A unique-guard loser is `FORFEITED` and never returns its unit to availability.
- `CONFIRMED` is projected only after remote standby acknowledgement in the production design.
- If a PSP cannot provide complete ledger discovery/metadata/finality, regional recovery fails closed.
- The local demo and Compose profile must not be described as a 500K-RPS or true cross-region RPO-0 benchmark.

## End-to-end verification

A host-only audit is available with `make audit-local`. It intentionally does not
claim that external services passed. For a live local stack, copy `.env.example`
to `.env`, replace every placeholder with random local secrets, then run:

```bash
make app-up
make smoke-compose
make integration
make race-integration
```

Use `make app-down` to stop the app while retaining local data, or
`docker compose --profile app --profile test down --volumes` for a destructive
cold reset. The CI workflow at `.github/workflows/ci.yml` provisions isolated
secrets, runs host and Docker-backed tests, starts the complete app profile,
smoke-tests the API, and runs a secret scan. CI must pass on the target Git
provider before treating Docker integration as verified.

`make smoke-compose` checks `/healthz`, `/metrics`, item availability, and the
authenticated operator pause/resume path against a running app. It is a smoke
test, not a throughput or cross-region durability benchmark.
