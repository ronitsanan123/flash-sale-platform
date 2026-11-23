# Verification and implementation boundary

This file separates executable evidence from production requirements. A passing
host test is not evidence that Redis Cluster, PostgreSQL, Redpanda, or a remote
standby works together.

## Verified in the supplied workspace

- Go unit suite, race suite, build, and `go vet` pass.
- `make audit-local` passes, including the safe/naive correctness demonstrations,
  Compose YAML parsing, shell/Python syntax, canonical plan mirror, and the HTTP end-to-end demo.
- HTTP end-to-end demo (`make demo-e2e`) starts the actual Go HTTP server and passes health, availability, queue join/poll, signed token admission, reserve, checkout, async order/payment processing, PSP authorization, confirmation, capture and final inventory/status checks.
- Safe in-memory demonstration: 20,000 waiters, 500 admitted/reserved/confirmed/
  captured, 500 PSP charges, 1,000 PSP events, zero duplicate units, and zero
  orphan authorizations. This is not a distributed benchmark.
- Unsafe baseline detects 90 oversells against 10 units.
- Authenticated PSP webhook signature/timestamp validation and fenced webhook
  processing have unit-level coverage in the Go suite.

## Added repeatable end-to-end gates

- `.github/workflows/ci.yml` provisions disposable credentials, runs the
  Compose-network integration suite and race suite, starts the app profile,
  smoke-tests the API, and runs Gitleaks.
- `make app-up`, `make smoke-compose`, and `make app-down` provide equivalent
  local stack operations.

## Not verified here / still outside the prototype's demonstrated guarantees

- Docker/Podman, k6, Gitleaks, and kubectl are not installed in this workspace;
  therefore live Compose integration, cold-start, load, secret scan, and
  Kubernetes validation were not executed here. CI must run successfully to
  close the local Docker integration gate.
- Compose uses a single PostgreSQL instance and an application-level standby
  ACK simulation. It does not demonstrate remote synchronous WAL replication,
  RPO-0 across regions, or WAN latency.
- The regional lease row does not globally fence two independently writable
  primaries during a network partition. Promotion requires infrastructure
  fencing/confirmed old-primary loss or an external consensus fencing authority.
- The durable-DB-to-Redis inventory rebuild/store-loss drill and outage-based
  extension/freezing of HELD deadlines remain unimplemented acceptance items.
- The in-process waiting-room queue, local Compose topology, and current k6
  profiles do not prove 10M-waiter edge behaviour or 500K-RPS production scale.
- Terraform/Kubernetes snippets are supporting examples, not a deployed,
  production-hardened cloud environment.

Do not describe this repository as production-ready or claim a 500K-RPS,
10M-waiter, cross-region RPO-0 result without the corresponding environment,
measurements, and independent failure tests.
