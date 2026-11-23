SHELL := /bin/bash
.DEFAULT_GOAL := help

PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROTOTYPE_DIR := $(PROJECT_ROOT)/prototype
PYTHON ?= python3
DOCKER ?= docker
COMPOSE ?= $(DOCKER) compose
GO ?= go

# These services are the local dependencies for the integration suite. Redis
# Cluster integration tests must execute from the Compose network so every
# MOVED/ASK endpoint advertised by the cluster is reachable.
DEPENDENCY_SERVICES := redis-inventory-1 redis-inventory-2 redis-inventory-3 redis-ctrl redis-kv postgres redpanda

.PHONY: help setup build test test-unit test-integration race race-unit race-integration \
        integration compose-ready check-compose app-up app-down smoke-compose diagrams demo demo-e2e demo-naive \
        test-regional-recovery smoke-compose audit-local clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"}; /^[a-zA-Z0-9_.-]+:.*##/ {printf "%-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check-compose: ## Verify Docker Compose project files are available
	@command -v $(DOCKER) >/dev/null 2>&1 || { echo "docker is required for this target" >&2; exit 2; }
	@test -f "$(PROJECT_ROOT)/docker-compose.yml" || { echo "docker-compose.yml is not present yet; complete Task 1 first" >&2; exit 2; }

setup: check-compose ## Build local application and integration images
	$(COMPOSE) build

build: ## Compile all Go commands and packages
	cd "$(PROTOTYPE_DIR)" && $(GO) build ./...

vet: ## Run go vet on all prototype packages
	cd "$(PROTOTYPE_DIR)" && $(GO) vet ./...

# Pure Go tests only. Cross-service tests must live under tests/integration so
# they can never accidentally run on the host against Docker-only Redis names.
test-unit: ## Run pure Go unit tests on the host
	@cd "$(PROTOTYPE_DIR)" && \
	if [[ -f go.mod ]]; then \
	  packages="$$( $(GO) list ./... | grep -Ev '(^|/)tests/integration($$|/)')"; \
	  [[ -n "$$packages" ]] || { echo "no host unit-test packages found" >&2; exit 2; }; \
	  $(GO) test -count=1 $$packages; \
	else \
	  GO111MODULE=off $(GO) test -count=1 ./internal/...; \
	fi

# -race requires CGO and a C compiler. For the current host-only router slice,
# GCC is checked here. The full integration race suite runs in the integration
# image, which must also set CGO_ENABLED=1 and install gcc/libc development files.
race-unit: ## Run pure Go unit tests on the host with the race detector
	@command -v gcc >/dev/null 2>&1 || { echo "gcc is required for Go -race tests on the host" >&2; exit 2; }
	@cd "$(PROTOTYPE_DIR)" && \
	if [[ -f go.mod ]]; then \
	  packages="$$( $(GO) list ./... | grep -Ev '(^|/)tests/integration($$|/)')"; \
	  [[ -n "$$packages" ]] || { echo "no host unit-test packages found" >&2; exit 2; }; \
	  CGO_ENABLED=1 $(GO) test -race -count=1 $$packages; \
	else \
	  CGO_ENABLED=1 GO111MODULE=off $(GO) test -race -count=1 ./internal/...; \
	fi

# Bring up healthy dependencies and initialize the three-primary Redis
# Cluster. The redis-cluster-init service must be idempotent.
compose-ready: check-compose
	$(COMPOSE) up -d --wait $(DEPENDENCY_SERVICES)
	$(COMPOSE) run --rm redis-cluster-init

integration: compose-ready ## Run integration tests inside the Compose network
	$(COMPOSE) run --rm integration go test -p 1 -parallel 1 ./tests/integration/...

race-integration: compose-ready ## Run integration tests inside Compose with -race
	$(COMPOSE) run --rm integration sh -ec 'test "$$($(GO) env CGO_ENABLED)" = "1"; command -v gcc >/dev/null; test -d /usr/include; echo "integration race toolchain: CGO_ENABLED=1, gcc present"'
	$(COMPOSE) run --rm integration go test -race -p 1 -parallel 1 ./tests/integration/...

test: test-unit integration ## Run host unit tests followed by Compose integration tests

race: race-unit race-integration ## Run host unit race tests and Compose integration race tests

diagrams: ## Regenerate HLD SVG, PNG and PDF from the checked-in renderer
	cd "$(PROJECT_ROOT)" && $(PYTHON) scripts/render_hld.py

demo: ## Run the deterministic in-memory full-flow correctness demo
	cd "$(PROTOTYPE_DIR)" && $(GO) run ./cmd/demo

demo-e2e: ## Run the HTTP queue-to-capture end-to-end demo with in-memory adapters
	cd "$(PROJECT_ROOT)" && $(PYTHON) scripts/demo-e2e.py

demo-naive: ## Run the intentionally broken baseline and detect oversell
	cd "$(PROTOTYPE_DIR)" && $(GO) run ./cmd/demo-naive

test-regional-recovery: compose-ready ## Run the unknown-order-ID regional recovery scenario
	$(COMPOSE) run --rm integration go test -race -p 1 -parallel 1 -run '^TestRegionalRecovery' ./tests/integration/...

smoke-compose: check-compose ## Smoke-test the running Compose app profile (requires .env and make app-up)
	$(PROJECT_ROOT)/scripts/smoke-compose.sh

app-up: check-compose ## Start the complete local app profile
	$(COMPOSE) --profile app up -d --build --wait

app-down: check-compose ## Stop local app profile and retain data volumes
	$(COMPOSE) --profile app down --remove-orphans

audit-local: ## Run all checks that do not require Docker or external services
	$(PROJECT_ROOT)/scripts/audit-local.sh

clean: check-compose ## Stop containers without deleting volumes
	$(COMPOSE) down --remove-orphans
