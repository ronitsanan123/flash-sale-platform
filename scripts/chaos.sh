#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
: "${ADMIN_TOKEN:?Set ADMIN_TOKEN in .env}"
: "${ITEM_ID:=flash-sale-demo}"
: "${BASE_URL:=http://localhost:8080}"
DOCKER="${DOCKER:-docker}"
COMPOSE=("$DOCKER" compose)

usage() {
  cat <<USAGE
Usage: scripts/chaos.sh <scenario>
Scenarios: psp-faults | pause-psp | recover-psp | pause-ctrl | resume-ctrl | pause-kv | resume-kv | pause-kafka | resume-kafka | kill-app | restore-app | kill-worker | restore-worker
This is a destructive local-development helper. Never point it at production.
USAGE
}
scenario="${1:-}"
case "$scenario" in
  psp-faults)
    curl --fail --silent --show-error -X POST http://localhost:8090/admin/faults \
      -H 'Content-Type: application/json' \
      -d '{"timeout_rate":0.20,"lost_response_rate":0.10,"error_rate":0.02,"rate_limit_rps":1400}'
    ;;
  pause-psp)
    curl --fail --silent --show-error -X POST http://localhost:8090/admin/faults \
      -H 'Content-Type: application/json' -d '{"rate_limit_rps":1,"timeout_rate":0.25,"lost_response_rate":0.15}'
    ;;
  recover-psp)
    curl --fail --silent --show-error -X POST http://localhost:8090/admin/faults \
      -H 'Content-Type: application/json' -d '{}'
    ;;
  pause-ctrl)
    "$DOCKER" compose pause redis-ctrl
    ;;
  resume-ctrl)
    "$DOCKER" compose unpause redis-ctrl
    ;;
  pause-kv)
    "$DOCKER" compose pause redis-kv
    ;;
  resume-kv)
    "$DOCKER" compose unpause redis-kv
    ;;
  pause-kafka)
    "$DOCKER" compose pause redpanda
    ;;
  resume-kafka)
    "$DOCKER" compose unpause redpanda
    ;;
  kill-app)
    "$DOCKER" compose kill flashsale
    ;;
  restore-app)
    "$DOCKER" compose up -d flashsale
    ;;
  kill-worker)
    "$DOCKER" compose kill worker
    ;;
  restore-worker)
    "$DOCKER" compose up -d --force-recreate worker
    ;;
  *) usage; exit 2 ;;
esac
