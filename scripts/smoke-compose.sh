#!/usr/bin/env bash
set -euo pipefail
if [[ -f .env ]]; then set -a; source .env; set +a; fi
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
ITEM_ID="${ITEM_ID:-flash-sale-demo}"
ADMIN_TOKEN="${ADMIN_TOKEN:?ADMIN_TOKEN is required}"

wait_for() {
  local url="$1" tries="${2:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl --fail --silent "$url" >/dev/null; then return 0; fi
    sleep 2
  done
  echo "timed out waiting for $url" >&2
  return 1
}

wait_for "$BASE_URL/healthz"
health="$(curl --fail --silent "$BASE_URL/healthz")"
grep -q '"status":"ok"' <<<"$health"
metrics="$(curl --fail --silent "$BASE_URL/metrics")"
grep -q '^flashsale_' <<<"$metrics"
availability="$(curl --fail --silent "$BASE_URL/items/$ITEM_ID/availability")"
grep -q '"total":' <<<"$availability"

# Admin control path must authenticate and round-trip a safe pause/resume.
curl --fail --silent -X POST "$BASE_URL/admin/queue/$ITEM_ID/pause" \
  -H "X-Admin-Token: $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"paused":true}' >/dev/null
curl --fail --silent -X POST "$BASE_URL/admin/queue/$ITEM_ID/pause" \
  -H "X-Admin-Token: $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"paused":false}' >/dev/null

printf 'compose_smoke=PASS health=%s availability=%s\n' "$health" "$availability"

# Exercise the distributed API, Redis inventory, PostgreSQL orders, Redpanda
# worker and mock PSP through the same end-to-end path as the local demo.
DEMO_EXTERNAL=1 BASE_URL="$BASE_URL" ITEM_ID="$ITEM_ID" \\
  EXPECTED_UNITS="${EXPECTED_UNITS:-500}" USER_ID="compose-smoke-$(date +%s%N)" \\
  python3 scripts/demo-e2e.py
