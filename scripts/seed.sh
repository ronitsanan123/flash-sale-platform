#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
: "${BASE_URL:=http://localhost:8080}"
: "${ITEM_ID:=flash-sale-demo}"
: "${WAITERS:=1000}"
for i in $(seq 1 "$WAITERS"); do
  curl --fail --silent --show-error -X POST "$BASE_URL/queue/$ITEM_ID/join" \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"seed-$i\"}" >/dev/null
done
printf 'joined=%s item=%s\n' "$WAITERS" "$ITEM_ID"
