#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

printf '\n== Go unit tests ==\n'
make test-unit
printf '\n== Go race tests ==\n'
make race-unit
printf '\n== Build and vet ==\n'
make build
make vet
printf '\n== Safe in-memory demo ==\n'
make demo
printf '\n== HTTP end-to-end demo ==\n'
make demo-e2e
printf '\n== Naive baseline (violations must be detected) ==\n'
naive_output="$(make demo-naive 2>&1)"
printf '%s\n' "$naive_output"
grep -q 'oversell=90' <<<"$naive_output"
grep -q 'reconciler_exit=1_expected' <<<"$naive_output"
printf '\n== Canonical plan mirror ==\n'
cmp -s plan.md docs/submission-plan.md || { echo 'plan.md and docs/submission-plan.md differ' >&2; exit 1; }
printf 'plan mirror: OK\n'
printf '\n== Compose YAML syntax ==\n'
python3 - <<'PY'
import yaml
with open('docker-compose.yml', encoding='utf-8') as f:
    doc = yaml.safe_load(f)
assert isinstance(doc, dict) and 'services' in doc
assert 'k6' in doc['services'] and 'loadtest' in doc['services']['k6'].get('profiles', [])
print('compose YAML: OK')
PY
printf '\n== Shell syntax ==\n'
bash -n scripts/chaos.sh scripts/seed.sh scripts/audit-local.sh scripts/smoke-compose.sh\npython3 -m py_compile scripts/demo-e2e.py scripts/render_hld.py
printf 'shell syntax: OK\n'
printf '\nLocal audit passed. Docker-backed integration, k6 execution, gitleaks and production topology checks are separate gates.\n'
