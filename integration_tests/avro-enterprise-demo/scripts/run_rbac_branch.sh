#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

ARTIFACT="${ROOT_DIR}/artifacts/rbac-branch-status.md"

ensure_stack_running

print_step_header \
  "R1-rbac-allow-deny" \
  "Restricted access to sources/views by team" \
  "marketing_user can read the permitted marketing view but is denied direct access to the raw source" \
  "SQL-R1-RBAC-CHECK" \
  "artifacts/rbac-branch-status.md" \
  "SCHEMA-R1-RBAC-PRINCIPALS" \
  "demo-local users/grants created by create_mv.sql"

echo "[rbac 1/2] Checking allowed view access"
allowed_output="$(docker compose run --rm demo-tools "PGPASSWORD=marketing123 psql -h risingwave-standalone -p 4566 -U marketing_user -d dev -At -c \"SELECT count(*) FROM demo_marketing.customer_analytics_dev;\"" | tail -n 1 | tr -d '[:space:]')"

echo "[rbac 2/2] Checking denied raw-source access"
set +e
denied_output="$(docker compose run --rm demo-tools "PGPASSWORD=marketing123 psql -h risingwave-standalone -p 4566 -U marketing_user -d dev -c \"SELECT count(*) FROM demo_core.customer_profiles;\"" 2>&1)"
status=$?
set -e

if [[ "${allowed_output}" =~ ^[0-9]+$ ]] && [[ ${status} -ne 0 ]] && echo "${denied_output}" | grep -qi 'permission denied'; then
  cat > "${ARTIFACT}" <<STATUS
# RBAC Branch Status

## STEP_ID
- R1-rbac-allow-deny

- [x] marketing_user can read the permitted marketing view
- [x] marketing_user is denied direct access to the raw source table

## Notes
- AD group mapping itself is not part of this demo scope.
STATUS
else
  cat > "${ARTIFACT}" <<STATUS
# RBAC Branch Status

## STEP_ID
- R1-rbac-allow-deny

- [ ] RBAC branch did not produce the expected allow/deny pattern

## Evidence
- allowed_output: ${allowed_output}
- denied_status: ${status}
STATUS
  exit 1
fi

echo "RBAC branch status written to: ${ARTIFACT}"
