#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

ARTIFACT="${ROOT_DIR}/artifacts/cdc-branch-status.md"
ACTION_ID="$(date +%s)"

ensure_stack_running

echo "[cdc 1/2] Inserting a dedicated CDC branch action"
print_step_header \
  "C1-postgres-cdc" \
  "Alternative source ingestion via Postgres CDC" \
  "a dedicated CDC input row appears downstream in customer_change_audit" \
  "SQL-C1-CDC-PROOF" \
  "artifacts/cdc-branch-status.md" \
  "SCHEMA-C1-CDC-SOURCE" \
  "source-postgres.public.customer_actions INSERT"
docker compose exec -T source-postgres psql -U postgres -d customerdb -c "INSERT INTO public.customer_actions (action_id, customer_key, action_type, amount, action_ts, channel, team) VALUES (${ACTION_ID}, 'c-1001', 'cdc_branch_demo', '1.23', '2026-04-07 09:25:00', 'cdc-branch', 'ops');" >/dev/null
sleep 5
flush_rw

echo "[cdc 2/2] Capturing CDC branch status"
run_demo_tools "psql -h risingwave-standalone -p 4566 -U root -d dev -At -c \"SELECT count(*) FROM demo_core.customer_change_audit WHERE channel='cdc-branch';\"" | tail -n 1 | {
  read -r count
  if [[ "${count}" =~ ^[0-9]+$ && "${count}" -ge 1 ]]; then
    cat > "${ARTIFACT}" <<STATUS
# CDC Branch Status

## STEP_ID
- C1-postgres-cdc

- [x] Postgres CDC branch produced downstream rows with channel \`cdc-branch\`

## Command evidence
- inserted source action_id: ${ACTION_ID}
- downstream matching rows: ${count}
STATUS
  else
    cat > "${ARTIFACT}" <<STATUS
# CDC Branch Status

## STEP_ID
- C1-postgres-cdc

- [ ] Postgres CDC branch did not produce the expected downstream row

## Command evidence
- inserted source action_id: ${ACTION_ID}
- downstream matching rows: ${count:-<empty>}
STATUS
    exit 1
  fi
}

echo "CDC branch status written to: ${ARTIFACT}"
