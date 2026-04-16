#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

ensure_stack_running

echo "[failures 1/3] Running source-recovery branch"
print_step_header \
  "F1-source-recovery" \
  "Source failure handling" \
  "source restart is visible and downstream recovery proof appears after restart" \
  "SQL-F1-SOURCE-RECOVERY" \
  "artifacts/failure-branch-status.md" \
  "no schema change" \
  "scripts/update_cdc_actions.py --scenario source_recovery"
docker compose stop source-postgres
sleep 8
docker compose start source-postgres
sleep 8
run_demo_tools "python /workspace/scripts/update_cdc_actions.py --scenario source_recovery"
flush_rw

echo "[failures 2/3] Running downstream delivery failure branch"
print_step_header \
  "F2-downstream-delivery-retry" \
  "Downstream delivery failure handling" \
  "the same key first fails delivery, then is delivered successfully after retry" \
  "N/A" \
  "artifacts/failure-branch-status.md" \
  "no schema change" \
  "scripts/update_cdc_actions.py --scenario sink_failure"
run_demo_tools "python /workspace/scripts/update_cdc_actions.py --scenario sink_failure"
sleep 8
flush_rw

echo "[failures 3/3] Writing failure branch status"
run_demo_tools "python /workspace/scripts/demo_checks.py --scope failures"

echo
echo "Failure branch status written to:"
echo "  ${ROOT_DIR}/artifacts/failure-branch-status.md"
