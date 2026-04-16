#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

wait_for_vip_note() {
  for _ in $(seq 1 10); do
    run_psql sql/refresh_add_optional_field.sql >/dev/null
    if wait_for_query_value "SELECT count(*) FROM information_schema.columns WHERE table_schema='demo_core' AND table_name='customer_profiles' AND column_name='vip_note';" "1" 1 0; then
      return 0
    fi
    sleep 2
  done
  echo "vip_note did not appear after repeated REFRESH SCHEMA attempts" >&2
  return 1
}

ensure_stack_running

echo "[schema 1/3] Applying add-optional-field evolution"
print_step_header \
  "S1-add-optional-field" \
  "Schema evolution: add optional field + continuity preservation" \
  "vip_note appears in source schema and source/view/latest-state remain readable" \
  "SQL-S1-REFRESH-SCHEMA" \
  "artifacts/schema-branch-status.md" \
  "SCHEMA-S1-VIP-NOTE" \
  "scripts/produce_avro.py --step schema_v2/stage2"
run_demo_tools "python /workspace/scripts/produce_avro.py --step schema_v2"
wait_for_vip_note
run_demo_tools "python /workspace/scripts/produce_avro.py --step stage2"
sleep 8
flush_rw

echo "[schema 2/3] Refreshing downstream JSON snapshot"
run_demo_tools "python /workspace/scripts/consume_downstream.py"

echo "[schema 3/3] Writing schema branch status"
run_demo_tools "python /workspace/scripts/demo_checks.py --scope schema"

echo
echo "Schema branch status written to:"
echo "  ${ROOT_DIR}/artifacts/schema-branch-status.md"
