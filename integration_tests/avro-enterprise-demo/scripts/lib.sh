#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

print_step_header() {
  local step_id="$1"
  local feature="$2"
  local expected_proof="$3"
  local sql_ref="$4"
  local output_ref="$5"
  local schema_note="$6"
  local input_ref="${7:-N/A}"

  cat <<EOF
============================================================
STEP_ID: ${step_id}
FEATURE: ${feature}
EXPECTED_PROOF: ${expected_proof}
SQL_REF: ${sql_ref}
INPUT_REF: ${input_ref}
OUTPUT_REF: ${output_ref}
SCHEMA_NOTE: ${schema_note}
============================================================
EOF
}

run_psql() {
  local sql_file="$1"
  docker compose run --rm demo-tools "psql -h risingwave-standalone -p 4566 -U root -d dev -v ON_ERROR_STOP=1 -f /workspace/${sql_file} -P pager=off"
}

run_psql_cmd() {
  local sql="$1"
  docker compose run --rm demo-tools "psql -h risingwave-standalone -p 4566 -U root -d dev -v ON_ERROR_STOP=1 -c \"${sql}\" -P pager=off"
}

run_demo_tools() {
  docker compose run --rm demo-tools "$1"
}

flush_rw() {
  run_demo_tools "psql -h risingwave-standalone -p 4566 -U root -d dev -v ON_ERROR_STOP=1 -c 'FLUSH;' -P pager=off"
}

wait_for_rw() {
  run_demo_tools '
    for i in $(seq 1 60); do
      if psql -h risingwave-standalone -p 4566 -U root -d dev -c "SELECT 1" >/dev/null 2>&1; then
        exit 0
      fi
      sleep 2
    done
    exit 1
  '
}

ensure_stack_running() {
  if ! docker compose ps --status running | grep -q "risingwave-standalone"; then
    echo "Demo stack is not running. Run ./scripts/run_backbone.sh first." >&2
    exit 1
  fi
}

wait_for_query_value() {
  local sql="$1"
  local expected="$2"
  local attempts="${3:-10}"
  local delay="${4:-2}"

  for _ in $(seq 1 "${attempts}"); do
    local result
    result="$(run_demo_tools "psql -h risingwave-standalone -p 4566 -U root -d dev -At -c \"${sql}\"" | tail -n 1 | tr -d '[:space:]')"
    if [[ "${result}" == "${expected}" ]]; then
      return 0
    fi
    sleep "${delay}"
  done

  return 1
}
