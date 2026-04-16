#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

ARTIFACT="${ROOT_DIR}/artifacts/observability-branch-status.md"

ensure_stack_running

print_step_header \
  "O1-observability-signals" \
  "Observability: metrics / logs / offset visibility / failure diagnostics" \
  "Prometheus, Kafka topic visibility, and RisingWave logs are all available for the current run" \
  "N/A" \
  "artifacts/observability-branch-status.md" \
  "no schema change" \
  "current running stack"

prom_result="$(curl -sS --get --data-urlencode 'query=up{job="prometheus"}' 'http://localhost:9500/api/v1/query' || true)"
topic_list="$(docker compose exec -T message_queue rpk topic list || true)"
log_tail="$(docker compose logs risingwave-standalone --tail 20 || true)"

prom_ok=0
topic_ok=0
log_ok=0

if echo "${prom_result}" | grep -q '"status":"success"'; then
  prom_ok=1
fi
if echo "${topic_list}" | grep -q 'demo.customer.profile.latest'; then
  topic_ok=1
fi
if [[ -n "${log_tail}" ]]; then
  log_ok=1
fi

if [[ ${prom_ok} -eq 1 && ${topic_ok} -eq 1 && ${log_ok} -eq 1 ]]; then
  cat > "${ARTIFACT}" <<STATUS
# Observability Branch Status

## STEP_ID
- O1-observability-signals

- [x] Prometheus query returns successfully
- [x] Kafka topic visibility is available through rpk
- [x] RisingWave logs are accessible for diagnostics
STATUS
else
  cat > "${ARTIFACT}" <<STATUS
# Observability Branch Status

## STEP_ID
- O1-observability-signals

- [ ] Observability branch did not produce the expected diagnostic signals

## Evidence flags
- prom_ok=${prom_ok}
- topic_ok=${topic_ok}
- log_ok=${log_ok}
STATUS
  exit 1
fi

echo "Observability branch status written to: ${ARTIFACT}"
