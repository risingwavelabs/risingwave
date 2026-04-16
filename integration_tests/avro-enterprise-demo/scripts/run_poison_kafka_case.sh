#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

ARTIFACT_DIR="${ROOT_DIR}/artifacts/poison-kafka"
mkdir -p "${ARTIFACT_DIR}"

capture_metrics() {
  local target="$1"
  curl -sS http://localhost:21250/metrics | rg '^user_source_error_cnt' > "${target}"
}

copy_latest_state() {
  local target="$1"
  run_demo_tools "python /workspace/scripts/consume_downstream.py"
  cp "${ROOT_DIR}/artifacts/downstream/latest_state.json" "${target}"
}

ensure_stack_running

print_step_header \
  "P1-poison-message" \
  "Poison-message behavior" \
  "source-error telemetry/logging appears, the bad payload is skipped, and a later valid record still materializes" \
  "N/A" \
  "artifacts/poison-kafka/poison-kafka-check.md" \
  "no schema change" \
  "scripts/produce_poison_kafka.py"

copy_latest_state "${ARTIFACT_DIR}/latest_state_before.json"
capture_metrics "${ARTIFACT_DIR}/metric_before.prom"

run_demo_tools "python /workspace/scripts/produce_poison_kafka.py"

sleep 15

capture_metrics "${ARTIFACT_DIR}/metric_after.prom"
docker compose logs risingwave-standalone --tail 400 > "${ARTIFACT_DIR}/risingwave.log"
copy_latest_state "${ARTIFACT_DIR}/latest_state_after.json"

timeout 20 docker compose exec message_queue rpk topic consume demo.customer.profile.upsert.avro -n 4 -o -4 -f 'offset=%o partition=%p key=%k value_size=%V\n' > "${ARTIFACT_DIR}/source_tail.txt" || true

python3 - <<'PY'
import json
from pathlib import Path

root = Path("artifacts/poison-kafka")
produce = json.loads((root / "produce-result.json").read_text(encoding="utf-8"))
state_before = json.loads((root / "latest_state_before.json").read_text(encoding="utf-8"))
state_after = json.loads((root / "latest_state_after.json").read_text(encoding="utf-8"))
log_text = (root / "risingwave.log").read_text(encoding="utf-8")


def metric_value(path: Path, source_name: str) -> float:
    total = 0.0
    for line in path.read_text(encoding="utf-8").splitlines():
        if f'source_name="{source_name}"' not in line:
            continue
        total += float(line.rsplit(" ", 1)[1])
    return total

before = metric_value(root / "metric_before.prom", "customer_profiles")
after = metric_value(root / "metric_after.prom", "customer_profiles")
deliveries = produce["deliveries"]
poison = next(item for item in deliveries if item["label"] == "poison")
followup = next(item for item in deliveries if item["label"] == "followup_valid")

log_error = "stream source reader error" in log_text or "failed to parse" in log_text
metric_increased = after > before
followup_materialized = "c-1999" in state_after
followup_after_poison = followup["offset"] > poison["offset"]
expected_behavior = log_error and metric_increased and followup_after_poison and followup_materialized

report = [
    "# Kafka Poison Message Check",
    "",
    "## STEP_ID",
    "- P1-poison-message",
    "",
    "## Expected demo behavior",
    "",
    "- The system surfaces source-error telemetry/logging for the poison message.",
    "- The bad payload is skipped.",
    "- Subsequent valid messages continue to be processed.",
    "",
    "## Rehearsal result",
    "",
    f"- [x] Poison message injected at offset `{poison['offset']}` and follow-up valid record injected at offset `{followup['offset']}`.",
    f"- [{'x' if log_error else ' '}] RisingWave log shows parser/source error. Evidence: `artifacts/poison-kafka/risingwave.log`.",
    f"- [{'x' if metric_increased else ' '}] `user_source_error_cnt` increased from `{before}` to `{after}`. Evidence: `artifacts/poison-kafka/metric_before.prom`, `artifacts/poison-kafka/metric_after.prom`.",
    f"- [{'x' if followup_after_poison else ' '}] The follow-up valid record was produced after the poison offset. Evidence: `artifacts/poison-kafka/produce-result.json`.",
    f"- [{'x' if followup_materialized else ' '}] The follow-up valid record materialized downstream, showing continued processing after the poison payload. Evidence: `artifacts/poison-kafka/latest_state_after.json`.",
    f"- [{'x' if expected_behavior else ' '}] Observed behavior matches the expected demo behavior statement.",
]

(root / "poison-kafka-check.md").write_text("\n".join(report) + "\n", encoding="utf-8")
PY

echo "Poison Kafka artifacts written to: ${ARTIFACT_DIR}"
