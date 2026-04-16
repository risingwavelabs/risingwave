#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[full 1/7] Running canonical backbone"
bash scripts/run_backbone.sh

echo "[full 2/7] Running schema evolution branch"
bash scripts/run_schema_branch.sh

echo "[full 3/7] Running CDC branch"
bash scripts/run_cdc_branch.sh

echo "[full 4/7] Running RBAC branch"
bash scripts/run_rbac_branch.sh

echo "[full 5/7] Running observability branch"
bash scripts/run_observability_branch.sh

echo "[full 6/7] Running failure branches"
bash scripts/run_failure_branches.sh

echo "[full 7/7] Running poison-message branch"
bash scripts/run_poison_kafka_case.sh

echo
echo "Demo artifacts written to:"
echo "  ${ROOT_DIR}/artifacts/backbone-status.md"
echo "  ${ROOT_DIR}/artifacts/schema-branch-status.md"
echo "  ${ROOT_DIR}/artifacts/cdc-branch-status.md"
echo "  ${ROOT_DIR}/artifacts/rbac-branch-status.md"
echo "  ${ROOT_DIR}/artifacts/observability-branch-status.md"
echo "  ${ROOT_DIR}/artifacts/failure-branch-status.md"
echo "  ${ROOT_DIR}/artifacts/poison-kafka/poison-kafka-check.md"
