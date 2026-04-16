#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

mkdir -p artifacts/http-receiver artifacts/exports artifacts/downstream artifacts/subscription artifacts/kafka-poison
rm -f artifacts/http-receiver/received.jsonl \
  artifacts/http-receiver/error.log \
  artifacts/http-receiver/fail-once-state.json \
  artifacts/subscription/native_subscribe.json \
  artifacts/webhook_poison.out \
  artifacts/downstream/latest_state.json \
  artifacts/backbone-status.md \
  artifacts/schema-branch-status.md \
  artifacts/failure-branch-status.md \
  artifacts/cdc-branch-status.md \
  artifacts/rbac-branch-status.md \
  artifacts/observability-branch-status.md
rm -f artifacts/kafka-poison/*

# Legacy export artifacts are still cleaned here for workspace hygiene, but CSV/XML/fixed-width
# are intentionally out of scope for the current customer demo.
rm -f artifacts/exports/customer_change_export.csv \
  artifacts/exports/customer_change_export.json \
  artifacts/exports/customer_change_export.xml \
  artifacts/exports/customer_change_export.txt

docker compose run --rm demo-tools "for i in \$(seq 1 30); do pg_isready -h source-postgres -p 5432 -U postgres -d customerdb && exit 0; sleep 2; done; exit 1"
docker compose run --rm demo-tools "PGPASSWORD=postgres psql -h source-postgres -p 5432 -U postgres -d customerdb -v ON_ERROR_STOP=1 -f /workspace/sql/source_postgres_init.sql -P pager=off"
docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step setup"
docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step stage1"
