#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
cd "${ROOT_DIR}"

echo "[backbone 1/6] Bringing up demo stack"
docker compose up -d --build --force-recreate
wait_for_rw

echo "[backbone 2/6] Preparing Kafka topics and seed Avro data"
bash prepare.sh

echo "[backbone 3/6] Applying core SQL objects"
print_step_header \
  "B1-source-create" \
  "Kafka Avro source creation / schema registry decode / Avro key as primary key" \
  "customer_profiles source table is created successfully and schema-decoded columns are visible" \
  "SQL-B1-SOURCE-DDL" \
  "artifacts/backbone-status.md" \
  "SCHEMA-B1-CUSTOMER-PROFILES" \
  "prepare.sh + scripts/produce_avro.py --step setup/stage1"
run_psql create_source.sql

print_step_header \
  "B2-mv-create" \
  "Materialized view creation / joins / windowing / UDFs" \
  "MV objects and SQL UDF are created and queryable in DEV" \
  "SQL-B2-MV-DDL" \
  "artifacts/backbone-status.md" \
  "SCHEMA-B2-MV-OBJECTS"
run_psql create_mv.sql

echo "[backbone 4/6] Sending valid CDC events needed for join/subscription flow"
print_step_header \
  "B3-updates-tombstones" \
  "Compacted-topic updates / tombstones / latest-state / complex Avro" \
  "latest-state keeps only the current valid row and deleted keys disappear from proof output" \
  "SQL-B3-LATEST-STATE-QUERIES" \
  "artifacts/downstream/latest_state.json" \
  "SCHEMA-NO-CHANGE" \
  "scripts/produce_avro.py --step stage1"
run_demo_tools "python /workspace/scripts/update_cdc_actions.py --scenario valid"
sleep 8
flush_rw

echo "[backbone 5/6] Verifying native SUBSCRIBE"
print_step_header \
  "B4-join-subscribe" \
  "Streaming joins + native SUBSCRIBE incremental proof" \
  "SUBSCRIBE observes a newly joined row produced by the stream path" \
  "SQL-B4-SUBSCRIBE-PROOF" \
  "artifacts/subscription/native_subscribe.json" \
  "SCHEMA-NO-CHANGE" \
  "scripts/update_cdc_actions.py --scenario valid"
run_demo_tools "python /workspace/scripts/demo_subscribe.py"

echo "[backbone 6/6] Capturing downstream JSON output and status"
print_step_header \
  "B5-json-output" \
  "JSON-format downstream output" \
  "processed output can be shown in JSON format at the downstream target" \
  "SQL-B5-SINK-DDL" \
  "artifacts/downstream/latest_state.json" \
  "SCHEMA-B5-SINK-OBJECTS"
run_psql create_sink.sql
run_demo_tools "python /workspace/scripts/produce_avro.py --step wait_sink"
flush_rw
run_demo_tools "python /workspace/scripts/consume_downstream.py"
run_demo_tools "python /workspace/scripts/demo_checks.py --scope backbone"

echo
echo "Backbone status written to:"
echo "  ${ROOT_DIR}/artifacts/backbone-status.md"
