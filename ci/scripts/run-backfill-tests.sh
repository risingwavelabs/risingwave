#!/usr/bin/env bash

# Runs backfill tests.
# NOTE(kwannoel):
# The following scenario is adapted in madsim's integration tests as well.
# But this script reproduces it more reliably (I'm not sure why.)
# Hence keeping it in case we ever need to debug backfill again.

# USAGE:
# Start a rw cluster then run this script.
# ```sh
# ./risedev d
# SNAPSHOT_COUNT=12 UPSTREAM_COUNT=5 ./ci/scripts/run-backfill-tests.sh
# ```


set -euo pipefail

PARENT_PATH=$(dirname "${BASH_SOURCE[0]}")

run_sql_file() {
  psql -h localhost -p 4566 -d dev -U root -f "$@"
}

run_sql() {
  psql -h localhost -p 4566 -d dev -U root -c "$1"
}

run_insert_with_count() {
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql | sed 's/.*INSERT [0-9]* \([0-9]*\)/\1/'
}

flush() {
  run_sql "FLUSH;"
}

echo "--- Creating base table"
run_sql_file "$PARENT_PATH"/sql/backfill/create_base_table.sql

echo "--- Creating snapshot"
run_sql_file "$PARENT_PATH"/sql/backfill/insert_seed.sql
TOTAL_SNAPSHOT_COUNT=1

for i in $(seq 1 "$SNAPSHOT_COUNT")
do
  COUNT=$(run_insert_with_count)
  TOTAL_SNAPSHOT_COUNT=$((TOTAL_SNAPSHOT_COUNT + COUNT))
  flush
done
echo "TOTAL_SNAPSHOT_ROWS: $TOTAL_SNAPSHOT_COUNT"

echo "--- Creating Materialized View in Background"
run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &

echo "--- Creating upstream updates concurrently"
for i in $(seq 1 "$UPSTREAM_COUNT")
do
  run_insert_with_count > COUNT_"$i" &
done

wait

echo "--- Running SELECT sanity check"
run_sql_file "$PARENT_PATH"/sql/backfill/select.sql </dev/null

echo "--- Recording upstream count"
TOTAL_UPSTREAM_COUNT=0
for i in $(seq 1 "$UPSTREAM_COUNT")
do
  COUNT=$(cat COUNT_"$i")
  TOTAL_UPSTREAM_COUNT=$((TOTAL_UPSTREAM_COUNT + COUNT))
  rm COUNT_"$i"
done
echo "TOTAL_UPSTREAM_ROWS: $TOTAL_UPSTREAM_COUNT"

echo "--- Recording total counts"

TOTAL_ACTUAL_ROWS=$(run_sql 'SELECT COUNT(*) FROM mv1;' | grep "[0-9]+" | grep -v "(")
echo "TOTAL_ACTUAL_ROWS: $TOTAL_ACTUAL_ROWS"
TOTAL_EXPECTED_ROWS=$((TOTAL_UPSTREAM_COUNT + TOTAL_SNAPSHOT_COUNT))
echo "TOTAL_EXPECTED_ROWS: $TOTAL_EXPECTED_ROWS"

echo "--- Checking results"
if [[ "$TOTAL_ACTUAL_ROWS" -ne "$TOTAL_EXPECTED_ROWS" ]]; then
  echo "Expected $TOTAL_EXPECTED_ROWS rows, got $TOTAL_ACTUAL_ROWS"
  exit 1
else
  echo "Backfill test passed."
fi
