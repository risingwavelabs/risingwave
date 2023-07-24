#!/usr/bin/env bash

./risedev k
./risedev clean-data

echo "--- e2e, ci-backfill, build"
RUST_LOG="risingwave_stream=trace" ./risedev d ci-backfill-snapshot

################ TESTS

echo "--- e2e, ci-backfill, run backfill test"

set -euo pipefail

PARENT_PATH=$(dirname "${BASH_SOURCE[0]}")

run_sql_file() {
  psql -h localhost -p 4566 -d dev -U root -f "$@"
}

run_sql() {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

flush() {
  run_sql "FLUSH;"
}

run_sql_file "$PARENT_PATH"/sql/backfill/create_base_table.sql
run_sql_file "$PARENT_PATH"/sql/backfill/insert_seed.sql

# Provide snapshot
for i in $(seq 1 17)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql
  flush
done

run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &
#
## Provide upstream updates
#for i in $(seq 1 5)
#do
#  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql &
#done

wait

run_sql_file "$PARENT_PATH"/sql/backfill/select.sql </dev/null
run_sql "SELECT count(*) FROM mv1;"

echo "Backfill tests complete"