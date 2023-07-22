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
# ./ci/scripts/run-backfill-tests.sh
# ```


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
for i in $(seq 1 12)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql
  flush
done

run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &

# Provide upstream updates
for i in $(seq 1 5)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql &
done

wait

run_sql_file "$PARENT_PATH"/sql/backfill/select.sql </dev/null

echo "Backfill tests complete"
