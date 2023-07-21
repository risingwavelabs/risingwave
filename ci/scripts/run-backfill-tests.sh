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

flush() {
  psql -h localhost -p 4566 -d dev -U root -c "FLUSH;"
}

run_sql_file "$PARENT_PATH"/sql/backfill/create_base_table.sql
run_sql_file "$PARENT_PATH"/sql/backfill/insert_seed.sql
for i in $(seq 1 18)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql
  flush
done

run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &

# Create lots of update + barrier,
for i in $(seq 1 1000)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_seed.sql &
done

wait

run_sql_file "$PARENT_PATH"/sql/backfill/select.sql