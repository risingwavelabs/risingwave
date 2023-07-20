#!/usr/bin/env bash

# Runs backfill tests.
# The following scenario can be reproduced in madsim's integration tests as well.
# But it seems easier (less INSERTs required, more reliable) to reproduce it via this script.
# Hence keeping it in case we ever need to debug backfill again.
# NOTE(kwannoel): For now this DOES NOT run in CI.
# You can run it locally simply with: ./ci/scripts/run-backfill-tests.sh

set -euo pipefail

PARENT_PATH=$(dirname "${BASH_SOURCE[0]}")

run_sql_file() {
  psql -h localhost -p 4566 -d dev -U root -f "$@"
}

flush() {
  psql -h localhost -p 4566 -d dev -U root -c "FLUSH;"
}

./risedev d
run_sql_file "$PARENT_PATH"/sql/backfill/create_base_table.sql
run_sql_file "$PARENT_PATH"/sql/backfill/insert_seed.sql
for i in $(seq 1 16)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_recurse.sql
  flush
done

run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &

# Create lots of update + barrier,
for i in $(seq 1 100)
do
  run_sql_file "$PARENT_PATH"/sql/backfill/insert_seed.sql &
done

wait

run_sql_file "$PARENT_PATH"/sql/backfill/select.sql