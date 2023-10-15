#!/usr/bin/env bash

# Runs backfill tests.
# NOTE(kwannoel):
# The following scenario is adapted in madsim's integration tests as well.
# But this script reproduces it more reliably (I'm not sure why.)
# Hence keeping it in case we ever need to debug backfill again.

# USAGE:
# ```sh
# cargo make ci-start ci-backfill
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

basic() {
  echo "--- e2e, test_backfill_basic"
  cargo make ci-start ci-backfill
  run_sql_file "$PARENT_PATH"/sql/backfill/create_base_table.sql

  # Provide snapshot
  run_sql_file "$PARENT_PATH"/sql/backfill/insert.sql
  run_sql_file "$PARENT_PATH"/sql/backfill/insert.sql &
  run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &

  wait

  run_sql_file "$PARENT_PATH"/sql/backfill/select.sql </dev/null

  echo "--- Kill cluster"
  cargo make kill
}

# Lots of upstream tombstone, backfill should still proceed.
test_backfill_tombstone() {
  echo "--- e2e, test_backfill_tombstone"
  cargo make ci-start ci-backfill
  ./risedev psql -c "
  CREATE TABLE tomb (v1 int)
  WITH (
    connector = 'datagen',
    fields.v1._.kind = 'sequence',
    datagen.rows.per.second = '10000000'
  )
  FORMAT PLAIN
  ENCODE JSON;
  "

  sleep 30

  bash -c '
    set -euo pipefail

    for i in $(seq 1 1000)
    do
      ./risedev psql -c "DELETE FROM tomb; FLUSH;"
      sleep 1
    done
  ' 1>deletes.log 2>&1 &

  ./risedev psql -c "CREATE MATERIALIZED VIEW m1 as select * from tomb;"
  echo "--- Kill cluster"
  cargo make kill
}

main() {
  basic
  test_backfill_tombstone
}

main
