#!/usr/bin/env bash

# Runs backfill tests.

# USAGE:
# ```sh
# cargo make ci-start ci-backfill
# ./ci/scripts/run-backfill-tests.sh
# ```
# Example progress:
# dev=> select * from rw_catalog.rw_ddl_progress;
# ddl_id |                 ddl_statement                  | progress |        initialized_at
#--------+------------------------------------------------+----------+-------------------------------
#   1002 | CREATE MATERIALIZED VIEW m1 AS SELECT * FROM t | 56.12%   | 2023-09-27 06:37:06.636+00:00
#(1 row)


set -euo pipefail

PARENT_PATH=$(dirname "${BASH_SOURCE[0]}")
TEST_DIR=$PWD/e2e_test
CLUSTER_PROFILE='ci-1cn-1fe-with-recovery'
export RUST_LOG="risingwave_meta=trace"

run_sql_file() {
  psql -h localhost -p 4566 -d dev -U root -f "$@"
}

run_sql() {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

flush() {
  run_sql "FLUSH;"
}

cancel_stream_jobs() {
  ID=$(run_sql "select ddl_id from rw_catalog.rw_ddl_progress;" | tail -3 | head -1 | grep -E -o "[0-9]*")
  echo "CANCELLING STREAM_JOB: $ID"
  run_sql "CANCEL JOBS $ID;" </dev/null
}

# Prefix logs, so they don't get overridden after node restart.
rename_logs_with_prefix() {
  prefix="$1"
  pushd .risingwave/log
  for log in *.log
    do
      mv -- "$log" "${prefix}-${log}"
    done
  popd
}

# Test snapshot and upstream read.
test_snapshot_and_upstream_read() {
  echo "--- e2e, ci-backfill, test_snapshot_and_upstream_read"
  cargo make ci-start ci-backfill

  run_sql_file "$PARENT_PATH"/sql/backfill/create_base_table.sql

  # Provide snapshot
  run_sql_file "$PARENT_PATH"/sql/backfill/insert.sql

  # Provide updates ...
  run_sql_file "$PARENT_PATH"/sql/backfill/insert.sql &

  # ... and concurrently create mv.
  run_sql_file "$PARENT_PATH"/sql/backfill/create_mv.sql &

  wait

  run_sql_file "$PARENT_PATH"/sql/backfill/select.sql </dev/null

  cargo make kill

  echo "Backfill tests complete"
}

# Test background ddl recovery
test_background_ddl_recovery() {
  echo "--- e2e, $CLUSTER_PROFILE, test_background_ddl_recovery"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/recovery/create.slt"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/recovery/validate.slt"

  OLD_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

  sleep 2

  rename_logs_with_prefix "before-restart"

  # Restart
  cargo make kill
  cargo make dev $CLUSTER_PROFILE

  # Test after recovery
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/recovery/validate.slt"

  # Recover the mview progress
  sleep 3

  NEW_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

  if [[ $OLD_PROGRESS < $NEW_PROGRESS ]]; then
    echo "OK: $OLD_PROGRESS smaller than $NEW_PROGRESS"
  else
    echo "FAILED: $OLD_PROGRESS larger or equal to $NEW_PROGRESS"
  fi

  sleep 60

  # Test after backfill finished
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/recovery/validate_finished.slt"

  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/recovery/drop.slt"

  cargo make kill

}

test_background_ddl_cancel() {
  echo "--- e2e, $CLUSTER_PROFILE, test background ddl"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/create_table.slt"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/create_mv.slt"
  sleep 1
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/validate.slt"

  cancel_stream_jobs
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/validate_after_cancel.slt"

  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/create_mv.slt"

  rename_logs_with_prefix "before-restart"

  # Restart
  cargo make kill
  cargo make dev $CLUSTER_PROFILE

  # Recover
  sleep 3

  cancel_stream_jobs
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/validate_after_cancel.slt"

  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/create_mv.slt"
  sleep 1
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/validate.slt"
  cancel_stream_jobs
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/cancel/drop.slt"
  cargo make kill
}

# Test foreground ddl should not recover
test_foreground_ddl_cancel() {
  echo "--- e2e, $CLUSTER_PROFILE, test_foreground_ddl_cancel"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/create_base_table.slt"
  run_sql "CREATE MATERIALIZED VIEW m1 as select * FROM t;" &
  sleep 1
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/validate.slt"

  cancel_stream_jobs
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/validate_after_cancel.slt"

  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/create_mv_after_cancel.slt"
  run_sql "DROP MATERIALIZED VIEW m1;"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/drop.slt"

  cargo make kill
}
# Test foreground ddl should not recover
test_foreground_ddl_no_recover() {
  echo "--- e2e, $CLUSTER_PROFILE, test_foreground_ddl_no_recover"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/create_base_table.slt"
  run_sql "CREATE MATERIALIZED VIEW m1 as select * FROM t;" &
  sleep 3
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/validate.slt"

  rename_logs_with_prefix "before-restart"

  # Restart
  cargo make kill
  cargo make dev $CLUSTER_PROFILE

  # Leave sometime for recovery
  sleep 5

  # Test after recovery
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/validate_restart.slt"
  sleep 30

  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/foreground/drop.slt"

  cargo make kill
}

test_foreground_index_cancel() {
   echo "--- e2e, $CLUSTER_PROFILE, test_index_ddl_no_recover"
   cargo make ci-start $CLUSTER_PROFILE

   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/create_base_table.slt"

   # Test cancel
   run_sql "CREATE INDEX i ON t (v1);" &
   sleep 3
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_one_job.slt"
   cancel_stream_jobs
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_no_jobs.slt"

   # Test index over recovery
   run_sql "CREATE INDEX i ON t (v1);" &
   sleep 3
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_one_job.slt"

   rename_logs_with_prefix "before-restart"

   # Restart
   cargo make kill
   cargo make dev ci-1cn-1fe-with-recovery

   # Leave sometime for recovery
   sleep 5

   # Test after recovery
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_no_jobs.slt"
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/index/create_index.slt"

   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/index/drop.slt"

   cargo make kill
}

test_foreground_sink_cancel() {
   echo "--- e2e, $CLUSTER_PROFILE, test_index_ddl_no_recover"
   cargo make ci-start $CLUSTER_PROFILE

   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/create_base_table.slt"

   # Test cancel
   run_sql "CREATE SINK i FROM t WITH (connector='blackhole');" &
   sleep 3
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_one_job.slt"
   cancel_stream_jobs
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_no_jobs.slt"

   # Test index over recovery
   run_sql "CREATE SINK i FROM t WITH (connector='blackhole');" &
   sleep 3
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_one_job.slt"

   rename_logs_with_prefix "before-restart"

   # Restart
   cargo make kill
   cargo make dev $CLUSTER_PROFILE

   # Leave sometime for recovery
   sleep 5

   # Test after recovery
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/common/validate_no_jobs.slt"
   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/sink/create_sink.slt"

   sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/background_ddl/sink/drop.slt"

   cargo make kill
}

main() {
  set -euo pipefail
  test_snapshot_and_upstream_read
  test_background_ddl_recovery
  test_background_ddl_cancel
  test_foreground_ddl_no_recover
  test_foreground_ddl_cancel
  test_foreground_index_cancel
  test_foreground_sink_cancel
}

main
