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
BACKGROUND_DDL_DIR=$TEST_DIR/background_ddl
COMMON_DIR=$BACKGROUND_DDL_DIR/common

CLUSTER_PROFILE='ci-1cn-1fe-with-recovery'
export RUST_LOG="risingwave_meta=trace"

export SLT_DB="dev"
export SLT_PORT="4566"

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

kill_cluster() {
  cargo make ci-kill
}

restart_cluster() {
  kill_cluster
  rename_logs_with_prefix "before-restart"
  cargo make dev $CLUSTER_PROFILE
}

restart_cn() {
  tmux list-windows -t risedev | grep compute-node | grep -o "^[0-9]*" | xargs -I {} tmux send-keys -t %{} C-c C-d
  sleep 4
  mv .risingwave/log/compute-node-*.log .risingwave/log/before-restart-cn-compute-node.log
  ./.risingwave/bin/risingwave/compute-node \
  --config-path \
  ./.risingwave/config/risingwave.toml \
  --listen-addr \
  127.0.0.1:5688 \
  --prometheus-listener-addr \
  127.0.0.1:1222 \
  --advertise-addr \
  127.0.0.1:5688 \
  --async-stack-trace \
  verbose \
  --connector-rpc-endpoint \
  127.0.0.1:50051 \
  --parallelism \
  4 \
  --total-memory-bytes \
  8589934592 \
  --role \
  both \
  --meta-address \
  http://127.0.0.1:5690 >.risingwave/log/compute-node.log 2>&1 &
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
  cargo make wait-processes-exit
}

# Test background ddl recovery
test_background_ddl_recovery() {
  echo "--- e2e, $CLUSTER_PROFILE, test_background_ddl_recovery"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest "$COMMON_DIR/create_table.slt"
  sqllogictest "$COMMON_DIR/create_bg_mv.slt"
  sleep 1
  OLD_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

  # Restart
  restart_cluster

  # Test after recovery
  sqllogictest "$COMMON_DIR/validate_one_job.slt"

  # Recover the mview progress
  sleep 5

  NEW_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

  if [[ ${OLD_PROGRESS%.*} -le ${NEW_PROGRESS%.*} ]]; then
    echo "OK: $OLD_PROGRESS smaller or equal to $NEW_PROGRESS"
  else
    echo "FAILED: $OLD_PROGRESS larger than $NEW_PROGRESS"
    exit 1
  fi

  sleep 60

  # Test after backfill finished
  sqllogictest "$COMMON_DIR/validate_backfilled_mv.slt"

  # After cluster restart(s), backfilled mv should still be present.
  restart_cluster
  sqllogictest "$COMMON_DIR/validate_backfilled_mv.slt"
  restart_cluster
  sqllogictest "$COMMON_DIR/validate_backfilled_mv.slt"

  sqllogictest "$COMMON_DIR/drop_mv.slt"
  sqllogictest "$COMMON_DIR/drop_table.slt"

  kill_cluster
}

test_background_ddl_cancel() {
  echo "--- e2e, $CLUSTER_PROFILE, test background ddl"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest "$COMMON_DIR/create_table.slt"
  sqllogictest "$COMMON_DIR/create_bg_mv.slt"
  sleep 1
  sqllogictest "$COMMON_DIR/validate_one_job.slt"

  cancel_stream_jobs
  sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

  sqllogictest "$COMMON_DIR/create_bg_mv.slt"

  # Restart
  restart_cluster

  # Recover
  sleep 3

  sqllogictest "$COMMON_DIR/validate_one_job.slt"

  cancel_stream_jobs
  sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

  sqllogictest "$COMMON_DIR/create_bg_mv.slt"
  sleep 1
  sqllogictest "$COMMON_DIR/validate_one_job.slt"
  cancel_stream_jobs
  sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

  # After cancel should be able to create MV
  sqllogictest "$COMMON_DIR/create_bg_mv.slt"
  sleep 1
  sqllogictest "$COMMON_DIR/validate_one_job.slt"
  cancel_stream_jobs
  sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

  sqllogictest "$COMMON_DIR/drop_table.slt"

  kill_cluster
}

# Test foreground ddl should not recover
test_foreground_ddl_cancel() {
  echo "--- e2e, $CLUSTER_PROFILE, test_foreground_ddl_cancel"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest "$COMMON_DIR/create_table.slt"
  run_sql "CREATE MATERIALIZED VIEW m1 as select * FROM t;" &
  sleep 1
  sqllogictest "$COMMON_DIR/validate_one_job.slt"

  cancel_stream_jobs
  sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

  sqllogictest "$COMMON_DIR/create_fg_mv.slt"
  sqllogictest "$COMMON_DIR/drop_mv.slt"
  sqllogictest "$COMMON_DIR/drop_table.slt"

  kill_cluster
}

# Test foreground ddl should not recover
test_foreground_ddl_no_recover() {
  echo "--- e2e, $CLUSTER_PROFILE, test_foreground_ddl_no_recover"
  cargo make ci-start $CLUSTER_PROFILE

  # Test before recovery
  sqllogictest "$COMMON_DIR/create_table.slt"
  run_sql "CREATE MATERIALIZED VIEW m1 as select * FROM t;" &
  sleep 3
  sqllogictest "$COMMON_DIR/validate_one_job.slt"


  # Restart
  restart_cluster

  # Leave sometime for recovery
  sleep 5

  # Test after recovery
  sqllogictest "$COMMON_DIR/validate_no_jobs.slt"
  sleep 30

  sqllogictest "$COMMON_DIR/drop_table.slt"

  kill_cluster
}

test_foreground_index_cancel() {
   echo "--- e2e, $CLUSTER_PROFILE, test_foreground_index_cancel"
   cargo make ci-start $CLUSTER_PROFILE

   sqllogictest "$COMMON_DIR/create_table.slt"

   # Test cancel
   run_sql "CREATE INDEX i ON t (v1);" &
   sleep 3
   sqllogictest "$COMMON_DIR/validate_one_job.slt"
   cancel_stream_jobs
   sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

   # Test index over recovery
   run_sql "CREATE INDEX i ON t (v1);" &
   sleep 3
   sqllogictest "$COMMON_DIR/validate_one_job.slt"


   # Restart
   restart_cluster

   # Leave sometime for recovery
   sleep 5

   # Test after recovery
   sqllogictest "$COMMON_DIR/validate_no_jobs.slt"
   sqllogictest "$COMMON_DIR/create_index.slt"

   sqllogictest "$COMMON_DIR/drop_index.slt"
   sqllogictest "$COMMON_DIR/drop_table.slt"

   kill_cluster
}

test_foreground_sink_cancel() {
   echo "--- e2e, $CLUSTER_PROFILE, test_foreground_sink_ddl_cancel"
   cargo make ci-start $CLUSTER_PROFILE

   sqllogictest "$COMMON_DIR/create_table.slt"

   # Test cancel
   run_sql "CREATE SINK i FROM t WITH (connector='blackhole');" &
   sleep 3
   sqllogictest "$COMMON_DIR/validate_one_job.slt"
   cancel_stream_jobs
   sqllogictest "$COMMON_DIR/validate_no_jobs.slt"

   # Test sink over recovery
   run_sql "CREATE SINK i FROM t WITH (connector='blackhole');" &
   sleep 3
   sqllogictest "$COMMON_DIR/validate_one_job.slt"


   # Restart
   restart_cluster

   # Leave sometime for recovery
   sleep 5

   # Test after recovery
   sqllogictest "$COMMON_DIR/validate_no_jobs.slt"
   sqllogictest "$COMMON_DIR/create_sink.slt"

   sqllogictest "$COMMON_DIR/drop_sink.slt"
   sqllogictest "$COMMON_DIR/drop_table.slt"

   kill_cluster
}

# Lots of upstream tombstone, backfill should still proceed.
test_backfill_tombstone() {
  echo "--- e2e, test_backfill_tombstone"
  cargo make ci-start $CLUSTER_PROFILE
  ./risedev psql -c "
  CREATE TABLE tomb (v1 int)
  WITH (
    connector = 'datagen',
    fields.v1._.kind = 'sequence',
    datagen.rows.per.second = '1000000'
  )
  FORMAT PLAIN
  ENCODE JSON;
  "

  sleep 10

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
  kill_cluster
  wait
}

test_backfill_restart_cn_recovery() {
   echo "--- e2e, $CLUSTER_PROFILE, test_background_restart_cn_recovery"
   cargo make ci-start $CLUSTER_PROFILE

   # Test before recovery
   sqllogictest "$COMMON_DIR/create_table.slt"
   sqllogictest "$COMMON_DIR/create_bg_mv.slt"
   sleep 1
   OLD_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

   # Restart 1 CN
   restart_cn

   # Give some time to recover.
   sleep 3

   # Test after recovery
   sqllogictest "$COMMON_DIR/validate_one_job.slt"

   # Recover the mview progress
   sleep 5

   NEW_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

   if [[ ${OLD_PROGRESS%.*} -le ${NEW_PROGRESS%.*} ]]; then
     echo "OK: $OLD_PROGRESS smaller or equal to $NEW_PROGRESS"
   else
     echo "FAILED: $OLD_PROGRESS larger than $NEW_PROGRESS"
     exit 1
   fi

   # Trigger a bootstrap recovery
   pkill compute-node
   kill_cluster
   rename_logs_with_prefix "before-restart"
   sleep 10
   cargo make dev $CLUSTER_PROFILE

   # Recover mview progress
   sleep 5

   OLD_PROGRESS=$NEW_PROGRESS
   NEW_PROGRESS=$(run_sql "SHOW JOBS;" | grep -E -o "[0-9]{1,2}\.[0-9]{1,2}")

   if [[ ${OLD_PROGRESS%.*} -le ${NEW_PROGRESS%.*} ]]; then
     echo "OK: $OLD_PROGRESS smaller or equal to $NEW_PROGRESS"
   else
     echo "FAILED: $OLD_PROGRESS larger than $NEW_PROGRESS"
     exit 1
   fi

   sleep 60

   # Test after backfill finished
   sqllogictest "$COMMON_DIR/validate_backfilled_mv.slt"

   # After cluster restart(s), backfilled mv should still be present.
   restart_cluster
   sqllogictest "$COMMON_DIR/validate_backfilled_mv.slt"
   restart_cluster
   sqllogictest "$COMMON_DIR/validate_backfilled_mv.slt"

   sqllogictest "$COMMON_DIR/drop_mv.slt"
   sqllogictest "$COMMON_DIR/drop_table.slt"

   kill_cluster
}

main() {
  set -euo pipefail
  test_snapshot_and_upstream_read
  test_backfill_tombstone
  test_background_ddl_recovery
  test_background_ddl_cancel
  test_foreground_ddl_no_recover
  test_foreground_ddl_cancel
  test_foreground_index_cancel
  test_foreground_sink_cancel
  test_backfill_restart_cn_recovery
}

main
