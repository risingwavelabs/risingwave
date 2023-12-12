#!/usr/bin/env bash

# Runs backfill tests.

# USAGE:
# ```sh
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

CLUSTER_PROFILE='ci-1cn-1fe-kafka-with-recovery'
export RUST_LOG="info,risingwave_meta::barrier::progress=debug,risingwave_meta::rpc::ddl_controller=debug"

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
  cargo make kill
  cargo make wait-processes-exit
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
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/create_base_table.sql

  # Provide snapshot
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/insert.sql

  # Provide updates ...
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/insert.sql &

  # ... and concurrently create mv.
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/create_mv.sql &

  wait

  run_sql_file "$PARENT_PATH"/sql/backfill/basic/select.sql </dev/null

  cargo make kill
  cargo make wait-processes-exit
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
    datagen.rows.per.second = '2000000'
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
  cargo make wait-processes-exit
  wait
}

test_replication_with_column_pruning() {
  echo "--- e2e, test_replication_with_column_pruning"
  cargo make ci-start ci-backfill
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/create_base_table.sql
  # Provide snapshot
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/insert.sql

  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/create_mv.sql &

  # Provide upstream updates
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/insert.sql &

  wait

  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/select.sql </dev/null
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/drop.sql
  echo "--- Kill cluster"
  cargo make kill
  cargo make wait-processes-exit
}

# Test sink backfill recovery
test_sink_backfill_recovery() {
  echo "--- e2e, test_sink_backfill_recovery"
  cargo make ci-start $CLUSTER_PROFILE

  # Check progress
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/sink/create_sink.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/background_ddl/common/validate_one_job.slt'

  # Restart
  restart_cluster
  sleep 3

  # FIXME(kwannoel): Sink's backfill progress is not recovered yet.
  # Check progress
  # sqllogictest -p 4566 -d dev 'e2e_test/background_ddl/common/validate_one_job.slt'

  # Sink back into rw
  run_sql "CREATE TABLE table_kafka (v1 int primary key)
    WITH (
      connector = 'kafka',
      topic = 's_kafka',
      properties.bootstrap.server = 'localhost:29092',
  ) FORMAT DEBEZIUM ENCODE JSON;"

  sleep 10

  # Verify data matches upstream table.
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/sink/validate_sink.slt'
}

main() {
  set -euo pipefail
  test_snapshot_and_upstream_read
  test_backfill_tombstone
  test_replication_with_column_pruning
  test_sink_backfill_recovery
}

main
