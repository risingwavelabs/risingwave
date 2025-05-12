#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

while getopts 'p:m:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        m )
            mode=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

source ci/scripts/common.sh

if [[ $mode == "standalone" ]]; then
  source ci/scripts/standalone-utils.sh
fi

if [[ $mode == "single-node" ]]; then
  source ci/scripts/single-node-utils.sh
fi

cluster_start() {
  if [[ $mode == "standalone" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    risedev dev standalone-minio-sqlite &
    PID=$!
    sleep 1
    start_standalone "$PREFIX_LOG"/standalone.log &
    wait $PID
  elif [[ $mode == "single-node" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    start_single_node "$PREFIX_LOG"/single-node.log &
    # Give it a while to make sure the single-node is ready.
    sleep 30
  else
    risedev ci-start "$mode"
  fi
}

cluster_stop() {
  if [[ $mode == "standalone" ]]
  then
    stop_standalone
    # Don't check standalone logs, they will exceed the limit.
    risedev kill
  elif [[ $mode == "single-node" ]]
  then
    stop_single_node
  else
    risedev ci-kill
  fi
}

download_and_prepare_rw "$profile" common

echo "--- Download artifacts"
# preparing for extended mode tests
download-and-decompress-artifact risingwave_e2e_extended_mode_test-"$profile" target/debug/
mv target/debug/risingwave_e2e_extended_mode_test-"$profile" target/debug/risingwave_e2e_extended_mode_test
chmod +x ./target/debug/risingwave_e2e_extended_mode_test

echo "--- Install Python Dependencies"
python3 -m pip install --break-system-packages -r ./e2e_test/requirements.txt

echo "--- e2e, $mode, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_stream::common::table::state_table=warn" \
cluster_start
# Please make sure the regression is expected before increasing the timeout.
risedev slt -p 4566 -d dev './e2e_test/streaming/**/*.slt' --junit "streaming-${profile}"
risedev slt -p 4566 -d dev './e2e_test/backfill/sink/different_pk_and_dist_key.slt'

if [[ "$profile" == "ci-release" ]]; then
  echo "--- e2e, $mode, backfill"
  # only run in release-mode. It's too slow for dev-mode.
  risedev slt -p 4566 -d dev './e2e_test/backfill/backfill_order_control.slt'
  risedev slt -p 4566 -d dev './e2e_test/backfill/backfill_order_control_recovery.slt'
fi

echo "--- Kill cluster"
cluster_stop

echo "--- e2e, $mode, batch"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
risedev slt -p 4566 -d dev './e2e_test/ddl/**/*.slt' --junit "batch-ddl-${profile}" --label "can-use-recover"
risedev slt -p 4566 -d dev './e2e_test/background_ddl/basic.slt' --junit "batch-ddl-${profile}"

if [[ "$mode" != "single-node" && "$mode" != "standalone" ]]; then
  risedev slt -p 4566 -d dev './e2e_test/visibility_mode/*.slt' --junit "batch-${profile}"
fi

risedev slt -p 4566 -d dev './e2e_test/ttl/ttl.slt'
risedev slt -p 4566 -d dev './e2e_test/dml/*.slt'

echo "--- e2e, $mode, misc"
risedev slt -p 4566 -d dev './e2e_test/misc/**/*.slt'

echo "--- e2e, $mode, python_client"
python3 ./e2e_test/python_client/main.py

echo "--- e2e, $mode, subscription"
risedev slt -p 4566 -d dev './e2e_test/subscription/check_sql_statement.slt'
python3 ./e2e_test/subscription/main.py

echo "--- e2e, $mode, Apache Superset"
risedev slt -p 4566 -d dev './e2e_test/superset/*.slt' --junit "batch-${profile}"

echo "--- Kill cluster"
cluster_stop

# only run if mode is not single-node or standalone
if [[ "$mode" != "single-node" && "$mode" != "standalone" ]]; then
  echo "--- e2e, ci-3cn-1fe-with-recovery, error ui"
  RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
  risedev ci-start ci-3cn-1fe-with-recovery
  risedev slt -p 4566 -d dev './e2e_test/error_ui/simple/**/*.slt'
  risedev slt -p 4566 -d dev -e postgres-extended './e2e_test/error_ui/extended/**/*.slt'

  echo "--- Kill cluster"
  risedev ci-kill
fi

echo "--- e2e, $mode, extended query"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
risedev slt -p 4566 -d dev -e postgres-extended './e2e_test/extended_mode/**/*.slt'
RUST_BACKTRACE=1 target/debug/risingwave_e2e_extended_mode_test --host 127.0.0.1 \
  -p 4566 \
  -u root

echo "--- Kill cluster"
cluster_stop

if [[ "$mode" == "standalone" ]]; then
  run_sql() {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
  }
  compactor_is_online() {
    set +e
    grep -q "risingwave_cmd_all::standalone: starting compactor-node thread" "${PREFIX_LOG}/standalone.log"
    local EXIT_CODE=$?
    set -e
    return $EXIT_CODE
  }

  echo "--- e2e, standalone, cluster-persistence-test"
  cluster_start
  run_sql "CREATE TABLE t (v1 int);
  INSERT INTO t VALUES (1);
  INSERT INTO t VALUES (2);
  INSERT INTO t VALUES (3);
  INSERT INTO t VALUES (4);
  INSERT INTO t VALUES (5);
  flush;"

  EXPECTED=$(run_sql "SELECT * FROM t ORDER BY v1;")
  echo -e "Expected:\n$EXPECTED"

  echo "Restarting standalone"
  restart_standalone

  ACTUAL=$(run_sql "SELECT * FROM t ORDER BY v1;")
  echo -e "Actual:\n$ACTUAL"

  if [[ "$EXPECTED" != "$ACTUAL" ]]; then
    echo "ERROR: Expected did not match Actual."
    exit 1
  else
    echo "PASSED"
  fi

  echo "--- Kill cluster"
  cluster_stop

  wait

  # Test that we can optionally include nodes in standalone mode.
  echo "--- e2e, standalone, cluster-opts-test"

  echo "test standalone without compactor"
  mkdir -p "$PREFIX_LOG"
  risedev clean-data
  risedev pre-start-dev
  risedev dev standalone-minio-sqlite-compactor &
  PID=$!
  sleep 1
  start_standalone_without_compactor "$PREFIX_LOG"/standalone.log &
  wait $PID
  wait_standalone
  if compactor_is_online
  then
    echo "ERROR: Compactor should not be online."
    exit 1
  fi
  cluster_stop
  echo "test standalone without compactor [TEST PASSED]"

  wait

  echo "test standalone with compactor"
  mkdir -p "$PREFIX_LOG"
  risedev clean-data
  risedev pre-start-dev
  risedev dev standalone-minio-sqlite &
  PID=$!
  sleep 1
  start_standalone "$PREFIX_LOG"/standalone.log &
  wait $PID
  wait_standalone
  if ! compactor_is_online
  then
    echo "ERROR: Compactor should be online."
    exit 1
  fi
  cluster_stop
  echo "test standalone with compactor [TEST PASSED]"

  # Make sure any remaining background task exits.
  wait
fi

echo "--- Upload JUnit test results"
buildkite-agent artifact upload "*-junit.xml"
