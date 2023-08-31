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

if [[ $mode == "standalone" ]]; then
  source ci/scripts/standalone-utils.sh
fi

cluster_start() {
  if [[ $mode == "standalone" ]]; then
    start_standalone "$PREFIX_LOG"/standalone.log &
    cargo make ci-start standalone-minio-etcd-compactor
  else
    cargo make ci-start "$mode"
  fi
}

cluster_stop() {
  if [[ $mode == "standalone" ]]; then
    stop_standalone
  fi
  cargo make ci-kill
}

download_and_prepare_rw "$profile" common

echo "--- Download artifacts"
download-and-decompress-artifact e2e_test_generated ./
download-and-decompress-artifact risingwave_e2e_extended_mode_test-"$profile" target/debug/
buildkite-agent artifact download risingwave-udf-example.jar ./
mv target/debug/risingwave_e2e_extended_mode_test-"$profile" target/debug/risingwave_e2e_extended_mode_test

chmod +x ./target/debug/risingwave_e2e_extended_mode_test

echo "--- e2e, $mode, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
# Please make sure the regression is expected before increasing the timeout.
sqllogictest -p 4566 -d dev './e2e_test/streaming/**/*.slt' --junit "streaming-${profile}"

echo "--- Kill cluster"
cluster_stop

echo "--- e2e, $mode, batch"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
sqllogictest -p 4566 -d dev './e2e_test/ddl/**/*.slt' --junit "batch-ddl-${profile}"
sqllogictest -p 4566 -d dev './e2e_test/visibility_mode/*.slt' --junit "batch-${profile}"
sqllogictest -p 4566 -d dev './e2e_test/database/prepare.slt'
sqllogictest -p 4566 -d test './e2e_test/database/test.slt'

echo "--- e2e, $mode, Apache Superset"
sqllogictest -p 4566 -d dev './e2e_test/superset/*.slt' --junit "batch-${profile}"

echo "--- e2e, $mode, python udf"
python3 e2e_test/udf/test.py &
sleep 2
sqllogictest -p 4566 -d dev './e2e_test/udf/udf.slt'
pkill python3

echo "--- e2e, $mode, java udf"
java -jar risingwave-udf-example.jar &
sleep 2
sqllogictest -p 4566 -d dev './e2e_test/udf/udf.slt'
pkill java

echo "--- Kill cluster"
cluster_stop

echo "--- e2e, $mode, generated"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
sqllogictest -p 4566 -d dev './e2e_test/generated/**/*.slt' --junit "generated-${profile}"

echo "--- Kill cluster"
cluster_stop

echo "--- e2e, $mode, extended query"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
sqllogictest -p 4566 -d dev -e postgres-extended './e2e_test/extended_mode/**/*.slt'
RUST_BACKTRACE=1 target/debug/risingwave_e2e_extended_mode_test --host 127.0.0.1 \
  -p 4566 \
  -u root

echo "--- Kill cluster"
cluster_stop

if [[ "$RUN_DELETE_RANGE" -eq "1" ]]; then
    echo "--- e2e, ci-delete-range-test"
    cargo make clean-data
    RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
    cargo make ci-start ci-delete-range-test
    download-and-decompress-artifact delete-range-test-"$profile" target/debug/
    mv target/debug/delete-range-test-"$profile" target/debug/delete-range-test
    chmod +x ./target/debug/delete-range-test

    config_path=".risingwave/config/risingwave.toml"
    ./target/debug/delete-range-test --ci-mode --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 --config-path "${config_path}"

    echo "--- Kill cluster"
    cluster_stop
fi

if [[ "$RUN_COMPACTION" -eq "1" ]]; then
    echo "--- e2e, ci-compaction-test, nexmark_q7"
    RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
    cargo make ci-start ci-compaction-test
    # Please make sure the regression is expected before increasing the timeout.
    sqllogictest -p 4566 -d dev './e2e_test/compaction/ingest_rows.slt'

    # We should ingest about 100 version deltas before the test
    echo "--- Wait for data ingestion"

    export RW_HUMMOCK_URL="hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001"
    export RW_META_ADDR="http://127.0.0.1:5690"

    # Poll the current version id until we have around 100 version deltas
    delta_log_cnt=0
    while [ $delta_log_cnt -le 90 ]
    do
        delta_log_cnt="$(./target/debug/risingwave risectl hummock list-version --verbose | grep -w '^ *id:' | grep -o '[0-9]\+' | head -n 1)"
        echo "Current version $delta_log_cnt"
        sleep 5
    done

    echo "--- Pause source and disable commit new epochs"
    ./target/debug/risingwave risectl meta pause
    ./target/debug/risingwave risectl hummock disable-commit-epoch

    echo "--- Start to run compaction test"
    download-and-decompress-artifact compaction-test-"$profile" target/debug/
    mv target/debug/compaction-test-"$profile" target/debug/compaction-test
    chmod +x ./target/debug/compaction-test
    # Use the config of ci-compaction-test for replay.
    config_path=".risingwave/config/risingwave.toml"
    RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
    ./target/debug/compaction-test --ci-mode --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 --config-path "${config_path}"

    echo "--- Kill cluster"
    cluster_stop
fi

if [[ "$RUN_META_BACKUP" -eq "1" ]]; then
    echo "--- e2e, ci-meta-backup-test"
    download-and-decompress-artifact backup-restore-"$profile" target/debug/
    mv target/debug/backup-restore-"$profile" target/debug/backup-restore
    chmod +x ./target/debug/backup-restore

    test_root="src/storage/backup/integration_tests"
    BACKUP_TEST_BACKUP_RESTORE="target/debug/backup-restore" \
    BACKUP_TEST_MCLI=".risingwave/bin/mcli" \
    BACKUP_TEST_MCLI_CONFIG=".risingwave/config/mcli" \
    BACKUP_TEST_RW_ALL_IN_ONE="target/debug/risingwave" \
    RW_HUMMOCK_URL="hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001" \
    RW_META_ADDR="http://127.0.0.1:5690" \
    RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
    bash "${test_root}/run_all.sh"
    echo "--- Kill cluster"
    cargo make kill
fi

if [[ "$mode" == "standalone" ]]; then
  run_sql() {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
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

  # Make sure any remaining background task exits.
  wait
fi
