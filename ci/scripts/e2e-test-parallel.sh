#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
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

download_and_prepare_rw "$profile" common

echo "--- Download artifacts"
download-and-decompress-artifact e2e_test_generated ./

kill_cluster() {
  echo "--- Kill cluster"
  risedev ci-kill
}

host_args=(-h localhost -p 4565 -h localhost -p 4566 -h localhost -p 4567)

RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_storage::hummock::compactor::compactor_runner=warn"

echo "--- e2e, ci-3streaming-2serving-3fe, streaming"
RUST_LOG=$RUST_LOG \
risedev ci-start ci-3streaming-2serving-3fe
risedev psql "${host_args[@]}" -d dev -c "ALTER SYSTEM SET max_concurrent_creating_streaming_jobs TO 0"
sqllogictest "${host_args[@]}" -d dev './e2e_test/streaming/**/*.slt' -j 16 --junit "parallel-streaming-${profile}" --label "parallel"

kill_cluster

echo "--- e2e, ci-3streaming-2serving-3fe, batch"
RUST_LOG=$RUST_LOG \
risedev ci-start ci-3streaming-2serving-3fe
sqllogictest "${host_args[@]}" -d dev './e2e_test/ddl/**/*.slt' --junit "parallel-batch-ddl-${profile}" --label "parallel"
sqllogictest "${host_args[@]}" -d dev './e2e_test/visibility_mode/*.slt' -j 16 --junit "parallel-batch-${profile}" --label "parallel"

kill_cluster

echo "--- e2e, ci-3streaming-2serving-3fe, generated"
RUST_LOG=$RUST_LOG \
risedev ci-start ci-3streaming-2serving-3fe
sqllogictest "${host_args[@]}" -d dev './e2e_test/generated/**/*.slt' -j 16 --junit "parallel-generated-${profile}" --label "parallel"

kill_cluster
