#!/usr/bin/env bash

if [[ -z "${RUST_MIN_STACK}" ]]; then
  export RUST_MIN_STACK=4194304
fi

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
sqllogictest "${host_args[@]}" -d dev './e2e_test/streaming/**/*.slt' --keep-db-on-failure -j 16 --junit "parallel-streaming-${profile}" --label "parallel"

kill_cluster

echo "--- e2e, ci-3streaming-2serving-3fe, batch"
RUST_LOG=$RUST_LOG \
risedev ci-start ci-3streaming-2serving-3fe
# Exclude files that contain ALTER SYSTEM commands
find ./e2e_test/ddl -name "*.slt" -type f -exec grep -L "ALTER SYSTEM" {} \; | xargs -r sqllogictest "${host_args[@]}" -d dev --junit "parallel-batch-ddl-${profile}" --label "parallel"
sqllogictest "${host_args[@]}" -d dev './e2e_test/visibility_mode/*.slt' --keep-db-on-failure -j 16 --junit "parallel-batch-${profile}" --label "parallel"

kill_cluster

echo "--- e2e, ci-3streaming-2serving-3fe, generated"
RUST_LOG=$RUST_LOG \
risedev ci-start ci-3streaming-2serving-3fe
sqllogictest "${host_args[@]}" -d dev './e2e_test/generated/**/*.slt' --keep-db-on-failure -j 16 --junit "parallel-generated-${profile}" --label "parallel"

kill_cluster
