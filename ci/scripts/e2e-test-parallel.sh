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
# preparing for embedded wasm udf tests
mkdir -p e2e_test/udf/embedded_wasm/target/wasm32-wasip1/release/
buildkite-agent artifact download udf.wasm e2e_test/udf/embedded_wasm/target/wasm32-wasip1/release/
# preparing for external java udf tests
mkdir -p e2e_test/udf/remote_java/target/
buildkite-agent artifact download udf.jar e2e_test/udf/remote_java/target/
# preparing for generated tests
download-and-decompress-artifact e2e_test_generated ./

mode=ci-3streaming-2serving-3fe
start_cluster() {
    risedev ci-start $mode
}

kill_cluster() {
    risedev ci-kill
}

host_args=(-h localhost -p 4565 -h localhost -p 4566 -h localhost -p 4567)

echo "--- e2e, $mode, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_storage::hummock::compactor::compactor_runner=warn" \
start_cluster
risedev slt "${host_args[@]}" -d dev './e2e_test/streaming/**/*.slt' -j 16 --junit "parallel-streaming-${profile}" --label "parallel"
kill_cluster

echo "--- e2e, $mode, batch"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_storage::hummock::compactor::compactor_runner=warn" \
start_cluster
# Exclude files that contain ALTER SYSTEM commands
find ./e2e_test/ddl -name "*.slt" -type f -exec grep -L "ALTER SYSTEM" {} \; | xargs -r risedev slt "${host_args[@]}" -d dev --junit "parallel-batch-ddl-${profile}" --label "parallel"
risedev slt "${host_args[@]}" -d dev './e2e_test/visibility_mode/*.slt' -j 16 --junit "parallel-batch-${profile}" --label "parallel"
kill_cluster

echo "--- e2e, $mode, udf"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_storage::hummock::compactor::compactor_runner=warn" \
start_cluster
python3 -m pip install --break-system-packages -r ./e2e_test/udf/remote_python/requirements.txt
risedev slt "${host_args[@]}" -d dev './e2e_test/udf/tests/**/*.slt' -j 16 --junit "parallel-udf-${profile}" --label "parallel"
kill_cluster

echo "--- e2e, $mode, generated"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_storage::hummock::compactor::compactor_runner=warn" \
start_cluster
risedev slt "${host_args[@]}" -d dev './e2e_test/generated/**/*.slt' -j 16 --junit "parallel-generated-${profile}" --label "parallel"
kill_cluster
