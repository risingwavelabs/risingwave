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

download_and_prepare_rw "$profile" common


echo "--- Download artifacts"
mkdir -p e2e_test/udf/wasm/target/wasm32-wasip1/release/
buildkite-agent artifact download udf.wasm e2e_test/udf/wasm/target/wasm32-wasip1/release/
buildkite-agent artifact download udf.jar ./

echo "--- e2e, $mode, slow-udf-tests"
python3 -m pip install --break-system-packages arrow-udf==0.2.1
RUST_LOG="info" \
risedev ci-start "$mode"
sqllogictest -p 4566 -d dev './e2e_test/slow_tests/udf/always_retry_python.slt'
sqllogictest -p 4566 -d dev './e2e_test/slow_tests/backfill/rate_limit/slow-udf.slt'
