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

download_and_prepare_rw "$profile"

echo "--- Download artifacts"
buildkite-agent artifact download "e2e_test/generated/*" ./



host_args="-h localhost -p 4565 -h localhost -p 4566 -h localhost -p 4567"

echo "--- e2e, ci-3cn-3fe, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-3cn-3fe
sqllogictest ${host_args} -d dev './e2e_test/streaming/**/*.slt' -j 16 --junit "parallel-streaming-${profile}"

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-3cn-3fe, batch"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-3cn-3fe
sqllogictest ${host_args} -d dev './e2e_test/ddl/**/*.slt' --junit "parallel-batch-ddl-${profile}"
sqllogictest ${host_args} -d dev './e2e_test/batch/**/*.slt' -j 16 --junit "parallel-batch-${profile}"

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-3cn-3fe, generated"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-3cn-3fe
sqllogictest ${host_args} -d dev './e2e_test/generated/**/*.slt' -j 16 --junit "parallel-generated-${profile}"

echo "--- Kill cluster"
cargo make ci-kill
