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

host_args=(-h localhost -p 4565 -h localhost -p 4566 -h localhost -p 4567)

echo "--- e2e, ci-3cn-3fe-opendal-fs-backend, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-3cn-3fe-opendal-fs-backend
sqllogictest "${host_args[@]}" -d dev  './e2e_test/streaming/**/*.slt' --keep-db-on-failure -j 16 --junit "parallel-opendal-fs-backend-${profile}" --label "parallel"

echo "--- Kill cluster Streaming"
risedev ci-kill
sleep 1
rm -rf /tmp/rw_ci

echo "--- e2e, ci-3cn-3fe-opendal-fs-backend, batch"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-3cn-3fe-opendal-fs-backend
sqllogictest "${host_args[@]}" -d dev  './e2e_test/ddl/**/*.slt' --junit "parallel-opendal-fs-backend-ddl-${profile}" --label "parallel"
sqllogictest "${host_args[@]}" -d dev  './e2e_test/visibility_mode/*.slt' --keep-db-on-failure -j 16 --junit "parallel-opendal-fs-backend-batch-${profile}" --label "parallel"

echo "--- Kill cluster Batch"
risedev ci-kill
sleep 1
rm -rf /tmp/rw_ci
