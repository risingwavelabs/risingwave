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

host_args="-h localhost -p 4565 -h localhost -p 4566 -h localhost -p 4567"

echo "--- e2e, ci-3cn-3fe-in-memory, streaming"
cargo make ci-start ci-3cn-3fe-in-memory
sqllogictest ${host_args} -d dev  './e2e_test/streaming/**/*.slt' -j 16 --junit "parallel-in-memory-streaming-${profile}"

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-3cn-3fe-in-memory, batch"
cargo make ci-start ci-3cn-3fe-in-memory
sqllogictest ${host_args} -d dev  './e2e_test/ddl/**/*.slt' --junit "parallel-in-memory-batch-ddl-${profile}"
sqllogictest ${host_args} -d dev  './e2e_test/read_barrier/batch.slt' -j 16 --junit "parallel-in-memory-batch-${profile}"

echo "--- Kill cluster"
cargo make ci-kill