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

download_and_prepare_rw "$profile" source

echo "--- download java binding integration test"
buildkite-agent artifact download java-binding-integration-test.tar.zst ./
tar xf java-binding-integration-test.tar.zst

echo "--- starting risingwave cluster"
cargo make ci-start java-binding-demo

echo "--- ingest data and run java binding"
cargo make ingest-data-and-run-java-binding

echo "--- Kill cluster"
cargo make ci-kill

echo "--- run stream chunk java binding"
RISINGWAVE_ROOT=$(git rev-parse --show-toplevel)

cd ${RISINGWAVE_ROOT}/java

(${RISINGWAVE_ROOT}/bin/data-chunk-payload-generator) | \
    java -cp "./java-binding-integration-test/target/dependency/*:./java-binding-integration-test/target/classes" \
    com.risingwave.java.binding.StreamChunkDemo
