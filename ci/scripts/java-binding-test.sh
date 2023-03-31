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

download_java_binding "$profile"

echo "--- starting risingwave cluster"
cargo make ci-start java-binding-demo

echo "--- Build java binding demo"
cargo make build-java-binding-java

echo "--- ingest data and run java binding"
cargo make ingest-data-and-run-java-binding

echo "--- Kill cluster"
cargo make ci-kill

echo "--- run stream chunk java binding"
cargo make run-java-binding-stream-chunk-demo
