#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

################ SETUP

source ci/scripts/common.sh

export RUST_LOG=info
export SQLSMITH_COUNT=100

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

git config --global --add safe.directory /risingwave

download_and_prepare_rw "$profile" common

echo "--- e2e, ci-3cn-1fe, build"
cargo make ci-start ci-3cn-1fe

################ TESTS

echo "--- e2e, ci-3cn-1fe, run backfill test"
./run-backfill-tests.sh

echo "Backfill tests complete"

echo "--- Kill cluster"
cargo make ci-kill
