#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

################ SETUP

source ci/scripts/common.sh

# NOTE: Don't enable RUST_LOG for this test in CI.
# Otherwise log size too big.

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

echo "--- e2e, ci-backfill, build"
cargo make ci-start ci-backfill

################ TESTS

echo "--- e2e, ci-backfill, run backfill test"
./ci/scripts/run-backfill-tests.sh

echo "--- Kill cluster"
cargo make kill
