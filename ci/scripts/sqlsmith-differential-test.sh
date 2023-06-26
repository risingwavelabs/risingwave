#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

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

# TODO(kwannoel): Support differential testing in madsim
#echo "--- Download artifacts"
#download-and-decompress-artifact risingwave_simulation .
#chmod +x ./risingwave_simulation

echo "--- Download sqlsmith e2e bin"
download-and-decompress-artifact sqlsmith-"$profile" target/debug/
mv target/debug/sqlsmith-"$profile" target/debug/sqlsmith
chmod +x ./target/debug/sqlsmith

echo "--- e2e, ci-3cn-1fe, build"
cargo make ci-start ci-3cn-1fe

echo "--- e2e, ci-3cn-1fe, run fuzzing"
./target/debug/sqlsmith test \
  --count "$SQLSMITH_COUNT" \
  --testdata ./src/tests/sqlsmith/tests/testdata \
  --differential-testing 1>.risingwave/log/diff.log 2>&1 && rm .risingwave/log/diff.log

# Sqlsmith does not write to stdout, so we need this to ensure buildkite
# shows the right timing.
echo "Fuzzing complete"

# Using `kill` instead of `ci-kill` avoids storing excess logs.
# If there's errors, the failing query will be printed to stderr.
# Use that to reproduce logs on local machine.
echo "--- Kill cluster"
cargo make kill
