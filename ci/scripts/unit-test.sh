#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "+++ Run unit tests with coverage"
# disable fuzzing tests (risingwave_sqlsmith)
# use tee to disable progress bar
# NOTE: We pass in arguments to nextest BEFORE
# nextest subcommand.
# This is required due to: <https://github.com/taiki-e/cargo-llvm-cov/issues/151>
NEXTEST_PROFILE=ci cargo llvm-cov \
    --workspace --exclude risingwave_sqlsmith \
    nextest \
    --lcov --output-path lcov.info \
    --features failpoints \
    2> >(tee)

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
