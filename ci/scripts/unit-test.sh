#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints -- -P ci 2> >(tee)

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust