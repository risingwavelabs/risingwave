#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Run unit tests with coverage"
# use tee to disable progress bar
# limit job number so that we won't OOM, need to mount swap in order to prevent OOM
cargo llvm-cov nextest --lcov --output-path lcov.info -j 14 --features failpoints -- --no-fail-fast 2> >(tee)

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust