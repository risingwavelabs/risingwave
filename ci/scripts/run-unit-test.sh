#!/bin/bash
set -euo pipefail

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints,sync_point 2> >(tee);
if [[ "$RUN_SQLSMITH" -eq "1" ]]; then
    NEXTEST_PROFILE=ci cargo nextest run run_sqlsmith_on_frontend --features "failpoints sync_point enable_sqlsmith_unit_test" 2> >(tee);
fi

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
