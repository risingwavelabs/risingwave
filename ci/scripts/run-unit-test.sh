#!/bin/bash
set -euo pipefail

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
if [[ "$RUN_SQLSMITH" -eq "1" ]]; then
    echo "Running sqlsmith and unit tests";
    NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features "failpoints enable_sqlsmith_unit_test" 2> >(tee);
else
    echo "Running unit tests";
    NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints 2> >(tee);
fi

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
